#!/usr/bin/env python3

# - Apr. 16, 2026
# - Chong Kim, ckim.phenix@gmail.com
# - Written by AI assistance (chatGPT 5.4)

from __future__ import annotations
import argparse
import asyncio
import json
import logging
import sys
import time
import traceback
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
	sys.path.insert(0, str(SCRIPT_DIR))

from v3.bootstrap import RuntimeBundle, build_runtime_from_files
from v3.daq import DAQRunResult
from v3.qa import QACheckResult
from v3.transport import V3TransportFatalError

logger = logging.getLogger('v3_qa_run')

def ensure_parent(path: Path) -> None:
	path.parent.mkdir(parents=True, exist_ok=True)

def expand_xml_path(value: str) -> str:
	p = Path(value)
	if p.exists():
		return str(p)
	candidate = SCRIPT_DIR / 'scripts' / 'config' / f'{value}.xml'
	return str(candidate)

def expand_yaml_paths(values: list[str]) -> list[str]:
	out: list[str] = []
	for value in values:
		p = Path(value)
		if p.exists():
			out.append(str(p))
			continue
		out.append(str(SCRIPT_DIR / 'scripts' / 'config' / f'{value}.yml'))
	return out

def normalize_chips_per_row(chips_per_row: list[int], nyaml: int) -> list[int]:
	if len(chips_per_row) == nyaml:
		return chips_per_row
	if len(chips_per_row) == 1 and nyaml > 1:
		return chips_per_row * nyaml
	raise ValueError(f'chipsPerRow length ({len(chips_per_row)}) must be 1 or match number of YAMLs ({nyaml})')

def write_json(path: Path, payload: Any) -> None:
	ensure_parent(path)
	with path.open('w', encoding='utf-8') as f:
		json.dump(payload, f, indent=4, sort_keys=True, default=str)

def flatten_run_raw(run: DAQRunResult) -> bytes:
	payload = bytearray()
	for chunk in run.chunks:
		if chunk.data:
			payload.extend(chunk.data)
	return bytes(payload)

def build_readout_index(run: DAQRunResult, *, record_size_bytes: int = 11) -> dict[str, Any]:
	"""
	Build a sidecar index for one raw .bin file without modifying the original bytes.

	The index maps each manual-IRQ DAQ chunk to a readout_number and a byte range,
	within the flattened raw binary file. Users can then assign decoded records	to readouts
	by comparing their starting byte offsets against these chunk ranges.
	"""

	offset = 0
	chunks: list[dict[str, Any]] = []
	for readout_number, chunk in enumerate(run.chunks):
		nbytes = chunk.nbytes
		start = offset
		end = start + nbytes
		chunks.append({
			"readout_number": readout_number,
			"byte_offset_start": start,
			"byte_offset_end": end,
			"byte_length": nbytes,
			"approx_record_size_bytes": record_size_bytes,
			"approx_record_count_floor": (nbytes // record_size_bytes) if record_size_bytes > 0 else None,
			"approx_trailing_bytes": (nbytes % record_size_bytes) if record_size_bytes > 0 else None,
			"lane": chunk.lane,
			"t_start": chunk.t_start,
			"t_end": chunk.t_end,
			"duration_s": chunk.t_end - chunk.t_start,
			"irq_seen": chunk.irq_seen,
			"rounds": chunk.rounds,
			"bytes_written_as_dummy": chunk.bytes_written_as_dummy,
			"buffer_sizes": chunk.buffer_sizes,
			"truncated": getattr(chunk, "truncated", None),
			"stop_reason": getattr(chunk, "stop_reason", None),
		})
		offset = end

	return {
		"format": "daq_readout_index_v1",
		"lanes": run.lanes,
		"t_start": run.t_start,
		"t_end": run.t_end,
		"duration_s": run.t_end - run.t_start,
		"raw_total_bytes": run.total_bytes,
		"record_size_bytes_hint": record_size_bytes,
		"readouts": chunks,
	}

def summarize_run(run: DAQRunResult) -> dict[str, Any]:
	return {
		'lanes': run.lanes,
		't_start': run.t_start,
		't_end': run.t_end,
		'duration_s': run.t_end - run.t_start,
		'total_chunks': run.total_chunks,
		'total_bytes': run.total_bytes,
		'chunks': [
			{
				'lane': c.lane,
				't_start': c.t_start,
				't_end': c.t_end,
				'duration_s': c.t_end - c.t_start,
				'irq_seen': c.irq_seen,
				'rounds': c.rounds,
				'bytes_written_as_dummy': c.bytes_written_as_dummy,
				'buffer_sizes': c.buffer_sizes,
				'nbytes': c.nbytes,
				'truncated': c.truncated,
				'stop_reason': c.stop_reason,
			}
			for c in run.chunks
		],
	}

def materialize_artifacts(obj: Any, *, stage_dir: Path, prefix: str) -> Any:
	if isinstance(obj, DAQRunResult):
		base = stage_dir / prefix
		raw = flatten_run_raw(obj)
		bin_path = base.with_suffix('.bin')
		run_json_path = stage_dir / f'{base.name}.json'
		readout_index_path = stage_dir / f"{base.name}_readout_index.json"
		bin_path.write_bytes(raw)
		write_json(run_json_path, summarize_run(obj))
		write_json(readout_index_path, build_readout_index(obj))

		return {
			'daq_run_prefix': prefix,
			'bin_file': bin_path.name,
			'run_json_file': run_json_path.name,
			"readout_index_file": readout_index_path.name,
			'run_summary': summarize_run(obj),
		}

	if isinstance(obj, dict):
		return {
			key: materialize_artifacts(value, stage_dir=stage_dir, prefix=f'{prefix}_{key}')
			for key, value in obj.items()
		}

	if isinstance(obj, list):
		return [
			materialize_artifacts(value, stage_dir=stage_dir, prefix=f'{prefix}_{idx:02d}')
			for idx, value in enumerate(obj)
		]

	return obj

def summarize_check_result(result: QACheckResult, *, stage_dir: Path) -> dict[str, Any]:
	artifacts = materialize_artifacts(result.artifacts, stage_dir=stage_dir, prefix='out')
	return {
		'name': result.name,
		'passed': result.passed,
		'metrics': result.metrics,
		'notes': result.notes,
		'artifacts': artifacts,
	}

# -----------------------------------------------

async def run_stage(
		*,
		name: str,
		coro,
		out_dir: Path,
		stop_on_fail: bool
		)	-> tuple[bool, dict[str, Any] | None, bool]:
	stage_dir = out_dir / name
	stage_dir.mkdir(parents=True, exist_ok=True)
	logger.info('=== START %s ===', name)
	t0 = time.time()

	try:
		result = await coro
		elapsed = time.time() - t0
		summary = summarize_check_result(result, stage_dir=stage_dir)
		summary['elapsed_s'] = elapsed
		write_json(stage_dir / 'result.json', summary)
		logger.info('=== END %s | passed=%s | elapsed=%.3fs ===', name, result.passed, elapsed)

		if stop_on_fail and result.passed is False:
			return False, summary, False
		return True, summary, False

	except V3TransportFatalError as exc:
		elapsed = time.time() - t0
		tb = traceback.format_exc()
		error_payload = {
			'stage': name,
			'elapsed_s': elapsed,
			'error_type': type(exc).__name__,
			'error_message': str(exc),
			'traceback': tb,
			'transport_fatal': True,
		}
		write_json(stage_dir / 'error.json', error_payload)
		logger.exception('=== TRANSPORT FATAL %s | elapsed=%.3fs ===', name, elapsed)
		return False, error_payload, True

	except Exception as exc: # noqa: BLE001
		elapsed = time.time() - t0
		tb = traceback.format_exc()
		error_payload = {
			'stage': name,
			'elapsed_s': elapsed,
			'error_type': type(exc).__name__,
			'error_message': str(exc),
			'traceback': tb,
			'transport_fatal': False,
		}
		write_json(stage_dir / 'error.json', error_payload)
		logger.exception('=== FAIL %s | elapsed=%.3fs ===', name, elapsed)
		return False, error_payload, False

# -----------------------------------------------

async def main(args: argparse.Namespace) -> int:
	out_dir = Path(args.output_dir).resolve()
	out_dir.mkdir(parents=True, exist_ok=True)

	session_summary: dict[str, Any] = {
		'started_at': time.time(),
		'argv': vars(args),
		'stages': {},
		'stack_info': {},
	}

	runtime: RuntimeBundle | None = None
	try:
		logger.info('Bootstrapping board connection via standalone bootstrap module')
		runtime = await build_runtime_from_files(
			fpgaxml=args.fpgaxml,
			yaml_paths=args.yaml,
			chips_per_row=args.chipsPerRow,
			configure_autoread_keepalive=False,
		)

		transport = runtime.transport
		protocol = runtime.protocol
		controller = runtime.controller
		qa = runtime.qa

		session_summary['stack_info'] = {
			'lanes': transport.lanes,
			'max_num_chips': transport.max_num_chips(),
		}

		for lane in transport.lanes:
			cfg = controller.get_lane_config(lane)
			write_json(out_dir / f'lane{lane}_config_snapshot.json', cfg.export_all())
			write_json(out_dir / f'lane{lane}_protocol_order.json', protocol.describe_order(cfg, chip=0))

		stages = [
			(
				'01_smoke_test',
				qa.smoke_test(
					lane=args.lane,
					first_chip_id=args.first_chip_id,
					autoread=False,
					reset_delay_s=args.reset_delay_s,
					flush_burst_bytes=args.flush_burst_bytes,
					flush_max_rounds=args.flush_max_rounds,
				),
			),
			(
				'02_sparse_injection_test',
				qa.sparse_injection_test(
					lane=args.lane,
					chip=args.chip,
					threshold_mode=args.threshold_mode,
					vinj_mv=args.vinj_mv,
					injection_thr_mv=args.injection_thr_mv,
					duration_s=args.injection_duration_s,
					autoread=False,
					injector_period=args.injector_period,
					injector_clkdiv=args.injector_clkdiv,
					injector_initdelay=args.injector_initdelay,
					injector_cycle=args.injector_cycle,
					injector_pulseperset=args.injector_pulseperset,
					decoder=None,
				),
			),
			(
				'03_threshold_scan',
				qa.threshold_scan(
					lane=args.lane,
					chip=args.chip,
					threshold_offsets_mv=[float(x) for x in args.threshold_scan_offsets_mv],
					threshold_mode=args.threshold_mode,
					duration_s=args.threshold_scan_duration_s,
					autoread=False,
					enable_full_matrix=True,
					enable_pixels=None,
					decoder=None,
				),
			),
		]

		for stage_name, stage_coro in stages:
			ok, payload, transport_fatal = await run_stage(
				name=stage_name,
				coro=stage_coro,
				out_dir=out_dir,
				stop_on_fail=args.stop_on_fail,
			)
			session_summary['stages'][stage_name] = payload
			if transport_fatal:
				session_summary['aborted_reason'] = 'transport_fatal'
				break
			if args.stop_on_fail and not ok:
				session_summary['aborted_reason'] = 'stage_failed'
				break

		session_summary['completed_at'] = time.time()
		write_json(out_dir / 'session_summary.json', session_summary)
		return 0

	except Exception: # noqa: BLE001
		session_summary['completed_at'] = time.time()
		session_summary['fatal_traceback'] = traceback.format_exc()
		write_json(out_dir / 'session_summary.json', session_summary)
		logger.exception('Fatal failure in v3_qa_run')
		return 2

	finally:
		if runtime is not None:
			try:
				await runtime.transport.close()
			except Exception:  # noqa: BLE001
				logger.exception('Failed while closing board connection')

# -----------------------------------------------

def build_argparser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(
		description='Run the v3 QA stack on a Nexys/GECCO + single-chip AstroPix-v3 setup',
		formatter_class=argparse.ArgumentDefaultsHelpFormatter,
	)

	parser.add_argument('-x', '--fpgaxml', type=str, default='gecco')
	parser.add_argument('-y', '--yaml', type=str, nargs='+', default=['singlechip_v3_qa'])
	parser.add_argument('-c', '--chipsPerRow', type=int, nargs='+', default=[1])
	parser.add_argument('-s', '--suffix', type=str, default=None)

	default_output = str(SCRIPT_DIR / 'data' / time.strftime('%Y%m%d-%H%M%S'))
	parser.add_argument('-o', '--output-dir', type=str, default=default_output)

	parser.add_argument('--lane', type=int, default=0)
	parser.add_argument('--chip', type=int, default=0)
	parser.add_argument('--col', type=int, default=10)
	parser.add_argument('--row', type=int, default=10)
	parser.add_argument('--first_chip_id', type=int, default=0)

	parser.add_argument('--vinj_mv', type=float, default=500.0)
	parser.add_argument('--injection_thr_mv', type=float, default=400.0)
	parser.add_argument('--injection_duration_s', type=float, default=5.0)
	parser.add_argument('--injector_period', type=int, default=162)
	parser.add_argument('--injector_clkdiv', type=int, default=300)
	parser.add_argument('--injector_initdelay', type=int, default=100)
	parser.add_argument('--injector_cycle', type=int, default=0)
	parser.add_argument('--injector_pulseperset', type=int, default=1)

	parser.add_argument('--threshold_scan_duration_s', type=float, default=30.0)
	parser.add_argument('--threshold_scan_offsets_mv', type=float, nargs='+', default=[180, 200, 220, 240, 260])
			#default=[150, 160, 170, 180, 190, 200, 210, 220, 230, 240, 250, 260, 270, 280, 290, 300])
			#default=[200, 210, 220, 230, 240, 250, 260])

	parser.add_argument('--threshold_mode',	type=str, choices=['internal', 'external_gecco'],
			default='external_gecco', help='Which threshold path to use')

	parser.add_argument('--reset-delay-s', type=float, default=0.5)
	parser.add_argument('--flush-burst-bytes', type=int, default=128)
	parser.add_argument('--flush-max-rounds', type=int, default=20)
	parser.add_argument('--stop-on-fail', action='store_true')
	parser.add_argument('--loglevel', type=int, default=20)

	return parser

# -----------------------------------------------

def prepare_args(args: argparse.Namespace) -> argparse.Namespace:
	if getattr(args, 'suffix', None):
		args.output_dir = f"{args.output_dir}-{args.suffix}"
	args.fpgaxml = expand_xml_path(args.fpgaxml)
	args.yaml = expand_yaml_paths(args.yaml)
	args.chipsPerRow = normalize_chips_per_row(args.chipsPerRow, len(args.yaml))

	if not Path(args.fpgaxml).exists():
		raise FileNotFoundError(f'XML config not found: {args.fpgaxml}')
	for y in args.yaml:
		if not Path(y).exists():
			raise FileNotFoundError(f'YAML config not found: {y}')
	return args

def setup_logging(args: argparse.Namespace) -> None:
	out_dir = Path(args.output_dir).resolve()
	out_dir.mkdir(parents=True, exist_ok=True)
	log_path = out_dir / 'v3_qa_run.log'

	logfmt = '%(asctime)s %(levelname)s %(name)s: %(message)s'
	logging.basicConfig(
		level=args.loglevel,
		format=logfmt,
		handlers=[
			logging.FileHandler(log_path, mode='a', encoding='utf-8'),
			logging.StreamHandler(sys.stdout),
		],
	)

	logger.info('Output directory: %s', out_dir)
	logger.info('Arguments: %s', vars(args))

# -----------------------------------------------

if __name__ == '__main__':
	parser = build_argparser()
	parsed = parser.parse_args()
	parsed = prepare_args(parsed)
	setup_logging(parsed)
	raise SystemExit(asyncio.run(main(parsed)))
