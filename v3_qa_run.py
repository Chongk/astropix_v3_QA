#!/usr/bin/env python3

# - Apr. 16, 2026
# - Chong Kim, ckim.phenix@gmail.com
# - Written by AI assistance (chatGPT 5.4)

from __future__ import annotations
import argparse
import asyncio
import json
import logging
import signal
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
from v3.probe_adapter_runner_integration import add_probe_adapter_args, cleanup_probe_adapter_bias, run_probe_adapter_preflight
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
		'readout_owner': getattr(run, 'readout_owner', 'manual_irq'),
		'stop_reason': getattr(run, 'stop_reason', None),
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

def materialize_artifacts(obj: Any, *, stage_dir: Path, prefix: str, emit_readout_index: bool = False) -> Any:
	if isinstance(obj, DAQRunResult):
		base = stage_dir / prefix
		raw = flatten_run_raw(obj)
		bin_path = base.with_suffix('.bin')
		run_json_path = stage_dir / f'{base.name}.json'
		readout_index_path = stage_dir / f"{base.name}_readout_index.json"
		bin_path.write_bytes(raw)
		run_summary = summarize_run(obj)
		write_json(run_json_path, run_summary)

		payload = {
			'daq_run_prefix': prefix,
			'bin_file': bin_path.name,
			'run_json_file': run_json_path.name,
			'run_summary': run_summary,
		}
		if emit_readout_index:
			write_json(readout_index_path, build_readout_index(obj))
			payload["readout_index_file"] = readout_index_path.name
		return payload

	if isinstance(obj, dict):
		return {
			key: materialize_artifacts(value, stage_dir=stage_dir, prefix=f'{prefix}_{key}', emit_readout_index=emit_readout_index)
			for key, value in obj.items()
		}

	if isinstance(obj, list):
		return [
			materialize_artifacts(value, stage_dir=stage_dir, prefix=f'{prefix}_{idx:02d}', emit_readout_index=emit_readout_index)
			for idx, value in enumerate(obj)
		]

	return obj

def summarize_check_result(result: QACheckResult, *, stage_dir: Path, emit_readout_index: bool = False) -> dict[str, Any]:
	artifacts = materialize_artifacts(result.artifacts, stage_dir=stage_dir, prefix='out', emit_readout_index=emit_readout_index)
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
		stop_on_fail: bool,
		emit_readout_index: bool = False,
		)	-> tuple[bool, dict[str, Any] | None, bool]:
	stage_dir = out_dir / name
	stage_dir.mkdir(parents=True, exist_ok=True)
	logger.info('=== START %s ===', name)
	t0 = time.time()

	try:
		result = await coro
		elapsed = time.time() - t0
		summary = summarize_check_result(result, stage_dir=stage_dir, emit_readout_index=emit_readout_index)
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



def write_hex_dump(
	data: bytes,
	path: Path,
	*,
	bytes_per_line: int = 16,
	max_bytes: int | None = None,
) -> dict[str, Any]:
	"""Write a human-readable hex dump with byte offsets."""
	ensure_parent(path)
	bytes_per_line = max(1, int(bytes_per_line))
	limit = len(data) if max_bytes is None else min(len(data), max(0, int(max_bytes)))
	with path.open('w', encoding='utf-8') as f:
		for offset in range(0, limit, bytes_per_line):
			chunk = data[offset:offset + bytes_per_line]
			hex_part = ' '.join(f'{b:02x}' for b in chunk)
			ascii_part = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
			f.write(f'{offset:08x}: {hex_part:<{bytes_per_line * 3}} {ascii_part}\n')
		if limit < len(data):
			f.write(f'\n# truncated: wrote {limit} of {len(data)} bytes\n')
	return {
		'hex_file': path.name,
		'hex_bytes_written': limit,
		'hex_total_raw_bytes': len(data),
		'hex_truncated': limit < len(data),
		'hex_bytes_per_line': bytes_per_line,
	}


def _install_sigint_stop_event() -> tuple[asyncio.Event, Any]:
	"""Install a one-shot SIGINT handler that requests clean capture shutdown."""
	loop = asyncio.get_running_loop()
	stop_event = asyncio.Event()
	previous = signal.getsignal(signal.SIGINT)

	def request_stop() -> None:
		if stop_event.is_set():
			raise KeyboardInterrupt
		logger.warning('SIGINT received; stopping raw capture after cleanup. Press Ctrl+C again to abort.')
		stop_event.set()

	try:
		loop.add_signal_handler(signal.SIGINT, request_stop)
		def cleanup() -> None:
			try:
				loop.remove_signal_handler(signal.SIGINT)
			finally:
				if callable(previous):
					signal.signal(signal.SIGINT, previous)
	except NotImplementedError:
		def handler(signum, frame):  # noqa: ANN001
			request_stop()
		signal.signal(signal.SIGINT, handler)
		def cleanup() -> None:
			signal.signal(signal.SIGINT, previous)

	return stop_event, cleanup


async def configure_raw_debug_chip_state(
	*,
	runtime: RuntimeBundle,
	args: argparse.Namespace,
) -> dict[str, Any]:
	"""Configure the chip condition to be observed by raw_debug capture.

	Raw debug mode is not itself a QA routine, but it still needs an explicit
	chip state.  The default ``as_configured`` state preserves the YAML-loaded
	configuration.  ``full_matrix_noise`` intentionally reproduces one point of
	the threshold-scan style background/noise condition: reset matrix, enable all
	pixels, apply the requested threshold, then capture without injection.
	"""
	state = str(args.raw_chip_state)
	info: dict[str, Any] = {
		'raw_chip_state': state,
		'lane': int(args.lane),
		'chip': int(args.chip),
		'injection_enabled': False,
	}

	if state == 'as_configured':
		info.update({
			'config_modified_for_raw_debug': False,
			'threshold_applied': False,
			'full_matrix_enabled': False,
		})
		return info

	if state != 'full_matrix_noise':
		raise ValueError(f'Unsupported raw chip state: {state!r}')

	# Keep injection routing off for a background/noise raw capture.
	try:
		await runtime.controller.route_injection_to_chip(enable=False)
		info['injection_route_to_chip_disabled'] = True
	except Exception as exc:  # noqa: BLE001
		# Some bring-up firmware/driver variants may not expose the injection route.
		# This is not fatal for a background-only capture, but must be visible.
		info['injection_route_to_chip_disabled'] = False
		info['injection_route_disable_error'] = repr(exc)

	enabled_pixels = runtime.qa._enable_full_matrix(lane=args.lane, chip=args.chip)  # noqa: SLF001
	threshold_mode = args.raw_threshold_mode or args.threshold_mode
	threshold_mv = float(args.raw_threshold_mv)
	threshold_apply_mode = await runtime.qa._apply_threshold(  # noqa: SLF001
		lane=args.lane,
		chip=args.chip,
		threshold_offset_mv=threshold_mv,
		threshold_mode=threshold_mode,
	)

	info.update({
		'config_modified_for_raw_debug': True,
		'full_matrix_enabled': True,
		'enabled_pixels': enabled_pixels,
		'threshold_applied': True,
		'threshold_mode_requested': threshold_mode,
		'threshold_offset_mv': threshold_mv,
		'threshold_apply_mode': threshold_apply_mode,
		'capture_type': 'background_only',
	})
	return info


async def run_raw_debug_capture_stage(
	*,
	runtime: RuntimeBundle,
	args: argparse.Namespace,
	out_dir: Path,
	session_summary: dict[str, Any],
) -> dict[str, Any]:
	"""Capture raw bytes without running decoded QA routines."""
	stage_name = '00_raw_debug_capture'
	stage_dir = out_dir / stage_name
	stage_dir.mkdir(parents=True, exist_ok=True)
	logger.info('=== START %s ===', stage_name)
	t0 = time.time()

	readout_owner = str(args.readout_owner)
	autoread = readout_owner == 'fpga_autoread'
	stop_event, cleanup_sigint = _install_sigint_stop_event()
	run: DAQRunResult | None = None
	try:
		chip_state_info = await configure_raw_debug_chip_state(runtime=runtime, args=args)
		logger.info('Raw debug chip state: %s', chip_state_info)

		await runtime.daq.prepare_run(
			lanes=[args.lane],
			reset_delay_s=args.reset_delay_s,
			first_chip_id=args.first_chip_id,
			flush_burst_bytes=args.flush_burst_bytes,
			flush_max_rounds=args.flush_max_rounds,
			autoread=autoread,
		)
		try:
			if autoread:
				run = await runtime.daq.run_for_autoread(
					duration_s=args.raw_duration_s,
					lane=args.lane,
					poll_interval_s=args.raw_buffer_poll_interval_s,
					max_read_bytes=args.raw_max_read_bytes,
					stop_event=stop_event,
				)
			else:
				if args.raw_manual_force_clock:
					run = await runtime.daq.run_for_manual_forced_clock(
						duration_s=args.raw_duration_s,
						lane=args.lane,
						dummy_chunk_bytes=args.raw_dummy_chunk_bytes,
						poll_interval_s=args.raw_force_clock_period_s,
						select_each_round=not args.raw_force_clock_hold_csn,
						max_read_bytes=args.raw_max_read_bytes,
						stop_event=stop_event,
					)
				else:
					run = await runtime.daq.run_for_manual_irq(
						duration_s=args.raw_duration_s,
						lane=args.lane,
						wait_irq_timeout_s=args.raw_wait_irq_timeout_s,
						wait_poll_interval_s=args.raw_wait_poll_interval_s,
						dummy_chunk_bytes=args.raw_dummy_chunk_bytes,
						trailing_idle_rounds=args.raw_trailing_idle_rounds,
						max_rounds_per_burst=args.raw_max_rounds_per_burst,
						read_buffer_each_round=True,
						stop_event=stop_event,
					)
		finally:
			await runtime.daq.finish_run()
	finally:
		cleanup_sigint()

	assert run is not None
	raw = flatten_run_raw(run)
	prefix = str(args.raw_output_prefix)
	bin_path = stage_dir / f'{prefix}.bin'
	bin_path.write_bytes(raw)

	artifacts: dict[str, Any] = {
		'bin_file': bin_path.name,
		'run_summary_file': f'{prefix}.json',
	}
	write_json(stage_dir / f'{prefix}.json', summarize_run(run))

	if args.raw_hex_dump:
		hex_max = args.raw_hex_max_bytes
		artifacts.update(write_hex_dump(
			raw,
			stage_dir / f'{prefix}.hex',
			bytes_per_line=args.raw_hex_bytes_per_line,
			max_bytes=hex_max,
		))

	if readout_owner == 'manual_irq' and args.raw_write_readout_index:
		idx_path = stage_dir / f'{prefix}_readout_index.json'
		write_json(idx_path, build_readout_index(run))
		artifacts['readout_index_file'] = idx_path.name

	elapsed = time.time() - t0
	metrics = {
		'mode': 'raw_debug',
		'setup_profile': args.setup_profile,
		'readout_owner': readout_owner,
		'raw_chip_state': chip_state_info,
		'raw_manual_force_clock': bool(args.raw_manual_force_clock),
		'requested_duration_s': args.raw_duration_s,
		'actual_duration_s': run.t_end - run.t_start,
		'stage_elapsed_s': elapsed,
		'total_bytes': run.total_bytes,
		'total_chunks': run.total_chunks,
		'stop_reason': getattr(run, 'stop_reason', None),
		'hex_dump_enabled': bool(args.raw_hex_dump),
		'readout_index_written': 'readout_index_file' in artifacts,
		'no_decoder_applied': True,
	}
	summary = {
		'name': 'raw_debug_capture',
		'passed': None,
		'metrics': metrics,
		'notes': ['Raw byte-stream capture only; no decoder or QA pass/fail criteria were applied.'],
		'artifacts': artifacts,
		'elapsed_s': elapsed,
	}
	write_json(stage_dir / 'result.json', summary)
	logger.info('=== END %s | bytes=%d | elapsed=%.3fs ===', stage_name, run.total_bytes, elapsed)
	session_summary['stages'][stage_name] = summary
	return summary

# -----------------------------------------------

async def main(args: argparse.Namespace) -> int:
	out_dir = Path(args.output_dir).resolve()
	out_dir.mkdir(parents=True, exist_ok=True)

	session_summary: dict[str, Any] = {
		'started_at': time.time(),
		'argv': vars(args),
		'setup_profile': args.setup_profile,
		'run_mode': args.run_mode,
		'readout_owner': args.readout_owner,
		'stages': {},
		'stack_info': {},
	}

	adapter_ok = await run_probe_adapter_preflight(
			args=args,
			out_dir=out_dir,
			run_stage=run_stage,
			session_summary=session_summary,
	)
	if not adapter_ok:
		session_summary["completed_at"] = time.time()
		write_json(out_dir / "session_summary.json", session_summary)
		return 0

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

		if args.run_mode == 'raw_debug':
			await run_raw_debug_capture_stage(
				runtime=runtime,
				args=args,
				out_dir=out_dir,
				session_summary=session_summary,
			)
			session_summary['completed_at'] = time.time()
			write_json(out_dir / 'session_summary.json', session_summary)
			return 0

		qa_autoread = args.readout_owner == 'fpga_autoread'
		stages = [
			(
				'01_smoke_test',
				qa.smoke_test(
					lane=args.lane,
					first_chip_id=args.first_chip_id,
					autoread=qa_autoread,
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
					autoread=qa_autoread,
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
					autoread=qa_autoread,
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
		if getattr(args, 'adapter_bias_v', None) is not None and not getattr(args, 'adapter_bias_no_cleanup_on_exit', False):
			ok = await asyncio.to_thread(cleanup_probe_adapter_bias, args)
			if not ok:
				logger.error('Failed to reset adapter bias to zero during session cleanup')

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

	parser.add_argument('--setup-profile', type=str,
		choices=['legacy_carrier', 'adapter_carrier_sim', 'adapter_probe_bare'], default='legacy_carrier',
		help='Physical setup profile used for metadata and safety checks.',
	)
	parser.add_argument('--run-mode', type=str,
		choices=['qa', 'raw_debug'], default='qa',
		help='qa runs normal QA routines; raw_debug only captures raw bytes.',
	)
	parser.add_argument('--readout-owner', type=str,
		choices=['fpga_autoread', 'manual_irq'], default='fpga_autoread',
		help='Readout ownership model. Default QA now uses FPGA autoread.',
	)

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
	parser.add_argument('--threshold_scan_offsets_mv', type=float, nargs='+', default=[150, 175, 200, 225, 250])
			#default=[150, 160, 170, 180, 190, 200, 210, 220, 230, 240, 250, 260, 270, 280, 290, 300])
			#default=[200, 210, 220, 230, 240, 250, 260])

	parser.add_argument('--threshold_mode',	type=str, choices=['internal', 'external_gecco'],
			default='external_gecco', help='Which threshold path to use')

	parser.add_argument('--reset-delay-s', type=float, default=0.5)
	parser.add_argument('--flush-burst-bytes', type=int, default=128)
	parser.add_argument('--flush-max-rounds', type=int, default=20)
	parser.add_argument('--stop-on-fail', action='store_true')
	parser.add_argument('--loglevel', type=int, default=20)

	# Raw byte-stream debug mode.  If --raw-duration-s is omitted, capture runs
	# until Ctrl+C requests a clean stop.
	parser.add_argument('--raw-duration-s', type=float, default=None)
	parser.add_argument('--raw-output-prefix', type=str, default='raw_debug')
	parser.add_argument('--raw-chip-state', type=str, choices=['as_configured', 'full_matrix_noise'],
		default='as_configured',
		help=('Chip condition for raw_debug capture. as_configured preserves the YAML-loaded state; '
			'full_matrix_noise resets/enables the full matrix and applies --raw-threshold-mv before capture.'))
	parser.add_argument('--raw-threshold-mv', type=float, default=200.0,
		help='Threshold offset used when --raw-chip-state full_matrix_noise is selected.')
	parser.add_argument('--raw-threshold-mode',	type=str, choices=['internal', 'external_gecco'], default=None,
		help='Threshold path for raw_debug full_matrix_noise; defaults to --threshold_mode.')
	parser.add_argument('--raw-buffer-poll-interval-s', type=float, default=0.001)
	parser.add_argument('--raw-wait-irq-timeout-s', type=float, default=0.01)
	parser.add_argument('--raw-wait-poll-interval-s', type=float, default=0.0005)
	parser.add_argument('--raw-dummy-chunk-bytes', type=int, default=32)
	parser.add_argument('--raw-trailing-idle-rounds', type=int, default=2)
	parser.add_argument('--raw-max-rounds-per-burst', type=int, default=512)
	parser.add_argument('--raw-manual-force-clock', action='store_true',
			help=('In raw_debug + manual_irq, do not wait for IRQ.  Instead, periodically write '
				'dummy bytes and drain the FPGA buffer.  Useful for low-level line/buffer inspection.'))
	parser.add_argument('--raw-force-clock-period-s', type=float, default=0.001)
	parser.add_argument('--raw-force-clock-hold-csn', action='store_true',
			help='In forced-clock manual raw capture, keep SPI CSN asserted across repeated dummy writes.')
	parser.add_argument('--raw-max-read-bytes',	type=int, default=None,
			help='Optional maximum bytes to read per FPGA-buffer drain in raw_debug capture.')
	parser.add_argument('--raw-write-readout-index', action='store_true', default=True)
	parser.add_argument('--no-raw-write-readout-index', dest='raw_write_readout_index', action='store_false')
	parser.add_argument('--raw-hex-dump', action='store_true', default=True)
	parser.add_argument('--no-raw-hex-dump', dest='raw_hex_dump', action='store_false')
	parser.add_argument('--raw-hex-bytes-per-line', type=int, default=16)
	parser.add_argument('--raw-hex-max-bytes', type=int, default=None,
		help='Maximum raw bytes to include in the human-readable .hex dump; default writes all captured bytes.',
	)

	add_probe_adapter_args(parser)

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

	if args.setup_profile in {'adapter_carrier_sim', 'adapter_probe_bare'} and not args.adapter_ip:
		raise ValueError(f"setup_profile={args.setup_profile!r} requires --adapter-ip")
	if args.run_mode == 'raw_debug' and args.raw_duration_s is not None and args.raw_duration_s <= 0:
		raise ValueError('--raw-duration-s must be positive, or omitted for Ctrl+C-controlled capture')
	if args.run_mode == 'raw_debug' and args.raw_hex_bytes_per_line <= 0:
		raise ValueError('--raw-hex-bytes-per-line must be positive')
	if args.run_mode == 'raw_debug' and args.raw_chip_state == 'full_matrix_noise' and args.raw_threshold_mv is None:
		raise ValueError('--raw-chip-state full_matrix_noise requires --raw-threshold-mv')
	if args.run_mode == 'raw_debug' and args.raw_manual_force_clock and args.readout_owner != 'manual_irq':
		raise ValueError('--raw-manual-force-clock is only valid with --readout-owner manual_irq')
	if args.run_mode == 'raw_debug' and args.raw_force_clock_period_s <= 0:
		raise ValueError('--raw-force-clock-period-s must be positive')
	if args.run_mode == 'raw_debug' and args.raw_max_read_bytes is not None and args.raw_max_read_bytes <= 0:
		raise ValueError('--raw-max-read-bytes must be positive when provided')
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
