#!/usr/bin/env python3

"""
Minimal end-to-end QA runner for the new v3 stack

Expected package layout
-----------------------
This script assumes the new modules live in a sibling package directory:

	./v3/
		__init__.py
		config.py
		protocol.py
		transport.py
		controller.py
		daq.py
		qa.py

It also reuses AstropixRun only for FPGA/board bootstrap + YAML loading,
then switches to the new stack for programming/readout/QA
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Any

# Make sibling imports work when running the script directly.
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path: sys.path.insert(0, str(SCRIPT_DIR))

from astropixrun import AstropixRun
from v3.config import V3Config
from v3.protocol import V3Protocol
from v3.transport import V3Transport
from v3.controller import V3Controller
from v3.daq import V3DAQ, DAQRunResult
from v3.qa import V3QA, QACheckResult
logger = logging.getLogger("v3_qa_run")

# =========================================================

def ensure_parent(path: Path) -> None:
	path.parent.mkdir(parents=True, exist_ok=True)

def expand_xml_path(value: str) -> str:
	p = Path(value)
	if p.exists(): return str(p)
	candidate = SCRIPT_DIR / "scripts" / "config" / f"{value}.xml"
	return str(candidate)

def expand_yaml_paths(values: list[str]) -> list[str]:
	out: list[str] = []
	for value in values:
		p = Path(value)
		if p.exists():
			out.append(str(p))
			continue
		candidate = SCRIPT_DIR / "scripts" / "config" / f"{value}.yml"
		out.append(str(candidate))
	return out

def normalize_chips_per_row(chips_per_row: list[int], nyaml: int) -> list[int]:
	if len(chips_per_row) == nyaml:
		return chips_per_row
	if len(chips_per_row) == 1 and nyaml > 1:
		return chips_per_row * nyaml
	raise ValueError(f"chipsPerRow length ({len(chips_per_row)}) must be 1 or match number of YAMLs ({nyaml})")

def write_json(path: Path, payload: Any) -> None:
	ensure_parent(path)
	with path.open("w", encoding="utf-8") as f:
		json.dump(payload, f, indent=4, sort_keys=True, default=str)

def write_text(path: Path, text: str) -> None:
	ensure_parent(path)
	path.write_text(text, encoding="utf-8")

def flatten_run_raw(run: DAQRunResult) -> bytes:
	payload = bytearray()
	for chunk in run.chunks:
		if chunk.data: payload.extend(chunk.data)
	return bytes(payload)

def run_external_decoder(bin_path: Path) -> None:
	subprocess.run(["python3.12", "Quad_Chip_Decoder.py", "-b", "True", "-n", str(bin_path)], check=True)

def summarize_run(run: DAQRunResult) -> dict[str, Any]:
	return {
		"lanes":        run.lanes,
		"t_start":      run.t_start,
		"t_end":        run.t_end,
		"duration_s":   run.t_end - run.t_start,
		"total_chunks": run.total_chunks,
		"total_bytes":  run.total_bytes,
		"chunks": [
			{
				"lane":       c.lane,
				"t_start":    c.t_start,
				"t_end":      c.t_end,
				"duration_s": c.t_end - c.t_start,
				"irq_seen":   c.irq_seen,
				"rounds":     c.rounds,
				"bytes_written_as_dummy": c.bytes_written_as_dummy,
				"buffer_sizes":           c.buffer_sizes,
				"nbytes":                 c.nbytes,
			}
			for c in run.chunks
		],
	}

def summarize_check_result(result: QACheckResult) -> dict[str, Any]:
	payload = {
		"name"     : result.name,
		"passed"   : result.passed,
		"metrics"  : result.metrics,
		"notes"    : result.notes,
		"artifacts": {},
	}

	for key, value in result.artifacts.items():
		if isinstance(value, DAQRunResult):
			payload["artifacts"][key] = summarize_run(value)
		else:
			payload["artifacts"][key] = value

	return payload

async def bootstrap_legacy_board(args: argparse.Namespace) -> AstropixRun:
	arun = AstropixRun(args.fpgaxml)
	await arun.open_fpga()
	arun.load_yaml(args.yaml, args.chipsPerRow)
	return arun

async def build_new_stack(arun: AstropixRun) -> tuple[V3Transport, V3Protocol, V3Controller, V3DAQ, V3QA]:
	transport = V3Transport(arun.boardDriver)

	# Use the new transport layer for board-wide configuration.
	await transport.configure_chipversion(flush=True)
	await transport.configure_clocks(flush=True)
	await transport.configure_autoread_keepalive(flush=False)

	lane_configs: dict[int, V3Config] = {}
	max_nchips = 1

	for lane in sorted(arun.boardDriver.asics.keys()):
		asic_obj = arun.boardDriver.asics[lane]
		nchips = int(getattr(asic_obj, "_num_chips", 1))
		max_nchips = max(max_nchips, nchips)
		lane_configs[lane] = V3Config.from_astep_asic_config(asic_obj.asic_config, nchips=nchips)

	protocol   = V3Protocol(nchips=max_nchips)
	controller = V3Controller(transport, protocol, lane_configs=lane_configs)
	daq        = V3DAQ(controller, default_lane=min(lane_configs.keys()))
	qa         = V3QA(controller, daq)
	return transport, protocol, controller, daq, qa

async def run_stage(
	*,
	name: str,
	coro,
	out_dir: Path,
	stop_on_fail: bool,
	run_ext_decoder: bool = False
) -> tuple[bool, dict[str, Any] | None]:
	stage_dir = out_dir / name
	stage_dir.mkdir(parents=True, exist_ok=True)
	logger.info("=== START %s ===", name)
	t0 = time.time()

	try:
		result = await coro
		elapsed = time.time() - t0
		summary = summarize_check_result(result)
		summary["elapsed_s"] = elapsed
		write_json(stage_dir / "result.json", summary)

		# Save raw DAQ payloads when present.
		for artifact_name, artifact in result.artifacts.items():
			if isinstance(artifact, DAQRunResult):
				raw = flatten_run_raw(artifact)
				if raw:	(stage_dir / f"{artifact_name}.bin").write_bytes(raw)
				write_json(stage_dir / f"{artifact_name}_run.json", summarize_run(artifact))

		logger.info("=== END %s | passed=%s | elapsed=%.3fs ===", name, result.passed, elapsed)

		if stop_on_fail and result.passed is False:
			raise V3QAStageFailure(f"Stage {name} returned passed=False")

		if run_ext_decoder:
			logger.info(f"%s: generating csv...", name)
			run_external_decoder(stage_dir / "run.bin")

		return True, summary

	except Exception as exc:
		elapsed = time.time() - t0
		tb = traceback.format_exc()
		error_payload = {
			"stage": name,
			"elapsed_s": elapsed,
			"error_type": type(exc).__name__,
			"error_message": str(exc),
			"traceback": tb,
		}
		write_json(stage_dir / "error.json", error_payload)
		write_text(stage_dir / "traceback.txt", tb)
		logger.exception("=== FAIL %s | elapsed=%.3fs ===", name, elapsed)
		if stop_on_fail: raise
		return False, error_payload

class V3QAStageFailure(RuntimeError):
	pass

# =========================================================

async def main(args: argparse.Namespace) -> int:
	out_dir = Path(args.output_dir).resolve()
	out_dir.mkdir(parents=True, exist_ok=True)

	session_summary: dict[str, Any] = {
		"started_at": time.time(),
		"argv": vars(args),
		"stages": {},
		"stack_info": {},
	}

	arun: AstropixRun | None = None

	try:
		logger.info("Bootstrapping legacy board connection via AstropixRun")
		arun = await bootstrap_legacy_board(args)
		transport, protocol, controller, daq, qa = await build_new_stack(arun)

		session_summary["stack_info"] = {
			"lanes": transport.lanes,
			"max_num_chips": transport.max_num_chips()
		}

		# Save initial config snapshots for diff/debug.
		for lane in transport.lanes:
			cfg = controller.get_lane_config(lane)
			write_json(out_dir / f"lane{lane}_config_snapshot.json", cfg.export_all())
			write_json(out_dir / f"lane{lane}_protocol_order.json", protocol.describe_order(cfg, chip=0))

		# ---------------------------------------

		# Stage 1: smoke test
		ok, payload = await run_stage(
			name="01_smoke_test",
			coro=qa.smoke_test(
				lane              = args.lane,
				first_chip_id     = args.first_chip_id,
				autoread          = args.autoread,
				reset_delay_s     = args.reset_delay_s,
				flush_burst_bytes = args.flush_burst_bytes,
				flush_max_rounds  = args.flush_max_rounds
			),
			out_dir=out_dir,
			stop_on_fail=args.stop_on_fail
		)
		session_summary["stages"]["01_smoke_test"] = payload

		# ---------------------------------------

		# Stage 2: single-pixel injection
		ok2, payload2 = await run_stage(
			name="02_single_pixel_injection",
			coro=qa.single_pixel_injection(
				lane = args.lane,
				chip = args.chip,
				col  = args.col,
				row  = args.row,
				threshold_offset_mv  = args.threshold_offset_mv,
				autoread             = args.autoread,
				vinj_mv              = args.vinj_mv,
				duration_s           = args.injection_duration_s,
				injector_period      = args.injector_period,
				injector_clkdiv      = args.injector_clkdiv,
				injector_initdelay   = args.injector_initdelay,
				injector_cycle       = args.injector_cycle,
				injector_pulseperset = args.injector_pulseperset,
				decoder = None
			),
			out_dir=out_dir,
			stop_on_fail=args.stop_on_fail,
			run_ext_decoder=args.run_external_decoder
		)
		session_summary["stages"]["02_single_pixel_injection"] = payload2

		# ---------------------------------------

		# Stage 3: threshold scan
		threshold_offsets = [float(x) for x in args.threshold_scan_offsets_mv]
		ok3, payload3 = await run_stage(
			name="03_threshold_scan",
			coro=qa.threshold_scan(
				lane = args.lane,
				chip = args.chip,
				col  = args.col,
				row  = args.row,
				threshold_offsets_mv = threshold_offsets,
				autoread             = args.autoread,
				vinj_mv              = args.vinj_mv,
				duration_s           = args.threshold_scan_duration_s,
				injector_period      = args.injector_period,
				injector_clkdiv      = args.injector_clkdiv,
				injector_initdelay   = args.injector_initdelay,
				injector_cycle       = args.injector_cycle,
				injector_pulseperset = args.injector_pulseperset,
				decoder = None
			),
			out_dir=out_dir,
			stop_on_fail=args.stop_on_fail,
		)
		session_summary["stages"]["03_threshold_scan"] = payload3

		# ---------------------------------------

		"""
		# Stage 4: noise occupancy/activity
		ok4, payload4 = await run_stage(
			name="04_noise_occupancy",
			coro=qa.noise_occupancy(
				lane=args.lane,
				chip=args.chip,
				duration_s=args.noise_duration_s,
				threshold_offset_mv=args.threshold_offset_mv,
				enable_full_matrix=args.noise_full_matrix,
				enable_pixels=None,
				autoread=args.autoread,
				decoder=None
			),
			out_dir=out_dir,
			stop_on_fail=args.stop_on_fail,
		)
		session_summary["stages"]["04_noise_occupancy"] = payload4
		"""

		session_summary["completed_at"] = time.time()
		write_json(out_dir / "session_summary.json", session_summary)
		return 0

	except Exception:
		session_summary["completed_at"] = time.time()
		session_summary["fatal_traceback"] = traceback.format_exc()
		write_json(out_dir / "session_summary.json", session_summary)
		logger.exception("Fatal failure in v3_qa_run")
		return 2

	finally:
		if arun is not None:
			try:
				await arun.fpga_close_connection()
			except Exception:
				logger.exception("Failed while closing FPGA connection")

# =========================================================

def build_argparser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(
		description="Run the new v3 QA stack on a Nexys/GECCO + single-chip AstroPix-v3 setup",
		formatter_class=argparse.ArgumentDefaultsHelpFormatter,
	)

	parser.add_argument("-x", "--fpgaxml",     type=str, default="gecco")
	parser.add_argument("-y", "--yaml",        type=str, nargs="+", default=["singlechip_testconfig_v3"])
	parser.add_argument("-c", "--chipsPerRow", type=int, nargs="+", default=[1])

	parser.add_argument("--lane", type=int, default=0)
	parser.add_argument("--chip", type=int, default=0)
	parser.add_argument("--col",  type=int, default=10)
	parser.add_argument("--row",  type=int, default=10)
	parser.add_argument("--first_chip_id", type=int, default=0)

	parser.add_argument("--vinj_mv",              type=float, default=500.0)
	parser.add_argument("--injection_duration_s", type=float, default=10.0)
	parser.add_argument("--injector_period",      type=int, default=162)
	parser.add_argument("--injector_clkdiv",      type=int, default=300)
	parser.add_argument("--injector_initdelay",   type=int, default=100)
	parser.add_argument("--injector_cycle",       type=int, default=0)
	parser.add_argument("--injector_pulseperset", type=int, default=1)

	parser.add_argument("--threshold_offset_mv",       type=float, default=400.0)
	parser.add_argument("--threshold_scan_duration_s", type=float, default=10.0)
	parser.add_argument("--threshold_scan_offsets_mv", type=float, nargs="+",
			default=[200.0, 250.0, 300.0, 350.0, 400.0, 450.0, 500.0]
			)

	parser.add_argument("--noise-duration-s", type=float, default=1.0)
	parser.add_argument("--noise-full-matrix", action="store_true", default=True,
			help="Enable the full matrix for the noise test")

	parser.add_argument("--autoread", action="store_true",
			help="Use FPGA autoread mode instead of manual IRQ-driven mode")
	parser.add_argument("--reset-delay-s", type=float, default=0.5)
	parser.add_argument("--flush-burst-bytes", type=int, default=128)
	parser.add_argument("--flush-max-rounds", type=int, default=20)
	parser.add_argument("--stop-on-fail", action="store_true")

	parser.add_argument("-o", "--output-dir", type=str,
			default=str(SCRIPT_DIR / "data" / time.strftime("%Y%m%d-%H%M%S")),
			)
	parser.add_argument("--run_external_decoder", action="store_true")
	parser.add_argument("--loglevel", type=int, default=20)
	return parser

def prepare_args(args: argparse.Namespace) -> argparse.Namespace:
	args.fpgaxml = expand_xml_path(args.fpgaxml)
	args.yaml = expand_yaml_paths(args.yaml)
	args.chipsPerRow = normalize_chips_per_row(args.chipsPerRow, len(args.yaml))

	if not Path(args.fpgaxml).exists():
		raise FileNotFoundError(f"XML config not found: {args.fpgaxml}")
	for y in args.yaml:
		if not Path(y).exists():
			raise FileNotFoundError(f"YAML config not found: {y}")

	return args

def setup_logging(args: argparse.Namespace) -> None:
	out_dir = Path(args.output_dir).resolve()
	out_dir.mkdir(parents=True, exist_ok=True)
	log_path = out_dir / "v3_qa_run.log"

	logfmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
	logging.basicConfig(
		level=args.loglevel,
		format=logfmt,
		handlers=[
			logging.FileHandler(log_path, mode="a", encoding="utf-8"),
			logging.StreamHandler(sys.stdout),
		],
	)

	logger.info("Output directory: %s", out_dir)
	logger.info("Arguments: %s", vars(args))

# =========================================================

if __name__ == "__main__":
	parser = build_argparser()
	parsed = parser.parse_args()
	parsed = prepare_args(parsed)
	setup_logging(parsed)
	raise SystemExit(asyncio.run(main(parsed)))
