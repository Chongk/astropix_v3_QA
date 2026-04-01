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
import csv
import json
import logging
import math
import os
import subprocess
import sys
import time
import traceback

from pathlib import Path
from statistics import mean, median
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
		"lanes":		run.lanes,
		"t_start":		run.t_start,
		"t_end":		run.t_end,
		"duration_s":   run.t_end - run.t_start,
		"total_chunks": run.total_chunks,
		"total_bytes":  run.total_bytes,
		"chunks": [
			{
				"lane":		  c.lane,
				"t_start":	  c.t_start,
				"t_end":	  c.t_end,
				"duration_s": c.t_end - c.t_start,
				"irq_seen":   c.irq_seen,
				"rounds":	  c.rounds,
				"bytes_written_as_dummy": c.bytes_written_as_dummy,
				"buffer_sizes":			  c.buffer_sizes,
				"nbytes":				  c.nbytes,
			}
			for c in run.chunks
		],
	}

def summarize_check_result(result: QACheckResult) -> dict[str, Any]:
	payload = {
		"name":	  result.name,
		"passed":	result.passed,
		"metrics":   result.metrics,
		"notes":	 result.notes,
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

	protocol = V3Protocol(nchips=max_nchips)
	controller = V3Controller(transport, protocol, lane_configs=lane_configs)
	daq = V3DAQ(controller, default_lane=min(lane_configs.keys()))
	qa = V3QA(controller, daq, legacy_arun=arun)
	return transport, protocol, controller, daq, qa

def _percentile(values: list[float], q: float) -> float | None:
	if not values:
		return None
	xs = sorted(values)
	if len(xs) == 1:
		return float(xs[0])
	pos = q * (len(xs) - 1)
	lo = int(math.floor(pos))
	hi = int(math.ceil(pos))
	if lo == hi:
		return float(xs[lo])
	frac = pos - lo
	return float(xs[lo] * (1.0 - frac) + xs[hi] * frac)

def _safe_float(value: Any) -> float | None:
	try:
		if value is None or value == "": return None
		return float(value)
	except Exception:
		return None

def summarize_decoded_csv(csv_path: Path) -> dict[str, Any]:
	rows: list[dict[str, Any]] = []
	with csv_path.open("r", encoding="utf-8", newline="") as f:
		reader = csv.DictReader(f)
		for row in reader: rows.append(row)

	out: dict[str, Any] = {"n_rows_total": len(rows), "csv_file": csv_path.name}
	if not rows: return out

	sample = rows[0]
	iscol_key = next((k for k in ("isCol", "iscol", "is_col") if k in sample), None)
	tot_key   = next((k for k in ("tot_total", "tot", "ToT", "ToT_total") if k in sample), None)
	ts_key	= next((k for k in ("timestamp", "ts", "fpga_ts", "toa") if k in sample), None)

	if ts_key is not None:
		out["n_unique_timestamp"] = len({row[ts_key] for row in rows if row.get(ts_key) not in (None, "")})

	if tot_key is not None:
		tot_vals = [_safe_float(row.get(tot_key)) for row in rows]
		tot_vals = [x for x in tot_vals if x is not None]
		if tot_vals:
			out["tot_key"] = tot_key
			out["tot_mean"] = mean(tot_vals)
			out["tot_median"] = median(tot_vals)
			out["tot_q10"] = _percentile(tot_vals, 0.10)
			out["tot_q90"] = _percentile(tot_vals, 0.90)

	if iscol_key is not None:
		out["iscol_key"] = iscol_key
		for iscol_val, label in (("0", "row"), ("1", "col")):
			sub = [row for row in rows if str(row.get(iscol_key, "")).strip() == iscol_val]
			out[f"n_{label}"] = len(sub)

			if tot_key is not None:
				sub_tot = [_safe_float(row.get(tot_key)) for row in sub]
				sub_tot = [x for x in sub_tot if x is not None]
				if sub_tot:
					out[f"{label}_tot_mean"] = mean(sub_tot)
					out[f"{label}_tot_median"] = median(sub_tot)
					out[f"{label}_tot_q10"] = _percentile(sub_tot, 0.10)
					out[f"{label}_tot_q90"] = _percentile(sub_tot, 0.90)

	return out

def run_external_decoder(bin_path: Path) -> Path | None:
	before = {p.resolve() for p in bin_path.parent.glob("*.csv")}
	subprocess.run(["python3.12", "Quad_Chip_Decoder.py", "-b", "True", "-n", str(bin_path)], check=True)

	preferred = bin_path.with_suffix(".csv")
	if preferred.exists():
		return preferred

	after = [p.resolve() for p in bin_path.parent.glob("*.csv")]
	new_csvs = [p for p in after if p not in before]
	if new_csvs:
		return max(new_csvs, key=lambda p: p.stat().st_mtime)

	existing = list(bin_path.parent.glob("*.csv"))
	if existing:
		return max(existing, key=lambda p: p.stat().st_mtime)

	return None

def materialize_artifacts(
	obj: Any,
	*,
	stage_dir: Path,
	prefix: str,
	extracted_runs: list[dict[str, Any]],
) -> Any:
	if isinstance(obj, DAQRunResult):
		base = stage_dir / prefix
		raw = flatten_run_raw(obj)
		bin_path = base.with_suffix(".bin")
		run_json_path = stage_dir / f"{base.name}_run.json"

		bin_path.write_bytes(raw)
		write_json(run_json_path, summarize_run(obj))

		extracted_runs.append(
			{
				"prefix": prefix,
				"bin_path": bin_path,
				"run_json_path": run_json_path,
				"run_summary": summarize_run(obj),
			}
		)

		return {
			"daq_run_prefix": prefix,
			"bin_file": bin_path.name,
			"run_json_file": run_json_path.name,
			"run_summary": summarize_run(obj),
		}

	if isinstance(obj, dict):
		return {
			key: materialize_artifacts(
				value,
				stage_dir=stage_dir,
				prefix=f"{prefix}_{key}",
				extracted_runs=extracted_runs,
			)
			for key, value in obj.items()
		}

	if isinstance(obj, list):
		return [
			materialize_artifacts(
				value,
				stage_dir=stage_dir,
				prefix=f"{prefix}_{idx:03d}",
				extracted_runs=extracted_runs,
			)
			for idx, value in enumerate(obj)
		]

	return obj

def summarize_check_result(
		result: QACheckResult,
		*,
		stage_dir: Path
)-> tuple[dict[str, Any], list[dict[str, Any]]]:
	extracted_runs: list[dict[str, Any]] = []
	artifacts = materialize_artifacts(
		result.artifacts,
		stage_dir=stage_dir,
		prefix="artifact",
		extracted_runs=extracted_runs,
	)

	payload = {
		"name": result.name,
		"passed": result.passed,
		"metrics": result.metrics,
		"notes": result.notes,
		"artifacts": artifacts,
	}
	return payload, extracted_runs

def assess_decoded_threshold_compare(compare_rows: list[dict[str, Any]]) -> dict[str, Any]:
	thresholds: list[float] = []
	decoded_total_hits: list[int] = []

	for row in compare_rows:
		ds = row.get("decoded_summary") or {}
		thr = float(row["threshold_offset_mv"])

		if "n_rows_total" in ds:
			nhits = int(ds["n_rows_total"])
		else:
			nhits = int(ds.get("n_row", 0)) + int(ds.get("n_col", 0))

		thresholds.append(thr)
		decoded_total_hits.append(nhits)

	out = {
		"available": len(decoded_total_hits) >= 2,
		"thresholds_mv": thresholds,
		"decoded_total_hits": decoded_total_hits,
		"lowest_threshold_hits": decoded_total_hits[0] if decoded_total_hits else None,
		"highest_threshold_hits": decoded_total_hits[-1] if decoded_total_hits else None,
		"monotonic_nonincreasing": (
			all(decoded_total_hits[i + 1] <= decoded_total_hits[i]
				for i in range(len(decoded_total_hits) - 1))
			if len(decoded_total_hits) >= 2 else None
		),
		"passed": (
			decoded_total_hits[-1] < decoded_total_hits[0]
			if len(decoded_total_hits) >= 2 else False
		),
	}
	return out

async def run_stage(
	*,
	name: str,
	coro,
	out_dir: Path,
	stop_on_fail: bool,
	run_ext_decoder: bool = False,
) -> tuple[bool, dict[str, Any] | None]:
	stage_dir = out_dir / name
	stage_dir.mkdir(parents=True, exist_ok=True)
	logger.info("=== START %s ===", name)
	t0 = time.time()

	try:
		result = await coro
		elapsed = time.time() - t0

		summary, extracted_runs = summarize_check_result(result, stage_dir=stage_dir)
		summary["elapsed_s"] = elapsed

		decoded_records: list[dict[str, Any]] = []

		if run_ext_decoder:
			for rec in extracted_runs:
				bin_path = rec["bin_path"]
				if not bin_path.exists():
					continue

				csv_path = run_external_decoder(bin_path)
				decoded_summary = None
				if csv_path is not None and csv_path.exists():
					decoded_summary = summarize_decoded_csv(csv_path)
					write_json(
						stage_dir / f"{rec['prefix']}_decoded_summary.json",
						decoded_summary,
					)

				decoded_records.append(
					{
						"prefix": rec["prefix"],
						"bin_file": bin_path.name,
						"csv_file": csv_path.name if csv_path is not None else None,
						"decoded_summary": decoded_summary,
					}
				)

			if decoded_records:
				write_json(stage_dir / "decoded_runs.json", decoded_records)
				summary["artifacts"]["decoded_runs_file"] = "decoded_runs.json"

			# Special aggregate comparison for threshold scan
			final_passed = result.passed

			if result.name == "threshold_scan" and "scan_points" in result.artifacts:
				compare_rows: list[dict[str, Any]] = []
				raw_points = result.artifacts["scan_points"]

				point_decoded = [
					rec for rec in decoded_records
					if "_scan_points_" in rec["prefix"] and rec["prefix"].endswith("_run")
				]

				for point, dec in zip(raw_points, point_decoded):
					compare_rows.append(
						{
							"threshold_offset_mv": point["threshold_offset_mv"],
							"threshold_apply_mode": point["threshold_apply_mode"],
							"summary": point["summary"],
							"decoded_summary": dec["decoded_summary"],
							"csv_file": dec["csv_file"],
							"bin_file": dec["bin_file"],
						}
					)

				if compare_rows:
					write_json(stage_dir / "decoded_compare.json", compare_rows)
					summary["artifacts"]["decoded_compare_file"] = "decoded_compare.json"

					decoded_eval = assess_decoded_threshold_compare(compare_rows)
					write_json(stage_dir / "decoded_threshold_assessment.json", decoded_eval)
					summary["artifacts"]["decoded_threshold_assessment_file"] = "decoded_threshold_assessment.json"

					summary["metrics"]["decoded_thresholds_mv"] = decoded_eval["thresholds_mv"]
					summary["metrics"]["decoded_total_hits"] = decoded_eval["decoded_total_hits"]

					# Preserve coarse judgement for debugging, but use decoded judgement as final.
					summary["metrics"]["coarse_passed"] = result.passed
					final_passed = decoded_eval["passed"]
					summary["passed"] = final_passed

					if decoded_eval["available"]:
						if final_passed:
							summary["notes"].append(
								"Decoded hit counts decrease with increasing threshold;\
								threshold application appears effective."
							)
						else:
							summary["notes"].append(
								"Decoded hit counts do not decrease with increasing threshold."
							)
			else:
				final_passed = result.passed

		write_json(stage_dir / "result.json", summary)

		logger.info("=== END %s | passed=%s | elapsed=%.3fs ===", name, result.passed, elapsed)

		if stop_on_fail and result.passed is False:
			raise V3QAStageFailure(f"Stage {name} returned passed=False")

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
		if stop_on_fail:
			raise
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
				lane			  = args.lane,
				first_chip_id	 = args.first_chip_id,
				autoread		  = args.autoread,
				reset_delay_s	 = args.reset_delay_s,
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
			name="02_sparse_injection_test",
			coro=qa.sparse_injection_test(
				lane=args.lane,
				chip=args.chip,
				# pixels=... # submit default
				threshold_mode=args.threshold_mode,
				vinj_mv=args.vinj_mv,
				injection_thr_mv=args.injection_thr_mv,
				duration_s=args.injection_duration_s,
				autoread=args.autoread,
				injector_period=args.injector_period,
				injector_clkdiv=args.injector_clkdiv,
				injector_initdelay=args.injector_initdelay,
				injector_cycle=args.injector_cycle,
				injector_pulseperset=args.injector_pulseperset,
				decoder=None,
			),
			out_dir=out_dir,
			stop_on_fail=args.stop_on_fail,
			run_ext_decoder=args.run_external_decoder
		)
		session_summary["stages"]["02_sparse_injection_test"] = payload2

		# ---------------------------------------

		# Stage 3: threshold scan vs. noise

		threshold_offsets = [float(x) for x in args.threshold_scan_offsets_mv]
		ok3, payload3 = await run_stage(
			name="03_threshold_scan",
			coro=qa.threshold_scan(
				lane=args.lane,
				chip=args.chip,
				threshold_offsets_mv=threshold_offsets,
				threshold_mode=args.threshold_mode,
				duration_s=args.threshold_scan_duration_s,
				autoread=args.autoread,
				enable_full_matrix=True,
				enable_pixels=None,
				decoder=None,
			),
			out_dir=out_dir,
			stop_on_fail=args.stop_on_fail,
			run_ext_decoder=args.run_external_decoder
		)
		session_summary["stages"]["03_threshold_scan"] = payload3

		# ---------------------------------------

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

	parser.add_argument("-x", "--fpgaxml",	 type=str, default="gecco")
	parser.add_argument("-y", "--yaml",		type=str, nargs="+", default=["singlechip_testconfig_v3"])
	parser.add_argument("-c", "--chipsPerRow", type=int, nargs="+", default=[1])

	parser.add_argument("--lane", type=int, default=0)
	parser.add_argument("--chip", type=int, default=0)
	parser.add_argument("--col",  type=int, default=10)
	parser.add_argument("--row",  type=int, default=10)
	parser.add_argument("--first_chip_id", type=int, default=0)

	parser.add_argument("--vinj_mv",			  type=float, default=500.0)
	parser.add_argument("--injection_thr_mv",	 type=float, default=400.0)
	parser.add_argument("--injection_duration_s", type=float, default=5.0)
	parser.add_argument("--injector_period",	  type=int, default=162)
	parser.add_argument("--injector_clkdiv",	  type=int, default=300)
	parser.add_argument("--injector_initdelay",   type=int, default=100)
	parser.add_argument("--injector_cycle",	   type=int, default=0)
	parser.add_argument("--injector_pulseperset", type=int, default=1)

	parser.add_argument("--threshold_scan_duration_s", type=float, default=5.0)
	parser.add_argument("--threshold_scan_offsets_mv", type=float, nargs="+", default=[50.0, 100.0, 150.0, 200.0])
	parser.add_argument("--threshold_mode", type=str,
			choices=["internal", "legacy_gecco_external"], default="legacy_gecco_external",
			help="Which threshold path to scan"
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
