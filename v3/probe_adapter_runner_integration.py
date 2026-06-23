"""
Integration helpers for inserting probe-adapter preflight stages into
v3_qa_run.py without entangling the adapter code with the AstroPix-v3 control
stack.

Recommended use in v3_qa_run.py
-------------------------------
1) Add imports near the other v3 imports:

    from v3.probe_adapter_runner_integration import (
        add_probe_adapter_args,
        run_probe_adapter_preflight,
    )

2) In build_argparser(), before `return parser`:

    add_probe_adapter_args(parser)

3) In main(), after `session_summary` is created but before
   `build_runtime_from_files(...)` opens/configures the normal AstroPix runtime:

    adapter_ok = await run_probe_adapter_preflight(
        args=args,
        out_dir=out_dir,
        run_stage=run_stage,
        session_summary=session_summary,
    )
    if not adapter_ok:
        session_summary['completed_at'] = time.time()
        write_json(out_dir / 'session_summary.json', session_summary)
        return 0

Safety policy
-------------
The adapter stages are deliberately run before the existing smoke/injection/
threshold stages.

By default, contact failure is a hard gate for later chip QA stages.  This is
important because the downstream AstroPix-v3 smoke/injection/threshold stages can
exercise the FPGA/GECCO path and still produce apparent activity even when no
probe card/chip is present.

For bench tests where the probe card/chip is intentionally absent, use
`--adapter-contact-nonfatal` or `--adapter-only`.  These options allow the
adapter preflight result to be recorded but still skip downstream chip QA.

Use `--adapter-force-chip-qa-without-contact` only for explicit debugging of the
software pipeline; it can produce false-positive chip-QA results and must never
be used for production qualification.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Awaitable
import argparse
import asyncio
import time

try:
    # Normal placement inside the existing package: sw/v3/probe_adapter.py
    from v3.probe_adapter import ProbeAdapterClient, ProbeAdapterQA, BiasDacCalibration
except ImportError:  # pragma: no cover - useful for standalone/local testing
    from probe_adapter import ProbeAdapterClient, ProbeAdapterQA, BiasDacCalibration


RunStageFn = Callable[..., Awaitable[tuple[bool, dict[str, Any] | None, bool]]]


def _int_auto(value: str) -> int:
    return int(value, 0)


def add_probe_adapter_args(parser: argparse.ArgumentParser) -> None:
    """Attach probe-adapter options to the existing v3_qa_run.py parser."""
    group = parser.add_argument_group("probe adapter preflight")
    group.add_argument(
        "--adapter-ip",
        type=str,
        default=None,
        help="Enable probe-adapter preflight stages and connect to this IP address.",
    )
    group.add_argument("--adapter-port", type=int, default=5000)
    group.add_argument("--adapter-timeout-s", type=float, default=2.0)
    group.add_argument("--adapter-echo-word", type=_int_auto, default=0xA5A55A5A)
    group.add_argument(
        "--adapter-interstage-delay-s",
        type=float,
        default=1.0,
        help=(
            "Delay between adapter TCP sessions.  Some adapter firmware versions "
            "allow only one host/session at a time and need a short close/reopen gap."
        ),
    )
    group.add_argument(
        "--skip-adapter-contact",
        action="store_true",
        help=(
            "Skip the contact-status test even when --adapter-ip is provided.  "
            "This bypasses the normal contact gate."
        ),
    )
    group.add_argument("--adapter-contact-settle-s", type=float, default=0.01)
    group.add_argument(
        "--adapter-contact-nonfatal",
        action="store_true",
        help=(
            "Record a failed contact test as a nonfatal adapter result, but still "
            "skip downstream chip QA unless --adapter-force-chip-qa-without-contact "
            "is also specified.  Use this for bench tests without probe card/chip."
        ),
    )
    group.add_argument(
        "--adapter-only",
        action="store_true",
        help=(
            "Run adapter preflight only and then stop before opening the normal "
            "AstroPix runtime.  This is useful when no probe card/chip is installed."
        ),
    )
    group.add_argument(
        "--adapter-force-chip-qa-without-contact",
        action="store_true",
        help=(
            "DANGEROUS: continue into normal chip QA even if the adapter contact "
            "test fails.  This can produce false-positive injection/threshold results "
            "when no chip is actually connected."
        ),
    )
    group.add_argument(
        "--adapter-keep-test-on",
        action="store_true",
        help=(
            "Do not force TEST off after the contact test.  The following chip-power "
            "stage preserves the TEST state and ignores it for pass/fail."
        ),
    )
    group.add_argument(
        "--no-adapter-enable-chip-power",
        action="store_true",
        help="Do not enable AVSS/AVDD/DVDD through the adapter before normal chip QA.",
    )
    group.add_argument(
        "--adapter-read-power-monitor",
        action="store_true",
        help=(
            "Record raw AVSS/AVDD/DVDD current monitor values.  These are not used "
            "for pass/fail until calibration constants are finalized."
        ),
    )
    group.add_argument(
        "--adapter-iv-scan",
        action="store_true",
        help=(
            "Run a simple adapter-based I-V scan after contact/power preflight "
            "and before normal AstroPix chip QA."
        ),
    )
    group.add_argument("--adapter-iv-start-v", type=float, default=-150.0)
    group.add_argument("--adapter-iv-stop-v", type=float, default=-200.0)
    group.add_argument("--adapter-iv-step-v", type=float, default=10.0)
    group.add_argument("--adapter-iv-settle-s", type=float, default=0.2)
    group.add_argument("--adapter-iv-current-limit-raw", type=int, default=None)
    group.add_argument(
        "--adapter-iv-no-init-hv-adc",
        action="store_true",
        help="Do not send the non-interactive HV ADC initialization sequence before I-V scan.",
    )
    group.add_argument(
        "--adapter-iv-no-cleanup-bias",
        action="store_true",
        help="Do not reset HV DAC to zero after I-V scan.  Use only for controlled debugging.",
    )
    group.add_argument(
        "--adapter-hv-calibration-mode",
        choices=["measured_table", "inverse_fit", "fullscale"],
        default="measured_table",
        help=(
            "HV DAC calibration mode.  measured_table uses the measured adapter "
            "DAC-|HV| table and is the default.  inverse_fit uses "
            "DAC = 7.54434*|V| - 57.68.  fullscale keeps the old placeholder."
        ),
    )
    group.add_argument("--adapter-hv-negative-fullscale-v", type=float, default=-500.0)
    group.add_argument("--adapter-hv-fullscale-code", type=int, default=4095)
    group.add_argument(
        "--adapter-iv-nonfatal",
        action="store_true",
        help=(
            "Record a failed I-V scan but skip downstream chip QA instead of marking "
            "the session as aborted.  This is useful while HV calibration is being tuned."
        ),
    )


def _make_client(args: argparse.Namespace) -> ProbeAdapterClient:
    return ProbeAdapterClient(
        args.adapter_ip,
        port=args.adapter_port,
        timeout_s=args.adapter_timeout_s,
    )


def _run_echo_stage(args: argparse.Namespace):
    with _make_client(args) as client:
        return ProbeAdapterQA(client).echo_test(value=args.adapter_echo_word)


def _run_contact_stage(args: argparse.Namespace):
    with _make_client(args) as client:
        return ProbeAdapterQA(client).contact_test(
            settle_s=args.adapter_contact_settle_s,
            cleanup_test_switch=not args.adapter_keep_test_on,
            read_power_monitor=args.adapter_read_power_monitor,
        )


def _run_enable_chip_power_stage(args: argparse.Namespace):
    with _make_client(args) as client:
        return ProbeAdapterQA(client).enable_chip_power(
            avss=True,
            avdd=True,
            dvdd=True,
            test=None,  # preserve TEST; ignore it for pass/fail during chip QA
            read_power_monitor=args.adapter_read_power_monitor,
        )


def _run_iv_scan_stage(args: argparse.Namespace):
    bias_cal = BiasDacCalibration(
        mode=args.adapter_hv_calibration_mode,
        negative_fullscale_v=args.adapter_hv_negative_fullscale_v,
        code_at_negative_fullscale=args.adapter_hv_fullscale_code,
    )
    with _make_client(args) as client:
        return ProbeAdapterQA(client).iv_scan(
            start_v=args.adapter_iv_start_v,
            stop_v=args.adapter_iv_stop_v,
            step_v=args.adapter_iv_step_v,
            settle_s=args.adapter_iv_settle_s,
            bias_calibration=bias_cal,
            current_limit_raw=args.adapter_iv_current_limit_raw,
            init_hv_adc=not args.adapter_iv_no_init_hv_adc,
            cleanup_bias=not args.adapter_iv_no_cleanup_bias,
        )


async def _run_blocking(fn, *args, **kwargs):  # noqa: ANN001
    return await asyncio.to_thread(fn, *args, **kwargs)


async def _adapter_gap(args: argparse.Namespace) -> None:
    delay = max(0.0, float(getattr(args, "adapter_interstage_delay_s", 0.0)))
    if delay > 0:
        await asyncio.sleep(delay)


def _mark_chip_qa_skipped(
    session_summary: dict[str, Any],
    *,
    reason: str,
    aborted: bool = False,
) -> None:
    session_summary["chip_qa_skipped"] = True
    session_summary["chip_qa_skipped_reason"] = reason
    if aborted:
        session_summary["aborted_reason"] = reason


async def run_probe_adapter_preflight(
    *,
    args: argparse.Namespace,
    out_dir: Path,
    run_stage: RunStageFn,
    session_summary: dict[str, Any],
) -> bool:
    """
    Run adapter preflight stages before the normal AstroPix-v3 runtime is opened.

    Returns True if the normal chip QA should continue.  Returns False if the
    normal chip QA should be skipped/stopped.  A False return is not necessarily
    a Python/software crash; the reason is recorded in session_summary.
    """
    if getattr(args, "adapter_ip", None) is None:
        return True

    session_summary.setdefault("adapter_preflight", {})
    session_summary.setdefault("stages", {})
    session_summary["adapter_preflight"].update({
        "enabled": True,
        "adapter_ip": args.adapter_ip,
        "adapter_port": args.adapter_port,
        "interstage_delay_s": float(getattr(args, "adapter_interstage_delay_s", 0.0)),
        "adapter_only": bool(getattr(args, "adapter_only", False)),
        "contact_policy": {
            "good_bit_value": 1,
            "required_contacts": 29,
            "all_contacts_required": True,
            "contact_test_switch_state": {
                "avss": False,
                "avdd": False,
                "dvdd": False,
                "test": True,
            },
            "contact_failure_blocks_chip_qa": not bool(
                getattr(args, "adapter_force_chip_qa_without_contact", False)
            ),
            "contact_failure_nonfatal": bool(getattr(args, "adapter_contact_nonfatal", False)),
            "chip_power_after_contact": {
                "avss": True,
                "avdd": True,
                "dvdd": True,
                "test": "preserve/ignored",
            },
        },
    })
    if getattr(args, "adapter_iv_scan", False):
        session_summary["adapter_preflight"]["iv_scan_policy"] = {
            "enabled": True,
            "start_v": float(args.adapter_iv_start_v),
            "stop_v": float(args.adapter_iv_stop_v),
            "step_v": float(args.adapter_iv_step_v),
            "settle_s": float(args.adapter_iv_settle_s),
            "current_limit_raw": args.adapter_iv_current_limit_raw,
            "hv_calibration_mode": str(args.adapter_hv_calibration_mode),
            "hv_negative_fullscale_v": float(args.adapter_hv_negative_fullscale_v),
            "hv_fullscale_code": int(args.adapter_hv_fullscale_code),
            "cleanup_bias": not bool(args.adapter_iv_no_cleanup_bias),
        }

    # Echo is a communication gate.  If it fails, there is no point trying the
    # contact register or enabling chip power.
    ok, payload, transport_fatal = await run_stage(
        name="0a_adapter_echo_test",
        coro=_run_blocking(_run_echo_stage, args),
        out_dir=out_dir,
        stop_on_fail=True,
    )
    session_summary["stages"]["0a_adapter_echo_test"] = payload
    if transport_fatal or not ok:
        session_summary["adapter_preflight"]["completed_at"] = time.time()
        _mark_chip_qa_skipped(session_summary, reason="adapter_echo_failed", aborted=True)
        return False

    await _adapter_gap(args)

    contact_ok: bool | None = None
    if not getattr(args, "skip_adapter_contact", False):
        ok, payload, transport_fatal = await run_stage(
            name="0b_adapter_contact_test",
            coro=_run_blocking(_run_contact_stage, args),
            out_dir=out_dir,
            stop_on_fail=True,
        )
        session_summary["stages"]["0b_adapter_contact_test"] = payload
        contact_ok = bool(ok and not transport_fatal)
        session_summary["adapter_preflight"]["contact_gate_passed"] = contact_ok

        if transport_fatal or not ok:
            force = bool(getattr(args, "adapter_force_chip_qa_without_contact", False))
            nonfatal = bool(getattr(args, "adapter_contact_nonfatal", False) or getattr(args, "adapter_only", False))
            session_summary["adapter_preflight"]["contact_failure_action"] = (
                "force_continue_chip_qa" if force else "skip_downstream_chip_qa"
            )

            if not force:
                session_summary["adapter_preflight"]["completed_at"] = time.time()
                _mark_chip_qa_skipped(
                    session_summary,
                    reason="adapter_contact_failed",
                    aborted=not nonfatal,
                )
                return False
    else:
        session_summary["adapter_preflight"]["contact_gate_passed"] = "skipped"
        session_summary["adapter_preflight"]["contact_failure_action"] = "bypassed_by_user"

    if getattr(args, "adapter_only", False):
        session_summary["adapter_preflight"]["completed_at"] = time.time()
        _mark_chip_qa_skipped(session_summary, reason="adapter_only", aborted=False)
        return False

    await _adapter_gap(args)

    # Only enable chip power after the contact gate passed, was explicitly skipped,
    # or was explicitly forced.  Otherwise we already returned above.
    if not getattr(args, "no_adapter_enable_chip_power", False):
        ok, payload, transport_fatal = await run_stage(
            name="0c_adapter_enable_chip_power",
            coro=_run_blocking(_run_enable_chip_power_stage, args),
            out_dir=out_dir,
            stop_on_fail=True,
        )
        session_summary["stages"]["0c_adapter_enable_chip_power"] = payload
        if transport_fatal or not ok:
            session_summary["adapter_preflight"]["completed_at"] = time.time()
            _mark_chip_qa_skipped(
                session_summary,
                reason="adapter_chip_power_enable_failed",
                aborted=True,
            )
            return False
    else:
        session_summary["adapter_preflight"]["chip_power_enable"] = "skipped_by_user"

    await _adapter_gap(args)

    if getattr(args, "adapter_iv_scan", False):
        await _adapter_gap(args)
        ok, payload, transport_fatal = await run_stage(
            name="0d_adapter_iv_scan",
            coro=_run_blocking(_run_iv_scan_stage, args),
            out_dir=out_dir,
            stop_on_fail=True,
        )
        session_summary["stages"]["0d_adapter_iv_scan"] = payload
        if transport_fatal or not ok:
            session_summary["adapter_preflight"]["completed_at"] = time.time()
            _mark_chip_qa_skipped(
                session_summary,
                reason="adapter_iv_scan_failed",
                aborted=not bool(getattr(args, "adapter_iv_nonfatal", False)),
            )
            return False

    await _adapter_gap(args)

    session_summary["adapter_preflight"]["completed_at"] = time.time()
    session_summary["chip_qa_skipped"] = False
    return True
