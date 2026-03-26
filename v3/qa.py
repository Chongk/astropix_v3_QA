from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from .controller import V3Controller
from .daq import DAQChunk, DAQRunResult, V3DAQ

class V3QAError(RuntimeError):
    pass

DecoderFn = Callable[[bytes], list[Any]]

@dataclass(slots=True)
class QAMetric:
    name: str
    value: Any
    unit: str | None = None

@dataclass(slots=True)
class QACheckResult:
    name: str
    passed: bool | None
    metrics: dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)
    artifacts: dict[str, Any] = field(default_factory=dict)

@dataclass(slots=True)
class QAScanPoint:
    x: float
    metrics: dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

class V3QA:
    """
    High-level QA routines for AstroPix v3

    Current design goals
    --------------------
    1. Keep routines simple and explicit for first hardware debugging
    2. Reuse controller/daq state machines instead of directly touching transport
    3. Return structured summaries that make debugging boundaries visible

    Important note
    --------------
    Without a decoder callback, some routines return only activity proxies:
		- raw byte counts
		- number of non-empty bursts
		- IRQ burst counts
    This is still useful for bring-up and threshold trend checks,
	but it is not yet a true per-pixel occupancy analysis
    """

    def __init__(
        self,
        controller: V3Controller,
        daq: V3DAQ,
    ) -> None:
        self.controller = controller
        self.daq = daq

    # ------------------------------------------------------------------
    # internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _count_nonempty_chunks(run: DAQRunResult) -> int:
        return sum(1 for chunk in run.chunks if chunk.nbytes > 0)

    @staticmethod
    def _flatten_raw(run: DAQRunResult) -> bytes:
        payload = bytearray()
        for chunk in run.chunks:
            if chunk.data:
                payload.extend(chunk.data)
        return bytes(payload)

    def _decode_hits(
        self,
        run: DAQRunResult,
        decoder: DecoderFn | None = None,
    ) -> list[Any] | None:
        if decoder is None:
            return None
        raw = self._flatten_raw(run)
        return decoder(raw)

    def _summarize_run(
        self,
        run: DAQRunResult,
        *,
        decoder: DecoderFn | None = None,
        duration_s: float | None = None,
        enabled_pixels: int | None = None,
    ) -> dict[str, Any]:
        raw = self._flatten_raw(run)
        hits = self._decode_hits(run, decoder=decoder)

        total_bytes = len(raw)
        total_chunks = run.total_chunks
        nonempty_chunks = self._count_nonempty_chunks(run)

        if duration_s is None:
            duration_s = max(run.t_end - run.t_start, 0.0)

        summary: dict[str, Any] = {
            "duration_s": duration_s,
            "total_chunks": total_chunks,
            "nonempty_chunks": nonempty_chunks,
            "total_bytes": total_bytes,
            "bytes_per_s": (total_bytes / duration_s) if duration_s > 0 else None,
            "bursts_per_s": (nonempty_chunks / duration_s) if duration_s > 0 else None,
        }

        if hits is not None:
            nhits = len(hits)
            summary["total_hits"] = nhits
            summary["hits_per_s"] = (nhits / duration_s) if duration_s > 0 else None

            if enabled_pixels is not None and enabled_pixels > 0 and duration_s > 0:
                summary["hits_per_pixel_per_s"] = nhits / (enabled_pixels * duration_s)

        return summary

    def _enable_full_matrix(
        self,
        *,
        lane: int,
        chip: int,
    ) -> int:
        cfg = self.controller.get_lane_config(lane)
        cfg.reset_matrix(chip)

        count = 0
        for col in range(cfg.ncols):
            for row in range(cfg.nrows):
                cfg.enable_pixel(chip, col, row)
                count += 1

        return count

    def _enable_selected_pixels(
        self,
        *,
        lane: int,
        chip: int,
        pixels: Iterable[tuple[int, int]],
        reset_first: bool = True,
    ) -> int:
        cfg = self.controller.get_lane_config(lane)
        if reset_first:
            cfg.reset_matrix(chip)

        count = 0
        for col, row in pixels:
            cfg.enable_pixel(chip, col, row)
            count += 1
        return count

    # ------------------------------------------------------------------
    # 1) smoke test
    # ------------------------------------------------------------------

    async def smoke_test(
        self,
        *,
        lane: int = 0,
        first_chip_id: int = 0,
        autoread: bool = False,
        reset_delay_s: float = 0.5,
        flush_burst_bytes: int = 128,
        flush_max_rounds: int = 20,
    ) -> QACheckResult:
        """
        Minimal hardware sanity check.

        What it checks
        --------------
        - board connection / firmware access
        - reset + program path
        - flush path
        - readout arming path
        - post-flush IRQ state

        What it does NOT check
        ----------------------
        - pixel-level signal correctness
        - threshold correctness
        - decoder correctness
        """
        notes: list[str] = []
        metrics: dict[str, Any] = {}
        artifacts: dict[str, Any] = {}

        try:
            fw = await self.controller.transport.read_firmware_id()
            metrics["firmware_id"] = fw

            status_before = await self.controller.transport.read_layer_status(lane)
            metrics["layer_status_before"] = status_before

            program_result = await self.daq.prepare_run(
                lanes=[lane],
                reset_delay_s=reset_delay_s,
                first_chip_id=first_chip_id,
                mirror_legacy=True,
                msbfirst=False,
                flush_burst_bytes=flush_burst_bytes,
                flush_max_rounds=flush_max_rounds,
                autoread=autoread,
            )
            artifacts["program_result"] = program_result

            status_after = await self.controller.transport.read_layer_status(lane)
            irq_high = await self.controller.transport.interruptn_is_high(lane)
            bufsize = await self.controller.transport.read_buffer_size()

            metrics["layer_status_after"] = status_after
            metrics["interruptn_high_after_prepare"] = irq_high
            metrics["buffer_size_after_prepare"] = bufsize

            await self.daq.finish_run()

            passed = True
            if not irq_high:
                passed = False
                notes.append(
                    "interruptn remained low after prepare_run();\
					stale data or readout state may still be uncleared."
                )

            return QACheckResult(
                name="smoke_test",
                passed=passed,
                metrics=metrics,
                notes=notes,
                artifacts=artifacts,
            )

        except Exception as exc:
            notes.append(f"Exception during smoke_test: {exc!r}")
            return QACheckResult(
                name="smoke_test",
                passed=False,
                metrics=metrics,
                notes=notes,
                artifacts=artifacts,
            )

    # ------------------------------------------------------------------
    # 2) single pixel injection
    # ------------------------------------------------------------------

    async def single_pixel_injection(
        self,
        *,
        lane: int = 0,
        chip: int = 0,
        col: int = 10,
        row: int = 10,
        threshold_offset_mv: float | None = None,
        vinj_mv: float | None = None,
        duration_s: float = 1.0,
        autoread: bool = False,
        injector_period: int = 162,
        injector_clkdiv: int = 300,
        injector_initdelay: int = 100,
        injector_cycle: int = 0,
        injector_pulseperset: int = 1,
        decoder: DecoderFn | None = None,
    ) -> QACheckResult:
        """
        First recommended end-to-end QA

        Pass heuristic (current draft)
        ------------------------------
        - pass if any non-empty burst is seen
        - if decoder is provided, also require at least one decoded hit
        """
        notes: list[str] = []
        artifacts: dict[str, Any] = {}

        cfg_summary = self.controller.configure_single_pixel_injection(
            lane=lane,
            chip=chip,
            col=col,
            row=row,
            threshold_offset_mv=threshold_offset_mv,
            vinj_mv=vinj_mv,
            reset_first=True,
            mirror_legacy=True,
        )
        artifacts["config_summary"] = cfg_summary

        await self.controller.route_injection_to_chip(enable=True)
        await self.controller.configure_injector(
            period=injector_period,
            clkdiv=injector_clkdiv,
            initdelay=injector_initdelay,
            cycle=injector_cycle,
            pulseperset=injector_pulseperset,
        )

        run = await self.daq.run_single_pixel_injection_test(
            lane=lane,
            chip=chip,
            col=col,
            row=row,
            threshold_offset_mv=threshold_offset_mv,
            vinj_mv=vinj_mv,
            prepare=True,
            duration_s=duration_s,
            autoread=autoread,
            start_injection=True,
            stop_injection_after=True,
        )

        artifacts["run"] = run
        metrics = self._summarize_run(
            run,
            decoder=decoder,
            duration_s=duration_s,
            enabled_pixels=1,
        )

        passed = metrics["nonempty_chunks"] > 0
        if decoder is not None:
            passed = passed and (metrics.get("total_hits", 0) > 0)

        if metrics["nonempty_chunks"] == 0:
            notes.append("No non-empty IRQ burst was recorded.")
        if decoder is not None and metrics.get("total_hits", 0) == 0:
            notes.append("Raw data were acquired, but decoder returned zero hits.")

        return QACheckResult(
            name="single_pixel_injection",
            passed=passed,
            metrics=metrics,
            notes=notes,
            artifacts=artifacts,
        )

    # ------------------------------------------------------------------
    # 3) threshold scan
    # ------------------------------------------------------------------

    async def threshold_scan(
        self,
        *,
        lane: int = 0,
        chip: int = 0,
        col: int = 10,
        row: int = 10,
        threshold_offsets_mv: Iterable[float] = (200, 250, 300, 350, 400, 450, 500),
        vinj_mv: float | None = None,
        duration_s: float = 1.0,
        autoread: bool = False,
        injector_period: int = 162,
        injector_clkdiv: int = 300,
        injector_initdelay: int = 100,
        injector_cycle: int = 0,
        injector_pulseperset: int = 1,
        decoder: DecoderFn | None = None,
    ) -> QACheckResult:
        """
        Injection-based threshold scan.

        Interpretation
        --------------
        - with decoder: trend in hit counts vs threshold offset
        - without decoder: trend in raw activity (bytes / non-empty bursts) vs threshold offset
        """
        notes: list[str] = []
        artifacts: dict[str, Any] = {"scan_points": []}

        await self.controller.route_injection_to_chip(enable=True)
        await self.controller.configure_injector(
            period=injector_period,
            clkdiv=injector_clkdiv,
            initdelay=injector_initdelay,
            cycle=injector_cycle,
            pulseperset=injector_pulseperset,
        )

        points: list[QAScanPoint] = []

        for thr_mv in threshold_offsets_mv:
            self.controller.configure_single_pixel_injection(
                lane=lane,
                chip=chip,
                col=col,
                row=row,
                threshold_offset_mv=thr_mv,
                vinj_mv=vinj_mv,
                reset_first=True,
                mirror_legacy=True,
            )

            await self.daq.prepare_run(
                lanes=[lane],
                autoread=autoread,
            )

            try:
                await self.controller.start_injection()
                run = await self.daq.run_for(
                    duration_s=duration_s,
                    lane=lane,
                    wait_irq_timeout_s=0.01,
                    wait_poll_interval_s=0.0005,
                    dummy_chunk_bytes=32,
                    trailing_idle_rounds=2,
                    max_rounds_per_burst=512,
                    read_buffer_each_round=True,
                )
            finally:
                await self.controller.stop_injection()
                await self.daq.finish_run()

            summary = self._summarize_run(
                run,
                decoder=decoder,
                duration_s=duration_s,
                enabled_pixels=1,
            )
            points.append(QAScanPoint(x=float(thr_mv), metrics=summary))
            artifacts["scan_points"].append(
                {
                    "threshold_offset_mv": float(thr_mv),
                    "summary": summary,
                }
            )

        # Very mild QA heuristic:
        # if scan is working, activity should not be identical at all threshold points.
        signatures = [
            (
                p.metrics.get("total_hits", None),
                p.metrics.get("total_bytes", None),
                p.metrics.get("nonempty_chunks", None),
            )
            for p in points
        ]
        passed = len(set(signatures)) > 1

        if not passed:
            notes.append(
                "Threshold scan produced identical activity signatures across all points;\
				threshold path may be ineffective or not actually changing."
            )

        # Compact metrics for quick review
        metrics = {
            "n_points": len(points),
            "x_values_mv": [p.x for p in points],
            "total_bytes": [p.metrics.get("total_bytes") for p in points],
            "nonempty_chunks": [p.metrics.get("nonempty_chunks") for p in points],
        }
        if decoder is not None:
            metrics["total_hits"] = [p.metrics.get("total_hits") for p in points]

        return QACheckResult(
            name="threshold_scan",
            passed=passed,
            metrics=metrics,
            notes=notes,
            artifacts=artifacts,
        )

    # ------------------------------------------------------------------
    # 4) noise occupancy / noise activity
    # ------------------------------------------------------------------

    async def noise_occupancy(
        self,
        *,
        lane: int = 0,
        chip: int = 0,
        duration_s: float = 1.0,
        threshold_offset_mv: float | None = None,
        enable_full_matrix: bool = True,
        enable_pixels: Iterable[tuple[int, int]] | None = None,
        autoread: bool = False,
        decoder: DecoderFn | None = None,
    ) -> QACheckResult:
        """
        Noise run without intentional injection.

        Important
        ---------
        If `decoder` is None, this returns a noise *activity proxy*:
          - bytes/s
          - non-empty bursts/s

        If `decoder` is provided and returns one item per decoded hit,
        this also returns:
          - total_hits
          - hits/s
          - hits/pixel/s (if number of enabled pixels is known)
        """
        notes: list[str] = []
        artifacts: dict[str, Any] = {}

        cfg = self.controller.get_lane_config(lane)

        if enable_full_matrix and enable_pixels is not None:
            raise V3QAError("Choose either enable_full_matrix=True or enable_pixels=..., not both.")

        if enable_full_matrix:
            enabled_pixels = self._enable_full_matrix(lane=lane, chip=chip)
        else:
            enabled_pixels = self._enable_selected_pixels(
                lane=lane,
                chip=chip,
                pixels=enable_pixels or [],
                reset_first=True,
            )

        if threshold_offset_mv is not None:
            cfg.set_threshold_offset_mv(chip, threshold_offset_mv)

        self.controller.mirror_config_to_board_driver_asic(lane, cfg)
        artifacts["config_summary"] = cfg.summary(chip)

        await self.daq.prepare_run(
            lanes=[lane],
            autoread=autoread,
        )

        try:
            run = await self.daq.run_for(
                duration_s=duration_s,
                lane=lane,
                wait_irq_timeout_s=0.01,
                wait_poll_interval_s=0.0005,
                dummy_chunk_bytes=32,
                trailing_idle_rounds=2,
                max_rounds_per_burst=512,
                read_buffer_each_round=True,
            )
        finally:
            await self.daq.finish_run()

        artifacts["run"] = run
        metrics = self._summarize_run(
            run,
            decoder=decoder,
            duration_s=duration_s,
            enabled_pixels=enabled_pixels,
        )
        metrics["enabled_pixels"] = enabled_pixels

        passed = True
        if decoder is None:
            notes.append(
                "Decoder not provided: returned values are noise activity proxies, not true pixel occupancy."
            )

        return QACheckResult(
            name="noise_occupancy",
            passed=passed,
            metrics=metrics,
            notes=notes,
            artifacts=artifacts,
        )
