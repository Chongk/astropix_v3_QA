# - Apr. 16, 2026
# - Chong Kim, ckim.phenix@gmail.com
# - Written by AI assistance (chatGPT 5.4)

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from .controller import V3Controller
from .daq import DAQRunResult, V3DAQ
from .threshold import ThresholdApplier

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
	def __init__(
		self,
		controller: V3Controller,
		daq: V3DAQ,
		*,
		threshold_applier: ThresholdApplier | None = None,
	) -> None:
		self.controller = controller
		self.daq = daq
		self.threshold_applier = threshold_applier

	@staticmethod
	def _count_nonempty_chunks(run: DAQRunResult) -> int:
		return sum(1 for chunk in run.chunks if chunk.nbytes > 0)

	@staticmethod
	def _count_truncated_chunks(run: DAQRunResult) -> int:
		return sum(1 for chunk in run.chunks if getattr(chunk, 'truncated', False))

	@staticmethod
	def _flatten_raw(run: DAQRunResult) -> bytes:
		payload = bytearray()
		for chunk in run.chunks:
			if chunk.data:
				payload.extend(chunk.data)
		return bytes(payload)

	def _decode_hits(self, run: DAQRunResult, decoder: DecoderFn | None = None) -> list[Any] | None:
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
		hits = self._decode_hits(run, decoder=decoder)
		total_bytes = len(self._flatten_raw(run))
		total_chunks = run.total_chunks
		nonempty_chunks = self._count_nonempty_chunks(run)
		truncated_chunks = self._count_truncated_chunks(run)

		if duration_s is None:
			duration_s = max(run.t_end - run.t_start, 0.0)

		summary: dict[str, Any] = {
			'duration_s': duration_s,
			'total_chunks': total_chunks,
			'nonempty_chunks': nonempty_chunks,
			'truncated_chunks': truncated_chunks,
			'total_bytes': total_bytes,
			'bytes_per_s': (total_bytes / duration_s) if duration_s > 0 else None,
			'bursts_per_s': (nonempty_chunks / duration_s) if duration_s > 0 else None,
		}

		if hits is not None:
			nhits = len(hits)
			summary['total_hits'] = nhits
			summary['hits_per_s'] = (nhits / duration_s) if duration_s > 0 else None
			if enabled_pixels is not None and enabled_pixels > 0 and duration_s > 0:
				summary['hits_per_pixel_per_s'] = nhits / (enabled_pixels * duration_s)

		return summary

	def _enable_full_matrix(self, *, lane: int, chip: int) -> int:
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

	@staticmethod
	def _normalize_threshold_mode(threshold_mode: str) -> str:
		aliases = {'legacy_gecco_external': 'external_gecco'}
		mode = aliases.get(threshold_mode, threshold_mode)
		allowed = {'internal', 'external_gecco'}
		if mode not in allowed:
			raise V3QAError(
				f"Unsupported threshold_mode={threshold_mode!r}; expected one of {sorted(allowed)}"
			)
		return mode

	def _require_manual_mode(self, autoread: bool) -> None:
		if autoread:
			raise V3QAError(
				'Current QA routines implement the manual IRQ-owned readout path only. '
				'Firmware autoread is intentionally disabled in this stack.'
			)

	def _preflight_common(self, *, lane: int, chip: int, autoread: bool) -> None:
		self._require_manual_mode(autoread)
		self.controller.get_lane_config(lane)
		nchips = self.controller.transport.num_chips_on_lane(lane)
		if not (0 <= chip < nchips):
			raise V3QAError(f'chip={chip} outside hardware range 0..{nchips - 1} for lane {lane}')

	def _preflight_injection(self) -> None:
		board = self.controller.transport.board
		if getattr(board, 'getInjector', None) is None:
			raise V3QAError('Board driver has no getInjector(); injection tests are unavailable.')
		if getattr(board, 'ioSetInjectionToChip', None) is None:
			raise V3QAError('Board driver has no ioSetInjectionToChip(); on-chip injection routing is unavailable.')

	def _preflight_threshold(self, *, threshold_mode: str, lane: int, chip: int) -> None:
		threshold_mode = self._normalize_threshold_mode(threshold_mode)
		if threshold_mode == 'external_gecco':
			if self.threshold_applier is None:
				raise V3QAError(
					"threshold_mode='external_gecco' requested, but no threshold applier was provided"
				)
			validate = getattr(self.threshold_applier, 'validate_capabilities', None)
			if callable(validate):
				validate(lane=lane, chip=chip)

	def _build_programming_dump(self, *, lane: int, first_chip_id: int = 0) -> dict[str, Any]:
		cfg = self.controller.get_lane_config(lane)
		nchips = min(int(cfg.nchips), int(self.controller.transport.num_chips_on_lane(lane)))
		routing_frame = self.controller.protocol.build_routing_frame(first_chip_id=first_chip_id)

		chip_frames: list[dict[str, Any]] = []
		for chip in range(nchips):
			frame = self.controller.protocol.build_spi_config_frame(
				cfg,
				target_chip=chip,
				broadcast=False,
				load=True,
				n_load=10,
				msbfirst=False,
			)
			chip_frames.append({
				'chip': chip,
				'frame_len_bytes': len(frame),
				'frame_hex': bytes(frame).hex(),
				'protocol_order': self.controller.protocol.describe_order(cfg, chip=chip),
			})

		return {
			'lane': lane,
			'nchips': nchips,
			'routing_frame_hex': bytes(routing_frame).hex(),
			'config_export': cfg.export_all(),
			'chip_frames': chip_frames,
		}

	async def _apply_threshold(
		self,
		*,
		lane: int,
		chip: int,
		threshold_offset_mv: float | None,
		threshold_mode: str,
	) -> str:
		threshold_mode = self._normalize_threshold_mode(threshold_mode)
		if threshold_offset_mv is None:
			return 'none'

		if threshold_mode == 'internal':
			self.controller.set_threshold_offset_mv(
				lane=lane,
				chip=chip,
				mv=float(threshold_offset_mv),
			)
			return 'internal'

		if self.threshold_applier is None:
			raise V3QAError(
				"threshold_mode='external_gecco' requested, but no threshold applier was provided"
			)
		return await self.threshold_applier.apply_threshold_offset_mv(
			lane=lane,
			chip=chip,
			mv=float(threshold_offset_mv),
		)

	# -----------------------------------------------------

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
		self._preflight_common(lane=lane, chip=0, autoread=autoread)
		notes: list[str] = []
		metrics: dict[str, Any] = {}
		artifacts: dict[str, Any] = {}

		try:
			metrics['firmware_id'] = await self.controller.transport.read_firmware_id()
			metrics['layer_status_before'] = await self.controller.transport.read_layer_status(lane)
			artifacts['programming_dump'] = self._build_programming_dump(lane=lane, first_chip_id=first_chip_id)

			program_result = await self.daq.prepare_run(
				lanes=[lane],
				reset_delay_s=reset_delay_s,
				first_chip_id=first_chip_id,
				mirror_legacy=False,
				msbfirst=False,
				flush_burst_bytes=flush_burst_bytes,
				flush_max_rounds=flush_max_rounds,
				autoread=False,
			)
			artifacts['program_result'] = program_result

			metrics['layer_status_after'] = await self.controller.transport.read_layer_status(lane)
			irq_high = await self.controller.transport.interruptn_is_high(lane)
			metrics['interruptn_high_after_prepare'] = irq_high
			metrics['buffer_size_after_prepare'] = await self.controller.transport.read_buffer_size()

			await self.daq.finish_run()

			passed = bool(irq_high)
			if not irq_high:
				notes.append('interruptn remained low after prepare_run();\
						stale data or readout state may still be uncleared.')

			return QACheckResult('smoke_test', passed, metrics, notes, artifacts)
		except Exception as exc:  # noqa: BLE001
			notes.append(f'Exception during smoke_test: {exc!r}')
			return QACheckResult('smoke_test', False, metrics, notes, artifacts)

	# -----------------------------------------------------

	async def single_pixel_injection(
		self,
		*,
		lane: int = 0,
		chip: int = 0,
		col: int = 10,
		row: int = 10,
		threshold_offset_mv: float | None = None,
		threshold_mode: str = 'internal',
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
		self._preflight_common(lane=lane, chip=chip, autoread=autoread)
		self._preflight_injection()
		self._preflight_threshold(threshold_mode=threshold_mode, lane=lane, chip=chip)
		notes: list[str] = []
		artifacts: dict[str, Any] = {}

		threshold_mode = self._normalize_threshold_mode(threshold_mode)
		cfg_threshold = threshold_offset_mv if threshold_mode == 'internal' else None

		artifacts['config_summary'] = self.controller.configure_single_pixel_injection(
			lane=lane,
			chip=chip,
			col=col,
			row=row,
			threshold_offset_mv=cfg_threshold,
			vinj_mv=vinj_mv,
			reset_first=True,
			mirror_legacy=False,
		)

		await self.controller.route_injection_to_chip(enable=True)
		await self.controller.configure_injector(
			period=injector_period,
			clkdiv=injector_clkdiv,
			initdelay=injector_initdelay,
			cycle=injector_cycle,
			pulseperset=injector_pulseperset,
		)

		artifacts['threshold_apply_mode'] = await self._apply_threshold(
			lane=lane,
			chip=chip,
			threshold_offset_mv=threshold_offset_mv,
			threshold_mode=threshold_mode,
		)

		await self.daq.prepare_run(lanes=[lane], autoread=False)
		try:
			await self.controller.start_injection()
			run = await self.daq.run_for_manual_irq(
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

		artifacts['run'] = run
		metrics = self._summarize_run(run, decoder=decoder, duration_s=duration_s, enabled_pixels=1)

		passed = metrics['nonempty_chunks'] > 0
		if decoder is not None:
			passed = passed and (metrics.get('total_hits', 0) > 0)
		if metrics['nonempty_chunks'] == 0:
			notes.append('No non-empty IRQ burst was recorded.')
		if metrics.get('truncated_chunks', 0) > 0:
			notes.append('At least one burst hit the max_rounds limit; inspect stop_reason for long-burst behavior.')
		if decoder is not None and metrics.get('total_hits', 0) == 0:
			notes.append('Raw data were acquired, but decoder returned zero hits.')

		return QACheckResult('single_pixel_injection', passed, metrics, notes, artifacts)

	# -----------------------------------------------------

	async def sparse_injection_test(
		self,
		*,
		lane: int = 0,
		chip: int = 0,
		pixels: Iterable[int] = (0, 8, 17, 26, 34),
		threshold_mode: str = 'external_gecco',
		vinj_mv: float | None = None,
		injection_thr_mv: float | None = None,
		duration_s: float = 1.0,
		autoread: bool = False,
		injector_period: int = 162,
		injector_clkdiv: int = 300,
		injector_initdelay: int = 100,
		injector_cycle: int = 0,
		injector_pulseperset: int = 1,
		decoder: DecoderFn | None = None,
	) -> QACheckResult:
		self._preflight_common(lane=lane, chip=chip, autoread=autoread)
		self._preflight_injection()
		self._preflight_threshold(threshold_mode=threshold_mode, lane=lane, chip=chip)
		notes: list[str] = []
		artifacts: dict[str, Any] = {'points': []}
		metrics: dict[str, Any] = {}
		point_results: list[dict[str, Any]] = []
		n_pass = 0

		for pix in pixels:
			result = await self.single_pixel_injection(
				lane=lane,
				chip=chip,
				col=pix,
				row=pix,
				threshold_offset_mv=injection_thr_mv,
				threshold_mode=threshold_mode,
				vinj_mv=vinj_mv,
				duration_s=duration_s,
				autoread=False,
				injector_period=injector_period,
				injector_clkdiv=injector_clkdiv,
				injector_initdelay=injector_initdelay,
				injector_cycle=injector_cycle,
				injector_pulseperset=injector_pulseperset,
				decoder=decoder,
			)
			point_payload = {
				'pixel': [pix, pix],
				'passed': result.passed,
				'metrics': result.metrics,
				'notes': result.notes
			}
			point_results.append(point_payload)
			artifacts['points'].append(point_payload)
			if result.passed:
				n_pass += 1

		metrics['n_tested'] = len(point_results)
		metrics['n_passed'] = n_pass
		metrics['tested_pixels'] = [[pix, pix] for pix in pixels]
		metrics['pass_fraction'] = (n_pass / len(point_results)) if point_results else 0.0
		passed = n_pass == len(point_results)
		if not passed:
			notes.append('One or more sparse injection points failed.')
		return QACheckResult('sparse_injection_test', passed, metrics, notes, artifacts)

	async def threshold_scan(
		self,
		*,
		lane: int = 0,
		chip: int = 0,
		threshold_offsets_mv: Iterable[float] = (100, 150, 200, 250),
		threshold_mode: str = 'external_gecco',
		duration_s: float = 1.0,
		autoread: bool = False,
		enable_full_matrix: bool = True,
		enable_pixels: Iterable[tuple[int, int]] | None = None,
		decoder: DecoderFn | None = None,
	) -> QACheckResult:
		self._preflight_common(lane=lane, chip=chip, autoread=autoread)
		self._preflight_threshold(threshold_mode=threshold_mode, lane=lane, chip=chip)
		notes: list[str] = []
		threshold_mode = self._normalize_threshold_mode(threshold_mode)
		artifacts: dict[str, Any] = {
			'thr_scan': [],
			'threshold_apply_strategy': threshold_mode,
			'scan_type': 'background_only',
		}

		cfg = self.controller.get_lane_config(lane)
		if enable_full_matrix and enable_pixels is not None:
			raise V3QAError('Choose either enable_full_matrix=True or enable_pixels=..., not both.')

		points: list[QAScanPoint] = []
		for thr_mv in threshold_offsets_mv:
			cfg.reset_matrix(chip)
			if enable_full_matrix:
				enabled_pixels = self._enable_full_matrix(lane=lane, chip=chip)
			else:
				enabled_pixels = self._enable_selected_pixels(
						lane=lane, chip=chip, pixels=enable_pixels or [], reset_first=True)

			threshold_apply_mode = await self._apply_threshold(
				lane=lane,
				chip=chip,
				threshold_offset_mv=thr_mv,
				threshold_mode=threshold_mode,
			)

			await self.daq.prepare_run(lanes=[lane], autoread=False)
			try:
				run = await self.daq.run_for_manual_irq(
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

			summary = self._summarize_run(run, decoder=decoder, duration_s=duration_s, enabled_pixels=enabled_pixels)
			summary['enabled_pixels'] = enabled_pixels
			points.append(QAScanPoint(x=float(thr_mv), metrics=summary))
			artifacts['thr_scan'].append({
				'threshold_offset_mv': float(thr_mv),
				'threshold_apply_mode': threshold_apply_mode,
				'summary': summary,
				'run': run,
			})

		total_bytes = [p.metrics.get('total_bytes', 0) for p in points]
		nonempty_chunks = [p.metrics.get('nonempty_chunks', 0) for p in points]
		truncated_chunks = [p.metrics.get('truncated_chunks', 0) for p in points]
		passed = True
		if len(points) >= 2:
			if not (total_bytes[-1] <= total_bytes[0] and nonempty_chunks[-1] <= nonempty_chunks[0]):
				passed = False
				notes.append('Background activity did not decrease vs. increasing threshold.')
		if any(x > 0 for x in truncated_chunks):
			notes.append('One or more threshold points produced long bursts that hit max_rounds.')

		metrics = {
			'n_points': len(points),
			'x_values_mv': [p.x for p in points],
			'total_bytes': total_bytes,
			'nonempty_chunks': nonempty_chunks,
			'truncated_chunks': truncated_chunks,
		}
		if decoder is not None:
			metrics['total_hits'] = [p.metrics.get('total_hits') for p in points]

		return QACheckResult('threshold_scan', passed, metrics, notes, artifacts)
