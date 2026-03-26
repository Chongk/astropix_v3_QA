from __future__ import annotations

from copy import deepcopy
from typing import Any, Iterable

from .config import V3Config
from .protocol import V3Protocol
from .transport import LayerControl, V3Transport

class V3ControllerError(RuntimeError):
	pass

class V3Controller:
	"""
	v3-only control plane.

	Responsibilities
	----------------
	- Own one V3Config per lane
	- Mirror config into legacy boardDriver.asics[*].asic_config when useful
	- Convert config -> SPI frames via V3Protocol
	- Apply routing/config frames via V3Transport
	- Expose small, explicit control helpers for threshold / injection setup
	"""

	def __init__(
		self,
		transport: V3Transport,
		protocol: V3Protocol,
		*,
		lane_configs: dict[int, V3Config] | None = None,
	) -> None:
		self.transport = transport
		self.protocol = protocol
		self._lane_configs: dict[int, V3Config] = {}
		self._injector: Any | None = None

		if lane_configs:
			for lane, cfg in lane_configs.items():
				self.set_lane_config(int(lane), cfg)

	# ------------------------------------------------------------------
	# lane config registry
	# ------------------------------------------------------------------

	def set_lane_config(self, lane: int, cfg: V3Config) -> None:
		if not isinstance(cfg, V3Config):
			raise V3ControllerError(f"lane {lane}: expected V3Config, got {type(cfg)}")
		self._lane_configs[int(lane)] = cfg

	def get_lane_config(self, lane: int) -> V3Config:
		lane = int(lane)
		if lane not in self._lane_configs:
			raise V3ControllerError(f"No V3Config registered for lane {lane}")
		return self._lane_configs[lane]

	def known_lanes(self) -> list[int]:
		return sorted(set(self.transport.lanes) | set(self._lane_configs.keys()))

	# ------------------------------------------------------------------
	# legacy compatibility helpers
	# ------------------------------------------------------------------

	def mirror_config_to_board_driver_asic(self, lane: int, cfg: V3Config | None = None) -> dict[str, Any]:
		"""
		Copy V3Config payload into the legacy boardDriver.asics[lane].asic_config dict

		Why this exists:
		----------------
		Current A-STEP helpers and scripts reach into boardDriver.asics[layer].asic_config directly
		This helper lets you keep the new config as the source of truth,
		while still using those legacy paths for comparison/debugging
		"""
		lane = int(lane)
		cfg = self.get_lane_config(lane) if cfg is None else cfg

		board = self.transport.board
		if not hasattr(board, "asics") or lane not in board.asics:
			raise V3ControllerError(f"boardDriver has no ASIC object for lane {lane}")

		asic_obj = board.asics[lane]
		legacy_cfg = deepcopy(getattr(asic_obj, "asic_config", {}))
		for key, value in cfg.export_all().items():
			legacy_cfg[key] = deepcopy(value)

		asic_obj.asic_config = legacy_cfg
		return legacy_cfg

	def mirror_all_configs_to_board_driver(
		self,
		lanes: Iterable[int] | None = None,
	) -> dict[int, dict[str, Any]]:
		out: dict[int, dict[str, Any]] = {}
		for lane in (self.known_lanes() if lanes is None else lanes):
			out[int(lane)] = self.mirror_config_to_board_driver_asic(int(lane))
		return out

	# ------------------------------------------------------------------
	# convenience config mutation helpers
	# ------------------------------------------------------------------

	def reset_matrix(self, lane: int, chip: int) -> None:
		self.get_lane_config(lane).reset_matrix(chip)

	def set_blpix_mv(self, lane: int, chip: int, mv: float) -> None:
		self.get_lane_config(lane).set_blpix_mv(chip, mv)

	def set_threshold_offset_mv(self, lane: int, chip: int, mv: float) -> None:
		self.get_lane_config(lane).set_threshold_offset_mv(chip, mv)

	def set_absolute_thpix_mv(self, lane: int, chip: int, mv: float) -> None:
		self.get_lane_config(lane).set_absolute_thpix_mv(chip, mv)

	def set_vinj_mv(self, lane: int, chip: int, mv: float) -> None:
		self.get_lane_config(lane).set_vinj_mv(chip, mv)

	def enable_pixel(self, lane: int, chip: int, col: int, row: int) -> None:
		self.get_lane_config(lane).enable_pixel(chip, col, row)

	def disable_pixel(self, lane: int, chip: int, col: int, row: int) -> None:
		self.get_lane_config(lane).disable_pixel(chip, col, row)

	def enable_inj_row(self, lane: int, chip: int, row: int) -> None:
		self.get_lane_config(lane).enable_inj_row(chip, row)

	def disable_inj_row(self, lane: int, chip: int, row: int) -> None:
		self.get_lane_config(lane).disable_inj_row(chip, row)

	def enable_inj_col(self, lane: int, chip: int, col: int) -> None:
		self.get_lane_config(lane).enable_inj_col(chip, col)

	def disable_inj_col(self, lane: int, chip: int, col: int) -> None:
		self.get_lane_config(lane).disable_inj_col(chip, col)

	def enable_injection_pixel(self, lane: int, chip: int, col: int, row: int) -> None:
		self.get_lane_config(lane).enable_injection_pixel(chip, col, row)

	def disable_injection_pixel(self, lane: int, chip: int, col: int, row: int) -> None:
		self.get_lane_config(lane).disable_injection_pixel(chip, col, row)

	def configure_single_pixel_injection(
		self,
		lane: int,
		chip: int,
		col: int,
		row: int,
		*,
		threshold_offset_mv: float | None = None,
		vinj_mv: float | None = None,
		reset_first: bool = True,
		mirror_legacy: bool = False,
	) -> dict[str, Any]:
		cfg = self.get_lane_config(lane)
		cfg.configure_single_injection_pixel(
			chip=chip,
			col=col,
			row=row,
			reset_first=reset_first,
			enable_pixel=True,
			enable_injection=True,
		)
		if threshold_offset_mv is not None:
			cfg.set_threshold_offset_mv(chip, threshold_offset_mv)
		if vinj_mv is not None:
			cfg.set_vinj_mv(chip, vinj_mv)

		if mirror_legacy:
			self.mirror_config_to_board_driver_asic(lane, cfg)

		return cfg.summary(chip)

	# ------------------------------------------------------------------
	# internal helpers
	# ------------------------------------------------------------------

	def _resolve_lanes(self, lanes: Iterable[int] | None = None) -> list[int]:
		resolved = self.known_lanes() if lanes is None else sorted(int(x) for x in lanes)
		if not resolved:
			raise V3ControllerError("No lanes are configured")
		for lane in resolved:
			self.get_lane_config(lane)
		return resolved

	def _nchips_for_lane(self, lane: int) -> int:
		cfg_nchips = int(self.get_lane_config(lane).nchips)
		hw_nchips = int(self.transport.num_chips_on_lane(lane))
		if cfg_nchips != hw_nchips:
			# Do not hard-fail yet; use the smaller value for programming.
			return min(cfg_nchips, hw_nchips)
		return cfg_nchips

	async def _set_programming_lane_state(self, lane: int) -> None:
		await self.transport.set_layer_control(
			lane,
			LayerControl(
				reset=False,
				hold=True,
				chip_select=False,
				autoread=False,
				disable_miso=True,
				flush=True,
			),
		)

	# ------------------------------------------------------------------
	# chip programming
	# ------------------------------------------------------------------

	async def program_lane(
		self,
		lane: int,
		*,
		first_chip_id: int = 0,
		mirror_legacy: bool = True,
		msbfirst: bool = False,
	) -> int:
		"""
		Program one lane only.

		Sequence:
		1. put lane in a safe programming state
		2. send routing frame under CS
		3. send one SPI config frame per chip under CS
		"""
		lane = int(lane)
		cfg = self.get_lane_config(lane)

		if mirror_legacy:
			self.mirror_config_to_board_driver_asic(lane, cfg)

		await self._set_programming_lane_state(lane)

		await self.transport.spi_select(flush=True)
		try:
			await self.transport.write_routing_frame(lane, first_chip_id=first_chip_id)
		finally:
			await self.transport.spi_deselect(flush=True)

		nchips = self._nchips_for_lane(lane)
		for chip in range(nchips):
			frame = self.protocol.build_spi_config_frame(
				cfg,
				target_chip=chip,
				broadcast=False,
				load=True,
				n_load=10,
				msbfirst=msbfirst,
			)
			await self.transport.spi_select(flush=True)
			try:
				await self.transport.write_spi_bytes(lane, frame)
			finally:
				await self.transport.spi_deselect(flush=True)

		return nchips

	async def program_all(
		self,
		*,
		lanes: Iterable[int] | None = None,
		first_chip_id: int = 0,
		mirror_legacy: bool = True,
		msbfirst: bool = False,
	) -> dict[int, int]:
		"""
		Program all requested lanes.

		This intentionally follows the old A-STEP multi-lane pattern:
		- routing frame once under shared CS
		- then for chip index 0..N-1: write chip's config to every lane that has such a chip while CS is asserted
		"""
		target_lanes = self._resolve_lanes(lanes)

		for lane in target_lanes:
			if mirror_legacy:
				self.mirror_config_to_board_driver_asic(lane)
			await self._set_programming_lane_state(lane)

		# Routing phase
		await self.transport.spi_select(flush=True)
		try:
			for lane in target_lanes:
				await self.transport.write_routing_frame(lane, first_chip_id=first_chip_id)
		finally:
			await self.transport.spi_deselect(flush=True)

		# Config phase
		lane_to_nchips = {lane: self._nchips_for_lane(lane) for lane in target_lanes}
		max_nchips = max(lane_to_nchips.values())

		for chip in range(max_nchips):
			await self.transport.spi_select(flush=True)
			try:
				for lane in target_lanes:
					if chip < lane_to_nchips[lane]:
						cfg = self.get_lane_config(lane)
						frame = self.protocol.build_spi_config_frame(
							cfg,
							target_chip=chip,
							broadcast=False,
							load=True,
							n_load=10,
							msbfirst=msbfirst,
						)
						await self.transport.write_spi_bytes(lane, frame)
			finally:
				await self.transport.spi_deselect(flush=True)

		return lane_to_nchips

	async def reset_and_program(
		self,
		*,
		lanes: Iterable[int] | None = None,
		reset_delay_s: float = 0.5,
		first_chip_id: int = 0,
		mirror_legacy: bool = True,
		msbfirst: bool = False,
		drain_fpga_buffer: bool = True,
	) -> dict[int, int]:
		"""
		Old A-STEP-compatible high-level sequence: reset -> disable readout -> program config
		"""
		target_lanes = self._resolve_lanes(lanes)

		await self.transport.reset_layers(delay_s=reset_delay_s)
		await self.transport.disable_readout(flush=True)

		results = await self.program_all(
			lanes=target_lanes,
			first_chip_id=first_chip_id,
			mirror_legacy=mirror_legacy,
			msbfirst=msbfirst,
		)

		if drain_fpga_buffer:
			await self.transport.drain_buffer()

		return results

	# ------------------------------------------------------------------
	# chip/buffer cleanup before acquisition
	# ------------------------------------------------------------------

	async def flush_stale_data(
		self,
		*,
		lanes: Iterable[int] | None = None,
		burst_bytes: int = 128,
		max_rounds: int = 20,
		reset_counters: bool = True,
		drain_fpga_buffer: bool = True,
	) -> dict[int, int]:
		"""
		Approximate the intent of the old chips_flush()/buffer_flush_new():
		- take chips out of hold
		- if interruptn is low, push dummy bytes until it goes high
		- optionally drain FPGA buffer
		- optionally reset counters
		"""
		target_lanes = self._resolve_lanes(lanes)

		await self.transport.hold_layers(False, flush=True)
		rounds = await self.transport.flush_all_lanes_until_irq_high(
			lanes=target_lanes,
			burst_bytes=burst_bytes,
			max_rounds=max_rounds,
		)
		await self.transport.hold_layers(True, flush=True)

		if drain_fpga_buffer:
			await self.transport.drain_buffer()

		if reset_counters:
			for lane in target_lanes:
				await self.transport.reset_stat_counters(lane)

		return rounds

	async def arm_readout(
		self,
		*,
		lanes: Iterable[int] | None = None,
		autoread: bool = True,
	) -> None:
		"""
		Put lanes in readout-ready state.
		"""
		target_lanes = self._resolve_lanes(lanes)

		for lane in target_lanes:
			await self.transport.setup_lane_for_readout(
				lane,
				autoread=autoread,
				flush=True,
			)

		await self.transport.enable_readout(autoread=autoread, flush=True)

	async def disarm_readout(self) -> None:
		await self.transport.disable_readout(flush=True)

	# ------------------------------------------------------------------
	# injector / on-chip injection routing
	# ------------------------------------------------------------------

	async def route_injection_to_chip(self, enable: bool = True) -> None:
		"""
		Board-specific hook. This is still exposed through the controller,
		because the old A-STEP stack uses boardDriver.ioSetInjectionToChip()
		"""
		fn = getattr(self.transport.board, "ioSetInjectionToChip", None)
		if fn is None:
			raise V3ControllerError("boardDriver has no ioSetInjectionToChip()")
		await fn(enable=enable, flush=True)

	async def configure_injector(
		self,
		*,
		period:      int = 162, # [0, 255]; @ clkdiv300, 10: ~125 Hz, 100: ~13 Hz, 162: 8.0 Hz, 255: 5.1 Hz
		clkdiv:      int = 300, # [1, 65535]
		initdelay:   int = 100, # [1, 65535]
		cycle:       int = 0,   # [1, 65535]
		pulseperset: int = 1,
	) -> Any:
		getter = getattr(self.transport.board, "getInjector", None)
		if getter is None:
			raise V3ControllerError("boardDriver has no getInjector()")

		injector = getter()
		injector.period = int(period)
		injector.clkdiv = int(clkdiv)
		injector.initdelay = int(initdelay)
		injector.cycle = int(cycle)
		injector.pulsesperset = int(pulseperset)
		self._injector = injector
		return injector

	async def start_injection(self) -> None:
		if self._injector is None:
			raise V3ControllerError("Injector not configured. Call configure_injector() first.")
		await self._injector.start()

	async def stop_injection(self) -> None:
		if self._injector is None:
			raise V3ControllerError("Injector not configured. Call configure_injector() first.")
		await self._injector.stop()
