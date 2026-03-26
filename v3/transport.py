# - Mar. 24, 2026
# - Chong Kim, ckim.phenix@gmail.com

from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Sequence, Any

class V3TransportError(RuntimeError):
	pass

@dataclass(slots=True)
class LayerControl:
	reset: bool = False
	hold: bool = False
	chip_select: bool = False
	autoread: bool = False
	disable_miso: bool = False
	flush: bool = True

class V3Transport:
	"""
	Thin hardware adapter over an existing A-STEP boardDriver

	Design rules
	------------
	- No chip semantics here
	- No recconfig / threshold / injection meaning here
	- Only wrap low-level FPGA / lane / buffer primitives
	"""

	def __init__(
		self,
		board_driver: Any,
		*,
		lane_count: int | None = None,
		chipversion: int = 3,
		default_spi_dummy: int = 0x00,
	) -> None:
		if board_driver is None:
			raise V3TransportError("board_driver must not be None")

		self.board = board_driver
		self.chipversion = int(chipversion)
		self.default_spi_dummy = int(default_spi_dummy) & 0xFF
		self._lane_count = lane_count

	# ------------------------------------------------------------------
	# lifecycle
	# ------------------------------------------------------------------

	async def open(self) -> None:
		await self.board.open()

	async def close(self) -> None:
		await self.board.close()

	async def read_firmware_id(self) -> Any:
		return await self.board.readFirmwareID()

	# ------------------------------------------------------------------
	# lane / topology helpers
	# ------------------------------------------------------------------

	@property
	def lanes(self) -> list[int]:
		if hasattr(self.board, "asics") and self.board.asics:
			return sorted(self.board.asics.keys())
		if self._lane_count is not None:
			return list(range(self._lane_count))
		return [0]

	def max_num_chips(self) -> int:
		if not hasattr(self.board, "asics") or not self.board.asics:
			return 1
		return max(getattr(asic, "_num_chips", 1) for asic in self.board.asics.values())

	def num_chips_on_lane(self, lane: int) -> int:
		try:
			return int(getattr(self.board.asics[lane], "_num_chips"))
		except Exception as exc:
			raise V3TransportError(f"Cannot determine chip count on lane {lane}") from exc

	# ------------------------------------------------------------------
	# FPGA global configuration
	# ------------------------------------------------------------------

	async def configure_chipversion(self, flush: bool = True) -> None:
		await self.board.rfg.write_chip_version(value=self.chipversion, flush=flush)

	async def configure_clocks(
		self,
		*,
		sample_clock: bool = True,
		timestamp_clock: bool = True,
		fpga_ts_freq_hz: int | None = None,
		use_tlu: bool = False,
		spi_freq_hz: int | None = None,
		flush: bool = True,
	) -> None:
		await self.board.setSampleClock(enable=sample_clock, flush=flush)
		await self.board.setTimestampClock(enable=timestamp_clock, flush=flush)

		if fpga_ts_freq_hz is not None:
			await self.board.layersConfigFPGATimestampFrequency(
				targetFrequencyHz=int(fpga_ts_freq_hz),
				flush=flush,
			)

		await self.board.layersConfigFPGATimestamp(
			enable=True,
			use_divider=True,
			use_tlu=use_tlu,
			flush=flush,
		)

		if spi_freq_hz is not None:
			await self.board.configureLayerSPIFrequency(int(spi_freq_hz), flush=flush)

	async def configure_autoread_keepalive(
		self,
		nchips: int | None = None,
		flush: bool = False,
	) -> int:
		"""
		Match current A-STEP behavior: nbytes = 5 + nchips - 1
		"""
		if nchips is None:
			nchips = self.max_num_chips()
		nbytes = 5 + int(nchips) - 1
		await self.board.rfg.write_layers_cfg_nodata_continue(value=nbytes, flush=flush)
		return nbytes

	# ------------------------------------------------------------------
	# global chip-side control
	# ------------------------------------------------------------------

	async def reset_layers(self, delay_s: float = 0.5) -> None:
		await self.board.resetLayers(float(delay_s))

	async def hold_layers(self, hold: bool, flush: bool = True) -> None:
		# A-STEP uses holdLayers(hold, flush=...)
		await self.board.holdLayers(hold=hold, flush=flush)

	async def set_chip_select_n(self, csn_high: bool) -> None:
		"""
		Naming:
		  csn_high=True  -> chip select not is high (inactive)
		  csn_high=False -> CSN low (active)
		"""
		await self.board.layersSetSPICSN(bool(csn_high))

	async def spi_select(self, flush: bool = True) -> None:
		await self.board.layersSelectSPI(flush=flush)

	async def spi_deselect(self, flush: bool = True) -> None:
		await self.board.layersDeselectSPI(flush=flush)

	async def enable_readout(self, *, autoread: bool = True, flush: bool = True) -> None:
		"""
		Keep this wrapper intentionally simple for now

		Existing A-STEP has board-type-specific signatures for enableLayersReadout() -
		we normalize that here using lane information when needed
		"""
		lanes = self.lanes

		try:
			# GECCO-style signature: enableLayersReadout(autoread, flush)
			await self.board.enableLayersReadout(autoread, flush)
			return
		except TypeError:
			pass

		try:
			# CMOD-style signature: enableLayersReadout(layerlst, autoread, flush)
			await self.board.enableLayersReadout(lanes, autoread, flush)
			return
		except TypeError:
			pass

		try:
			await self.board.enableLayersReadout()
		except Exception as exc:
			raise V3TransportError("Failed to enable readout") from exc

	async def disable_readout(self, flush: bool = True) -> None:
		await self.board.disableLayersReadout(flush)

	# ------------------------------------------------------------------
	# per-lane configuration
	# ------------------------------------------------------------------

	async def set_layer_control(self, lane: int, ctrl: LayerControl) -> None:
		await self.board.setLayerConfig(
			layer=lane,
			reset=ctrl.reset,
			hold=ctrl.hold,
			chipSelect=ctrl.chip_select,
			autoread=ctrl.autoread,
			disableMISO=ctrl.disable_miso,
			flush=ctrl.flush,
		)

	async def setup_lane_for_readout(
		self,
		lane: int,
		*,
		autoread: bool = True,
		flush: bool = True,
	) -> None:
		await self.board.setLayerConfig(
			layer=lane,
			reset=False,
			hold=False,
			autoread=autoread,
			flush=flush,
		)

	# ------------------------------------------------------------------
	# SPI TX primitives
	# ------------------------------------------------------------------

	@staticmethod
	def _coerce_bytes(data: bytes | bytearray | Sequence[int]) -> list[int]:
		if isinstance(data, (bytes, bytearray)):
			return list(data)
		return [int(x) & 0xFF for x in data]

	async def write_spi_bytes(
		self,
		lane: int,
		data: bytes | bytearray | Sequence[int],
	) -> None:
		await self.board.writeSPIBytesToLane(lane=lane, bytes=self._coerce_bytes(data))

	async def write_dummy_bytes(
		self,
		lane: int,
		count: int,
		value: int | None = None,
	) -> None:
		if count <= 0:
			return
		byte = self.default_spi_dummy if value is None else (int(value) & 0xFF)
		await self.board.writeSPIBytesToLane(lane=lane, bytes=[byte] * int(count))

	async def write_routing_frame(self, lane: int, first_chip_id: int = 0) -> None:
		await self.board.writeRoutingFrame(lane=lane, firstChipID=int(first_chip_id))

	# ------------------------------------------------------------------
	# status / counters
	# ------------------------------------------------------------------

	async def read_layer_status(self, lane: int) -> int:
		return int(await self.board.getLayerStatus(lane))

	async def interruptn_is_high(self, lane: int) -> bool:
		"""
		Existing status printing interprets status bit0 as interruptn
		High means idle / no pending readout
		"""
		status = await self.read_layer_status(lane)
		return (status & 0x1) != 0

	async def interrupt_asserted(self, lane: int) -> bool:
		"""
		Active-low interrupt convention
		"""
		return not await self.interruptn_is_high(lane)

	async def read_idle_counter(self, lane: int) -> int:
		return int(await self.board.getLayerStatIDLECounter(lane))

	async def read_frame_counter(self, lane: int) -> int:
		return int(await self.board.getLayerStatFRAMECounter(lane))

	async def reset_stat_counters(self, lane: int) -> None:
		await self.board.resetLayerStatCounters(lane)

	# ------------------------------------------------------------------
	# FPGA readout buffer
	# ------------------------------------------------------------------

	async def read_buffer_size(self) -> int:
		return int(await self.board.readoutGetBufferSize())

	async def read_buffer(self, count: int | None = None) -> bytes:
		if count is None:
			count = await self.read_buffer_size()
		return await self.board.readoutReadBytes(int(count))

	async def drain_buffer(self) -> bytes:
		size = await self.read_buffer_size()
		if size <= 0:
			return b""
		return await self.read_buffer(size)

	# ------------------------------------------------------------------
	# convenience helpers used later by controller/daq
	# ------------------------------------------------------------------

	async def flush_lane_until_irq_high(
		self,
		lane: int,
		*,
		burst_bytes: int = 128,
		max_rounds: int = 20,
		select_each_round: bool = True,
	) -> int:
		"""
		Mirrors the current A-STEP flush logic: while interruptn is low, push dummy bytes
		"""
		rounds = 0
		while rounds < max_rounds and await self.interrupt_asserted(lane):
			if select_each_round:
				await self.spi_select(flush=True)
			await self.write_dummy_bytes(lane, burst_bytes)
			if select_each_round:
				await self.spi_deselect(flush=True)
			rounds += 1
		return rounds

	async def flush_all_lanes_until_irq_high(
		self,
		*,
		lanes: Iterable[int] | None = None,
		burst_bytes: int = 128,
		max_rounds: int = 20,
	) -> dict[int, int]:
		results: dict[int, int] = {}
		for lane in (self.lanes if lanes is None else lanes):
			results[int(lane)] = await self.flush_lane_until_irq_high(
				int(lane),
				burst_bytes=burst_bytes,
				max_rounds=max_rounds,
			)
		return results
