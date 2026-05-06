# - Mar. 24, 2026
# - Chong Kim, ckim.phenix@gmail.com
# - Written by AI assistance (chatGPT 5.4)

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

class V3TransportError(RuntimeError):
	pass

class V3TransportFatalError(V3TransportError):
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
	"""Thin hardware adapter over an existing A-STEP boardDriver."""

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

	@staticmethod
	def _is_fatal_exception(exc: BaseException) -> bool:
		text = f"{type(exc).__module__}.{type(exc).__name__}: {exc}".upper()
		fatal_markers = (
			"IO_ERROR",
			"DEVICEERROR",
			"USB",
			"FTD2XX",
			"BROKENPIPE",
			"DISCONNECT",
			"NO SUCH DEVICE",
		)
		return any(marker in text for marker in fatal_markers)

	def _raise_transport(self, action: str, exc: BaseException) -> None:
		if isinstance(exc, V3TransportError):
			raise exc
		cls = V3TransportFatalError if self._is_fatal_exception(exc) else V3TransportError
		raise cls(f"Transport error during {action}: {exc}") from exc

	async def _call(self, action: str, awaitable):
		try:
			return await awaitable
		except Exception as exc:  # noqa: BLE001
			self._raise_transport(action, exc)

	async def open(self) -> None:
		await self._call("open board", self.board.open())

	async def close(self) -> None:
		await self._call("close board", self.board.close())

	async def read_firmware_id(self) -> Any:
		return await self._call("read firmware ID", self.board.readFirmwareID())

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
		except Exception as exc:  # noqa: BLE001
			raise V3TransportError(f"Cannot determine chip count on lane {lane}") from exc

	async def configure_chipversion(self, flush: bool = True) -> None:
		await self._call(
			"configure chip version",
			self.board.rfg.write_chip_version(value=self.chipversion, flush=flush),
		)

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
		await self._call("enable sample clock", self.board.setSampleClock(enable=sample_clock, flush=flush))
		await self._call("enable timestamp clock", self.board.setTimestampClock(enable=timestamp_clock, flush=flush))

		if fpga_ts_freq_hz is not None:
			await self._call(
				"configure FPGA timestamp frequency",
				self.board.layersConfigFPGATimestampFrequency(
					targetFrequencyHz=int(fpga_ts_freq_hz),
					flush=flush,
				),
			)

		await self._call(
			"configure FPGA timestamp mode",
			self.board.layersConfigFPGATimestamp(
				enable=True,
				use_divider=True,
				use_tlu=use_tlu,
				flush=flush,
			),
		)

		if spi_freq_hz is not None:
			await self._call(
				"configure SPI frequency",
				self.board.configureLayerSPIFrequency(int(spi_freq_hz), flush=flush),
			)

	async def configure_autoread_keepalive(
		self,
		nchips: int | None = None,
		flush: bool = False,
	) -> int:
		if nchips is None:
			nchips = self.max_num_chips()
		nbytes = 5 + int(nchips) - 1
		await self._call(
			"configure autoread keepalive",
			self.board.rfg.write_layers_cfg_nodata_continue(value=nbytes, flush=flush),
		)
		return nbytes

	async def configure_autoread_keepalive_bytes(self, nbytes: int, flush: bool = False) -> int:
		nbytes = max(0, int(nbytes))
		await self._call(
			"configure autoread keepalive bytes",
			self.board.rfg.write_layers_cfg_nodata_continue(value=nbytes, flush=flush),
		)
		return nbytes

	async def reset_layers(self, delay_s: float = 0.5) -> None:
		await self._call("reset layers", self.board.resetLayers(float(delay_s)))

	async def hold_layers(self, hold: bool, flush: bool = True) -> None:
		await self._call("set hold on layers", self.board.holdLayers(hold=hold, flush=flush))

	async def set_chip_select_n(self, csn_high: bool) -> None:
		await self._call("set chip select N", self.board.layersSetSPICSN(bool(csn_high)))

	async def spi_select(self, flush: bool = True) -> None:
		await self._call("assert SPI chip select", self.board.layersSelectSPI(flush=flush))

	async def spi_deselect(self, flush: bool = True) -> None:
		await self._call("deassert SPI chip select", self.board.layersDeselectSPI(flush=flush))

	async def enable_readout(self, *, autoread: bool = True, flush: bool = True) -> None:
		lanes = self.lanes
		try:
			await self._call("enable readout", self.board.enableLayersReadout(autoread, flush))
			return
		except V3TransportError as exc:
			if not isinstance(exc, V3TransportFatalError):
				pass
			else:
				raise
		except Exception:
			pass

		try:
			await self._call("enable readout", self.board.enableLayersReadout(lanes, autoread, flush))
			return
		except V3TransportError as exc:
			if not isinstance(exc, V3TransportFatalError):
				pass
			else:
				raise
		except Exception:
			pass

		await self._call("enable readout", self.board.enableLayersReadout())

	async def disable_readout(self, flush: bool = True) -> None:
		await self._call("disable readout", self.board.disableLayersReadout(flush))

	async def set_layer_control(self, lane: int, ctrl: LayerControl) -> None:
		await self._call(
			f"set layer {lane} control",
			self.board.setLayerConfig(
				layer=lane,
				reset=ctrl.reset,
				hold=ctrl.hold,
				chipSelect=ctrl.chip_select,
				autoread=ctrl.autoread,
				disableMISO=ctrl.disable_miso,
				flush=ctrl.flush,
			),
		)

	async def setup_lane_for_readout(
		self,
		lane: int,
		*,
		autoread: bool = True,
		flush: bool = True,
	) -> None:
		await self._call(
			f"setup lane {lane} for readout",
			self.board.setLayerConfig(
				layer=lane,
				reset=False,
				hold=False,
				autoread=autoread,
				flush=flush,
			),
		)

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
		await self._call(
			f"write SPI bytes to lane {lane}",
			self.board.writeSPIBytesToLane(lane=lane, bytes=self._coerce_bytes(data)),
		)

	async def write_dummy_bytes(
		self,
		lane: int,
		count: int,
		value: int | None = None,
	) -> None:
		if count <= 0:
			return
		byte = self.default_spi_dummy if value is None else (int(value) & 0xFF)
		await self._call(
			f"write dummy bytes to lane {lane}",
			self.board.writeSPIBytesToLane(lane=lane, bytes=[byte] * int(count)),
		)

	async def write_routing_frame(self, lane: int, first_chip_id: int = 0) -> None:
		await self._call(
			f"write routing frame to lane {lane}",
			self.board.writeRoutingFrame(lane=lane, firstChipID=int(first_chip_id)),
		)

	async def read_layer_status(self, lane: int) -> int:
		return int(await self._call(f"read layer {lane} status", self.board.getLayerStatus(lane)))

	async def interruptn_is_high(self, lane: int) -> bool:
		status = await self.read_layer_status(lane)
		return (status & 0x1) != 0

	async def interrupt_asserted(self, lane: int) -> bool:
		return not await self.interruptn_is_high(lane)

	async def read_idle_counter(self, lane: int) -> int:
		return int(await self._call(f"read layer {lane} idle counter", self.board.getLayerStatIDLECounter(lane)))

	async def read_frame_counter(self, lane: int) -> int:
		return int(await self._call(f"read layer {lane} frame counter", self.board.getLayerStatFRAMECounter(lane)))

	async def reset_stat_counters(self, lane: int) -> None:
		await self._call(f"reset layer {lane} counters", self.board.resetLayerStatCounters(lane))

	async def read_buffer_size(self) -> int:
		return int(await self._call("read FPGA buffer size", self.board.readoutGetBufferSize()))

	async def read_buffer(self, count: int | None = None) -> bytes:
		if count is None:
			count = await self.read_buffer_size()
		return await self._call(f"read {int(count)} bytes from FPGA buffer", self.board.readoutReadBytes(int(count)))

	async def drain_buffer(self) -> bytes:
		size = await self.read_buffer_size()
		if size <= 0:
			return b""
		return await self.read_buffer(size)

	async def flush_lane_until_irq_high(
		self,
		lane: int,
		*,
		burst_bytes: int = 128,
		max_rounds: int = 20,
		select_each_round: bool = True,
	) -> int:
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
