# - Mar. 24, 2026
# - Chong Kim, ckim.phenix@gmail.com
# - Written by AI assistance (chatGPT 5.4)
# - Updated Jun. 2026: split manual IRQ and FPGA-autoread capture paths.

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Iterable
import asyncio
import time

from .controller import V3Controller
from .transport import V3Transport

class V3DAQError(RuntimeError):
	pass

@dataclass(slots=True)
class DAQChunk:
	lane: int
	t_start: float
	t_end: float
	irq_seen: bool
	rounds: int
	bytes_written_as_dummy: int
	buffer_sizes: list[int] = field(default_factory=list)
	data: bytes = b""
	truncated: bool = False
	stop_reason: str = "unknown"

	@property
	def nbytes(self) -> int:
		return len(self.data)

@dataclass(slots=True)
class DAQRunResult:
	lanes: list[int]
	t_start: float
	t_end: float
	chunks: list[DAQChunk] = field(default_factory=list)
	readout_owner: str = "manual_irq"
	stop_reason: str = "unknown"

	@property
	def total_bytes(self) -> int:
		return sum(chunk.nbytes for chunk in self.chunks)

	@property
	def total_chunks(self) -> int:
		return len(self.chunks)

class V3DAQ:
	"""v3 DAQ layer with explicit manual-IRQ and FPGA-autoread ownership."""

	def __init__(
		self,
		controller: V3Controller,
		*,
		default_lane: int = 0,
	) -> None:
		self.controller = controller
		self.transport: V3Transport = controller.transport
		self.default_lane = int(default_lane)
		self._armed_autoread = False

	async def prepare_run(
		self,
		*,
		lanes: Iterable[int] | None = None,
		reset_delay_s: float = 0.5,
		first_chip_id: int = 0,
		mirror_legacy: bool = False,
		msbfirst: bool = False,
		flush_burst_bytes: int = 128,
		flush_max_rounds: int = 20,
		autoread: bool = False,
		configure_autoread_keepalive: bool = True,
		autoread_keepalive_bytes: int | None = None,
	) -> dict[int, int]:
		"""Reset/program and arm readout.

		``autoread=False`` keeps the original software/manual-IRQ ownership model.
		``autoread=True`` lets FPGA firmware own continuous readout; software only
		polls and drains the output buffer.
		"""
		results = await self.controller.reset_and_program(
			lanes=lanes,
			reset_delay_s=reset_delay_s,
			first_chip_id=first_chip_id,
			mirror_legacy=mirror_legacy,
			msbfirst=msbfirst,
			drain_fpga_buffer=True,
		)

		await self.controller.flush_stale_data(
			lanes=lanes,
			burst_bytes=flush_burst_bytes,
			max_rounds=flush_max_rounds,
			reset_counters=True,
			drain_fpga_buffer=True,
		)

		if autoread and configure_autoread_keepalive:
			if autoread_keepalive_bytes is None:
				await self.transport.configure_autoread_keepalive(flush=False)
			else:
				await self.transport.configure_autoread_keepalive_bytes(
					int(autoread_keepalive_bytes), flush=False
				)

		await self.controller.arm_readout(
			lanes=lanes,
			autoread=bool(autoread),
		)
		self._armed_autoread = bool(autoread)
		return results

	async def finish_run(self) -> None:
		await self.controller.disarm_readout()
		self._armed_autoread = False

	async def wait_for_irq(
		self,
		lane: int | None = None,
		*,
		timeout_s: float | None = None,
		poll_interval_s: float = 0.0005,
		stop_event: asyncio.Event | None = None,
	) -> bool:
		lane = self.default_lane if lane is None else int(lane)
		t0 = time.monotonic()

		while True:
			if stop_event is not None and stop_event.is_set():
				return False

			if await self.transport.interrupt_asserted(lane):
				return True

			if timeout_s is not None and (time.monotonic() - t0) >= timeout_s:
				return False

			await asyncio.sleep(poll_interval_s)

	async def acquire_irq_burst(
		self,
		lane: int | None = None,
		*,
		dummy_chunk_bytes: int = 32,
		trailing_idle_rounds: int = 2,
		max_rounds: int = 512,
		read_buffer_each_round: bool = True,
		deselect_after: bool = True,
		absolute_deadline: float | None = None,
		stop_event: asyncio.Event | None = None,
	) -> DAQChunk:
		lane = self.default_lane if lane is None else int(lane)
		irq_seen = await self.transport.interrupt_asserted(lane)
		t_start = time.monotonic()

		if not irq_seen:
			return DAQChunk(
				lane=lane,
				t_start=t_start,
				t_end=t_start,
				irq_seen=False,
				rounds=0,
				bytes_written_as_dummy=0,
				buffer_sizes=[],
				data=b"",
				truncated=False,
				stop_reason="irq_not_asserted",
			)

		rounds = 0
		idle_rounds = 0
		bytes_written_as_dummy = 0
		buffer_sizes: list[int] = []
		payload = bytearray()
		stop_reason = "unknown"
		truncated = False

		await self.transport.spi_select(flush=True)
		try:
			while rounds < max_rounds:
				now = time.monotonic()
				if stop_event is not None and stop_event.is_set():
					truncated = True
					stop_reason = "stop_event_set"
					break
				if absolute_deadline is not None and now >= absolute_deadline:
					truncated = True
					stop_reason = "deadline_reached"
					break

				await self.transport.write_dummy_bytes(lane, dummy_chunk_bytes)
				bytes_written_as_dummy += dummy_chunk_bytes
				rounds += 1

				if read_buffer_each_round:
					size = await self.transport.read_buffer_size()
					buffer_sizes.append(size)
					if size > 0:
						payload.extend(await self.transport.read_buffer(size))
						idle_rounds = 0
					else:
						if not await self.transport.interrupt_asserted(lane):
							idle_rounds += 1
				else:
					if not await self.transport.interrupt_asserted(lane):
						idle_rounds += 1

				irq_now = await self.transport.interrupt_asserted(lane)
				if (not irq_now) and (idle_rounds >= trailing_idle_rounds):
					stop_reason = "irq_deasserted_tail_drained"
					break
			else:
				truncated = True
				stop_reason = "max_rounds_reached"

			tail = await self.transport.drain_buffer()
			if tail:
				payload.extend(tail)
				buffer_sizes.append(len(tail))

		finally:
			if deselect_after:
				await self.transport.spi_deselect(flush=True)

		t_end = time.monotonic()
		return DAQChunk(
			lane=lane,
			t_start=t_start,
			t_end=t_end,
			irq_seen=True,
			rounds=rounds,
			bytes_written_as_dummy=bytes_written_as_dummy,
			buffer_sizes=buffer_sizes,
			data=bytes(payload),
			truncated=truncated,
			stop_reason=stop_reason,
		)

	async def run_for_manual_irq(
		self,
		*,
		duration_s: float | None,
		lane: int | None = None,
		wait_irq_timeout_s: float = 0.01,
		wait_poll_interval_s: float = 0.0005,
		dummy_chunk_bytes: int = 32,
		trailing_idle_rounds: int = 2,
		max_rounds_per_burst: int = 512,
		read_buffer_each_round: bool = True,
		stop_on_empty_burst: bool = False,
		stop_event: asyncio.Event | None = None,
	) -> DAQRunResult:
		if self._armed_autoread:
			raise V3DAQError(
				"run_for_manual_irq() cannot be used while firmware autoread owns readout."
			)

		lane = self.default_lane if lane is None else int(lane)
		t0 = time.monotonic()
		deadline = None if duration_s is None else (t0 + float(duration_s))
		result = DAQRunResult(
			lanes=[lane],
			t_start=t0,
			t_end=t0,
			chunks=[],
			readout_owner="manual_irq",
			stop_reason="unknown",
		)

		while True:
			if stop_event is not None and stop_event.is_set():
				result.stop_reason = "stop_event_set"
				break
			if deadline is not None and time.monotonic() >= deadline:
				result.stop_reason = "duration_reached"
				break

			seen = await self.wait_for_irq(
				lane,
				timeout_s=wait_irq_timeout_s,
				poll_interval_s=wait_poll_interval_s,
				stop_event=stop_event,
			)
			if not seen:
				continue

			chunk = await self.acquire_irq_burst(
				lane=lane,
				dummy_chunk_bytes=dummy_chunk_bytes,
				trailing_idle_rounds=trailing_idle_rounds,
				max_rounds=max_rounds_per_burst,
				read_buffer_each_round=read_buffer_each_round,
				absolute_deadline=deadline,
				stop_event=stop_event,
			)
			result.chunks.append(chunk)

			if stop_on_empty_burst and chunk.nbytes == 0:
				result.stop_reason = "empty_burst"
				break
			if chunk.stop_reason in {"deadline_reached", "stop_event_set"}:
				result.stop_reason = chunk.stop_reason
				break

		if result.stop_reason == "unknown":
			result.stop_reason = "completed"
		result.t_end = time.monotonic()
		return result


	async def run_for_manual_forced_clock(
		self,
		*,
		duration_s: float | None,
		lane: int | None = None,
		dummy_chunk_bytes: int = 32,
		poll_interval_s: float = 0.001,
		select_each_round: bool = True,
		max_read_bytes: int | None = None,
		stop_event: asyncio.Event | None = None,
	) -> DAQRunResult:
		"""Manual-owner raw capture that clocks the chip even without IRQ.

		This is intentionally a debug-only path.  It periodically writes dummy
		bytes on the SPI lane and drains the FPGA readout buffer, so it can expose
		low-level byte activity even when the software IRQ gate would otherwise
		never enter acquire_irq_burst().  Empty drains are not stored as chunks to
		keep artifacts compact.
		"""
		if self._armed_autoread:
			raise V3DAQError(
				"run_for_manual_forced_clock() cannot be used while firmware autoread owns readout."
			)
		if dummy_chunk_bytes <= 0:
			raise V3DAQError("dummy_chunk_bytes must be positive for forced-clock capture")

		lane = self.default_lane if lane is None else int(lane)
		t0 = time.monotonic()
		deadline = None if duration_s is None else (t0 + float(duration_s))
		result = DAQRunResult(
			lanes=[lane],
			t_start=t0,
			t_end=t0,
			chunks=[],
			readout_owner="manual_irq_forced_clock",
			stop_reason="unknown",
		)

		csn_held = False
		try:
			if not select_each_round:
				await self.transport.spi_select(flush=True)
				csn_held = True

			while True:
				if stop_event is not None and stop_event.is_set():
					result.stop_reason = "stop_event_set"
					break
				if deadline is not None and time.monotonic() >= deadline:
					result.stop_reason = "duration_reached"
					break

				t_start = time.monotonic()
				if select_each_round:
					await self.transport.spi_select(flush=True)
				try:
					await self.transport.write_dummy_bytes(lane, dummy_chunk_bytes)
				finally:
					if select_each_round:
						await self.transport.spi_deselect(flush=True)

				size = await self.transport.read_buffer_size()
				buffer_sizes = [size]
				payload = b""
				truncated = False
				if size > 0:
					to_read = size if max_read_bytes is None else min(size, int(max_read_bytes))
					payload = await self.transport.read_buffer(to_read)
					truncated = to_read < size
					t_end = time.monotonic()
					result.chunks.append(DAQChunk(
						lane=lane,
						t_start=t_start,
						t_end=t_end,
						irq_seen=await self.transport.interrupt_asserted(lane),
						rounds=1,
						bytes_written_as_dummy=int(dummy_chunk_bytes),
						buffer_sizes=buffer_sizes,
						data=payload,
						truncated=truncated,
						stop_reason="forced_clock_buffer_polled" if not truncated else "forced_clock_partial_buffer_read",
					))

				await asyncio.sleep(max(0.0, float(poll_interval_s)))

		finally:
			if csn_held:
				await self.transport.spi_deselect(flush=True)

		# Final drain after duration/stop-event.
		tail_size = await self.transport.read_buffer_size()
		if tail_size > 0:
			t_start = time.monotonic()
			data = await self.transport.read_buffer(tail_size)
			t_end = time.monotonic()
			result.chunks.append(DAQChunk(
				lane=lane,
				t_start=t_start,
				t_end=t_end,
				irq_seen=await self.transport.interrupt_asserted(lane),
				rounds=1,
				bytes_written_as_dummy=0,
				buffer_sizes=[tail_size],
				data=data,
				truncated=False,
				stop_reason="final_drain",
			))

		if result.stop_reason == "unknown":
			result.stop_reason = "completed"
		result.t_end = time.monotonic()
		return result


	async def run_for_autoread(
		self,
		*,
		duration_s: float | None,
		lane: int | None = None,
		poll_interval_s: float = 0.001,
		max_read_bytes: int | None = None,
		stop_event: asyncio.Event | None = None,
	) -> DAQRunResult:
		if not self._armed_autoread:
			raise V3DAQError(
				"run_for_autoread() requires prepare_run(..., autoread=True)."
			)

		lane = self.default_lane if lane is None else int(lane)
		t0 = time.monotonic()
		deadline = None if duration_s is None else (t0 + float(duration_s))
		result = DAQRunResult(
			lanes=[lane],
			t_start=t0,
			t_end=t0,
			chunks=[],
			readout_owner="fpga_autoread",
			stop_reason="unknown",
		)

		while True:
			if stop_event is not None and stop_event.is_set():
				result.stop_reason = "stop_event_set"
				break
			if deadline is not None and time.monotonic() >= deadline:
				result.stop_reason = "duration_reached"
				break

			t_start = time.monotonic()
			size = await self.transport.read_buffer_size()
			if size > 0:
				to_read = size if max_read_bytes is None else min(size, int(max_read_bytes))
				data = await self.transport.read_buffer(to_read)
				t_end = time.monotonic()
				result.chunks.append(DAQChunk(
					lane=lane,
					t_start=t_start,
					t_end=t_end,
					irq_seen=True,
					rounds=1,
					bytes_written_as_dummy=0,
					buffer_sizes=[size],
					data=data,
					truncated=(to_read < size),
					stop_reason="buffer_polled" if to_read >= size else "partial_buffer_read",
				))
				continue

			await asyncio.sleep(max(0.0, float(poll_interval_s)))

		# one final drain after duration/stop-event
		tail_size = await self.transport.read_buffer_size()
		if tail_size > 0:
			t_start = time.monotonic()
			data = await self.transport.read_buffer(tail_size)
			t_end = time.monotonic()
			result.chunks.append(DAQChunk(
				lane=lane,
				t_start=t_start,
				t_end=t_end,
				irq_seen=True,
				rounds=1,
				bytes_written_as_dummy=0,
				buffer_sizes=[tail_size],
				data=data,
				truncated=False,
				stop_reason="final_drain",
			))

		if result.stop_reason == "unknown":
			result.stop_reason = "completed"
		result.t_end = time.monotonic()
		return result

	async def run_for(self, **kwargs) -> DAQRunResult:
		return await self.run_for_manual_irq(**kwargs)
