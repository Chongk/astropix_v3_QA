from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Iterable

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

	@property
	def nbytes(self) -> int:
		return len(self.data)

@dataclass(slots=True)
class DAQRunResult:
	lanes: list[int]
	t_start: float
	t_end: float
	chunks: list[DAQChunk] = field(default_factory=list)

	@property
	def total_bytes(self) -> int:
		return sum(chunk.nbytes for chunk in self.chunks)

	@property
	def total_chunks(self) -> int:
		return len(self.chunks)

class V3DAQ:
	"""
	v3-only DAQ layer

	Scope of this draft
	-------------------
	- Manual IRQ-driven readout
	- Single-lane-first debugging
	- No decoding here; raw bytes only
	- Designed to sit on top of controller + transport

	Recommended first use
	---------------------
	Use with:
	- one lane
	- one chip
	- autoread = False
	- on-chip injection
	"""

	def __init__(
		self,
		controller: V3Controller,
		*,
		default_lane: int = 0,
	) -> None:
		self.controller = controller
		self.transport: V3Transport = controller.transport
		self.default_lane = int(default_lane)

	# ------------------------------------------------------------------
	# bring-up / setup helpers
	# ------------------------------------------------------------------

	async def prepare_run(
		self,
		*,
		lanes: Iterable[int] | None = None,
		reset_delay_s: float = 0.5,
		first_chip_id: int = 0,
		mirror_legacy: bool = True,
		msbfirst: bool = False,
		flush_burst_bytes: int = 128,
		flush_max_rounds: int = 20,
		autoread: bool = False,
	) -> dict[int, int]:
		"""
		Safe run preparation sequence:
		1. reset + program
		2. flush stale chip/periphery data
		3. arm readout

		For first hardware debug, use autoread=False.
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

		await self.controller.arm_readout(
			lanes=lanes,
			autoread=autoread,
		)

		return results

	async def finish_run(self) -> None:
		await self.controller.disarm_readout()

	# ------------------------------------------------------------------
	# basic IRQ helpers
	# ------------------------------------------------------------------

	async def wait_for_irq(
		self,
		lane: int | None = None,
		*,
		timeout_s: float | None = None,
		poll_interval_s: float = 0.0005,
	) -> bool:
		"""
		Wait until interrupt is asserted (active-low in current A-STEP status usage)
		Returns True if IRQ seen, False on timeout
		"""
		lane = self.default_lane if lane is None else int(lane)
		t0 = time.monotonic()

		while True:
			if await self.transport.interrupt_asserted(lane):
				return True

			if timeout_s is not None and (time.monotonic() - t0) >= timeout_s:
				return False

			await asyncio.sleep(poll_interval_s)

	# ------------------------------------------------------------------
	# manual IRQ burst acquisition
	# ------------------------------------------------------------------

	async def acquire_irq_burst(
		self,
		lane: int | None = None,
		*,
		dummy_chunk_bytes: int = 32,
		trailing_idle_rounds: int = 2,
		max_rounds: int = 512,
		read_buffer_each_round: bool = True,
		deselect_after: bool = True,
	) -> DAQChunk:
		"""
		Acquire one IRQ burst with manual dummy clocks.

		State machine:
		- if IRQ not asserted: return empty chunk
		- assert CS
		- while IRQ is asserted:
			send dummy clocks
			read FPGA buffer
		- after IRQ deasserts:
			keep sending a few extra dummy chunks ("tail drain")
			stop once enough consecutive empty rounds are seen
		- deassert CS

		Notes
		-----
		This follows the *intent* of the old flush/readout flow,
		but keeps the run-time acquisition state machine explicitly
		"""
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
			)

		rounds = 0
		idle_rounds = 0
		bytes_written_as_dummy = 0
		buffer_sizes: list[int] = []
		payload = bytearray()

		await self.transport.spi_select(flush=True)
		try:
			while rounds < max_rounds:
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
					# If not draining every round, still use IRQ state to decide when to leave
					if not await self.transport.interrupt_asserted(lane):
						idle_rounds += 1

				irq_now = await self.transport.interrupt_asserted(lane)

				# Core exit condition: IRQ already low, and we have drained a small tail with no new bytes
				if (not irq_now) and (idle_rounds >= trailing_idle_rounds):
					break

			# Final drain to catch any remaining FPGA-side bytes
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
		)

	# ------------------------------------------------------------------
	# polling loop for a timed run
	# ------------------------------------------------------------------

	async def run_for(
		self,
		*,
		duration_s: float,
		lane: int | None = None,
		wait_irq_timeout_s: float = 0.01,
		wait_poll_interval_s: float = 0.0005,
		dummy_chunk_bytes: int = 32,
		trailing_idle_rounds: int = 2,
		max_rounds_per_burst: int = 512,
		read_buffer_each_round: bool = True,
		stop_on_empty_burst: bool = False,
	) -> DAQRunResult:
		"""
		Timed acquisition loop
		This does not perform setup/programming by itself: call prepare_run() first
		"""
		lane = self.default_lane if lane is None else int(lane)

		t0 = time.monotonic()
		deadline = t0 + float(duration_s)
		result = DAQRunResult(
			lanes=[lane],
			t_start=t0,
			t_end=t0,
			chunks=[],
		)

		while time.monotonic() < deadline:
			seen = await self.wait_for_irq(
				lane,
				timeout_s=wait_irq_timeout_s,
				poll_interval_s=wait_poll_interval_s,
			)

			if not seen:
				continue

			chunk = await self.acquire_irq_burst(
				lane=lane,
				dummy_chunk_bytes=dummy_chunk_bytes,
				trailing_idle_rounds=trailing_idle_rounds,
				max_rounds=max_rounds_per_burst,
				read_buffer_each_round=read_buffer_each_round,
			)

			result.chunks.append(chunk)

			if stop_on_empty_burst and chunk.nbytes == 0:
				break

		result.t_end = time.monotonic()
		return result

	# ------------------------------------------------------------------
	# convenience helper for the first hardware test
	# ------------------------------------------------------------------

	async def run_single_pixel_injection_test(
		self,
		*,
		lane: int = 0,
		chip: int = 0,
		col: int = 10,
		row: int = 10,
		threshold_offset_mv: float | None = None,
		vinj_mv: float | None = None,
		prepare: bool = True,
		duration_s: float = 1.0,
		autoread: bool = False,
		start_injection: bool = True,
		stop_injection_after: bool = True,
	) -> DAQRunResult:
		"""
		First recommended end-to-end smoke test

		Sequence:
		- configure one pixel + row/col injection path
		- optionally prepare/program/flush/arm
		- optionally start injection
		- run manual IRQ DAQ
		- optionally stop injection
		"""
		self.controller.configure_single_pixel_injection(
			lane=lane,
			chip=chip,
			col=col,
			row=row,
			threshold_offset_mv=threshold_offset_mv,
			vinj_mv=vinj_mv,
			reset_first=True,
			mirror_legacy=True,
		)

		if prepare:
			await self.prepare_run(
				lanes=[lane],
				autoread=autoread,
			)

		if start_injection:
			await self.controller.start_injection()

		try:
			result = await self.run_for(
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
			if stop_injection_after:
				await self.controller.stop_injection()
			await self.finish_run()

		return result
