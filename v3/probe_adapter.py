"""
Probe-adapter board control layer for AstroPix electrical contact tests.

This module is a Python port of the essential register-access and helpers from the original (`test_APIX.c`).
It is intentionally kept separate from the AstroPix chip-control/readout stack:
the adapter board is used for probe-card contact, power-switch, current-monitor and HV-control operations,
not for normal AstroPix slow-control/readout.

Register protocol copied from the C runner
------------------------------------------
TCP port: 5000
Write:	[0x01, addr, data0, data1, data2, data3] with little-endian data then receive one acknowledgement byte.
Read:	 [0x02, addr] then receive four little-endian data bytes.

Contact-test policy used here
-----------------------------
- contact_status bit value 1 means successful contact.
- all 29 contact bits are required by default.
- during contact test, only the TEST switch may be ON; AVSS/AVDD/DVDD must be OFF.
- after contact test, chip-test stages may enable AVSS/AVDD/DVDD;
  the TEST switch state is not part of the chip-power pass/fail criterion.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable
import argparse
import json
import socket
import time

class ProbeAdapterError(RuntimeError):
	"""Base exception for probe-adapter communication/control failures."""

class ProbeAdapterProtocolError(ProbeAdapterError):
	"""Raised when the adapter TCP/register protocol behaves unexpectedly."""

# -----------------------------------------------------------------------------
# Register map
# -----------------------------------------------------------------------------

REG_SWITCH          = 0x00 # w4/r4: AVSS, AVDD, DVDD, TEST
REG_CONTACT_STATUS  = 0x01 # r29
REG_MEASURE_AVSS    = 0x02 # w0/r12: trigger measurement / read AVSS current raw
REG_AVDD_POWER      = 0x03 # r12
REG_DVDD_POWER      = 0x04 # r12
REG_HV_DAC          = 0x05 # w12
REG_HV_ADC_SET_HIGH = 0x06 # w28/r24
REG_HV_START_LOW    = 0x07 # w2/r24
REG_ECHO            = 0x0F # w32/r32

CONTACT_SIGNALS: tuple[str, ...] = (
	"DAC_THPMOS",
	"DAC_BLPIX",
	"DAC_VCASC2",
	"DAC_THPIX",
	"DAC_VMINUSPIX",
	"INJ",
	"TIMESTAMP_CLK",
	"RES_N",
	"HOLD",
	"DIGINJ",
	"INTERRUPT",
	"SAMPLE_CLOCK_P",
	"SAMPLE_CLOCK_N",
	"SR_CK1",
	"SR_CK2",
	"SR_LOAD",
	"SR_RB",
	"SR_SIN",
	"SR_SOUT",
	"SPI_LEFT_CSN",
	"SPI_LEFT_CLK",
	"SPI_LEFT_MOSI",
	"SPI_LEFT_MISO0",
	"SPI_LEFT_MISO1",
	"SPI_RIGHT_CSN",
	"SPI_RIGHT_CLK",
	"SPI_RIGHT_MOSI",
	"SPI_RIGHT_MISO0",
	"SPI_RIGHT_MISO1",
)

CONTACT_NBITS = len(CONTACT_SIGNALS)
CONTACT_REQUIRED_MASK_ALL = (1 << CONTACT_NBITS) - 1
SWITCH_MASK = 0xF

# -----------------------------------------------------------------------------
# Data containers
# -----------------------------------------------------------------------------

@dataclass(slots=True)
class AdapterSwitchState:
	avss: bool = False
	avdd: bool = False
	dvdd: bool = False
	test: bool = False

	@classmethod
	def from_word(cls, word: int) -> "AdapterSwitchState":
		word = int(word) & SWITCH_MASK
		return cls(
			avss=bool(word & 0x1),
			avdd=bool(word & 0x2),
			dvdd=bool(word & 0x4),
			test=bool(word & 0x8),
		)

	def to_word(self) -> int:
		word = 0
		if self.avss:
			word |= 0x1
		if self.avdd:
			word |= 0x2
		if self.dvdd:
			word |= 0x4
		if self.test:
			word |= 0x8
		return word

	def as_dict(self) -> dict[str, bool]:
		return asdict(self)

@dataclass(slots=True)
class LinearCalibration:
	"""Simple y = slope * raw + offset calibration placeholder."""

	slope: float = 1.0
	offset: float = 0.0
	unit: str = "raw"

	def apply(self, raw: int | float) -> float:
		return self.slope * float(raw) + self.offset

@dataclass(slots=True)
class PowerReadback:
	avss_raw: int
	avdd_raw: int
	dvdd_raw: int
	avss: float | None = None
	avdd: float | None = None
	dvdd: float | None = None
	unit: str = "raw"

	def as_dict(self) -> dict[str, Any]:
		return asdict(self)

@dataclass(slots=True)
class HVReadback:
	hv_high_raw: int
	hv_low_raw: int
	hv_current_raw: int
	hv_high: float | None = None
	hv_low: float | None = None
	hv_current: float | None = None
	unit_hv: str = "raw"
	unit_current: str = "raw"

	def as_dict(self) -> dict[str, Any]:
		return asdict(self)

@dataclass(slots=True)
class BiasDacCalibration:
	"""
	Convert requested negative bias voltage to the adapter HV DAC code.

	The adapter firmware exposes a 12-bit DAC code in REG_HV_DAC.  The default
	calibration is now based on the measured adapter-board DAC--HV points:

		DAC : |HV| [V]
		   0 :   0.0
		 500 :  77.2
		1000 : 142.7
		1500 : 208.5
		2000 : 274.3
		2500 : 340.0
		3000 : 407.1
		3500 : 473.9
		4000 : 531.0

	For the normal I-V scan range (-150 .. -200 V), the measured-table mode
	uses piecewise-linear interpolation between the measured points.  This is
	safer than using the older placeholder 0..4095 -> 0..-500 V full-scale
	approximation.

	Optional modes:
	  - "measured_table" : piecewise-linear interpolation of the table above.
	  - "inverse_fit"	: DAC = slope_dac_per_v * |Vbias| + offset_code.
	  - "fullscale"	  : old placeholder full-scale mapping.
	"""

	negative_fullscale_v: float = -500.0
	code_at_zero_v: int = 0
	code_at_negative_fullscale: int = 4095
	label: str = "measured_adapter_hv_2026_piecewise"
	mode: str = "measured_table"
	measured_points: tuple[tuple[int, float], ...] = (
		(0, 0.0),
		(500, 77.2),
		(1000, 142.7),
		(1500, 208.5),
		(2000, 274.3),
		(2500, 340.0),
		(3000, 407.1),
		(3500, 473.9),
		(4000, 531.0),
	)
	# Least-squares fit of DAC as a function of measured |HV| [V].
	# The user-supplied expression was written as "bias = 7.54434*DAC - 56.6773";
	# the measured table shows that the physically meaningful orientation is
	# instead approximately DAC = 7.54434*|bias[V]| - 57.68.
	slope_dac_per_v: float = 7.54434174
	offset_code: float = -57.67729574

	def _validate_requested_bias(self, bias_v: float) -> float:
		bias_v = float(bias_v)
		if bias_v > 0:
			raise ProbeAdapterError(f"Positive HV bias is not allowed: {bias_v} V")
		return abs(bias_v)

	def _sorted_points_by_voltage(self) -> list[tuple[float, int]]:
		points = [(float(vmag), int(code)) for code, vmag in self.measured_points]
		points.sort(key=lambda x: x[0])
		if not points or points[0][0] != 0.0:
			raise ProbeAdapterError("Measured HV calibration must include the 0 V point")
		for (v0, _), (v1, _) in zip(points, points[1:]):
			if v1 <= v0:
				raise ProbeAdapterError("Measured HV calibration voltages must be strictly increasing")
		return points

	def _sorted_points_by_code(self) -> list[tuple[int, float]]:
		points = [(int(code), float(vmag)) for code, vmag in self.measured_points]
		points.sort(key=lambda x: x[0])
		if not points or points[0][0] != 0:
			raise ProbeAdapterError("Measured HV calibration must include the DAC=0 point")
		for (c0, _), (c1, _) in zip(points, points[1:]):
			if c1 <= c0:
				raise ProbeAdapterError("Measured HV calibration DAC codes must be strictly increasing")
		return points

	@staticmethod
	def _interp(x: float, x0: float, y0: float, x1: float, y1: float) -> float:
		if x1 == x0:
			raise ProbeAdapterError("Invalid interpolation interval with zero span")
		return y0 + (x - x0) * (y1 - y0) / (x1 - x0)

	def _voltage_to_code_measured_table(self, vmag: float) -> int:
		points = self._sorted_points_by_voltage()
		if abs(vmag) < 1e-12:
			return 0
		if vmag < points[0][0] or vmag > points[-1][0]:
			raise ProbeAdapterError(
				f"Requested |bias|={vmag} V is outside measured HV calibration range "
				f"{points[0][0]}..{points[-1][0]} V"
			)
		for (v0, c0), (v1, c1) in zip(points, points[1:]):
			if v0 <= vmag <= v1:
				return max(0, min(4095, int(round(self._interp(vmag, v0, c0, v1, c1)))))
		raise ProbeAdapterError("Failed to interpolate measured HV calibration")

	def _code_to_voltage_measured_table(self, code: int) -> float:
		code = max(0, min(4095, int(code)))
		points = self._sorted_points_by_code()
		if code == 0:
			return 0.0
		if code < points[0][0] or code > points[-1][0]:
			raise ProbeAdapterError(
				f"DAC code {code} is outside measured HV calibration range "
				f"{points[0][0]}..{points[-1][0]}"
			)
		for (c0, v0), (c1, v1) in zip(points, points[1:]):
			if c0 <= code <= c1:
				return self._interp(code, c0, v0, c1, v1)
		raise ProbeAdapterError("Failed to interpolate measured HV calibration")

	def _voltage_to_code_inverse_fit(self, vmag: float) -> int:
		if abs(vmag) < 1e-12:
			return 0
		code = int(round(self.slope_dac_per_v * vmag + self.offset_code))
		return max(0, min(4095, code))

	def _code_to_voltage_inverse_fit(self, code: int) -> float:
		code = max(0, min(4095, int(code)))
		if code == 0:
			return 0.0
		if self.slope_dac_per_v == 0:
			raise ProbeAdapterError("Invalid inverse-fit HV calibration: zero slope")
		return (code - self.offset_code) / self.slope_dac_per_v

	def _voltage_to_code_fullscale(self, bias_v: float) -> int:
		if self.negative_fullscale_v >= 0:
			raise ProbeAdapterError(
				f"negative_fullscale_v must be negative, got {self.negative_fullscale_v}"
			)
		if bias_v < self.negative_fullscale_v:
			raise ProbeAdapterError(
				f"Requested bias {bias_v} V is beyond configured full scale "
				f"{self.negative_fullscale_v} V"
			)
		span_code = int(self.code_at_negative_fullscale) - int(self.code_at_zero_v)
		if span_code == 0:
			raise ProbeAdapterError("Invalid HV DAC calibration: zero code span")
		fraction = abs(bias_v) / abs(float(self.negative_fullscale_v))
		code = int(round(int(self.code_at_zero_v) + fraction * span_code))
		return max(0, min(4095, code))

	def _code_to_voltage_fullscale(self, code: int) -> float:
		code = max(0, min(4095, int(code)))
		span_code = int(self.code_at_negative_fullscale) - int(self.code_at_zero_v)
		if span_code == 0:
			raise ProbeAdapterError("Invalid HV DAC calibration: zero code span")
		fraction = (code - int(self.code_at_zero_v)) / span_code
		return abs(float(self.negative_fullscale_v)) * fraction

	def voltage_to_code(self, bias_v: float) -> int:
		bias_v = float(bias_v)
		vmag = self._validate_requested_bias(bias_v)
		mode = str(self.mode).lower()
		if mode == "measured_table":
			return self._voltage_to_code_measured_table(vmag)
		if mode == "inverse_fit":
			return self._voltage_to_code_inverse_fit(vmag)
		if mode == "fullscale":
			return self._voltage_to_code_fullscale(bias_v)
		raise ProbeAdapterError(
			f"Unsupported HV DAC calibration mode {self.mode!r}; "
			"expected 'measured_table', 'inverse_fit', or 'fullscale'"
		)

	def code_to_voltage(self, code: int) -> float:
		"""Return the estimated signed bias voltage for a DAC code."""
		mode = str(self.mode).lower()
		if mode == "measured_table":
			return -self._code_to_voltage_measured_table(code)
		if mode == "inverse_fit":
			return -self._code_to_voltage_inverse_fit(code)
		if mode == "fullscale":
			return -self._code_to_voltage_fullscale(code)
		raise ProbeAdapterError(
			f"Unsupported HV DAC calibration mode {self.mode!r}; "
			"expected 'measured_table', 'inverse_fit', or 'fullscale'"
		)

	def as_dict(self) -> dict[str, Any]:
		return asdict(self)

@dataclass(slots=True)
class IVScanPoint:
	requested_bias_v: float
	hv_code: int
	estimated_bias_v: float
	t_set: float
	settle_s: float
	hv_high_raw: int
	hv_low_raw: int
	hv_current_raw: int
	hv_high: float | None = None
	hv_low: float | None = None
	hv_current: float | None = None
	hv_unit: str = "raw"
	current_unit: str = "raw"

	def as_dict(self) -> dict[str, Any]:
		return asdict(self)

@dataclass(slots=True)
class AdapterQACheckResult:
	"""
	Same loose shape as v3.qa.QACheckResult so the existing runner's
	summarize_check_result()/materialize_artifacts() can consume it.
	"""

	name: str
	passed: bool | None
	metrics: dict[str, Any] = field(default_factory=dict)
	notes: list[str] = field(default_factory=list)
	artifacts: dict[str, Any] = field(default_factory=dict)

# -----------------------------------------------------------------------------
# Low-level TCP/register client
# -----------------------------------------------------------------------------

class ProbeAdapterClient:
	def __init__(
		self,
		ip: str,
		*,
		port: int = 5000,
		timeout_s: float = 2.0,
		tcp_nodelay: bool = True,
	) -> None:
		self.ip = str(ip)
		self.port = int(port)
		self.timeout_s = float(timeout_s)
		self.tcp_nodelay = bool(tcp_nodelay)
		self._sock: socket.socket | None = None

	@property
	def is_open(self) -> bool:
		return self._sock is not None

	def connect(self) -> None:
		if self._sock is not None:
			return
		try:
			sock = socket.create_connection((self.ip, self.port), timeout=self.timeout_s)
			sock.settimeout(self.timeout_s)
			if self.tcp_nodelay:
				sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
			self._sock = sock
		except OSError as exc:
			raise ProbeAdapterError(
				f"Could not connect to probe adapter at {self.ip}:{self.port}"
			) from exc

	def close(self) -> None:
		sock = self._sock
		self._sock = None
		if sock is not None:
			try:
				sock.close()
			except OSError:
				pass

	def __enter__(self) -> "ProbeAdapterClient":
		self.connect()
		return self

	def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
		self.close()

	def _require_sock(self) -> socket.socket:
		if self._sock is None:
			raise ProbeAdapterError("Probe adapter TCP connection is not open")
		return self._sock

	def _send_all(self, payload: bytes) -> None:
		try:
			self._require_sock().sendall(payload)
		except OSError as exc:
			raise ProbeAdapterError("Failed while sending data to probe adapter") from exc

	def _recv_exact(self, nbytes: int) -> bytes:
		sock = self._require_sock()
		chunks = bytearray()
		while len(chunks) < nbytes:
			try:
				chunk = sock.recv(nbytes - len(chunks))
			except socket.timeout as exc:
				raise ProbeAdapterError(
					f"Timed out while reading {nbytes} bytes from probe adapter"
				) from exc
			except OSError as exc:
				raise ProbeAdapterError("Failed while reading from probe adapter") from exc
			if not chunk:
				raise ProbeAdapterProtocolError(
					"Probe adapter TCP connection closed before expected data were received"
				)
			chunks.extend(chunk)
		return bytes(chunks)

	def write_reg(self, addr: int, data: int) -> int:
		"""
		Write a 32-bit little-endian word to an adapter register.
		Returns the one-byte acknowledgement value from the board.
		"""
		addr = int(addr) & 0xFF
		data = int(data) & 0xFFFFFFFF
		payload = bytes([0x01, addr]) + data.to_bytes(4, byteorder="little", signed=False)
		self._send_all(payload)
		ack = self._recv_exact(1)[0]
		return ack

	def read_reg(self, addr: int) -> int:
		"""Read a 32-bit little-endian word from an adapter register."""
		addr = int(addr) & 0xFF
		self._send_all(bytes([0x02, addr]))
		data = self._recv_exact(4)
		return int.from_bytes(data, byteorder="little", signed=False)

	# ------------------------------------------------------------------
	# High-level helpers corresponding to the original C runner
	# ------------------------------------------------------------------

	def write_echo(self, value: int) -> int:
		return self.write_reg(REG_ECHO, int(value))

	def read_echo(self) -> int:
		return self.read_reg(REG_ECHO)

	def echo_roundtrip(self, value: int = 0xA5A55A5A) -> tuple[int, int, bool]:
		value = int(value) & 0xFFFFFFFF
		self.write_echo(value)
		readback = self.read_echo()
		return value, readback, value == readback

	def set_switch_state(self, state: AdapterSwitchState) -> int:
		return self.write_reg(REG_SWITCH, state.to_word())

	def set_switches(
		self,
		*,
		avss: bool | int = False,
		avdd: bool | int = False,
		dvdd: bool | int = False,
		test: bool | int = False,
	) -> int:
		return self.set_switch_state(
			AdapterSwitchState(bool(avss), bool(avdd), bool(dvdd), bool(test))
		)

	def read_switch_state(self) -> AdapterSwitchState:
		return AdapterSwitchState.from_word(self.read_reg(REG_SWITCH))

	def configure_contact_test_switches(self) -> AdapterSwitchState:
		"""Force the required contact-test state: only TEST ON."""
		self.set_switches(avss=False, avdd=False, dvdd=False, test=True)
		return self.read_switch_state()

	def configure_chip_power_switches(
		self,
		*,
		avss: bool | int = True,
		avdd: bool | int = True,
		dvdd: bool | int = True,
		test: bool | int | None = None,
	) -> AdapterSwitchState:
		"""
		Enable/disable chip power rails.  If test=None, preserve the current
		TEST switch state, because TEST is not a pass/fail criterion during
		later chip tests.
		"""
		current = self.read_switch_state()
		next_state = AdapterSwitchState(
			avss=bool(avss),
			avdd=bool(avdd),
			dvdd=bool(dvdd),
			test=current.test if test is None else bool(test),
		)
		self.set_switch_state(next_state)
		return self.read_switch_state()

	def read_contact_raw(self) -> int:
		return self.read_reg(REG_CONTACT_STATUS) & CONTACT_REQUIRED_MASK_ALL

	@staticmethod
	def decode_contact(raw: int) -> dict[str, int]:
		raw = int(raw)
		return {
			signal: (raw >> bit) & 0x1
			for bit, signal in enumerate(CONTACT_SIGNALS)
		}

	@staticmethod
	def missing_contact_signals(
		raw: int,
		*,
		required_mask: int = CONTACT_REQUIRED_MASK_ALL,
		good_value: int = 1,
	) -> list[str]:
		raw = int(raw)
		required_mask = int(required_mask) & CONTACT_REQUIRED_MASK_ALL
		good_value = 1 if int(good_value) else 0
		missing: list[str] = []
		for bit, signal in enumerate(CONTACT_SIGNALS):
			if not (required_mask & (1 << bit)):
				continue
			value = (raw >> bit) & 0x1
			if value != good_value:
				missing.append(signal)
		return missing

	def read_power_raw(self, *, settle_s: float = 0.001) -> PowerReadback:
		# Original C code writes 0 to 0x02 to trigger measurement, waits 1 ms,
		# then reads 0x02/0x03/0x04.
		self.write_reg(REG_MEASURE_AVSS, 0)
		if settle_s > 0:
			time.sleep(float(settle_s))
		return PowerReadback(
			avss_raw=self.read_reg(REG_MEASURE_AVSS) & 0xFFF,
			avdd_raw=self.read_reg(REG_AVDD_POWER) & 0xFFF,
			dvdd_raw=self.read_reg(REG_DVDD_POWER) & 0xFFF,
		)

	def read_power(
		self,
		*,
		avss_cal: LinearCalibration | None = None,
		avdd_cal: LinearCalibration | None = None,
		dvdd_cal: LinearCalibration | None = None,
		settle_s: float = 0.001,
	) -> PowerReadback:
		rb = self.read_power_raw(settle_s=settle_s)
		if avss_cal is not None:
			rb.avss = avss_cal.apply(rb.avss_raw)
			rb.unit = avss_cal.unit
		if avdd_cal is not None:
			rb.avdd = avdd_cal.apply(rb.avdd_raw)
			rb.unit = avdd_cal.unit
		if dvdd_cal is not None:
			rb.dvdd = dvdd_cal.apply(rb.dvdd_raw)
			rb.unit = dvdd_cal.unit
		return rb

	def write_hv_code(self, code: int | float) -> int:
		code_i = int(code)
		code_i = max(0, min(4095, code_i))
		return self.write_reg(REG_HV_DAC, code_i)

	def init_hv_adc(self, *, start: bool = True) -> None:
		"""
		Non-interactive version of APIX_init_HV_ADC().  Not used by the contact
		test itself, but provided for later HV readback/power-sanity stages.
		"""
		for addr, data in (
			(0x1, 0xB2),
			(0x2, 0x30),
			(0x3, 0x89),
			(0x4, 0xA0),
			(0x5, 0x72),
		):
			self.set_hv_adc(addr, data)
		if start:
			self.write_reg(REG_HV_START_LOW, 1)

	def set_hv_adc(self, addr: int, data: int) -> int:
		word = ((int(addr) & 0xF) << 24) | (int(data) & 0xFFFFFF)
		return self.write_reg(REG_HV_ADC_SET_HIGH, word)

	def read_hv_raw(self) -> HVReadback:
		# Original C code writes 3 to 0x07, then reads high from 0x06 and low
		# from 0x07.  hv_current_raw is high-low until calibration is known.
		self.write_reg(REG_HV_START_LOW, 3)
		high = self.read_reg(REG_HV_ADC_SET_HIGH) & 0xFFFFFF
		low = self.read_reg(REG_HV_START_LOW) & 0xFFFFFF
		return HVReadback(
			hv_high_raw=high,
			hv_low_raw=low,
			hv_current_raw=high - low,
		)

	def read_hv(
		self,
		*,
		hv_high_cal: LinearCalibration | None = None,
		hv_low_cal: LinearCalibration | None = None,
		hv_current_cal: LinearCalibration | None = None,
	) -> HVReadback:
		"""
		Read HV monitor values and optionally apply calibration.

		Until the resistor/current calibration is finalized, the raw values from
		read_hv_raw() are the source of truth.  Calibration is intentionally
		optional and kept outside the register access layer.
		"""
		rb = self.read_hv_raw()
		if hv_high_cal is not None:
			rb.hv_high = hv_high_cal.apply(rb.hv_high_raw)
			rb.unit_hv = hv_high_cal.unit
		if hv_low_cal is not None:
			rb.hv_low = hv_low_cal.apply(rb.hv_low_raw)
			rb.unit_hv = hv_low_cal.unit
		if hv_current_cal is not None:
			rb.hv_current = hv_current_cal.apply(rb.hv_current_raw)
			rb.unit_current = hv_current_cal.unit
		return rb

	def set_bias_voltage(
		self,
		bias_v: float,
		*,
		calibration: BiasDacCalibration | None = None,
	) -> int:
		"""Set the negative HV bias using a voltage-to-DAC calibration.

		Returns the DAC code written to REG_HV_DAC.  This method only performs
		the DAC write.  Use set_bias_voltage_and_wait() or the QA-level I-V/fixed
		bias stages when the hardware needs settling time before readback or
		subsequent chip QA.
		"""
		cal = calibration or BiasDacCalibration()
		code = cal.voltage_to_code(float(bias_v))
		self.write_hv_code(code)
		return code

	def set_bias_voltage_and_wait(
		self,
		bias_v: float,
		*,
		calibration: BiasDacCalibration | None = None,
		settle_s: float = 2.0,
		read_hv_after: bool = True,
	) -> tuple[int, HVReadback | None]:
		"""Set a fixed negative bias, wait for settling, and optionally read HV monitor.

		The default 2 s delay is intentionally conservative for early bring-up.
		It should be shortened only after the adapter HV readback is observed to
		settle reproducibly.
		"""
		code = self.set_bias_voltage(bias_v, calibration=calibration)
		if settle_s > 0:
			time.sleep(float(settle_s))
		rb = self.read_hv() if read_hv_after else None
		return code, rb

	def set_bias_zero(self) -> int:
		"""Convenience helper to command 0 V / DAC code 0."""
		return self.write_hv_code(0)


# -----------------------------------------------------------------------------
# QA-level helpers
# -----------------------------------------------------------------------------

class ProbeAdapterQA:
	def __init__(self, client: ProbeAdapterClient) -> None:
		self.client = client

	def echo_test(self, *, value: int = 0xA5A55A5A) -> AdapterQACheckResult:
		notes: list[str] = []
		written, readback, passed = self.client.echo_roundtrip(value)
		if not passed:
			notes.append(
				f"Echo mismatch: wrote 0x{written:08X}, read back 0x{readback:08X}."
			)
		return AdapterQACheckResult(
			name="adapter_echo_test",
			passed=passed,
			metrics={
				"written_hex": f"0x{written:08X}",
				"readback_hex": f"0x{readback:08X}",
			},
			notes=notes,
		)

	def contact_test(
		self,
		*,
		required_mask: int = CONTACT_REQUIRED_MASK_ALL,
		settle_s: float = 0.01,
		cleanup_test_switch: bool = True,
		read_power_monitor: bool = False,
	) -> AdapterQACheckResult:
		notes: list[str] = []
		artifacts: dict[str, Any] = {}
		metrics: dict[str, Any] = {}

		initial_switch = self.client.read_switch_state()
		metrics["initial_switch"] = initial_switch.as_dict()

		switch_after = self.client.configure_contact_test_switches()
		metrics["contact_test_switch"] = switch_after.as_dict()
		switch_ok = switch_after == AdapterSwitchState(
			avss=False,
			avdd=False,
			dvdd=False,
			test=True,
		)
		metrics["switch_state_ok"] = switch_ok
		if not switch_ok:
			notes.append(
				"Adapter switch readback is not the required contact-test state "
				"(AVSS=OFF, AVDD=OFF, DVDD=OFF, TEST=ON)."
			)

		if settle_s > 0:
			time.sleep(float(settle_s))

		raw = self.client.read_contact_raw()
		decoded = self.client.decode_contact(raw)
		missing = self.client.missing_contact_signals(
			raw,
			required_mask=required_mask,
			good_value=1,
		)
		required_mask = int(required_mask) & CONTACT_REQUIRED_MASK_ALL
		required_count = required_mask.bit_count()
		good_required_count = required_count - len(missing)
		contact_ok = len(missing) == 0

		metrics.update({
			"contact_raw_dec": raw,
			"contact_raw_hex": f"0x{raw:0{(CONTACT_NBITS + 3) // 4}X}",
			"required_mask_hex": f"0x{required_mask:0{(CONTACT_NBITS + 3) // 4}X}",
			"contact_good_polarity": 1,
			"n_required_contacts": required_count,
			"n_good_required_contacts": good_required_count,
			"n_missing_contacts": len(missing),
			"missing_contacts": missing,
			"contact_ok": contact_ok,
		})
		artifacts["contact_bits"] = {
			signal: {
				"bit": bit,
				"value": decoded[signal],
				"required": bool(required_mask & (1 << bit)),
				"passed": (decoded[signal] == 1) if (required_mask & (1 << bit)) else None,
			}
			for bit, signal in enumerate(CONTACT_SIGNALS)
		}

		if missing:
			notes.append(
				"Missing or failed required contacts: " + ", ".join(missing)
			)

		if read_power_monitor:
			# Current calibration is still being tuned, so this is stored only
			# as raw monitoring information and is never part of pass/fail here.
			artifacts["power_monitor_raw"] = self.client.read_power_raw().as_dict()

		cleanup_switch: AdapterSwitchState | None = None
		if cleanup_test_switch:
			try:
				self.client.set_switches(avss=False, avdd=False, dvdd=False, test=False)
				cleanup_switch = self.client.read_switch_state()
				metrics["cleanup_switch"] = cleanup_switch.as_dict()
			except ProbeAdapterError as exc:
				notes.append(f"Failed to clean up TEST switch after contact read: {exc!r}")

		return AdapterQACheckResult(
			name="adapter_contact_test",
			passed=bool(switch_ok and contact_ok),
			metrics=metrics,
			notes=notes,
			artifacts=artifacts,
		)

	def enable_chip_power(
		self,
		*,
		avss: bool = True,
		avdd: bool = True,
		dvdd: bool = True,
		test: bool | None = None,
		read_power_monitor: bool = True,
		settle_s: float = 0.05,
	) -> AdapterQACheckResult:
		notes: list[str] = []
		artifacts: dict[str, Any] = {}
		state = self.client.configure_chip_power_switches(
			avss=avss,
			avdd=avdd,
			dvdd=dvdd,
			test=test,
		)
		if settle_s > 0:
			time.sleep(float(settle_s))
		expected_ok = (state.avss == avss) and (state.avdd == avdd) and (state.dvdd == dvdd)
		if not expected_ok:
			notes.append(
				"Chip power switch readback does not match requested AVSS/AVDD/DVDD state."
			)
		if read_power_monitor:
			artifacts["power_monitor_raw"] = self.client.read_power_raw().as_dict()
		return AdapterQACheckResult(
			name="adapter_enable_chip_power",
			passed=expected_ok,
			metrics={
				"requested": {"avss": avss, "avdd": avdd, "dvdd": dvdd, "test": test},
				"switch_readback": state.as_dict(),
				"power_switches_ok": expected_ok,
				"test_switch_ignored_for_pass_fail": test is None,
			},
			notes=notes,
			artifacts=artifacts,
		)

	def iv_scan(
		self,
		*,
		start_v: float = -150.0,
		stop_v: float = -200.0,
		step_v: float = 10.0,
		settle_s: float = 0.2,
		bias_calibration: BiasDacCalibration | None = None,
		hv_high_cal: LinearCalibration | None = None,
		hv_low_cal: LinearCalibration | None = None,
		hv_current_cal: LinearCalibration | None = None,
		current_limit_raw: int | None = None,
		current_limit: float | None = None,
		init_hv_adc: bool = True,
		cleanup_bias: bool = True,
	) -> AdapterQACheckResult:
		"""Run a simple adapter-based I-V scan.

		The scan commands negative bias values through the HV DAC and records the
		HV/current monitor readback at each point.  The result only indicates
		whether the scan completed without tripping the optional current limit;
		it does not decide sensor quality.
		"""
		notes: list[str] = []
		artifacts: dict[str, Any] = {"points": []}
		metrics: dict[str, Any] = {}

		start_v = float(start_v)
		stop_v = float(stop_v)
		step_abs = abs(float(step_v))
		if step_abs <= 0:
			raise ProbeAdapterError("step_v must be non-zero")
		if start_v > 0 or stop_v > 0:
			raise ProbeAdapterError("I-V scan bias points must be non-positive voltages")

		direction = -1.0 if stop_v < start_v else 1.0
		signed_step = direction * step_abs
		cal = bias_calibration or BiasDacCalibration()

		requested_points: list[float] = []
		v = start_v
		# Include the stop point with a small tolerance against floating errors.
		while (v >= stop_v - 1e-9) if direction < 0 else (v <= stop_v + 1e-9):
			requested_points.append(round(v, 9))
			v += signed_step

		metrics.update({
			"scan_type": "adapter_iv_scan",
			"requested_start_v": start_v,
			"requested_stop_v": stop_v,
			"requested_step_v": step_abs,
			"n_requested_points": len(requested_points),
			"settle_s": float(settle_s),
			"bias_dac_calibration": cal.as_dict(),
			"hv_adc_initialized": bool(init_hv_adc),
			"cleanup_bias": bool(cleanup_bias),
			"current_limit_raw": current_limit_raw,
			"current_limit": current_limit,
			"physics_pass_criteria_defined": False,
		})

		tripped = False
		trip_reason: str | None = None

		try:
			if init_hv_adc:
				self.client.init_hv_adc(start=True)

			for requested_v in requested_points:
				code = self.client.set_bias_voltage(requested_v, calibration=cal)
				t_set = time.time()
				if settle_s > 0:
					time.sleep(float(settle_s))

				rb = self.client.read_hv(
					hv_high_cal=hv_high_cal,
					hv_low_cal=hv_low_cal,
					hv_current_cal=hv_current_cal,
				)
				point = IVScanPoint(
					requested_bias_v=float(requested_v),
					hv_code=int(code),
					estimated_bias_v=cal.code_to_voltage(code),
					t_set=t_set,
					settle_s=float(settle_s),
					hv_high_raw=rb.hv_high_raw,
					hv_low_raw=rb.hv_low_raw,
					hv_current_raw=rb.hv_current_raw,
					hv_high=rb.hv_high,
					hv_low=rb.hv_low,
					hv_current=rb.hv_current,
					hv_unit=rb.unit_hv,
					current_unit=rb.unit_current,
				)
				artifacts["points"].append(point.as_dict())

				if current_limit_raw is not None and abs(rb.hv_current_raw) > abs(int(current_limit_raw)):
					tripped = True
					trip_reason = (
						f"raw current limit exceeded at {requested_v} V: "
						f"abs({rb.hv_current_raw}) > {abs(int(current_limit_raw))}"
					)
					break

				if (
					current_limit is not None
					and hv_current_cal is not None
					and rb.hv_current is not None
					and abs(rb.hv_current) > abs(float(current_limit))
				):
					tripped = True
					trip_reason = (
						f"calibrated current limit exceeded at {requested_v} V: "
						f"abs({rb.hv_current}) > {abs(float(current_limit))} {rb.unit_current}"
					)
					break

		finally:
			if cleanup_bias:
				try:
					self.client.set_bias_zero()
					metrics["cleanup_bias_code"] = 0
				except ProbeAdapterError as exc:
					notes.append(f"Failed to reset HV bias to zero after I-V scan: {exc!r}")

		if tripped:
			notes.append(trip_reason or "I-V scan stopped due to current limit.")

		metrics.update({
			"n_completed_points": len(artifacts["points"]),
			"completed_bias_points_v": [p["requested_bias_v"] for p in artifacts["points"]],
			"completed_hv_codes": [p["hv_code"] for p in artifacts["points"]],
			"completed_current_raw": [p["hv_current_raw"] for p in artifacts["points"]],
			"current_limit_tripped": tripped,
			"trip_reason": trip_reason,
		})

		return AdapterQACheckResult(
			name="adapter_iv_scan",
			passed=bool((not tripped) and len(artifacts["points"]) == len(requested_points)),
			metrics=metrics,
			notes=notes,
			artifacts=artifacts,
		)

	def set_fixed_bias(
		self,
		*,
		bias_v: float,
		settle_s: float = 2.0,
		bias_calibration: BiasDacCalibration | None = None,
		read_hv_after: bool = True,
	) -> AdapterQACheckResult:
		"""Set and leave a fixed negative bias for subsequent chip QA.

		This stage deliberately does not clean up the bias, because normal chip
		QA may need to run at that bias.  Runner-level cleanup should set the
		bias back to zero after the full session unless explicitly disabled.
		"""
		notes: list[str] = []
		artifacts: dict[str, Any] = {}
		metrics: dict[str, Any] = {
			"requested_bias_v": float(bias_v),
			"settle_s": float(settle_s),
			"read_hv_after": bool(read_hv_after),
		}
		cal = bias_calibration or BiasDacCalibration()
		metrics["bias_dac_calibration"] = cal.as_dict()
		try:
			code, rb = self.client.set_bias_voltage_and_wait(
				float(bias_v),
				calibration=cal,
				settle_s=float(settle_s),
				read_hv_after=read_hv_after,
			)
			metrics["hv_code"] = int(code)
			metrics["estimated_bias_v"] = cal.code_to_voltage(code)
			if rb is not None:
				artifacts["hv_readback_raw"] = rb.as_dict()
		except ProbeAdapterError as exc:
			notes.append(f"Failed to set fixed adapter bias: {exc!r}")
			return AdapterQACheckResult(
				name="adapter_set_fixed_bias",
				passed=False,
				metrics=metrics,
				notes=notes,
				artifacts=artifacts,
			)

		return AdapterQACheckResult(
			name="adapter_set_fixed_bias",
			passed=True,
			metrics=metrics,
			notes=notes,
			artifacts=artifacts,
		)

# -----------------------------------------------------------------------------
# Standalone CLI for quick bench checks
# -----------------------------------------------------------------------------

def _write_json(path: str | Path, payload: Any) -> None:
	p = Path(path)
	p.parent.mkdir(parents=True, exist_ok=True)
	with p.open("w", encoding="utf-8") as f:
		json.dump(payload, f, indent=4, sort_keys=True, default=str)

def _result_to_dict(result: AdapterQACheckResult) -> dict[str, Any]:
	return {
		"name": result.name,
		"passed": result.passed,
		"metrics": result.metrics,
		"notes": result.notes,
		"artifacts": result.artifacts,
	}

def build_argparser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(
		description="Standalone probe-adapter contact/power test client",
		formatter_class=argparse.ArgumentDefaultsHelpFormatter,
	)
	parser.add_argument("--ip", required=True, help="Probe-adapter board IP address")
	parser.add_argument("--port", type=int, default=5000)
	parser.add_argument("--timeout-s", type=float, default=2.0)
	parser.add_argument("--echo-word", type=lambda x: int(x, 0), default=0xA5A55A5A)
	parser.add_argument("--contact-test", action="store_true")
	parser.add_argument("--enable-chip-power", action="store_true")
	parser.add_argument("--keep-test-on", action="store_true")
	parser.add_argument("--read-power-monitor", action="store_true")
	parser.add_argument("--iv-scan", action="store_true", help="Run a simple adapter HV I-V scan.")
	parser.add_argument("--iv-start-v", type=float, default=-150.0)
	parser.add_argument("--iv-stop-v", type=float, default=-200.0)
	parser.add_argument("--iv-step-v", type=float, default=10.0)
	parser.add_argument("--iv-settle-s", type=float, default=0.2)
	parser.add_argument("--iv-current-limit-raw", type=int, default=None)
	parser.add_argument("--iv-no-init-hv-adc", action="store_true")
	parser.add_argument("--iv-no-cleanup-bias", action="store_true")
	parser.add_argument("--hv-calibration-mode", choices=["measured_table", "inverse_fit", "fullscale"], default="measured_table")
	parser.add_argument("--hv-negative-fullscale-v", type=float, default=-500.0)
	parser.add_argument("--hv-fullscale-code", type=int, default=4095)
	parser.add_argument("--output-json", type=str, default=None)
	return parser

def main() -> int:
	args = build_argparser().parse_args()
	results: list[dict[str, Any]] = []
	with ProbeAdapterClient(args.ip, port=args.port, timeout_s=args.timeout_s) as client:
		qa = ProbeAdapterQA(client)
		results.append(_result_to_dict(qa.echo_test(value=args.echo_word)))
		if args.contact_test:
			results.append(_result_to_dict(
				qa.contact_test(
					cleanup_test_switch=not args.keep_test_on,
					read_power_monitor=args.read_power_monitor,
				)
			))
		if args.enable_chip_power:
			results.append(_result_to_dict(
				qa.enable_chip_power(
					test=None,
					read_power_monitor=args.read_power_monitor,
				)
			))
		if args.iv_scan:
			bias_cal = BiasDacCalibration(
				mode=args.hv_calibration_mode,
				negative_fullscale_v=args.hv_negative_fullscale_v,
				code_at_negative_fullscale=args.hv_fullscale_code,
			)
			results.append(_result_to_dict(
				qa.iv_scan(
					start_v=args.iv_start_v,
					stop_v=args.iv_stop_v,
					step_v=args.iv_step_v,
					settle_s=args.iv_settle_s,
					bias_calibration=bias_cal,
					current_limit_raw=args.iv_current_limit_raw,
					init_hv_adc=not args.iv_no_init_hv_adc,
					cleanup_bias=not args.iv_no_cleanup_bias,
				)
			))

	payload = {"results": results, "passed": all(r["passed"] is not False for r in results)}
	if args.output_json:
		_write_json(args.output_json, payload)
	else:
		print(json.dumps(payload, indent=4, sort_keys=True))
	return 0 if payload["passed"] else 1

if __name__ == "__main__":
	raise SystemExit(main())
