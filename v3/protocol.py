# - Mar. 24, 2026
# - Chong Kim, ckim.phenix@gmail.com

from __future__ import annotations
from copy import deepcopy
from typing import Any
from bitstring import BitArray

class V3ProtocolError(ValueError):
	pass

class V3Protocol:
	"""
	v3-only protocol layer

	Design goals
	------------
	1. Preserve the currently observed A-STEP bitstream behavior
	2. Make section/field ordering explicit when desired
	3. Keep the SPI framing logic independent from board transport

	Important compatibility note
	----------------------------
	Current A-STEP behavior depends on dictionary insertion order:

		for key in self.asic_config[f"config_{chip}"]:
			for values in self.asic_config[f"config_{chip}"][key].values():
				...

	This class preserves that behavior by default,
	but also lets you override section/field order explicitly once you verify the YAML/spec
	"""

	SPI_SR_BROADCAST = 0x7E
	SPI_SR_BIT0 = 0x00
	SPI_SR_BIT1 = 0x01
	SPI_SR_LOAD = 0x03
	SPI_SR_TDAC_LOAD = 0x06
	SPI_EMPTY_BYTE = 0x00

	SPI_HEADER_EMPTY = 0b001 << 5
	SPI_HEADER_ROUTING = 0b010 << 5
	SPI_HEADER_SR = 0b011 << 5

	def __init__(
			self,
			nchips: int = 1,
			*,
			section_order: list[str] | None = None,
			field_order: dict[str, list[str]] | None = None,
			strict_order: bool = False,
	) -> None:
		self.nchips = int(nchips)
		self.section_order = section_order
		self.field_order = field_order or {}
		self.strict_order = strict_order

	# ------------------------------------------------------------------
	# basic bit helpers
	# ------------------------------------------------------------------

	@staticmethod
	def _int_to_bits(value: int, nbits: int) -> BitArray:
		try:
			return BitArray(uint=int(value), length=int(nbits))
		except ValueError as exc:
			raise V3ProtocolError(f"Value {value} does not fit into {nbits} bits") from exc

	@staticmethod
	def _normalize_field_entry(entry: Any, *, section: str, field: str) -> tuple[int, int]:
		"""
		Expect A-STEP/YAML-style entries as [nbits, value].
		"""
		if not isinstance(entry, list) or len(entry) < 2:
			raise V3ProtocolError(f"Invalid entry for {section}.{field}: expected [nbits, value], got {entry!r}")
		nbits, value = entry[0], entry[1]
		return int(nbits), int(value)

	# ------------------------------------------------------------------
	# ordering helpers
	# ------------------------------------------------------------------

	def _ordered_section_names(self, chip_cfg: dict[str, Any]) -> list[str]:
		keys_in_cfg = list(chip_cfg.keys())

		if self.section_order is None:
			return keys_in_cfg

		ordered: list[str] = []
		for key in self.section_order:
			if key in chip_cfg:
				ordered.append(key)
			elif self.strict_order:
				raise V3ProtocolError(f"Missing expected section: {key}")

		for key in keys_in_cfg:
			if key not in ordered:
				if self.strict_order:
					raise V3ProtocolError(f"Unexpected section outside declared order: {key}")
				ordered.append(key)

		return ordered

	def _ordered_field_names(self, section_name: str, section_cfg: dict[str, Any]) -> list[str]:
		keys_in_cfg = list(section_cfg.keys())
		forced = self.field_order.get(section_name)

		if forced is None:
			return keys_in_cfg

		ordered: list[str] = []
		for key in forced:
			if key in section_cfg:
				ordered.append(key)
			elif self.strict_order:
				raise V3ProtocolError(f"Missing expected field: {section_name}.{key}")

		for key in keys_in_cfg:
			if key not in ordered:
				if self.strict_order:
					raise V3ProtocolError(f"Unexpected field outside declared order: {section_name}.{key}")
				ordered.append(key)

		return ordered

	# ------------------------------------------------------------------
	# config extraction
	# ------------------------------------------------------------------

	def _extract_configs(self, config_source: Any) -> dict[str, dict[str, Any]]:
		"""
		Accept either:
		  - V3Config-like object with export_all()
		  - raw dict shaped like {"config_0": {...}, "config_1": {...}}
		"""
		if hasattr(config_source, "export_all"):
			cfgs = config_source.export_all()
		elif isinstance(config_source, dict):
			cfgs = deepcopy(config_source)
		else:
			raise V3ProtocolError("config_source must be a config dict or an object with export_all()")

		config_keys = [k for k in cfgs.keys() if k.startswith("config_")]
		if not config_keys:
			raise V3ProtocolError("No config_N entries found in config source")

		return cfgs

	# ------------------------------------------------------------------
	# bitstream builders
	# ------------------------------------------------------------------

	def build_chip_bits(
		self,
		chip_cfg: dict[str, Any],
		*,
		msbfirst: bool = False,
	) -> BitArray:
		"""
		Reproduce current A-STEP per-chip serialization logic

		A-STEP behavior:
		  - walk sections in dict order
		  - walk fields in dict order
		  - reverse each VDAC field before append
		  - reverse whole chip bitvector if msbfirst=False
		"""
		chip_bits = BitArray()

		for section_name in self._ordered_section_names(chip_cfg):
			section_cfg = chip_cfg[section_name]
			if not isinstance(section_cfg, dict):
				raise V3ProtocolError(f"Section {section_name} must be a dict, got {type(section_cfg)}")

			for field_name in self._ordered_field_names(section_name, section_cfg):
				nbits, value = self._normalize_field_entry(
					section_cfg[field_name],
					section=section_name,
					field=field_name,
				)
				field_bits = self._int_to_bits(value, nbits)

				# Preserve current A-STEP behavior: reverse VDAC field bits first
				if section_name == "vdacs":
					field_bits.reverse()

				chip_bits.append(field_bits)

		if not msbfirst:
			chip_bits.reverse()

		return chip_bits

	def build_config_bits(
		self,
		config_source: Any,
		*,
		target_chip: int = -1,
		msbfirst: bool = False,
	) -> BitArray:
		"""
		Reproduce current A-STEP multi-chip concatenation logic

		A-STEP behavior:
		  - if target_chip == -1:
				concatenate chip_(n-1), ..., chip_0
		  - else:
				serialize only target_chip
		"""
		cfgs = self._extract_configs(config_source)

		if target_chip == -1:
			chip_indices = list(range(self.nchips - 1, -1, -1))
		else:
			if not (0 <= target_chip < self.nchips):
				raise V3ProtocolError(f"target_chip={target_chip} out of range 0..{self.nchips - 1}")
			chip_indices = [target_chip]

		bits = BitArray()
		for chip in chip_indices:
			key = f"config_{chip}"
			if key not in cfgs:
				raise V3ProtocolError(f"Missing {key} in config source")
			bits.append(self.build_chip_bits(cfgs[key], msbfirst=msbfirst))

		return bits

	# ------------------------------------------------------------------
	# SPI framing
	# ------------------------------------------------------------------

	def build_routing_frame(
		self,
		*,
		first_chip_id: int = 0,
		padding_bytes: int | None = None,
	) -> bytearray:
		"""
		A-STEP currently uses: [SPI_HEADER_ROUTING | firstChipID] + [0x00] * paddingBytes
		Original helper default is paddingBytes=2
		"""
		if padding_bytes is None:
			padding_bytes = 2

		return bytearray([self.SPI_HEADER_ROUTING | first_chip_id] + [0x00] * padding_bytes)

	def build_spi_config_frame(
		self,
		config_source: Any,
		*,
		target_chip: int = 0,
		broadcast: bool = False,
		load: bool = True,
		n_load: int = 10,
		msbfirst: bool = False,
		tdac: bool = False,
	) -> bytearray:
		"""
		Reproduce current A-STEP SPI SR frame generation
		Frame shape: [SR header] + [0x00/0x01 for each config bit] + [load bytes] + [empty bytes]

		* tdac=True is included only for compatibility in framing -
		  this draft does not build TDAC config payloads yet
		"""
		if tdac:
			raise NotImplementedError("TDAC frame generation is not implemented in this v3-only draft")

		if not broadcast and not (0 <= target_chip < self.nchips):
			raise V3ProtocolError(f"target_chip={target_chip} out of range 0..{self.nchips - 1}")

		config_bits = self.build_config_bits(
			config_source,
			target_chip=target_chip,
			msbfirst=msbfirst,
		)

		if broadcast:
			data = bytearray([self.SPI_SR_BROADCAST])
		else:
			data = bytearray([self.SPI_HEADER_SR | target_chip])

		for bit in config_bits:
			data.append(self.SPI_SR_BIT1 if bit else self.SPI_SR_BIT0)

		if load:
			data.extend([self.SPI_SR_LOAD] * n_load)

		# Preserve current A-STEP behavior: append 2 empty bytes per extra chip in chain
		data.extend([self.SPI_EMPTY_BYTE] * ((self.nchips - 1) * 2))

		return data

	# ------------------------------------------------------------------
	# debug helpers
	# ------------------------------------------------------------------

	def describe_order(self, config_source: Any, chip: int = 0) -> dict[str, list[str]]:
		"""
		Human-checkable summary: which sections and fields are currently used for serialization?
		"""
		cfgs = self._extract_configs(config_source)
		key = f"config_{chip}"
		if key not in cfgs:
			raise V3ProtocolError(f"Missing {key}")

		chip_cfg = cfgs[key]
		out: dict[str, list[str]] = {}
		for section_name in self._ordered_section_names(chip_cfg):
			out[section_name] = self._ordered_field_names(
				section_name,
				chip_cfg[section_name],
			)
		return out
