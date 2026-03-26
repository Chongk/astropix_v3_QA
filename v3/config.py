# - Mar. 24, 2026
# - Chong Kim, ckim.phenix@gmail.com

from __future__ import annotations
from copy import deepcopy
from typing import Any

class V3ConfigError(ValueError):
	pass

class V3Config:
	"""
	v3-only configuration layer

	Design goals
	------------
	1. Keep one single source of truth for pixel/injection/threshold state
	2. Preserve the current A-STEP/YAML-style dictionary layout where useful
	3. Make the chip-facing semantics explicit and chip-aware
	4. Be easy to diff against the current `asic.py` / YAML-loaded config

	Notes on inferred recconfig bit layout
	--------------------------------------
	From the current helper methods in asic.py:
		- bit   0		: row injection enable
		- bits  1 .. 35	: per-row pixel disable bits (0 = enabled, 1 = disabled)
		- bit  36		: column injection enable
		- bit  37		: analog mux / ampout enable

	Default recconfig word in the existing code:
		0b001_11111_11111_11111_11111_11111_11111_11110

	which corresponds to:
		- all pixels disabled
		- row injection off
		- column injection off
		- ampout off
	"""

	DEFAULT_NUM_ROWS = 35
	DEFAULT_NUM_COLS = 35
	DEFAULT_NUM_CHIPS = 1

	RECCONFIG_BITS = 38
	VDAC_BITS = 10
	VREF_MV = 1800.0

	# Default word copied from current asic.py helper logic
	DEFAULT_RECCONFIG_WORD = int(
			"001_11111_11111_11111_11111_11111_11111_11110".replace("_", ""),
			2,
	)

	# Bit positions inferred from current helper methods
	ROW_INJ_BIT = 0
	PIXEL_DISABLE_LSB = 1
	COL_INJ_BIT = 36
	AMPOUT_BIT = 37

	def __init__(
			self,
			nchips: int = DEFAULT_NUM_CHIPS,
			nrows: int = DEFAULT_NUM_ROWS,
			ncols: int = DEFAULT_NUM_COLS,
			vref_mv: float = VREF_MV,
			vdac_bits: int = VDAC_BITS,
			compat_astep_dac: bool = True,
			initial_config: dict[str, Any] | None = None,
	) -> None:
		self.nchips = nchips
		self.nrows = nrows
		self.ncols = ncols
		self.vref_mv = float(vref_mv)
		self.vdac_bits = int(vdac_bits)
		self.compat_astep_dac = compat_astep_dac

		"""
		Internal storage keeps the same broad shape as A-STEP:
		{
			"config_0": {
				"vdacs": {"blpix": [10, ...], "thpix": [10, ...], ...},
				"recconfig": {"col0": [38, ...], ..., "col34": [38, ...]},
				...
			},
			...
		}
		"""
		self._configs: dict[str, dict[str, Any]] = {}

		if initial_config is None:
			for chip in range(self.nchips):
				self._configs[f"config_{chip}"] = self._make_empty_chip_config()
		else:
			self._load_from_existing(initial_config)

		self._sanitize_all()

	# ------------------------------------------------------------------
	# Construction helpers
	# ------------------------------------------------------------------

	@classmethod
	def from_astep_asic_config(
			cls,
			asic_config: dict[str, Any],
			*,
			nchips: int | None = None,
			nrows: int = DEFAULT_NUM_ROWS,
			ncols: int = DEFAULT_NUM_COLS,
			vref_mv: float = VREF_MV,
			vdac_bits: int = VDAC_BITS,
			compat_astep_dac: bool = True,
	) -> "V3Config":
		"""
		Build from an existing A-STEP-style `asic_config` dict
		For the case of diff behavior required against the currently loaded YAML + helper stack
		"""
		if nchips is None:
			nchips = sum(1 for key in asic_config if key.startswith("config_"))
			if nchips == 0:
				nchips = 1

		return cls(
				nchips=nchips,
				nrows=nrows,
				ncols=ncols,
				vref_mv=vref_mv,
				vdac_bits=vdac_bits,
				compat_astep_dac=compat_astep_dac,
				initial_config=asic_config,
		)

	def _make_empty_chip_config(self) -> dict[str, Any]:
		recconfig = {
			f"col{col}": [self.RECCONFIG_BITS, self.DEFAULT_RECCONFIG_WORD]
			for col in range(self.ncols)
		}

		# Only the DACs we actively manipulate are guaranteed here
		# You can extend this set later if protocol.py needs more fields
		vdacs = {
			"blpix": [self.vdac_bits, 0],
			"thpix": [self.vdac_bits, 0],
			"vinj": [self.vdac_bits, 0],
		}

		return {
			"vdacs": vdacs,
			"recconfig": recconfig,
		}

	def _load_from_existing(self, initial_config: dict[str, Any]) -> None:
		for chip in range(self.nchips):
			key = f"config_{chip}"
			if key in initial_config:
				self._configs[key] = deepcopy(initial_config[key])
			else:
				self._configs[key] = self._make_empty_chip_config()

	def _sanitize_all(self) -> None:
		for chip in range(self.nchips):
			self._sanitize_chip(chip)

	def _sanitize_chip(self, chip: int) -> None:
		cfg = self._chip_cfg(chip)
		cfg.setdefault("vdacs", {})
		cfg.setdefault("recconfig", {})

		# Normalize required VDAC entries to [nbits, code]
		for name in ("blpix", "thpix", "vinj"):
			self._ensure_vdac_entry(chip, name)

		# Normalize recconfig entries
		for col in range(self.ncols):
			key = f"col{col}"
			entry = cfg["recconfig"].get(key)
			if entry is None:
				cfg["recconfig"][key] = [self.RECCONFIG_BITS, self.DEFAULT_RECCONFIG_WORD]
			elif isinstance(entry, int):
				cfg["recconfig"][key] = [self.RECCONFIG_BITS, entry]
			elif isinstance(entry, list):
				if len(entry) < 2:
					cfg["recconfig"][key] = [self.RECCONFIG_BITS, self.DEFAULT_RECCONFIG_WORD]
				else:
					cfg["recconfig"][key][0] = self.RECCONFIG_BITS
			else:
			 	raise V3ConfigError(f"Unsupported recconfig entry type for {key}: {type(entry)}")

	def _ensure_vdac_entry(self, chip: int, name: str) -> None:
		vdacs = self._chip_cfg(chip)["vdacs"]
		entry = vdacs.get(name)

		if entry is None:
			vdacs[name] = [self.vdac_bits, 0]
		elif isinstance(entry, int):
			vdacs[name] = [self.vdac_bits, self._clamp_dac(entry)]
		elif isinstance(entry, list):
			if len(entry) < 2:
				vdacs[name] = [self.vdac_bits, 0]
			else:
				vdacs[name][0] = self.vdac_bits
				vdacs[name][1] = self._clamp_dac(vdacs[name][1])
		else:
			raise V3ConfigError(f"Unsupported VDAC entry type for {name}: {type(entry)}")

	# ------------------------------------------------------------------
	# Basic access
	# ------------------------------------------------------------------

	def _chip_cfg(self, chip: int) -> dict[str, Any]:
		self._validate_chip(chip)
		return self._configs[f"config_{chip}"]

	def export_chip_dict(self, chip: int) -> dict[str, Any]:
		""" Deep-copy of one chip config in A-STEP/YAML-like shape """
		return deepcopy(self._chip_cfg(chip))

	def export_all(self) -> dict[str, Any]:
		""" Deep-copy of all chip configs """
		return deepcopy(self._configs)

	def attach_into_asic_config(self, asic_config: dict[str, Any]) -> dict[str, Any]:
		""" Convenience helper: update an existing top-level `asic_config` dict with this config payload """
		out = deepcopy(asic_config)
		for chip in range(self.nchips):
			out[f"config_{chip}"] = self.export_chip_dict(chip)
		return out

	# ------------------------------------------------------------------
	# Validation
	# ------------------------------------------------------------------

	def _validate_chip(self, chip: int) -> None:
		if not (0 <= chip < self.nchips):
			raise V3ConfigError(f"chip={chip} outside valid range 0..{self.nchips - 1}")

	def _validate_row(self, row: int) -> None:
		if not (0 <= row < self.nrows):
			raise V3ConfigError(f"row={row} outside valid range 0..{self.nrows - 1}")

	def _validate_col(self, col: int) -> None:
		if not (0 <= col < self.ncols):
			raise V3ConfigError(f"col={col} outside valid range 0..{self.ncols - 1}")

	# ------------------------------------------------------------------
	# DAC helpers
	# ------------------------------------------------------------------

	@property
	def dac_full_scale(self) -> int:
		return (1 << self.vdac_bits) - 1

	def _clamp_dac(self, code: int) -> int:
		return max(0, min(int(code), self.dac_full_scale))

	def mv_to_dac(self, mv: float) -> int:
		"""
		Convert mV -> 10-bit DAC code.

		compat_astep_dac=True:
			follows the current A-STEP/AstropixRun style: int(V * 2**nbits / Vref)
			but clamps the result into 0..1023

		compat_astep_dac=False:
			uses the stricter full-scale form: round(V * (2**nbits - 1) / Vref)
		"""
		mv = float(mv)
		if mv < 0:
			raise V3ConfigError(f"Negative voltage is not allowed: {mv} mV")

		if self.compat_astep_dac:
			code = int(mv * (1 << self.vdac_bits) / self.vref_mv)
		else:
			code = round(mv * self.dac_full_scale / self.vref_mv)

		return self._clamp_dac(code)

	def dac_to_mv(self, code: int) -> float:
		code = self._clamp_dac(code)
		if self.compat_astep_dac:
			return code * self.vref_mv / (1 << self.vdac_bits)
		return code * self.vref_mv / self.dac_full_scale

	def get_vdac_code(self, chip: int, name: str) -> int:
		self._ensure_vdac_entry(chip, name)
		return int(self._chip_cfg(chip)["vdacs"][name][1])

	def set_vdac_code(self, chip: int, name: str, code: int) -> None:
		self._ensure_vdac_entry(chip, name)
		self._chip_cfg(chip)["vdacs"][name][1] = self._clamp_dac(code)

	def get_vdac_mv(self, chip: int, name: str) -> float:
		return self.dac_to_mv(self.get_vdac_code(chip, name))

	def set_vdac_mv(self, chip: int, name: str, mv: float) -> None:
		self.set_vdac_code(chip, name, self.mv_to_dac(mv))

	# ------------------------------------------------------------------
	# Public VDAC API used in QA
	# ------------------------------------------------------------------

	def set_blpix_mv(self, chip: int, mv: float) -> None:
		self.set_vdac_mv(chip, "blpix", mv)

	def set_absolute_thpix_mv(self, chip: int, mv: float) -> None:
		self.set_vdac_mv(chip, "thpix", mv)

	def set_threshold_offset_mv(self, chip: int, offset_mv: float) -> None:
		"""
		Current A-STEP-compatible threshold model: thpix = blpix + offset
		a threshold increment is converted to DAC code and added to blpix
		"""
		bl_code = self.get_vdac_code(chip, "blpix")
		delta_code = self.mv_to_dac(offset_mv)
		self.set_vdac_code(chip, "thpix", bl_code + delta_code)

	def set_vinj_mv(self, chip: int, mv: float) -> None:
		self.set_vdac_mv(chip, "vinj", mv)

	# ------------------------------------------------------------------
	# recconfig low-level helpers
	# ------------------------------------------------------------------

	def _rec_key(self, col: int) -> str:
		return f"col{col}"

	def _get_rec_word(self, chip: int, col: int) -> int:
		self._validate_chip(chip)
		self._validate_col(col)
		return int(self._chip_cfg(chip)["recconfig"][self._rec_key(col)][1])

	def _set_rec_word(self, chip: int, col: int, word: int) -> None:
		self._validate_chip(chip)
		self._validate_col(col)
		mask = (1 << self.RECCONFIG_BITS) - 1
		self._chip_cfg(chip)["recconfig"][self._rec_key(col)][1] = int(word) & mask

	def _set_bit(self, chip: int, col: int, bit: int) -> None:
		self._set_rec_word(chip, col, self._get_rec_word(chip, col) | (1 << bit))

	def _clear_bit(self, chip: int, col: int, bit: int) -> None:
		self._set_rec_word(chip, col, self._get_rec_word(chip, col) & ~(1 << bit))

	# ------------------------------------------------------------------
	# Matrix reset
	# ------------------------------------------------------------------

	def reset_matrix(self, chip: int) -> None:
		"""
		Reset recconfig to the default A-STEP word:
		- all pixels disabled
		- all injection switches off
		- ampout off
		"""
		self._validate_chip(chip)
		for col in range(self.ncols):
			self._set_rec_word(chip, col, self.DEFAULT_RECCONFIG_WORD)

	# ------------------------------------------------------------------
	# Pixel comparator control
	# ------------------------------------------------------------------

	def enable_pixel(self, chip: int, col: int, row: int) -> None:
		"""
		Enable comparator in one pixel.

		Existing code uses:	word &= ~(2 << row), which means the disable bit is at position (row + 1)
		"""
		self._validate_chip(chip)
		self._validate_col(col)
		self._validate_row(row)
		self._clear_bit(chip, col, self.PIXEL_DISABLE_LSB + row)

	def disable_pixel(self, chip: int, col: int, row: int) -> None:
		self._validate_chip(chip)
		self._validate_col(col)
		self._validate_row(row)
		self._set_bit(chip, col, self.PIXEL_DISABLE_LSB + row)

	def is_pixel_enabled(self, chip: int, col: int, row: int) -> bool:
		self._validate_chip(chip)
		self._validate_col(col)
		self._validate_row(row)
		word = self._get_rec_word(chip, col)
		return (word & (1 << (self.PIXEL_DISABLE_LSB + row))) == 0

	# ------------------------------------------------------------------
	# Injection control
	# ------------------------------------------------------------------

	def enable_inj_row(self, chip: int, row: int) -> None:
		"""
		Preserve the current square-matrix A-STEP convention:
		row injection is encoded in recconfig["col{row}"] bit 0
		"""
		self._validate_chip(chip)
		self._validate_row(row)
		self._set_bit(chip, row, self.ROW_INJ_BIT)

	def disable_inj_row(self, chip: int, row: int) -> None:
		self._validate_chip(chip)
		self._validate_row(row)
		self._clear_bit(chip, row, self.ROW_INJ_BIT)

	def enable_inj_col(self, chip: int, col: int) -> None:
		self._validate_chip(chip)
		self._validate_col(col)
		self._set_bit(chip, col, self.COL_INJ_BIT)

	def disable_inj_col(self, chip: int, col: int) -> None:
		self._validate_chip(chip)
		self._validate_col(col)
		self._clear_bit(chip, col, self.COL_INJ_BIT)

	def enable_injection_pixel(self, chip: int, col: int, row: int) -> None:
		"""
		High-level helper for the usual single-pixel injection use case

		v3 architecture uses separated row/column injection control,
		so a "pixel injection" means enabling both switches
		"""
		self.enable_inj_col(chip, col)
		self.enable_inj_row(chip, row)

	def disable_injection_pixel(self, chip: int, col: int, row: int) -> None:
		self.disable_inj_col(chip, col)
		self.disable_inj_row(chip, row)

	def configure_single_injection_pixel(
		self,
		chip: int,
		col: int,
		row: int,
		*,
		reset_first: bool = True,
		enable_pixel: bool = True,
		enable_injection: bool = True,
	) -> None:
		"""
		Handy helper for the first QA routine:
		reset matrix -> enable one pixel -> enable row/column injection path
		"""
		if reset_first:
			self.reset_matrix(chip)
		if enable_pixel:
			self.enable_pixel(chip, col, row)
		if enable_injection:
			self.enable_injection_pixel(chip, col, row)

	# ------------------------------------------------------------------
	# Analog mux / ampout control
	# ------------------------------------------------------------------

	def disable_all_ampout(self, chip: int) -> None:
		self._validate_chip(chip)
		for col in range(self.ncols):
			self._clear_bit(chip, col, self.AMPOUT_BIT)

	def enable_ampout_col(self, chip: int, col: int) -> None:
		"""
		Select one column for analog mux output

		Unlike the current helper, this really clears ampout on *all* columns first,
		then enables it on the requested column
		"""
		self._validate_chip(chip)
		self._validate_col(col)
		self.disable_all_ampout(chip)
		self._set_bit(chip, col, self.AMPOUT_BIT)

	# ------------------------------------------------------------------
	# Convenience utilities
	# ------------------------------------------------------------------

	def summary(self, chip: int) -> dict[str, Any]:
		"""
		Compact human-checkable summary for quick debugging / diffs.
		"""
		self._validate_chip(chip)

		enabled_pixels: list[tuple[int, int]] = []
		inj_rows: list[int] = []
		inj_cols: list[int] = []
		ampout_cols: list[int] = []

		for col in range(self.ncols):
			word = self._get_rec_word(chip, col)

			if (word & (1 << self.COL_INJ_BIT)) != 0:
				inj_cols.append(col)
			if (word & (1 << self.AMPOUT_BIT)) != 0:
				ampout_cols.append(col)

			# row injection is stored in recconfig["col{row}"] bit 0
			if (word & (1 << self.ROW_INJ_BIT)) != 0:
				inj_rows.append(col)

			for row in range(self.nrows):
				if self.is_pixel_enabled(chip, col, row):
					enabled_pixels.append((col, row))

		return {
			"chip": chip,
			"vdacs": {
				"blpix_code": self.get_vdac_code(chip, "blpix"),
				"thpix_code": self.get_vdac_code(chip, "thpix"),
				"vinj_code": self.get_vdac_code(chip, "vinj"),
				"blpix_mv": self.get_vdac_mv(chip, "blpix"),
				"thpix_mv": self.get_vdac_mv(chip, "thpix"),
				"vinj_mv": self.get_vdac_mv(chip, "vinj"),
			},
			"enabled_pixels": enabled_pixels,
			"enabled_pixel_count": len(enabled_pixels),
			"inj_rows": inj_rows,
			"inj_cols": inj_cols,
			"ampout_cols": ampout_cols,
		}
