# - Mar. 24, 2026
# - Chong Kim, ckim.phenix@gmail.com
# - Written by AI assistance (chatGPT 5.4)

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Protocol

class ThresholdApplierError(RuntimeError):
    pass

class ThresholdApplier(Protocol):
    def validate_capabilities(self, *, lane: int, chip: int) -> None: ...
    async def apply_threshold_offset_mv(self, lane: int, chip: int, mv: float) -> str: ...

@dataclass(slots=True)
class InternalThresholdApplier:
    controller: Any

    def validate_capabilities(self, *, lane: int, chip: int) -> None:
        # Internal threshold path only requires a valid controller/config path.
        self.controller.get_lane_config(lane)

    async def apply_threshold_offset_mv(self, lane: int, chip: int, mv: float) -> str:
        self.controller.set_threshold_offset_mv(lane=lane, chip=chip, mv=float(mv))
        return "internal"

@dataclass(slots=True)
class ExternalGeccoThresholdApplier:
    board: Any
    per_lane_metadata: dict[int, dict[str, Any]] = field(default_factory=dict)
    vcal: float = 0.989
    vsupply: float = 2.7
    default_volt_slot: int = 4
    default_dacs_v3: tuple[float, ...] = (1.1, 0.0, 1.1, 1.0, 0.0, 0.0, 1.0, 1.100)

    def _board_has_voltage_api(self) -> bool:
        return getattr(self.board, "geccoGetVoltageBoard", None) is not None

    def _lane_asic_config(self, lane: int) -> dict[str, Any]:
        try:
            return getattr(self.board.asics[lane], "asic_config", {})
        except Exception:
            return {}

    @staticmethod
    def _extract_voltagecard_from_mapping(mapping: dict[str, Any] | None) -> dict[str, Any] | None:
        if not isinstance(mapping, dict):
            return None
        cfgcards = mapping.get("configcards")
        if not isinstance(cfgcards, dict):
            return None
        voltagecard = cfgcards.get("voltagecard")
        if not isinstance(voltagecard, dict):
            return None
        return voltagecard

    def resolve_voltagecard_config(self, *, lane: int, chip: int) -> dict[str, Any]:
        # 1) Explicit YAML metadata preserved by bootstrap.py
        lane_meta = self.per_lane_metadata.get(int(lane), {})
        voltagecard = self._extract_voltagecard_from_mapping(lane_meta)
        if voltagecard is not None:
            dacs = list(voltagecard.get("dacs", []))
            return {
                "source": "yaml_root_metadata",
                "volt_slot": int(voltagecard.get("pos", self.default_volt_slot)),
                "default_dacs": dacs if dacs else list(self.default_dacs_v3),
            }

        # 2) Legacy asic_config path, when setupASIC preserved configcards.
        asic_cfg = self._lane_asic_config(lane)
        voltagecard = self._extract_voltagecard_from_mapping(asic_cfg)
        if voltagecard is not None:
            dacs = list(voltagecard.get("dacs", []))
            return {
                "source": "asic_config_configcards",
                "volt_slot": int(voltagecard.get("pos", self.default_volt_slot)),
                "default_dacs": dacs if dacs else list(self.default_dacs_v3),
            }

        # 3) Legacy-compatible fallback: assume GECCO voltage card in slot 4 with v3 defaults.
        return {
            "source": "legacy_fallback",
            "volt_slot": int(self.default_volt_slot),
            "default_dacs": list(self.default_dacs_v3),
        }

    def validate_capabilities(self, *, lane: int, chip: int) -> None:
        if not self._board_has_voltage_api():
            raise ThresholdApplierError(
                "Board driver has no geccoGetVoltageBoard(); external GECCO threshold path is unavailable."
            )

        if not hasattr(self.board, "asics") or lane not in getattr(self.board, "asics", {}):
            raise ThresholdApplierError(f"No ASIC object registered for lane {lane}.")

        resolved = self.resolve_voltagecard_config(lane=lane, chip=chip)
        dacs = list(resolved["default_dacs"])
        if len(dacs) < 4:
            raise ThresholdApplierError(
                "Voltage card DAC map is too short to derive threshold baseline."
            )

    async def apply_threshold_offset_mv(self, lane: int, chip: int, mv: float) -> str:
        self.validate_capabilities(lane=lane, chip=chip)
        resolved = self.resolve_voltagecard_config(lane=lane, chip=chip)

        volt_slot = int(resolved["volt_slot"])
        default_dacs = list(resolved["default_dacs"])
        dac_count = len(default_dacs)

        absolute_v = (float(mv) / 1000.0) + float(default_dacs[3])
        if absolute_v <= 0:
            absolute_v = 1.100
        if absolute_v > 1.5:
            raise ThresholdApplierError(
                f"Requested threshold {mv} mV exceeds supported external range after baseline offset."
            )

        dacs = list(default_dacs)
        dacs[-1] = absolute_v

        getter = getattr(self.board, "geccoGetVoltageBoard", None)
        if getter is None:
            raise ThresholdApplierError(
                "Board driver has no geccoGetVoltageBoard(); external GECCO threshold is unavailable."
            )

        vboard = getter(volt_slot=volt_slot)
        vboard.dacvalues = (dac_count, dacs)
        vboard.vcal = self.vcal
        vboard.vsupply = self.vsupply
        await vboard.update()
        return "external_gecco"
