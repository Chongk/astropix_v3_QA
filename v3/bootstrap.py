# - Apr. 16, 2026
# - Chong Kim, ckim.phenix@gmail.com
# - Written by AI assistance (chatGPT 5.4)

from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import xml.etree.ElementTree as ET
import yaml

from .config import V3Config
from .controller import V3Controller
from .daq import V3DAQ
from .protocol import V3Protocol
from .qa import V3QA
from .threshold import ExternalGeccoThresholdApplier
from .transport import V3Transport

def _parse_bool(text: str | None, default: bool = False) -> bool:
	if text is None:
		return default
	return str(text).strip().lower() in {"1", "true", "yes", "on"}

def _require_attr(root: ET.Element, tag: str, default: str | None = None) -> str:
	node = root.find(tag)
	if node is None:
		if default is None:
			raise ValueError(f"Missing XML node: {tag}")
		return default
	return str(node.attrib.get("value", default if default is not None else ""))

@dataclass(slots=True)
class FPGASettings:
	fpga: str
	protocol: str
	port: str | None
	chipversion: int
	fpgatsfreq_hz: int | None
	use_tlu: bool
	spi_freq_hz: int | None

@dataclass(slots=True)
class RuntimeBundle:
	board: Any
	transport: V3Transport
	protocol: V3Protocol
	controller: V3Controller
	daq: V3DAQ
	qa: V3QA
	settings: FPGASettings
	per_lane_yaml_metadata: dict[int, dict[str, Any]] = field(default_factory=dict)

def load_fpga_settings(xml_path: str) -> FPGASettings:
	root = ET.parse(xml_path).getroot()
	return FPGASettings(
		fpga=_require_attr(root, "fpga"),
		protocol=_require_attr(root, "protocol"),
		port=_require_attr(root, "port", default="") or None,
		chipversion=int(_require_attr(root, "chipversion")),
		fpgatsfreq_hz=(int(_require_attr(root, "FPGATSfreq")) if root.find("FPGATSfreq") is not None else None),
		use_tlu=_parse_bool(_require_attr(root, "useTLU", default="False"), default=False),
		spi_freq_hz=(int(_require_attr(root, "SPIfreq")) if root.find("SPIfreq") is not None else None),
	)

async def open_board_from_settings(settings: FPGASettings) -> Any:
	import drivers.astep.serial
	import drivers.boards

	fpga = settings.fpga.lower()
	proto = settings.protocol.lower()

	if fpga == "cmod":
		if proto == "uart":
			if not settings.port:
				raise ValueError("CMOD/UART requires a port in the XML config")
			board = drivers.boards.getCMODUartDriver(settings.port)
		elif proto == "spi":
			raise NotImplementedError("CMOD/SPI is not supported in this bootstrap")
		else:
			board = drivers.boards.getCMODDriver()
	elif fpga == "gecco":
		if proto == "uart":
			board = drivers.boards.getGeccoUARTDriver(drivers.astep.serial.getFirstCOMPort())
		elif proto == "ftdi":
			board = drivers.boards.getGeccoFTDIDriver()
		else:
			raise RuntimeError(f"Unsupported GECCO protocol: {settings.protocol}")
	else:
		raise RuntimeError(f"Unsupported FPGA board: {settings.fpga}")

	await board.open()
	try:
		await board.readFirmwareID()
	except Exception as exc:
		raise RuntimeError("Could not read firmware ID after opening the board") from exc
	return board

def load_asics_from_yaml(board: Any, yaml_paths: list[str], chips_per_row: list[int], chipversion: int) -> None:
	if len(yaml_paths) != len(chips_per_row):
		raise ValueError("yaml_paths and chips_per_row must have the same length")

	for lane, (yaml_path, nchips) in enumerate(zip(yaml_paths, chips_per_row)):
		if not Path(yaml_path).exists():
			raise FileNotFoundError(f"YAML config not found: {yaml_path}")
		board.setupASIC(
			version=chipversion,
			lane=lane,
			chipsPerLane=int(nchips),
			configFile=str(yaml_path),
		)

def _read_yaml_lane_metadata(yaml_path: str) -> dict[str, Any]:
	p = Path(yaml_path)
	if not p.exists():
		raise FileNotFoundError(f"YAML config not found: {yaml_path}")

	with p.open("r", encoding="utf-8") as f:
		payload = yaml.safe_load(f) or {}

	if not isinstance(payload, dict) or not payload:
		return {}

	# Expect a single root key such as 'astropix3'. Keep only board/card metadata.
	_, root_value = next(iter(payload.items()))
	if not isinstance(root_value, dict):
		return {}

	out: dict[str, Any] = {}
	for key in ("configcards", "general", "geometry", "telescope"):
		value = root_value.get(key)
		if value is not None:
			out[key] = value
	return out

async def build_runtime_from_files(
	*,
	fpgaxml: str,
	yaml_paths: list[str],
	chips_per_row: list[int],
	configure_autoread_keepalive: bool = False,
) -> RuntimeBundle:
	settings = load_fpga_settings(fpgaxml)
	board = await open_board_from_settings(settings)
	try:
		load_asics_from_yaml(board, yaml_paths, chips_per_row, settings.chipversion)

		per_lane_yaml_metadata: dict[int, dict[str, Any]] = {
			lane: _read_yaml_lane_metadata(yaml_path)
			for lane, yaml_path in enumerate(yaml_paths)
		}

		transport = V3Transport(board, chipversion=settings.chipversion)
		await transport.configure_chipversion(flush=True)
		await transport.configure_clocks(
			fpga_ts_freq_hz=settings.fpgatsfreq_hz,
			use_tlu=settings.use_tlu,
			spi_freq_hz=settings.spi_freq_hz,
			flush=True,
		)
		if configure_autoread_keepalive:
			await transport.configure_autoread_keepalive(flush=False)

		lane_configs: dict[int, V3Config] = {}
		max_nchips = 1
		for lane in sorted(board.asics.keys()):
			asic_obj = board.asics[lane]
			nchips = int(getattr(asic_obj, "_num_chips", 1))
			max_nchips = max(max_nchips, nchips)
			lane_configs[lane] = V3Config.from_astep_asic_config(asic_obj.asic_config, nchips=nchips)

		protocol = V3Protocol(nchips=max_nchips)
		controller = V3Controller(transport, protocol, lane_configs=lane_configs)
		daq = V3DAQ(controller, default_lane=min(lane_configs.keys()))
		qa = V3QA(
			controller,
			daq,
			threshold_applier=ExternalGeccoThresholdApplier(
				board,
				per_lane_metadata=per_lane_yaml_metadata,
			),
		)
		return RuntimeBundle(board, transport, protocol, controller, daq, qa, settings, per_lane_yaml_metadata)
	except Exception:
		await board.close()
		raise
