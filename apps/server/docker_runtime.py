"""Docker-specific runtime helpers without weakening the normal local-only policy."""
from __future__ import annotations

import functools
import ipaddress
import os
from pathlib import Path
from typing import Any

DOCKER_MODE_ENV = "BILIPDJ_DOCKER"
TRUSTED_CIDRS_ENV = "BILIPDJ_DOCKER_TRUSTED_CIDRS"
_TRUE_VALUES = {"1", "true", "yes", "on"}


def docker_mode_enabled() -> bool:
    return str(os.getenv(DOCKER_MODE_ENV, "") or "").strip().lower() in _TRUE_VALUES


def _linux_default_gateways(route_path: Path = Path("/proc/net/route")) -> tuple[str, ...]:
    """Return exact IPv4 default gateway addresses from Linux procfs."""

    try:
        lines = route_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return ()

    gateways: list[str] = []
    for line in lines[1:]:
        fields = line.split()
        if len(fields) < 4 or fields[1] != "00000000":
            continue
        try:
            flags = int(fields[3], 16)
            raw = bytes.fromhex(fields[2])
            if len(raw) != 4 or not (flags & 0x2):
                continue
            address = str(ipaddress.IPv4Address(int.from_bytes(raw, "little")))
        except (ValueError, OverflowError):
            continue
        if address not in gateways:
            gateways.append(address)
    return tuple(gateways)


def trusted_docker_networks(
    raw: str | None = None,
    *,
    route_path: Path = Path("/proc/net/route"),
) -> tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]:
    """Resolve explicitly trusted Docker proxy/gateway networks.

    ``auto`` trusts only the exact default gateway address(es), never an entire
    RFC1918 range. Explicit CIDRs can be supplied when a Docker runtime uses a
    different proxy address.
    """

    if not docker_mode_enabled():
        return ()

    spec = str(os.getenv(TRUSTED_CIDRS_ENV, "auto") if raw is None else raw).strip()
    tokens = [item.strip() for item in spec.replace(";", ",").split(",") if item.strip()]
    if not tokens:
        tokens = ["auto"]

    result: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    for token in tokens:
        if token.lower() == "auto":
            for gateway in _linux_default_gateways(route_path):
                address = ipaddress.ip_address(gateway)
                result.append(ipaddress.ip_network(f"{address}/{address.max_prefixlen}", strict=False))
            continue
        try:
            if "/" not in token:
                address = ipaddress.ip_address(token)
                network = ipaddress.ip_network(f"{address}/{address.max_prefixlen}", strict=False)
            else:
                network = ipaddress.ip_network(token, strict=False)
        except ValueError:
            continue
        if network not in result:
            result.append(network)
    return tuple(result)


def is_trusted_docker_client(
    host: str,
    *,
    raw: str | None = None,
    route_path: Path = Path("/proc/net/route"),
) -> bool:
    if not docker_mode_enabled():
        return False
    try:
        address = ipaddress.ip_address(str(host or "").strip())
    except ValueError:
        return False
    return any(address in network for network in trusted_docker_networks(raw, route_path=route_path))


def install_docker_local_access(server_module: Any) -> bool:
    """Treat only the exact trusted Docker gateway as local transport.

    The existing runtime guard still requires a loopback Host header for
    management requests, so this only fixes Docker NAT/proxy source-address
    translation; it does not make LAN Host headers valid.
    """

    handler_class = getattr(server_module, "ApiHandler", None)
    if not isinstance(handler_class, type):
        return False
    if bool(getattr(handler_class, "_bilipdj_docker_local_access_installed", False)):
        return True

    original = getattr(handler_class, "_is_loopback_client", None)
    if not callable(original):
        return False

    @functools.wraps(original)
    def loopback_or_docker_gateway(self: Any) -> bool:
        if original(self):
            return True
        host = str(self.client_address[0] if getattr(self, "client_address", None) else "").strip()
        return is_trusted_docker_client(host)

    setattr(handler_class, "_is_loopback_client", loopback_or_docker_gateway)
    setattr(handler_class, "_bilipdj_docker_local_access_installed", True)
    return True


def install_plugin_data_root(plugin_manager_module: Any) -> bool:
    """Make PluginManager honor server.PLUGINS_DIR while preserving old defaults."""

    manager_class = getattr(plugin_manager_module, "PluginManager", None)
    if not isinstance(manager_class, type):
        return False
    if bool(getattr(manager_class, "_bilipdj_data_root_installed", False)):
        return True

    original_init = getattr(manager_class, "__init__", None)
    if not callable(original_init):
        return False

    @functools.wraps(original_init)
    def init_with_data_root(self: Any, server_module: Any, *args: Any, **kwargs: Any) -> None:
        original_init(self, server_module, *args, **kwargs)
        configured = getattr(server_module, "PLUGINS_DIR", None)
        if configured is None:
            return
        plugins_root = Path(configured).resolve()
        if plugins_root == Path(self.plugins_root).resolve():
            return
        self.plugins_root = plugins_root
        self.data_root = plugins_root / "data"
        self.state_path = plugins_root / "state.json"
        self.trusted_keys_path = plugins_root / "trusted_keys.json"
        self.plugins_root.mkdir(parents=True, exist_ok=True)
        self.data_root.mkdir(parents=True, exist_ok=True)

    setattr(manager_class, "__init__", init_with_data_root)
    setattr(manager_class, "_bilipdj_data_root_installed", True)
    return True


__all__ = [
    "DOCKER_MODE_ENV",
    "TRUSTED_CIDRS_ENV",
    "docker_mode_enabled",
    "install_docker_local_access",
    "install_plugin_data_root",
    "is_trusted_docker_client",
    "trusted_docker_networks",
]
