from __future__ import annotations

import importlib.util
import os
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load_file_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _with_env(**values: str | None):
    class EnvContext:
        def __enter__(self):
            self.before = {key: os.environ.get(key) for key in values}
            for key, value in values.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
            return self

        def __exit__(self, exc_type, exc, tb):
            for key, value in self.before.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    return EnvContext()


def check_data_dir_layout() -> None:
    layout = _load_file_module("issue202_runtime_layout", ROOT / "apps/server/runtime_layout.py")

    with tempfile.TemporaryDirectory() as raw:
        app = Path(raw) / "app"
        app.mkdir()
        with _with_env(BILIPDJ_DATA_DIR=None):
            core, key = layout.ensure_runtime_layout(app)
            assert core == app / "core"
            assert key == app / "key"

    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        app = root / "app"
        data = root / "data"
        defaults = root / "defaults"
        app.mkdir()
        defaults.mkdir()
        (app / "config.yaml").write_text("server:\n  port: 9816\n", encoding="utf-8")
        (defaults / "style.json").write_text('{"show_sequence": false}', encoding="utf-8")
        (defaults / "appearance.json").write_text('{"mode": "system"}', encoding="utf-8")

        with _with_env(BILIPDJ_DATA_DIR=str(data)):
            core, key = layout.ensure_runtime_layout(app, defaults_dir=defaults)
            assert layout.resolve_data_dir(app) == data.resolve()
            assert core == data / "core"
            assert key == data / "key"
            assert (core / "config.yaml").is_file()
            assert not (app / "config.yaml").exists()
            assert (data / "style.json").is_file()
            assert (data / "appearance.json").is_file()
            for relative in ("log", "plugins", "backup", "core/cd", "key"):
                assert (data / relative).is_dir(), relative


def check_docker_gateway_security() -> None:
    docker = _load_file_module("issue202_docker_runtime", ROOT / "apps/server/docker_runtime.py")
    with tempfile.TemporaryDirectory() as raw:
        route = Path(raw) / "route"
        route.write_text(
            "Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\tMTU\tWindow\tIRTT\n"
            "eth0\t00000000\t01001EAC\t0003\t0\t0\t0\t00000000\t0\t0\t0\n",
            encoding="utf-8",
        )
        with _with_env(BILIPDJ_DOCKER="1", BILIPDJ_DOCKER_TRUSTED_CIDRS="auto"):
            networks = docker.trusted_docker_networks(route_path=route)
            assert [str(item) for item in networks] == ["172.30.0.1/32"]
            assert docker.is_trusted_docker_client("172.30.0.1", route_path=route)
            assert not docker.is_trusted_docker_client("172.30.0.2", route_path=route)
            assert not docker.is_trusted_docker_client("192.168.1.50", route_path=route)

        with _with_env(BILIPDJ_DOCKER="1", BILIPDJ_DOCKER_TRUSTED_CIDRS="10.20.30.40/32"):
            assert docker.is_trusted_docker_client("10.20.30.40", route_path=route)
            assert not docker.is_trusted_docker_client("10.20.30.41", route_path=route)

        with _with_env(BILIPDJ_DOCKER="0", BILIPDJ_DOCKER_TRUSTED_CIDRS="0.0.0.0/0"):
            assert not docker.is_trusted_docker_client("172.30.0.1", route_path=route)


def check_wiring_and_compose() -> None:
    init_source = (ROOT / "apps/server/__init__.py").read_text(encoding="utf-8")
    assert "module.DATA_DIR = data_dir" in init_source
    assert "module.PLUGINS_DIR" in init_source
    assert "install_plugin_data_root(_plugin_manager)" in init_source
    assert "install_docker_local_access(server)" in init_source

    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert '"127.0.0.1:9816:9816"' in compose
    assert "BILIPDJ_DATA_DIR: /data" in compose
    assert 'BILIPDJ_DOCKER: "1"' in compose
    assert "BILIPDJ_DOCKER_TRUSTED_CIDRS: auto" in compose
    assert "./data:/data" in compose
    assert "/health" in compose
    assert "restart: unless-stopped" in compose

    dockerfile = (ROOT / "apps/server/Dockerfile").read_text(encoding="utf-8")
    assert "BILIPDJ_DATA_DIR=/data" in dockerfile
    assert "BILIPDJ_DOCKER=1" in dockerfile
    assert "BILIPDJ_DOCKER_TRUSTED_CIDRS=auto" in dockerfile
    assert "HEALTHCHECK" in dockerfile and "/health" in dockerfile

    quality = (ROOT / ".github/workflows/quality.yml").read_text(encoding="utf-8")
    assert "python scripts/issue202_docker_guard.py" in quality
    assert "docker-compose.yml" in quality


def main() -> None:
    check_data_dir_layout()
    check_docker_gateway_security()
    check_wiring_and_compose()
    print("issue #202 Docker foundation guard: OK")


if __name__ == "__main__":
    main()
