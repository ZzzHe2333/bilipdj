from __future__ import annotations

import datetime as dt
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any

DEFAULT_RETENTION_DAYS = 31
LOG_PREFIXES = ("common_", "error_", "update_")


def application_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]


def log_dir(app_dir: Path | None = None) -> Path:
    return (Path(app_dir) if app_dir is not None else application_dir()) / "log"


def _safe_room_token(value: Any) -> str:
    text = str(value or "").strip()
    if not text or text in {"0", "None", "null"}:
        return "unknow"
    normalized = re.sub(r"[^0-9A-Za-z_-]+", "-", text).strip("-_")
    return normalized[:64] or "unknow"


def room_token_from_config(config: Any) -> str:
    raw = config if isinstance(config, dict) else {}
    platform = str(raw.get("platform", "bilibili") or "bilibili").strip().lower()
    if platform == "douyin":
        douyin = raw.get("douyin", {}) if isinstance(raw.get("douyin", {}), dict) else {}
        live_info = douyin.get("live_info", {}) if isinstance(douyin.get("live_info", {}), dict) else {}
        for value in (douyin.get("live_id"), live_info.get("room_id"), live_info.get("user_unique_id")):
            token = _safe_room_token(value)
            if token != "unknow":
                return token
        return "unknow"
    bilibili = raw.get("bilibili", raw.get("api", {}))
    if not isinstance(bilibili, dict):
        bilibili = {}
    return _safe_room_token(bilibili.get("roomid"))


def room_token_from_config_file(app_dir: Path | None = None) -> str:
    root = Path(app_dir) if app_dir is not None else application_dir()
    candidates = (root / "config.yaml", root / "core" / "config.yaml")
    text = ""
    for path in candidates:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        if text.strip():
            break
    if not text:
        return "unknow"
    platform_match = re.search(r"(?m)^platform:\s*([^#\r\n]+)", text)
    platform = str(platform_match.group(1) if platform_match else "bilibili").strip().strip('"\'').lower()
    if platform == "douyin":
        for pattern in (
            r"(?m)^\s{2}live_id:\s*([^#\r\n]+)",
            r"(?m)^\s{4}room_id:\s*([^#\r\n]+)",
        ):
            match = re.search(pattern, text)
            if match:
                token = _safe_room_token(match.group(1).strip().strip('"\''))
                if token != "unknow":
                    return token
        return "unknow"
    match = re.search(r"(?m)^\s{2}roomid:\s*([^#\r\n]+)", text)
    return _safe_room_token(match.group(1).strip().strip('"\'')) if match else "unknow"


def daily_log_path(kind: str, room_token: str, app_dir: Path | None = None, *, when: dt.datetime | None = None) -> Path:
    normalized_kind = str(kind or "common").strip().lower()
    if normalized_kind not in {"common", "error", "update"}:
        raise ValueError(f"未知日志类型：{kind}")
    current = when or dt.datetime.now()
    directory = log_dir(app_dir)
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{normalized_kind}_{current.strftime('%Y%m%d')}_{_safe_room_token(room_token)}.log"


def cleanup_logs(app_dir: Path | None = None, retention_days: int = DEFAULT_RETENTION_DAYS) -> int:
    days = max(1, int(retention_days or DEFAULT_RETENTION_DAYS))
    directory = log_dir(app_dir)
    directory.mkdir(parents=True, exist_ok=True)
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)
    deleted = 0
    for path in directory.glob("*.log"):
        if not path.name.startswith(LOG_PREFIXES):
            # Also clean legacy backend/update logs so migration does not leave them forever.
            if not (path.name.startswith("backend_") or path.name.endswith("-update.log")):
                continue
        try:
            modified = dt.datetime.fromtimestamp(path.stat().st_mtime, dt.timezone.utc)
            if modified < cutoff:
                path.unlink(missing_ok=True)
                deleted += 1
        except OSError:
            continue
    return deleted


class _BelowErrorFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno < logging.ERROR


class _AtLeastErrorFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno >= logging.ERROR


def _utf8_stream_handler() -> logging.StreamHandler[Any]:
    handler = logging.StreamHandler()
    try:
        handler.stream = open(
            handler.stream.fileno(),
            mode="w",
            encoding="utf-8",
            buffering=1,
            closefd=False,
        )
    except Exception:
        pass
    return handler


def configure_backend_logging(module: Any, config: dict[str, Any]) -> logging.Logger:
    directory = Path(getattr(module, "LOG_DIR", log_dir()))
    directory.mkdir(parents=True, exist_ok=True)
    log_cfg = config.get("logging", {}) if isinstance(config, dict) else {}
    if not isinstance(log_cfg, dict):
        log_cfg = {}
    to_int = getattr(module, "_to_int", lambda value, default: int(value or default))
    retention_days = max(1, to_int(log_cfg.get("retention_days", DEFAULT_RETENTION_DAYS), DEFAULT_RETENTION_DAYS))
    cleanup_logs(directory.parent, retention_days)
    level_name = str(log_cfg.get("level", "INFO") or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    room = room_token_from_config(config)
    common_path = daily_log_path("common", room, directory.parent)
    error_path = daily_log_path("error", room, directory.parent)

    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass
    root.setLevel(level)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    common_handler = logging.FileHandler(common_path, mode="a", encoding="utf-8")
    common_handler.setLevel(level)
    common_handler.addFilter(_BelowErrorFilter())
    common_handler.setFormatter(formatter)

    error_handler = logging.FileHandler(error_path, mode="a", encoding="utf-8")
    error_handler.setLevel(logging.ERROR)
    error_handler.addFilter(_AtLeastErrorFilter())
    error_handler.setFormatter(formatter)

    stream_handler = _utf8_stream_handler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    root.addHandler(common_handler)
    root.addHandler(error_handler)
    root.addHandler(stream_handler)

    logger = logging.getLogger("danmuji.backend")
    logger.info("正常日志文件：%s", common_path)
    logger.info("错误日志文件：%s", error_path)
    logger.info("日志自动清理：保留最近 %s 天", retention_days)
    return logger


def append_update_log(app_dir: Path, message: str, *, room_token: str | None = None) -> Path:
    root = Path(app_dir)
    token = room_token or room_token_from_config_file(root)
    path = daily_log_path("update", token, root)
    timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"[{timestamp}] {message}\n")
    return path


def patch_server_logging(module: Any) -> bool:
    if module is None or not callable(getattr(module, "setup_logging", None)):
        return False
    defaults = getattr(module, "DEFAULT_CONFIG", None)
    if isinstance(defaults, dict):
        logging_defaults = defaults.setdefault("logging", {})
        if isinstance(logging_defaults, dict):
            logging_defaults["retention_days"] = DEFAULT_RETENTION_DAYS
    current = getattr(module, "setup_logging")
    if bool(getattr(current, "_bilipdj_categorized_logging", False)):
        return True

    def setup_logging(config: dict[str, Any]) -> logging.Logger:
        return configure_backend_logging(module, config)

    setattr(setup_logging, "_bilipdj_categorized_logging", True)
    setattr(module, "setup_logging", setup_logging)
    return True


__all__ = [
    "DEFAULT_RETENTION_DAYS",
    "append_update_log",
    "cleanup_logs",
    "configure_backend_logging",
    "daily_log_path",
    "patch_server_logging",
    "room_token_from_config",
    "room_token_from_config_file",
]
