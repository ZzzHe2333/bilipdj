from __future__ import annotations

import json
import re
import threading
from dataclasses import replace
from typing import Any

from . import update_client, update_ui, update_version_selector
from .update_channel import RELEASES_API, version_key

CHANNEL_ORDER = ("正式版", "公测版", "创新版", "内测版", "废弃版")
RECENT_RELEASE_LIMIT = 10
MAX_RELEASE_SCAN = 100
_RELEASE_INCREMENTAL_META: dict[str, tuple[str, str]] = {}
_RELEASE_CHANNEL_BY_TAG: dict[str, str] = {}


def _release_version(payload: dict[str, Any]) -> str:
    tag = str(payload.get("tag_name", "") or "").strip()
    return tag[1:] if tag.lower().startswith("v") else tag


def _version_suffix(version: str) -> str:
    text = str(version or "").strip().lower()
    if text.startswith("v"):
        text = text[1:]
    if "-" not in text:
        return ""
    return text.split("-", 1)[1].split("+", 1)[0]


def _base_triplet(version: str) -> tuple[int, int, int] | None:
    text = str(version or "").strip().lower().lstrip("v")
    match = re.match(r"^(\d+)\.(\d+)\.(\d+)", text)
    if not match:
        return None
    return tuple(int(part) for part in match.groups())  # type: ignore[return-value]


def _legacy_ui_release(version: str) -> bool:
    base = _base_triplet(version)
    if base is None:
        return False
    return (3, 0, 7) <= base <= (3, 0, 11) and _version_suffix(version) == "test"


def release_channel(version: str) -> str:
    suffix = _version_suffix(version)
    if _legacy_ui_release(version) or suffix in {"feiqi", "loss"}:
        return "废弃版"
    if suffix in {"gc", "g"}:
        return "公测版"
    if suffix in {"cx", "c"}:
        return "创新版"
    if suffix in {"text", "t", "dev", "test"}:
        # -test remains a compatibility alias for already-published test builds.
        # New release naming should use the explicit channel suffix table.
        return "内测版"
    return "正式版" if not suffix else "内测版"


def display_version(version: str) -> str:
    text = str(version or "").strip()
    if _legacy_ui_release(text):
        return re.sub(r"-test(?=$|\+)", "-feiqi", text, flags=re.IGNORECASE)
    return text


def is_deprecated_version(version: str) -> bool:
    return release_channel(version) == "废弃版"


def _manifest_asset_url(payload: dict[str, Any]) -> str:
    assets = payload.get("assets", [])
    if not isinstance(assets, list):
        return ""
    for raw in assets:
        if not isinstance(raw, dict):
            continue
        if str(raw.get("name", "") or "").strip() == "update-manifest.json":
            return str(raw.get("browser_download_url", "") or "").strip()
    return ""


def _read_release_list(*, timeout: float = 15.0) -> list[dict[str, Any]]:
    # RELEASES_API historically includes its own per_page parameter. Strip the
    # query first so we never send two competing per_page values to GitHub.
    base_url = RELEASES_API.split("?", 1)[0]
    url = f"{base_url}?per_page={MAX_RELEASE_SCAN}"
    with update_client._request(url, timeout=timeout) as response:  # noqa: SLF001
        try:
            payload = json.loads(response.read().decode("utf-8-sig"))
        except (AttributeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise update_client.UpdateError("GitHub Release 列表无法解析") from exc
    if not isinstance(payload, list):
        raise update_client.UpdateError("GitHub Release 列表格式无效")
    releases = [item for item in payload if isinstance(item, dict) and not bool(item.get("draft"))]
    releases.sort(key=lambda item: str(item.get("published_at") or item.get("created_at") or ""), reverse=True)
    return releases


def _record_incremental_metadata(raw: dict[str, Any], manifest: dict[str, object]) -> None:
    tag = str(raw.get("tag_name", "") or "").strip()
    if not tag:
        return
    base_version = ""
    transport = ""
    packages = manifest.get("packages")
    if isinstance(packages, dict):
        package = packages.get(update_client.WINDOWS_PACKAGE_KEY)
        if isinstance(package, dict):
            incremental = package.get("incremental")
            if isinstance(incremental, dict):
                base_version = str(incremental.get("base_version", "") or "").strip()
                transport = str(incremental.get("transport", "") or "").strip().lower()
    _RELEASE_INCREMENTAL_META[tag] = (base_version, transport)


def _release_info(raw: dict[str, Any], *, timeout: float = 15.0) -> update_client.ReleaseInfo:
    tag = str(raw.get("tag_name", "") or "").strip()
    if tag:
        _RELEASE_INCREMENTAL_META.pop(tag, None)
    manifest_url = _manifest_asset_url(raw)
    info: update_client.ReleaseInfo
    if manifest_url:
        try:
            with update_client._request(manifest_url, timeout=timeout) as response:  # noqa: SLF001
                manifest = update_client._decode_json_response(response, "更新清单")  # noqa: SLF001
            if isinstance(manifest.get("packages"), dict):
                _record_incremental_metadata(raw, manifest)
                info = update_client._release_from_manifest(manifest, source_url=manifest_url)  # noqa: SLF001
            else:
                info = update_client._release_from_github_payload(raw)  # noqa: SLF001
        except Exception:
            info = update_client._release_from_github_payload(raw)  # noqa: SLF001
    else:
        info = update_client._release_from_github_payload(raw)  # noqa: SLF001

    # update-manifest.json deliberately contains a short machine-facing summary.
    # Replace it with the complete GitHub Release body for the selected version.
    full_body = str(raw.get("body", "") or "").strip()
    page_url = str(raw.get("html_url", "") or info.page_url)
    release_name = str(raw.get("name", "") or info.name)
    if is_deprecated_version(info.version):
        warning = (
            "【废弃版本：不可使用】\n"
            "该版本处于 3.0.7～3.0.11 UI 切换阶段，已标记为废弃版本。"
            "请不要安装或降级到该版本。\n\n"
        )
        full_body = warning + (full_body or "该 Release 未提供更多说明。")
    elif not full_body:
        full_body = "该 Release 未提供更新说明。"
    return replace(info, name=release_name, body=full_body, page_url=page_url)


def fetch_release_choices(*, timeout: float = 15.0) -> list[update_client.ReleaseInfo]:
    releases = _read_release_list(timeout=timeout)
    counts = {channel: 0 for channel in CHANNEL_ORDER}
    infos: list[update_client.ReleaseInfo] = []
    errors: list[str] = []
    seen_tags: set[str] = set()
    _RELEASE_INCREMENTAL_META.clear()
    _RELEASE_CHANNEL_BY_TAG.clear()

    for raw in releases:
        tag = str(raw.get("tag_name", "") or "").strip()
        if not tag or tag in seen_tags:
            continue
        version = _release_version(raw)
        channel = release_channel(version)
        if counts[channel] >= RECENT_RELEASE_LIMIT:
            continue
        try:
            info = _release_info(raw, timeout=timeout)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{tag}: {exc}")
            continue
        seen_tags.add(tag)
        counts[channel] += 1
        _RELEASE_CHANNEL_BY_TAG[info.tag_name] = channel
        infos.append(info)
        if all(counts[item] >= RECENT_RELEASE_LIMIT for item in CHANNEL_ORDER):
            break

    if not infos:
        raise update_client.UpdateError("无法读取可用云端版本" + (f"：{'；'.join(errors)}" if errors else ""))
    return infos


def _build_version_candidates(app_dir, cloud_releases):
    candidates: list[update_version_selector.VersionCandidate] = []
    used_labels: set[str] = set()
    for release in cloud_releases or []:
        channel = release_channel(release.version)
        shown_version = display_version(release.version)
        label = f"v{shown_version}"
        if label in used_labels:
            continue
        used_labels.add(label)
        _RELEASE_CHANNEL_BY_TAG[release.tag_name] = channel
        candidates.append(update_version_selector.VersionCandidate(label=label, source="cloud", version=release.version, release=release))

    # Local backups remain available without adding a sixth release channel.
    for backup in update_version_selector.discover_local_backups(app_dir):
        base = f"本地备份 · v{backup.version} · {backup.created_at}"
        label = base
        suffix = 2
        while label in used_labels:
            label = f"{base} · #{suffix}"
            suffix += 1
        used_labels.add(label)
        candidates.append(update_version_selector.VersionCandidate(label=label, source="local", version=backup.version, backup=backup))
    return candidates


def _candidate_channel(candidate: Any) -> str:
    if getattr(candidate, "source", "") == "local":
        return ""
    release = getattr(candidate, "release", None)
    tag = str(getattr(release, "tag_name", "") or "")
    if tag in _RELEASE_CHANNEL_BY_TAG:
        return _RELEASE_CHANNEL_BY_TAG[tag]
    return release_channel(str(getattr(candidate, "version", "") or ""))


def _set_channel_version_picker(app: Any, candidates: list[update_version_selector.VersionCandidate]) -> None:
    app._update_all_candidates = list(candidates)
    on_channel_selected(app)


def on_channel_selected(app: Any) -> None:
    picker = getattr(app, "_update_version_combo", None)
    selected_channel = str(getattr(app, "update_channel_var", None).get() if hasattr(app, "update_channel_var") else "正式版")
    all_candidates = list(getattr(app, "_update_all_candidates", []) or [])
    visible = [candidate for candidate in all_candidates if candidate.source == "local" or _candidate_channel(candidate) == selected_channel]
    mapping = {candidate.label: candidate for candidate in visible}
    app._update_version_candidates = mapping
    values = tuple(mapping)
    if picker is not None:
        picker.configure(values=values, state="readonly" if values else "disabled")
    if values:
        app.update_version_var.set(values[0])
        app._selected_version_label = values[0]
        update_version_selector.on_version_selected(app)
    else:
        app.update_version_var.set(f"{selected_channel}暂无版本")
        app._selected_version_label = ""
        app._available_update = None
        update_version_selector._configure_buttons_for_candidate(app, None)  # noqa: SLF001
        update_ui._set_notes(app, f"当前未读取到“{selected_channel}”版本。")  # noqa: SLF001


def _incremental_eligible_tags(current_version: str, releases: list[update_client.ReleaseInfo]) -> set[str]:
    try:
        current_key = version_key(current_version)
    except ValueError:
        return set()
    eligible: set[str] = set()
    for release in releases:
        if is_deprecated_version(release.version):
            continue
        base_version, transport = _RELEASE_INCREMENTAL_META.get(release.tag_name, ("", ""))
        if transport != "http-range" or not base_version:
            continue
        try:
            if version_key(base_version) == current_key:
                eligible.add(release.tag_name)
        except ValueError:
            continue
    return eligible


def _operation_text(candidate_version: str, current_version: str) -> str:
    try:
        candidate_key = version_key(candidate_version)
        current_key = version_key(current_version)
    except ValueError:
        return "安装"
    if candidate_key > current_key:
        return "升级"
    if candidate_key < current_key:
        return "降级"
    return "重新安装"


def _install_candidate_button_policy() -> None:
    current = getattr(update_version_selector, "_configure_buttons_for_candidate", None)
    if callable(current) and not bool(getattr(current, "_release_channel_policy", False)):
        def configure_buttons(app: Any, candidate: Any) -> None:
            current(app, candidate)
            if getattr(app, "_update_busy", False) or candidate is None or getattr(candidate, "source", "") != "cloud":
                return
            full_button = getattr(app, "_update_full_button", None)
            incremental_button = getattr(app, "_update_incremental_button", None)
            if is_deprecated_version(str(getattr(candidate, "version", ""))):
                if full_button is not None:
                    full_button.configure(text="废弃版本", state="disabled")
                if incremental_button is not None:
                    incremental_button.configure(state="disabled")
                return
            operation = _operation_text(str(getattr(candidate, "version", "")), str(getattr(app, "_update_current_version", "")))
            if full_button is not None:
                label = {"升级": "全量升级", "降级": "全量降级", "重新安装": "重新安装"}.get(operation, "全量安装")
                full_button.configure(text=label, state="normal")
            release = getattr(candidate, "release", None)
            tag = str(getattr(release, "tag_name", "") or "")
            eligible = tag in set(getattr(app, "_incremental_eligible_tags", set()) or set())
            if incremental_button is not None:
                incremental_button.configure(state="normal" if eligible else "disabled")

        setattr(configure_buttons, "_release_channel_policy", True)
        update_version_selector._configure_buttons_for_candidate = configure_buttons  # type: ignore[attr-defined]

    current_selected = getattr(update_version_selector, "on_version_selected", None)
    if callable(current_selected) and not bool(getattr(current_selected, "_release_channel_status", False)):
        def on_version_selected(app: Any) -> None:
            current_selected(app)
            candidate = update_version_selector._selected_candidate(app)  # noqa: SLF001
            if candidate is None or candidate.source != "cloud" or candidate.release is None:
                return
            if is_deprecated_version(candidate.version):
                app.update_status_var.set(
                    f"已选择废弃版本 v{display_version(candidate.version)}。该版本无法使用，安装与增量按钮已禁用。"
                )
                update_version_selector._configure_buttons_for_candidate(app, candidate)  # noqa: SLF001
                return
            operation = _operation_text(candidate.version, str(getattr(app, "_update_current_version", "")))
            incremental = candidate.release.tag_name in set(getattr(app, "_incremental_eligible_tags", set()) or set())
            size_mb = candidate.release.zip_asset.size / 1024**2
            suffix = "增量可用" if incremental else "该目标与当前版本无兼容增量基线，仅支持全量"
            app.update_status_var.set(f"已选择 v{display_version(candidate.version)} · {operation} · 完整包约 {size_mb:.1f} MB；{suffix}。")
            update_version_selector._configure_buttons_for_candidate(app, candidate)  # noqa: SLF001

        setattr(on_version_selected, "_release_channel_status", True)
        update_version_selector.on_version_selected = on_version_selected


def _check_for_versions(app: Any, *, silent: bool = False) -> None:
    if getattr(app, "_update_busy", False):
        return
    update_ui._discard_all_prepared(app)  # noqa: SLF001
    app._available_update = None
    update_ui._set_busy(app, True)  # noqa: SLF001
    app.update_progress_var.set(0)
    app.update_status_var.set("正在读取五个版本类型，每类最近 10 个云端 Release…")
    picker = getattr(app, "_update_version_combo", None)
    channel_picker = getattr(app, "_update_channel_combo", None)
    if picker is not None:
        picker.configure(state="disabled")
    if channel_picker is not None:
        channel_picker.configure(state="disabled")

    def worker() -> None:
        releases: list[update_client.ReleaseInfo] = []
        cloud_error = ""
        try:
            releases = fetch_release_choices()
        except Exception as exc:  # noqa: BLE001
            cloud_error = str(exc)
        app._incremental_eligible_tags = _incremental_eligible_tags(str(getattr(app, "_update_current_version", "")), releases)
        candidates = _build_version_candidates(app._update_app_dir, releases)
        default_release = next((release for release in releases if release_channel(release.version) == "正式版"), releases[0] if releases else None)

        def finish() -> None:
            if hasattr(app, "update_channel_var"):
                app.update_channel_var.set("正式版")
            update_version_selector._finish_version_check(app, candidates, default_release, cloud_error, silent)  # noqa: SLF001
            if channel_picker is not None:
                channel_picker.configure(state="readonly")

        app.root.after(0, finish)

    threading.Thread(target=worker, name="bilipdj-version-catalog", daemon=True).start()


def install_release_selector() -> bool:
    if bool(getattr(update_version_selector, "_release_selector_installed", False)):
        return True
    update_version_selector.build_version_candidates = _build_version_candidates  # type: ignore[assignment]
    update_version_selector.check_for_versions = _check_for_versions  # type: ignore[assignment]
    update_version_selector._fetch_release_choices = fetch_release_choices
    update_version_selector._set_version_picker = _set_channel_version_picker  # type: ignore[attr-defined]
    update_version_selector.on_channel_selected = on_channel_selected  # type: ignore[attr-defined]
    _install_candidate_button_policy()
    update_version_selector._release_selector_installed = True
    return True


__all__ = [
    "CHANNEL_ORDER",
    "RECENT_RELEASE_LIMIT",
    "display_version",
    "fetch_release_choices",
    "install_release_selector",
    "is_deprecated_version",
    "on_channel_selected",
    "release_channel",
]
