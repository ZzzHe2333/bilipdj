from __future__ import annotations

import json
import re
import threading
from dataclasses import replace
from typing import Any

from . import update_client, update_page, update_ui, update_version_selector
from .update_channel import RELEASES_API, version_key

CHANNEL_ORDER = ("发行包", "全部")
RELEASE_ONLY_LIMIT = 3
ALL_RELEASE_LIMIT = 10
RECENT_RELEASE_LIMIT = ALL_RELEASE_LIMIT  # historical import compatibility
MAX_RELEASE_SCAN = 100
_RELEASE_INCREMENTAL_META: dict[str, tuple[str, str]] = {}
_RELEASE_PRERELEASE_BY_TAG: dict[str, bool] = {}
_RELEASE_TAGS_BY_FILTER: dict[str, list[str]] = {"发行包": [], "全部": []}


def _release_version(payload: dict[str, Any]) -> str:
    tag = str(payload.get("tag_name", "") or "").strip()
    return tag[1:] if tag.lower().startswith("v") else tag


def _version_suffix(version: str) -> str:
    text = str(version or "").strip().lower().lstrip("v")
    if "-" not in text:
        return ""
    return text.split("-", 1)[1].split("+", 1)[0]


def _base_triplet(version: str) -> tuple[int, int, int] | None:
    text = str(version or "").strip().lower().lstrip("v")
    match = re.match(r"^(\d+)\.(\d+)\.(\d+)", text)
    if not match:
        return None
    return tuple(int(part) for part in match.groups())  # type: ignore[return-value]


def _legacy_deprecated_version(version: str) -> bool:
    suffix = _version_suffix(version)
    if suffix in {"feiqi", "loss"}:
        return True
    base = _base_triplet(version)
    return base is not None and (3, 0, 7) <= base <= (3, 0, 11) and suffix == "test"


def release_channel(_version: str) -> str:
    """Compatibility shim for callers from the old suffix-channel model.

    A version string can no longer determine release state. New code must read
    GitHub Release.prerelease instead; the updater exposes only 发行包 / 全部.
    """
    return "全部"


def display_version(version: str) -> str:
    return str(version or "").strip()


def is_deprecated_version(version: str) -> bool:
    return _legacy_deprecated_version(version)


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

    full_body = str(raw.get("body", "") or "").strip()
    page_url = str(raw.get("html_url", "") or info.page_url)
    release_name = str(raw.get("name", "") or info.name)
    if is_deprecated_version(info.version):
        warning = (
            "【历史废弃版本：不可使用】\n"
            "该版本处于旧 UI 切换阶段，仅保留历史记录，请不要安装或降级到该版本。\n\n"
        )
        full_body = warning + (full_body or "该 Release 未提供更多说明。")
    elif not full_body:
        full_body = "该 Release 未提供更新说明。"
    return replace(info, name=release_name, body=full_body, page_url=page_url)


def fetch_release_choices(*, timeout: float = 15.0) -> list[update_client.ReleaseInfo]:
    releases = _read_release_list(timeout=timeout)
    all_raw = releases[:ALL_RELEASE_LIMIT]
    stable_raw = [raw for raw in releases if not bool(raw.get("prerelease"))][:RELEASE_ONLY_LIMIT]

    all_tags = [str(raw.get("tag_name", "") or "").strip() for raw in all_raw]
    stable_tags = [str(raw.get("tag_name", "") or "").strip() for raw in stable_raw]
    _RELEASE_TAGS_BY_FILTER["全部"] = [tag for tag in all_tags if tag]
    _RELEASE_TAGS_BY_FILTER["发行包"] = [tag for tag in stable_tags if tag]
    _RELEASE_INCREMENTAL_META.clear()
    _RELEASE_PRERELEASE_BY_TAG.clear()

    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in [*all_raw, *stable_raw]:
        tag = str(raw.get("tag_name", "") or "").strip()
        if not tag or tag in seen:
            continue
        seen.add(tag)
        selected.append(raw)
        _RELEASE_PRERELEASE_BY_TAG[tag] = bool(raw.get("prerelease"))

    infos: list[update_client.ReleaseInfo] = []
    errors: list[str] = []
    for raw in selected:
        tag = str(raw.get("tag_name", "") or "").strip()
        try:
            infos.append(_release_info(raw, timeout=timeout))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{tag}: {exc}")

    if not infos:
        raise update_client.UpdateError("无法读取可用云端版本" + (f"：{'；'.join(errors)}" if errors else ""))
    return infos


def _build_version_candidates(app_dir, cloud_releases):
    candidates: list[update_version_selector.VersionCandidate] = []
    used_labels: set[str] = set()
    for release in cloud_releases or []:
        prerelease = bool(_RELEASE_PRERELEASE_BY_TAG.get(release.tag_name, False))
        kind = "预发行包" if prerelease else "发行包"
        label = f"{kind} · v{display_version(release.version)}"
        if label in used_labels:
            continue
        used_labels.add(label)
        candidates.append(
            update_version_selector.VersionCandidate(
                label=label,
                source="cloud",
                version=release.version,
                release=release,
            )
        )

    for backup in update_version_selector.discover_local_backups(app_dir):
        base = f"本地备份 · v{backup.version} · {backup.created_at}"
        label = base
        suffix = 2
        while label in used_labels:
            label = f"{base} · #{suffix}"
            suffix += 1
        used_labels.add(label)
        candidates.append(
            update_version_selector.VersionCandidate(
                label=label,
                source="local",
                version=backup.version,
                backup=backup,
            )
        )
    return candidates


def _set_channel_version_picker(app: Any, candidates: list[update_version_selector.VersionCandidate]) -> None:
    app._update_all_candidates = list(candidates)
    on_channel_selected(app)


def on_channel_selected(app: Any) -> None:
    picker = getattr(app, "_update_version_combo", None)
    selected_filter = str(
        getattr(app, "update_channel_var", None).get() if hasattr(app, "update_channel_var") else "发行包"
    )
    if selected_filter not in CHANNEL_ORDER:
        selected_filter = "发行包"
        if hasattr(app, "update_channel_var"):
            app.update_channel_var.set(selected_filter)

    all_candidates = list(getattr(app, "_update_all_candidates", []) or [])
    cloud_by_tag = {
        str(getattr(candidate.release, "tag_name", "") or ""): candidate
        for candidate in all_candidates
        if candidate.source == "cloud" and candidate.release is not None
    }
    visible: list[update_version_selector.VersionCandidate] = []
    for tag in _RELEASE_TAGS_BY_FILTER.get(selected_filter, []):
        candidate = cloud_by_tag.get(tag)
        if candidate is not None:
            visible.append(candidate)
    visible.extend(candidate for candidate in all_candidates if candidate.source == "local")

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
        app.update_version_var.set(f"{selected_filter}暂无版本")
        app._selected_version_label = ""
        app._available_update = None
        update_version_selector._configure_buttons_for_candidate(app, None)  # noqa: SLF001
        update_ui._set_notes(app, f"当前未读取到“{selected_filter}”版本。")  # noqa: SLF001


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
    if callable(current) and not bool(getattr(current, "_release_status_policy", False)):
        def configure_buttons(app: Any, candidate: Any) -> None:
            current(app, candidate)
            if getattr(app, "_update_busy", False) or candidate is None or getattr(candidate, "source", "") != "cloud":
                return
            full_button = getattr(app, "_update_full_button", None)
            incremental_button = getattr(app, "_update_incremental_button", None)
            if is_deprecated_version(str(getattr(candidate, "version", ""))):
                if full_button is not None:
                    full_button.configure(text="历史废弃版本", state="disabled")
                if incremental_button is not None:
                    incremental_button.configure(state="disabled")
                return
            operation = _operation_text(
                str(getattr(candidate, "version", "")),
                str(getattr(app, "_update_current_version", "")),
            )
            if full_button is not None:
                label = {"升级": "全量升级", "降级": "全量降级", "重新安装": "重新安装"}.get(operation, "全量安装")
                full_button.configure(text=label, state="normal")
            release = getattr(candidate, "release", None)
            tag = str(getattr(release, "tag_name", "") or "")
            eligible = tag in set(getattr(app, "_incremental_eligible_tags", set()) or set())
            if incremental_button is not None:
                incremental_button.configure(state="normal" if eligible else "disabled")

        setattr(configure_buttons, "_release_status_policy", True)
        update_version_selector._configure_buttons_for_candidate = configure_buttons  # type: ignore[attr-defined]

    current_selected = getattr(update_version_selector, "on_version_selected", None)
    if callable(current_selected) and not bool(getattr(current_selected, "_release_status_status", False)):
        def on_version_selected(app: Any) -> None:
            current_selected(app)
            candidate = update_version_selector._selected_candidate(app)  # noqa: SLF001
            if candidate is None or candidate.source != "cloud" or candidate.release is None:
                return
            if is_deprecated_version(candidate.version):
                app.update_status_var.set(
                    f"已选择历史废弃版本 v{display_version(candidate.version)}。该版本无法安装。"
                )
                update_version_selector._configure_buttons_for_candidate(app, candidate)  # noqa: SLF001
                return
            operation = _operation_text(candidate.version, str(getattr(app, "_update_current_version", "")))
            incremental = candidate.release.tag_name in set(getattr(app, "_incremental_eligible_tags", set()) or set())
            size_mb = candidate.release.zip_asset.size / 1024**2
            kind = "预发行包" if _RELEASE_PRERELEASE_BY_TAG.get(candidate.release.tag_name, False) else "发行包"
            suffix = "增量可用" if incremental else "该目标与当前版本无兼容增量基线，仅支持全量"
            app.update_status_var.set(
                f"已选择{kind} v{display_version(candidate.version)} · {operation} · 完整包约 {size_mb:.1f} MB；{suffix}。"
            )
            update_version_selector._configure_buttons_for_candidate(app, candidate)  # noqa: SLF001

        setattr(on_version_selected, "_release_status_status", True)
        update_version_selector.on_version_selected = on_version_selected


def _rewrite_update_page_text(frame: Any) -> None:
    replacements = {
        "云端版本按类型筛选；每类最多加载最近 10 个。废弃版仅供查看说明，禁止安装。":
            "云端版本可查看“发行包”或“全部”：发行包显示最近 3 个，全部显示最近 10 个；默认以最新发行包为更新目标。",
    }

    def walk(widget: Any) -> None:
        try:
            text = str(widget.cget("text") or "")
        except Exception:
            text = ""
        if text in replacements:
            try:
                widget.configure(text=replacements[text])
            except Exception:
                pass
        try:
            children = widget.winfo_children()
        except Exception:
            children = []
        for child in children:
            walk(child)

    walk(frame)


def _install_update_page_policy() -> None:
    update_page.RELEASE_CHANNELS = CHANNEL_ORDER
    current = getattr(update_page, "build_update_tab", None)
    if not callable(current) or bool(getattr(current, "_release_status_policy", False)):
        return

    def build_update_tab(app: Any, frame: Any, app_name: str, current_version: str, app_dir: Any) -> None:
        current(app, frame, app_name, current_version, app_dir)
        app.update_channel_var.set("发行包")
        combo = getattr(app, "_update_channel_combo", None)
        if combo is not None:
            combo.configure(values=CHANNEL_ORDER)
        _rewrite_update_page_text(frame)
        update_ui._set_notes(  # noqa: SLF001
            app,
            "点击“检查更新”加载最近 3 个发行包；切换到“全部”可查看最近 10 个 Release（含预发行包）以及完整更新说明。",
        )

    setattr(build_update_tab, "_release_status_policy", True)
    update_page.build_update_tab = build_update_tab  # type: ignore[assignment]


def _check_for_versions(app: Any, *, silent: bool = False) -> None:
    if getattr(app, "_update_busy", False):
        return
    update_ui._discard_all_prepared(app)  # noqa: SLF001
    app._available_update = None
    update_ui._set_busy(app, True)  # noqa: SLF001
    app.update_progress_var.set(0)
    app.update_status_var.set("正在读取最近 3 个发行包和最近 10 个全部 Release…")
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
        app._incremental_eligible_tags = _incremental_eligible_tags(
            str(getattr(app, "_update_current_version", "")), releases
        )
        candidates = _build_version_candidates(app._update_app_dir, releases)
        by_tag = {release.tag_name: release for release in releases}
        default_release = next(
            (by_tag[tag] for tag in _RELEASE_TAGS_BY_FILTER["发行包"] if tag in by_tag),
            None,
        )

        def finish() -> None:
            if hasattr(app, "update_channel_var"):
                app.update_channel_var.set("发行包")
            update_version_selector._finish_version_check(  # noqa: SLF001
                app, candidates, default_release, cloud_error, silent
            )
            if channel_picker is not None:
                channel_picker.configure(state="readonly")

        app.root.after(0, finish)

    threading.Thread(target=worker, name="bilipdj-version-catalog", daemon=True).start()


def install_release_selector() -> bool:
    if bool(getattr(update_version_selector, "_release_selector_installed", False)):
        return True
    _install_update_page_policy()
    update_version_selector.build_version_candidates = _build_version_candidates  # type: ignore[assignment]
    update_version_selector.check_for_versions = _check_for_versions  # type: ignore[assignment]
    update_version_selector._fetch_release_choices = fetch_release_choices
    update_version_selector._set_version_picker = _set_channel_version_picker  # type: ignore[attr-defined]
    update_version_selector.on_channel_selected = on_channel_selected  # type: ignore[attr-defined]
    _install_candidate_button_policy()
    update_version_selector._release_selector_installed = True
    return True


__all__ = [
    "ALL_RELEASE_LIMIT",
    "CHANNEL_ORDER",
    "RECENT_RELEASE_LIMIT",
    "RELEASE_ONLY_LIMIT",
    "display_version",
    "fetch_release_choices",
    "install_release_selector",
    "is_deprecated_version",
    "on_channel_selected",
    "release_channel",
]
