"""Server-rendered dynamic plugin configuration UI for Issue #136."""
from __future__ import annotations

import html
import json
import secrets
from http import HTTPStatus
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

MAX_FORM_BYTES = 128 * 1024


def _escape(value: Any) -> str:
    return html.escape(str(value if value is not None else ""), quote=True)


def _write_html(handler: Any, text: str, status: HTTPStatus = HTTPStatus.OK) -> None:
    body = text.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("X-Frame-Options", "SAMEORIGIN")
    handler.send_header("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'self'")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _read_form(handler: Any, pm: Any) -> dict[str, list[str]]:
    media_type = str(handler.headers.get("Content-Type", "") or "").split(";", 1)[0].strip().lower()
    if media_type != "application/x-www-form-urlencoded":
        raise pm.PluginError("plugin config form requires application/x-www-form-urlencoded")
    try:
        length = int(handler.headers.get("Content-Length", "0") or 0)
    except (TypeError, ValueError) as exc:
        raise pm.PluginError("invalid Content-Length") from exc
    if length <= 0 or length > MAX_FORM_BYTES:
        raise pm.PluginError("plugin config form body size is invalid")
    raw = handler.rfile.read(length).decode("utf-8", errors="strict")
    return parse_qs(raw, keep_blank_values=True, strict_parsing=False)


def _plugin_choices(manager: Any) -> list[dict[str, Any]]:
    result = []
    for item in manager.list_public():
        if item.get("source") == "external" and item.get("configurable"):
            result.append(item)
    return result


def _field_name(field: str) -> str:
    return "field__" + field


def _render_field(field: str, spec: dict[str, Any], value: Any, is_set: bool, required: bool) -> str:
    title = _escape(spec.get("title") or field)
    description = _escape(spec.get("description") or "")
    name = _escape(_field_name(field))
    label = f"{title}{' *' if required else ''}"
    enum = spec.get("enum")
    if isinstance(enum, list) and enum:
        options = []
        for index, item in enumerate(enum):
            selected = " selected" if value is not None and type(item) is type(value) and item == value else ""
            options.append(f'<option value="{index}"{selected}>{_escape(item)}</option>')
        control = f'<select name="{name}">' + "".join(options) + "</select>"
    elif spec.get("type") == "boolean":
        checked = " checked" if bool(value) else ""
        control = f'<input type="checkbox" name="{name}" value="1"{checked}>'
    elif spec.get("type") in {"integer", "number"}:
        attrs = []
        if "minimum" in spec:
            attrs.append(f'min="{_escape(spec["minimum"])}"')
        if "maximum" in spec:
            attrs.append(f'max="{_escape(spec["maximum"])}"')
        step = "1" if spec.get("type") == "integer" else "any"
        shown = "" if value is None else _escape(value)
        control = f'<input type="number" name="{name}" step="{step}" value="{shown}" {" ".join(attrs)}>'
    else:
        secret = bool(spec.get("secret", False))
        attrs = []
        if "minLength" in spec:
            attrs.append(f'minlength="{int(spec["minLength"])}"')
        if "maxLength" in spec:
            attrs.append(f'maxlength="{int(spec["maxLength"])}"')
        input_type = "password" if secret else "text"
        shown = "" if secret or value is None else _escape(value)
        placeholder = ' placeholder="已保存，留空保持不变"' if secret and is_set else ""
        control = f'<input type="{input_type}" name="{name}" value="{shown}"{placeholder} {" ".join(attrs)}>'
        if secret and is_set:
            clear_name = _escape("clear__" + field)
            control += f'<label class="clear"><input type="checkbox" name="{clear_name}" value="1"> 清除已保存值</label>'
    hint = f'<small>{description}</small>' if description else ""
    return f'<label class="field"><span>{label}</span>{control}{hint}</label>'


def _render_page(manager: Any, selected_id: str, csrf_token: str, message: str = "", error: bool = False) -> str:
    choices = _plugin_choices(manager)
    ids = [str(item.get("id", "")) for item in choices]
    if selected_id not in ids:
        selected_id = ids[0] if ids else ""
    nav = "".join(
        f'<a class="plugin {"active" if item.get("id") == selected_id else ""}" href="/plugin-config?id={quote(str(item.get("id", "")))}">'
        f'{_escape(item.get("name") or item.get("id"))}<small>{_escape(item.get("id"))}</small></a>'
        for item in choices
    ) or '<p class="empty">没有安装声明 config_schema 的第三方插件。</p>'

    form = ""
    if selected_id:
        data = manager.get_plugin_config(selected_id, public=True)
        schema = data.get("schema") or {}
        values = data.get("values") or {}
        secret_fields = data.get("secret_fields") or {}
        required = set(schema.get("required") or [])
        fields = "".join(
            _render_field(field, spec if isinstance(spec, dict) else {}, values.get(field), bool(secret_fields.get(field)), field in required)
            for field, spec in (schema.get("properties") or {}).items()
        )
        form = (
            f'<section class="panel"><h2>{_escape(schema.get("title") or selected_id)}</h2>'
            f'<p>{_escape(schema.get("description") or "配置保存后会自动重连对应弹幕 Relay。")}</p>'
            f'<form method="post" action="/plugin-config">'
            f'<input type="hidden" name="csrf" value="{_escape(csrf_token)}">'
            f'<input type="hidden" name="id" value="{_escape(selected_id)}">'
            f'<div class="grid">{fields}</div><button type="submit">保存配置并重连</button></form></section>'
        )
    message_html = f'<div class="message {"error" if error else "ok"}">{_escape(message)}</div>' if message else ""
    return f'''<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>插件动态配置</title><style>
:root{{color-scheme:light dark;font-family:system-ui,-apple-system,"Segoe UI",sans-serif}}body{{margin:0;padding:18px;background:transparent;color:CanvasText}}h1{{font-size:20px;margin:0 0 6px}}h2{{font-size:17px;margin:0 0 6px}}p{{opacity:.72;margin:4px 0 14px}}.layout{{display:grid;grid-template-columns:minmax(180px,240px) 1fr;gap:14px}}.nav,.panel{{border:1px solid rgba(127,127,127,.3);border-radius:12px;padding:12px}}.plugin{{display:block;padding:9px 10px;border-radius:8px;text-decoration:none;color:inherit;margin-bottom:6px}}.plugin:hover,.plugin.active{{background:rgba(127,127,127,.15)}}.plugin small{{display:block;opacity:.6;margin-top:2px}}.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px}}.field{{display:flex;flex-direction:column;gap:5px}}input,select{{box-sizing:border-box;width:100%;padding:8px;border:1px solid rgba(127,127,127,.35);border-radius:7px;background:Canvas;color:CanvasText}}input[type=checkbox]{{width:auto}}small{{opacity:.65}}.clear{{font-size:12px;opacity:.75}}button{{margin-top:14px;padding:9px 14px;border:0;border-radius:8px;cursor:pointer}}.message{{margin:0 0 12px;padding:9px 11px;border-radius:8px}}.ok{{background:rgba(40,160,80,.13)}}.error{{background:rgba(210,50,70,.13)}}.empty{{font-size:13px}}@media(max-width:700px){{.layout{{grid-template-columns:1fr}}}}
</style></head><body><h1>插件动态配置</h1><p>字段由插件 manifest 的 config_schema 动态生成；敏感字段不会明文回显。</p>{message_html}<div class="layout"><nav class="nav">{nav}</nav>{form}</div></body></html>'''


def _typed_form_values(data: dict[str, list[str]], schema: dict[str, Any], pm: Any) -> tuple[dict[str, Any], list[str]]:
    values: dict[str, Any] = {}
    unset: list[str] = []
    for field, raw_spec in (schema.get("properties") or {}).items():
        spec = raw_spec if isinstance(raw_spec, dict) else {}
        name = _field_name(field)
        kind = str(spec.get("type") or "string")
        if data.get("clear__" + field):
            unset.append(field)
            continue
        raw = data.get(name, [""])[0]
        if bool(spec.get("secret", False)) and raw == "":
            continue
        enum = spec.get("enum")
        if isinstance(enum, list) and enum:
            try:
                values[field] = enum[int(raw)]
            except (ValueError, IndexError) as exc:
                raise pm.PluginError(f"invalid enum selection for {field}") from exc
        elif kind == "boolean":
            values[field] = name in data
        elif kind == "integer":
            if raw == "":
                unset.append(field)
            else:
                try:
                    values[field] = int(raw)
                except ValueError as exc:
                    raise pm.PluginError(f"plugin config field '{field}' must be integer") from exc
        elif kind == "number":
            if raw == "":
                unset.append(field)
            else:
                try:
                    values[field] = float(raw)
                except ValueError as exc:
                    raise pm.PluginError(f"plugin config field '{field}' must be number") from exc
        else:
            values[field] = raw
    return values, unset


def install_plugin_config_web(server_module: Any, pm: Any) -> bool:
    if bool(getattr(server_module, "_plugin_config_web_installed", False)):
        return True
    manager = getattr(server_module, "plugin_manager", None)
    registry = getattr(server_module, "danmu_plugin_registry", None)
    if manager is None or registry is None or not hasattr(manager, "get_plugin_config"):
        raise RuntimeError("plugin config schema support must be installed before config UI")
    csrf_token = secrets.token_urlsafe(32)
    handler_class = server_module.ApiHandler
    original_get = handler_class.do_GET
    original_post = handler_class.do_POST

    def do_GET(self: Any) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path != "/plugin-config":
            return original_get(self)
        if not self._require_loopback():
            return
        selected = str(parse_qs(parsed.query).get("id", [""])[0] or "").strip().lower()
        try:
            _write_html(self, _render_page(manager, selected, csrf_token))
        except pm.PluginError as exc:
            _write_html(self, _render_page(manager, "", csrf_token, str(exc), True), HTTPStatus.BAD_REQUEST)

    def do_POST(self: Any) -> None:  # noqa: N802
        if urlparse(self.path).path != "/plugin-config":
            return original_post(self)
        if not self._require_loopback():
            return
        selected = ""
        try:
            data = _read_form(self, pm)
            if not secrets.compare_digest(str(data.get("csrf", [""])[0]), csrf_token):
                raise pm.PluginError("invalid plugin config CSRF token")
            selected = str(data.get("id", [""])[0] or "").strip().lower()
            current = manager.get_plugin_config(selected, public=True)
            schema = current.get("schema") or {}
            values, unset = _typed_form_values(data, schema, pm)
            manager.save_plugin_config(selected, values, unset)
            pm._reconcile_runtime(server_module, self.server, registry)
            _write_html(self, _render_page(manager, selected, csrf_token, "配置已保存并应用。"))
        except pm.PluginError as exc:
            _write_html(self, _render_page(manager, selected, csrf_token, str(exc), True), HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # noqa: BLE001
            logger = getattr(self.server, "logger", None)
            if logger is not None:
                logger.exception("plugin config form failed")
            _write_html(self, _render_page(manager, selected, csrf_token, f"保存失败：{exc}", True), HTTPStatus.INTERNAL_SERVER_ERROR)

    do_GET._issue136_plugin_config_web = True  # type: ignore[attr-defined]
    do_POST._issue136_plugin_config_web = True  # type: ignore[attr-defined]
    handler_class.do_GET = do_GET
    handler_class.do_POST = do_POST
    server_module._plugin_config_web_installed = True
    return True


__all__ = ["MAX_FORM_BYTES", "install_plugin_config_web"]
