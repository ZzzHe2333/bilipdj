# Plugin `config_schema`

BiliPDJ Plugin API v1 allows an external `.bilipdj-plugin` to declare an optional `config_schema` in `manifest.json`. Plugins without it keep the legacy platform-level configuration behavior.

Supported root shape:

```json
{
  "config_schema": {
    "type": "object",
    "title": "Kuaishou settings",
    "description": "Connection settings for this plugin.",
    "additionalProperties": false,
    "required": ["room_id"],
    "properties": {
      "room_id": {
        "type": "string",
        "title": "Room ID",
        "minLength": 1,
        "maxLength": 100
      },
      "retry": {
        "type": "integer",
        "title": "Retry count",
        "default": 3,
        "minimum": 0,
        "maximum": 10
      },
      "mode": {
        "type": "string",
        "enum": ["live", "test"],
        "default": "live"
      },
      "access_token": {
        "type": "string",
        "title": "Access token",
        "secret": true,
        "maxLength": 4096
      }
    }
  }
}
```

Supported field types are `string`, `integer`, `number`, and `boolean`. Supported annotations and constraints are `title`, `description`, `default`, `enum`, `minimum`, `maximum`, `minLength`, `maxLength`, and `secret`. `required` is supported at the root. Nested objects and arrays are intentionally not part of this version.

A field with `secret: true` must be a string, cannot declare a default, and requires the plugin manifest to request the `secrets` permission. The Web UI never returns the current secret value to the browser; a blank secret input preserves the stored value, while the explicit clear checkbox removes it.

Plugin-owned configuration is stored separately from the plugin package in `plugins/.plugin-config.json`. Reinstalling/upgrading a plugin keeps its configuration. Uninstalling the plugin removes its configuration entry. Each plugin runtime receives only the configuration selected by its own plugin record through `PluginContext.get_config()` / JavaScript `host.getConfig()`.

Local management endpoints:

```text
GET  /api/plugins/config?id=<plugin-id>
POST /api/plugins/config
GET  /plugin-config
POST /plugin-config
```

The JSON POST endpoint accepts `{"id":"...","values":{...},"unset":[...]}` and uses the same local browser management safety boundary as the rest of the plugin manager. `/plugin-config` is the server-rendered UI used by Web Control and uses a process-local CSRF token.
