# BiliPDJ 语言类插件

BiliPDJ Plugin API v1 支持 `type: "language"` 的纯资源插件。语言插件不会执行 Python 或 JavaScript，也不能申请网络、文件、子进程或 secrets 权限；启用后只向 Windows / Web 界面提供翻译表。

## 设计原则

- 内置默认语言始终为 `zh-CN`（简体中文）。
- 翻译表直接使用**当前中文界面文字作为 key**，因此语言包可以覆盖现有界面，不要求核心代码为每段文字单独维护 i18n key。
- 找不到翻译时自动显示原中文，不会出现空白文字。
- 同一个 BCP-47 语言代码同时只能启用一个语言插件。
- Windows 与 Web 共用 `language.json` 中的当前语言设置。
- Windows 会翻译 Tk/ttk 控件、Notebook 标签和 Treeview 表头；Web 控制台通过 MutationObserver 翻译现有和后续动态创建的文字节点及 placeholder/title/aria-label。

## 包结构

```text
en-us.bilipdj-plugin
├─ manifest.json
└─ translations.json
```

示例 `manifest.json`：

```json
{
  "schema": 1,
  "id": "example.en-us.language",
  "name": "English UI language pack",
  "version": "1.0.0",
  "plugin_api": 1,
  "type": "language",
  "platform": "language",
  "runtime": "resource",
  "entry": "translations.json",
  "language": "en-US",
  "language_name": "English",
  "translations": "translations.json",
  "min_bilipdj_version": "3.0.0",
  "permissions": [],
  "capabilities": ["ui_translation"],
  "files": {
    "translations.json": "<translations.json 的 SHA-256>"
  }
}
```

`language` 使用规范 BCP-47 形式，例如 `en-US`、`ja-JP`、`ko-KR`。`zh-CN` 是内置默认语言，不能由外部语言包覆盖。

示例 `translations.json`：

```json
{
  "运行日志": "Runtime Logs",
  "当前排队": "Current Queue",
  "设置": "Settings",
  "插件管理": "Plugin Manager",
  "安装插件": "Install Plugin",
  "刷新状态": "Refresh Status",
  "启用插件": "Enable Plugin",
  "禁用插件": "Disable Plugin"
}
```

也可以使用：

```json
{
  "translations": {
    "运行日志": "Runtime Logs",
    "设置": "Settings"
  }
}
```

## 使用

1. 将语言包打包为 `.bilipdj-plugin`。
2. 在“设置 → 插件管理”安装语言包；未签名包仍需要明确确认来源可信。
3. 启用语言插件。
4. Windows 在插件管理页的“界面语言”下拉框中选择语言；Web 控制台顶部会出现同一语言选择器。
5. 切回“简体中文 (zh-CN)”即可恢复原始中文。

语言选择保存在 `language.json`，并纳入统一设置备份。
