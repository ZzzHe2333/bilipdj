# BiliPDJ 语言类插件

BiliPDJ Plugin API v1 支持 `type: "language"` 的纯资源插件。语言插件不会执行 Python 或 JavaScript，也不能申请网络、文件、子进程或 secrets 权限；启用后只向 Windows / Web 界面提供翻译表。

## 内置语言

- `zh-CN`（简体中文）：默认语言，首次启动时唯一活动语言。
- `en-US`（English）：随 Windows / Web 程序一起打包，但默认**不加载、不启用**；用户切换到 English 后才读取英语翻译表。

`zh-CN` 与 `en-US` 都属于 BiliPDJ 保留的内置语言代码，外部 `.bilipdj-plugin` 不能覆盖这两个代码。

## 设计原则

- 任意时刻**必须且只能有一个活动语言**。
- 选择某个外部语言插件时，会自动停用其他语言插件；切换到内置中文或英语时，所有外部语言插件都会停用。
- 如果当前外部语言包被删除、损坏或校验失败，系统自动收敛回 `zh-CN`。
- 翻译表直接使用**当前中文界面文字作为 key**，因此语言包可以覆盖现有界面，不要求核心代码为每段文字单独维护 i18n key。
- 找不到翻译时自动显示原中文，不会出现空白文字。
- Windows 与 Web 共用 `language.json` 中的当前语言设置。
- Windows 会翻译 Tk/ttk 控件、Notebook 标签和 Treeview 表头；Web 控制台通过 MutationObserver 翻译现有和后续动态创建的文字节点及 placeholder/title/aria-label。

## 外部语言包结构

```text
ja-jp.bilipdj-plugin
├─ manifest.json
└─ translations.json
```

示例 `manifest.json`：

```json
{
  "schema": 1,
  "id": "example.ja-jp.language",
  "name": "Japanese UI language pack",
  "version": "1.0.0",
  "plugin_api": 1,
  "type": "language",
  "platform": "language",
  "runtime": "resource",
  "entry": "translations.json",
  "language": "ja-JP",
  "language_name": "日本語",
  "translations": "translations.json",
  "min_bilipdj_version": "3.0.0",
  "permissions": [],
  "capabilities": ["ui_translation"],
  "files": {
    "translations.json": "<translations.json 的 SHA-256>"
  }
}
```

`language` 使用规范 BCP-47 形式，例如 `ja-JP`、`ko-KR`、`fr-FR`。`zh-CN` 与 `en-US` 已由程序内置，不能由外部语言包覆盖。

示例 `translations.json`：

```json
{
  "运行日志": "ランタイムログ",
  "当前排队": "現在のキュー",
  "设置": "設定",
  "插件管理": "プラグイン管理",
  "安装插件": "プラグインをインストール",
  "刷新": "更新"
}
```

也可以使用：

```json
{
  "translations": {
    "运行日志": "ランタイムログ",
    "设置": "設定"
  }
}
```

## 使用

1. 简体中文与 English 无需安装，都会随程序提供；首次启动保持简体中文。
2. 如需第三方语言，将语言包打包为 `.bilipdj-plugin`，在“设置 → 插件管理”安装；未签名包仍需要明确确认来源可信。
3. Windows 在插件管理页的“界面语言”下拉框中选择语言；Web 控制台顶部使用同一语言选择器。
4. 选择新语言时系统自动保证其他语言不再处于启用状态。
5. 切回“简体中文 (zh-CN)”即可恢复原始中文。

语言选择保存在 `language.json`，并纳入统一设置备份。
