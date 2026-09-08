# Core 文档与兼容层索引

`core/` 目前承担两类职责：

1. 保存旧导入/旧运行路径所需的轻量兼容层与兼容数据位置；
2. 集中存放项目说明、使用教程、更新历史、发行说明和 AI 修改上下文。

> 业务实现仍以 `apps/server/`、`apps/windows/`、`apps/web/` 为准。`core/` 中的兼容 Python 文件不应重新发展成第二套业务实现。

## 文档索引

| 文档 | 用途 |
|---|---|
| [GUIDE.md](./GUIDE.md) | 用户使用教程、平台接入与常见问题 |
| [UPDATE.md](./UPDATE.md) | 历史更新日志 |
| [RELEASE_NOTES.md](./RELEASE_NOTES.md) | 当前版本发行说明，GitHub Release 工作流直接读取 |
| [CONTRIBUTORS.md](./CONTRIBUTORS.md) | 项目维护者与贡献说明 |
| [ai.md](./ai.md) | AI / 自动化工具的仓库结构、边界与修改流程 |

## 主要入口

- [项目 README](../README.md)
- [API 契约](../packages/shared/api-contract.md)
- [Server](../apps/server/)
- [Windows](../apps/windows/)
- [Web](../apps/web/)
- [GitHub Releases](https://github.com/ZzzHe2333/bilipdj/releases)

## 路径约定

从 2026-09-08 起，上述 5 个 Markdown 文档统一位于 `core/`。根目录 `README.md` 应通过 `./core/...` 链接到这些文件；构建或 Release workflow 若需要 `RELEASE_NOTES.md`，应从 `core/RELEASE_NOTES.md` 读取。
