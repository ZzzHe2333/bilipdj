# 弹幕排队姬（Bilipdj）

弹幕排队姬是一个面向 Bilibili 直播间的弹幕排队管理工具。  
当前版本采用 **Python 后端统一处理排队逻辑**，前端页面只负责展示，避免前后端重复算队列导致的不一致问题。

## 核心功能

- 弹幕入队、取消、修改、删除等排队命令统一由后端处理
- 权限体系：`super_admin` / `admin` / `jianzhang` / `member` / `blacklist`
- 黑名单机制：黑名单用户无法触发任何指令，并在日志中明确记录拦截
- 功能开关体系：支持总开关与分项开关（官服/B服/超级/米服等）
- 排队存档：支持 `1~10` 槽位，支持切换、恢复、清空
- 每条排队记录保留独立的“最后操作时间”
- GUI 控制台：日志、当前排队、黑名单、设置、权限、开关、样式等标签页
- 透明弹窗（无边框）：支持任务栏显示、OBS 窗口捕获、拖拽缩放，仅显示”ID + 事情”；置顶 / 关闭由主控制台统一管控

## 目录结构（主要）

- `core/server.py`：后端服务与弹幕解析、指令处理
- `core/control_panel.py`：桌面 GUI 控制面板
- `core/ui/`：Web 展示页（`index`/`config`）
- `core/config.yaml`：主配置
- `core/quanxian.yaml`：权限配置
- `core/kaiguan.yaml`：功能开关配置
- `core/cd/`：排队/黑名单存档数据
- `bilipdj_onedir.spec`：PyInstaller 打包配置

## 快速启动（源码运行）

1. 进入项目目录（本 README 所在目录）。
2. 安装依赖（至少需要 `Pillow`、`qrcode`、`brotli`、`psutil`）。
3. 启动 GUI：

```bash
python core/control_panel.py
```

启动后可在 GUI 顶部按钮打开：

- 配置页（扫码登录）：`http://127.0.0.1:9816/config`，也可在设置页 Cookie 字段旁点击「获取」直接打开
- 展示页：`http://127.0.0.1:9816/index`

透明弹窗（OBS 捕获）：在 OBS 中添加「窗口捕获」，按标题 **排队透明弹窗** 选择窗口，勾选「允许透明」即可获得透明叠加效果。

## 打包说明（Windows）

项目包含两个 spec：

| spec | 说明 | 产物 |
|------|------|------|
| `bilipdj_onedir.spec` | 主程序（onedir） | `dist\bilipdj\bilipdj.exe` |
| `paiduijitm.spec` | 透明弹窗独立进程（onefile） | `dist\paiduijitm.exe` |

- 软件名称：`弹幕排队姬`
- 打包图标：`core/256x.ico`（256x256）

打包时需依次执行两个 spec，并将 `paiduijitm.exe` 放至主程序同级目录：

```bash
pyinstaller --noconfirm --clean bilipdj_onedir.spec
pyinstaller --noconfirm --clean paiduijitm.spec
copy dist\paiduijitm.exe dist\bilipdj\paiduijitm.exe
```

CI（`package-windows-x64.yml`）会自动完成上述步骤并打包成 zip 发布。

## 配置说明（简要）

- `core/config.yaml`：直播间、日志、存档槽位、UI 参数等
- `core/quanxian.yaml`：权限与黑名单
- `core/kaiguan.yaml`：命令开关和排队总开关

说明：

- 关闭 `kaiguan.paidui` 后，除管理员命令外其余排队指令不处理
- 恢复排队功能可重新开启总开关

## 运行数据与日志

- 日志目录：`log/`
- 存档目录：`core/cd/`

建议把 `core/cd/` 下的运行时数据作为本地状态管理，不要误提交个人直播数据。

## 许可证

本项目使用仓库内 `LICENSE` 文件所示许可协议。
