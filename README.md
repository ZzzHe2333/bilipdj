# 弹幕排队姬（Bilipdj）

弹幕排队姬是一个面向 **Bilibili / 抖音**直播间的弹幕排队管理工具。
排队逻辑由 Python 后端统一处理，前端页面只负责展示，避免前后端计算不一致。

## 核心功能

- 弹幕入队、取消、修改、管理员删除等指令统一由后端处理
- **权限体系**：`super_admin` / `admin` / `jianzhang` / `member` / `blacklist`
- **黑名单机制**：黑名单用户无法触发任何指令，拦截行为写入日志
- **功能开关**：总开关与分项开关（官服 / B 服 / 超级 / 米服 / 舰长插队等，共 9 项）
- **排队存档**：10 个槽位，支持切换、恢复、清空；每条记录保留最后操作时间
- **多平台支持**：Bilibili（WebSocket 二进制协议）、抖音（HTTPS 轮询 + Protobuf）
- **GUI 控制台**：10 个标签页，涵盖日志、队列、黑名单、设置、权限、开关、性能、样式等；暗夜/明亮双主题
- **透明弹窗**：支持 OBS 窗口捕获、拖拽移动、置顶控制

## 目录结构（主要）

```
bilipdj/
├── core/
│   ├── control_panel.py        # 桌面 GUI（主入口）
│   ├── server.py               # 后端 HTTP/WS 服务器
│   ├── bilibili_protocol.py    # Bilibili 弹幕协议
│   ├── douyin_protocol.py      # 抖音直播协议
│   ├── overlay_host.py         # OBS 透明弹窗
│   ├── config.yaml             # 主配置（运行时生成）
│   ├── quanxian.yaml           # 权限配置（运行时生成）
│   ├── kaiguan.yaml            # 功能开关（运行时生成）
│   ├── style.json              # 样式配置（运行时生成）
│   ├── ui/                     # Web 前端（index/config/cookie_login）
│   └── cd/                     # 排队存档 CSV + 状态 JSON
├── bilipdj_onedir.spec         # PyInstaller 主程序配置
├── paiduijitm.spec             # PyInstaller 透明弹窗配置（onefile）
├── package-windows-local.ps1   # Windows 本地打包脚本
├── README.md
├── UPDATE.md
└── GUIDE.md                    # 用户教学文档
```

## 快速启动（源码运行）

1. 进入 `bilipdj/` 子目录（本 README 所在位置）。
2. 安装依赖：

```bash
pip install Pillow qrcode brotli psutil pyyaml protobuf
```

3. 启动 GUI：

```bash
python core/control_panel.py
```

启动后可通过 GUI 顶部按钮访问：

- **配置页**（扫码登录）：`http://127.0.0.1:9816/config`
- **展示页**（队列看板）：`http://127.0.0.1:9816/index`

**透明弹窗（OBS 捕获）**：在 OBS 中添加「窗口捕获」，按标题 **排队透明弹窗** 选择窗口，勾选「允许透明」即可。

## 弹幕指令

### 普通用户

| 弹幕内容 | 效果 |
|---|---|
| `排队` | 加入排队 |
| `官服排` / `官服排队` | 以 `官\|昵称` 加入排队 |
| `B服排` / `排B服` | 以 `B\|昵称` 加入排队 |
| `超级排` / `超级排队` | 以 `<昵称>` 加入排队 |
| `小米排` / `排米服` | 以 `米\|昵称` 加入排队 |
| `排队 [内容]` | 以自定义内容加入排队 |
| `取消排队` | 离开排队 |
| `替换 [内容]` / `修改 [内容]` | 修改已有排队内容 |

### 管理员 / 主播

| 弹幕内容 | 效果 |
|---|---|
| `完成` / `del [ID]` | 删除队列中指定用户 |
| `add [ID] [内容]` | 在队首插入指定内容 |
| `无影插 [ID]` | 静默插队（不广播通知） |
| `暂停排队功能` | 关闭排队总开关 |
| `恢复排队功能` | 开启排队总开关 |
| `设置排队上限 [N]` | 设置最大排队人数 |

### super_admin 专属

| 弹幕内容 | 效果 |
|---|---|
| `添加管理员 [昵称]` | 提升为 admin |
| `取消管理员 [昵称]` | 降级为 member |
| `拉黑 [昵称]` | 加入黑名单 |
| `取消拉黑 [昵称]` | 移出黑名单 |

## 配置说明（简要）

- `core/config.yaml`：直播间信息、日志、存档槽位、UI 参数
- `core/quanxian.yaml`：权限名单（每行一个用户名）
- `core/kaiguan.yaml`：9 项功能开关（布尔值）
- `core/style.json`：队列展示颜色、字体、描边等

关闭 `kaiguan.paidui` 后，除管理员命令外所有排队指令不处理；
`恢复排队功能` 弹幕或在 GUI 开关页勾选可重新开启。

## 打包说明（Windows）

| spec | 说明 | 产物 |
|---|---|---|
| `bilipdj_onedir.spec` | 主程序（onedir） | `dist\bilipdj\main.exe` |
| `paiduijitm.spec` | 透明弹窗独立进程（onefile） | `dist\paiduijitm.exe` |

本地打包推荐执行脚本：

```powershell
powershell -ExecutionPolicy Bypass -File .\package-windows-local.ps1 -InstallDependencies
```

脚本会构建 `dist\bilipdj\main.exe` 并自动将 `dist\paiduijitm.exe` 复制至主程序目录。
CI（`package-windows-x64.yml`）同样调用此脚本，将 onedir 打包成 zip 发布。

## 运行数据与日志

- 日志目录：`log/`（按日期命名，保留天数可配置）
- 存档目录：`core/cd/`（10 个槽位 CSV + 状态 JSON）

建议将 `core/cd/` 和 `core/*.yaml` 加入 `.gitignore`，避免误提交个人直播数据。

## 许可证

本项目使用仓库内 `LICENSE` 文件所示许可协议。
