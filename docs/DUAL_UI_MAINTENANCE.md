# BiliPDJ 双 UI 长期维护规范

> 架构决议：长期维护 **Windows Tk** 和 **Web 控制台** 两个独立前端，不计划以 WebView2 / pywebview 合并或替换现有 Tk 端。
> 决议来源：Issue #299；Web/官网优化遗留项参考 Issue #298。

## 1. 架构和职责

- **唯一业务状态源：** `apps/server/`。排队规则、权限、礼物、平台 Relay、存档、配置等行为和数据都由 Server 负责；两端调用统一 HTTP / WebSocket 接口。
- **Windows Tk：** `apps/windows/`。保留原生桌面窗口、托盘、开关、便携部署、独立更新器等桌面交互。
- **Web 控制台：** `apps/web/static/`。保留浏览器布局、响应式控件、样式实时预览、网页队列管理等交互。Web Portable 的打包入口在 `apps/web/`。
- **OBS 展示层：** 仍通过 `/index` 提供展示，不应另建独立业务状态。
- **共同契约：** `packages/shared/api-contract.md`；通用主题来自 Server 的 `appearance.json`，OBS 样式由 `style.json` 管理。

两个 UI 不要求逐像素一致，可以保留不同布局、导航和快捷键；**共有业务能力的结果、数据、安全约束与配置格式必须一致。** 一端仅有的原生功能可保留，不得因此迫使另一端实现不可用的操作。

## 2. 共同维护的基础功能

| 功能领域 | 共享约束与主要接口 | 双端维护要求 |
| --- | --- | --- |
| 直播平台配置与激活 | `/api/config`、`/api/platforms/active`、`/api/runtime-status` | 平台选择/多平台激活、异常与重连状态的含义一致；不把“当前编辑平台”误当唯一激活平台 |
| 实时队列与存档 | `/api/queue/state`、`/api/queue/insert`、`/api/queue/delete`、`/api/queue/move`、`/api/queue/update`、存档接口 | 队列顺序、手动添加、编辑、移除和切换槽位的结果一致；快捷键和拖动手势可以只属于特定 UI |
| 权限、礼物、黑名单与开关 | Server 对应 API | 共用权限检查和业务限制；两个 UI 不应绕过权限或另算插队规则 |
| 外观主题与 OBS 样式 | `/api/appearance`、`/api/appearance/profile`、`/api/style` | 导入/导出双向兼容，保存成功后另一端读取到相同主题与样式；预览模拟数据不得写入真实队列 |
| 更新与发行 | `update-manifest.json` 及 SHA-256 校验 | 两端遵循相同版本与更新来源策略；Windows 原生独立更新器与 Web 更新交互可以不同 |
| 本地数据与备份 | Server 的配置和备份接口 | 配置、凭据与运行数据不能因切换 UI 而丢失；只在本机受信任边界进行管理操作 |
| OBS / 展示入口 | `http://127.0.0.1:9816/index` | 两端都能找到并复制展示地址；浏览器源共享 Server 实时状态 |

> 此表定义**维护目标**，不是宣称当前每一项都已经实现完全对等。任何已知缺口应留在 Issue 中追踪。

## 3. 实施与验收流程

1. **先创建 Issue**：列明目标、具体实施步骤和开始时间（注明时区）。判断该需求是共用业务功能还是端专属交互。
2. **先改 Server 或共享 API（若必要）**：明确数据结构、授权、异常与兼容性，不在 Windows/Web 复制规则。
3. **列双端影响清单**：同一 Issue/PR 中分别检查 `apps/windows/` 与 `apps/web/static/`；需要分批时建立关联 Issue，明确每端的完成状态。
4. **验证双端行为**：至少包含后端 API / 共享数据一致性、Windows 入口和 GUI 相关回归、Web 构建与前端脚本检查，以及改动涉及的 Windows Tk/Web Portable 打包校验。
5. **逐端记录结果**：以“Windows Tk 已验证 / Web 已验证 / 平台专属 / 尚未实现或待实机测试”说明差异；不将单端 CI 通过描述为两端均已人工验收。
6. **按仓库规则发布**：除用户明确要求外不创建 Release；若要求发布，核对两端下载资源与 `VERSION`、Tag、manifest 和校验文件。

### 审查清单

- [ ] 功能属于共享业务还是某一 UI 的原生能力已说明
- [ ] Server/API/配置向后兼容，避免重复业务状态源
- [ ] Windows Tk 的入口、保存、异常与权限已核对
- [ ] Web 的入口、保存、异常与权限已核对
- [ ] 两端共用队列、平台状态、主题/OBS 样式一致性已核对
- [ ] 文档、测试与发布说明反映实际完成范围
- [ ] 无需升级版本或发布时，没有创建新 Tag / Release

## 4. 明确不采用的路线

**目前不进行 Tk → WebView2、pywebview 或“一个 Web UI 取代两个 UI”的迁移。** 如果以后确实需要重新评估，应另开架构 Issue，比较 Windows 运行时依赖、安装包体积、更新器、启动性能、离线使用、桌面功能及回滚能力；在获得用户明确批准前维持双端架构。

## 5. 关联

- `README.md`：产品入口、运行方式和双 UI 声明
- `core/ai.md`：架构边界与自动化工具修改要求
- `packages/shared/api-contract.md`：后端接口和安全边界
- `core/GUIDE.md`：用户使用说明
- Issue #298：现有两端 UI/官网优化中尚未闭环的工作
