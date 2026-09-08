# BiliPDJ Web

Web 管理/展示端的唯一源码目录。项目 Web UI 使用原生 HTML/CSS/JavaScript，不依赖 Node。

## v2.0.1 Web 便携版

正式客户包：

```text
BiliPDJ-v2.0.1-Web-Portable-x64.zip
```

完整解压后直接运行 `BiliPDJ-Web.exe`。启动器会：

1. 读取便携目录中的 Server 配置并确定端口；
2. 检查本机对应后端是否已运行；
3. 未运行时自动以自身 `--backend` 模式启动内置 Server；
4. 等待 `/health` 就绪；
5. 自动打开 `http://127.0.0.1:<port>/config`；
6. 退出启动器时关闭它自己启动的后端。

该包已经通过 PyInstaller 内置 `apps/server` 和 `apps/web/static`，用户无需安装 Python。

本地构建 Web 便携版：

```powershell
powershell -ExecutionPolicy Bypass -File .\apps\web\package-portable.ps1 -InstallDependencies
```

## 开发

源文件位于：

```text
apps/web/static/
```

`core/ui/` 已移除，Server、Windows 打包和 Web 独立构建都读取同一套资源。

后端默认直接加载源码目录：

```bash
python -m apps.server.main
```

## 独立静态构建

```bash
python apps/web/build.py
```

产物：

```text
apps/web/dist/
```

可以把该目录交给静态服务器，也可以显式让后端加载：

```bash
python -m apps.server.main --web-dir apps/web/dist
```
