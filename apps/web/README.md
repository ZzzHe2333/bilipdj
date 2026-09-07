# BiliPDJ Web

Web 管理/展示端的独立源目录。当前项目 Web UI 是原生 HTML/CSS/JavaScript，因此不引入 Node 依赖。

## 开发

源文件位于 `apps/web/static/`。后端独立入口会默认直接读取这里的资源：

```bash
python -m apps.server.main
```

## 独立构建

```bash
python apps/web/build.py
```

产物位于：

```text
apps/web/dist/
```

可以把该目录交给静态服务器，也可以让后端通过 `--web-dir apps/web/dist` 加载。

Windows PyInstaller 打包时也从本目录取 Web 资源，再放入兼容的 `core/ui` 运行时路径，因此 Windows 和 Web 源码不再共用同一个物理目录。
