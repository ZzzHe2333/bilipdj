# BiliPDJ Web

Web 管理/展示端的唯一源码目录。项目 Web UI 使用原生 HTML/CSS/JavaScript，不依赖 Node。

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

## 独立构建

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
