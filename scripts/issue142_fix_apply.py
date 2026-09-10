import re
from pathlib import Path

path = Path(__file__).with_name("issue142_apply.py")
text = path.read_text(encoding="utf-8")
old_start = "GUARD_SOURCE = r'''\\\n"
new_start = 'GUARD_SOURCE = r"""\\\n'
old_end = "\n'''\nwrite(\"scripts/issue142_danmu_event_guard.py\", GUARD_SOURCE)"
new_end = '\n"""\nwrite("scripts/issue142_danmu_event_guard.py", GUARD_SOURCE)'
if text.count(old_start) != 1:
    raise SystemExit("GUARD_SOURCE start delimiter not found exactly once")
if text.count(old_end) != 1:
    raise SystemExit("GUARD_SOURCE end delimiter not found exactly once")
text = text.replace(old_start, new_start, 1).replace(old_end, new_end, 1)
text, js_count = re.subn(
    r"JS_SOURCE = b'''\\\\+\nfunction createRelay",
    "JS_SOURCE = b'''function createRelay",
    text,
    count=1,
)
if js_count != 1:
    raise SystemExit(f"JS_SOURCE leading backslash anchor matched {js_count} times")
old_docs = '    write("docs/PLUGIN_API_V1.md", docs.rstrip() + DOC_SECTION + "\\n")'
new_docs = '    write("docs/PLUGIN_API_V1.md", docs.rstrip() + DOC_SECTION.rstrip() + "\\n")'
if text.count(old_docs) != 1:
    raise SystemExit("docs EOF normalization anchor not found exactly once")
text = text.replace(old_docs, new_docs, 1)
path.write_text(text, encoding="utf-8")
Path(__file__).unlink()
print("Issue #142 staging fixes applied")
