from pathlib import Path

path = Path(__file__).with_name("issue144_apply.py")
text = path.read_text(encoding="utf-8")
old = '''AUDIT_GUARD = r\'\'\'from __future__ import annotations\n\nimport json\nimport tempfile\nfrom pathlib import Path\n\nfrom apps.server import issue79_guard\n'''
new = '''AUDIT_GUARD = r\'\'\'from __future__ import annotations\n\nimport json\nimport sys\nimport tempfile\nfrom pathlib import Path\n\nROOT = Path(__file__).resolve().parents[1]\nif str(ROOT) not in sys.path:\n    sys.path.insert(0, str(ROOT))\n\nfrom apps.server import issue79_guard\n'''
if text.count(old) != 1:
    raise SystemExit(f"audit guard import anchor matched {text.count(old)} times")
text = text.replace(old, new, 1)
text = text.replace('''\nROOT = Path(__file__).resolve().parents[1]\nHEX = "a" * 64\n''', '''\nHEX = "a" * 64\n''', 1)
path.write_text(text, encoding="utf-8")
Path(__file__).unlink()
print("Issue #144 audit guard import path fixed")
