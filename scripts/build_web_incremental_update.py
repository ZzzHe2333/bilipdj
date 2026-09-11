from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tempfile
import zlib
import zipfile
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from apps.windows.update_manifest import (  # noqa: E402
    PRESERVED_FILES,
    PRESERVED_PREFIXES,
    calculate_sha256,
    is_preserved_path,
    normalize_manifest_path,
)

PACKAGE_KEY = "web-portable-x64"


def _collect_files(root: Path) -> dict[str, dict[str, object]]:
    root = Path(root).resolve()
    result: dict[str, dict[str, object]] = {}
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix().casefold()):
        if path.is_symlink():
            raise RuntimeError(f"web incremental manifest forbids symlinks: {path}")
        if not path.is_file():
            continue
        relative = normalize_manifest_path(path.relative_to(root).as_posix())
        if is_preserved_path(relative):
            continue
        result[relative] = {
            "path": relative,
            "size": path.stat().st_size,
            "sha256": calculate_sha256(path),
        }
    return result


def _safe_extract(archive_path: Path, destination: Path) -> None:
    destination = destination.resolve()
    with zipfile.ZipFile(archive_path, "r") as archive:
        seen: set[str] = set()
        for member in archive.infolist():
            name = str(member.filename or "").replace("\\", "/")
            if not name:
                continue
            target = (destination / Path(name)).resolve()
            try:
                target.relative_to(destination)
            except ValueError as exc:
                raise RuntimeError(f"previous Web release contains unsafe path: {name}") from exc
            key = str(target).casefold()
            if key in seen:
                raise RuntimeError(f"previous Web release contains duplicate path: {name}")
            seen.add(key)
        archive.extractall(destination)


def _directories(files: dict[str, dict[str, object]]) -> list[str]:
    directories: set[str] = set()
    for relative in files:
        parent = Path(relative).parent
        while parent.as_posix() not in {"", "."}:
            directories.add(parent.as_posix().replace("\\", "/"))
            parent = parent.parent
    return sorted(directories, key=str.casefold)


def _pack_files(package_dir: Path, files: dict[str, dict[str, object]], output_pack: Path) -> None:
    output_pack.parent.mkdir(parents=True, exist_ok=True)
    output_pack.unlink(missing_ok=True)
    offset = 0
    with output_pack.open("wb") as pack:
        for relative in sorted(files, key=str.casefold):
            raw = (package_dir / Path(relative)).read_bytes()
            compressed = zlib.compress(raw, level=9)
            if len(compressed) < len(raw):
                payload, compression = compressed, "zlib"
            else:
                payload, compression = raw, "store"
            metadata = files[relative]
            metadata.update(
                {
                    "offset": offset,
                    "packed_size": len(payload),
                    "packed_sha256": hashlib.sha256(payload).hexdigest(),
                    "compression": compression,
                }
            )
            pack.write(payload)
            offset += len(payload)


def build(
    *,
    package_dir: Path,
    version: str,
    package_sha256: str,
    output_manifest: Path,
    output_pack: Path,
    previous_zip: Path | None,
    base_version: str,
) -> None:
    package_dir = package_dir.resolve()
    if not package_dir.is_dir():
        raise RuntimeError(f"package directory does not exist: {package_dir}")

    current = _collect_files(package_dir)
    if not current:
        raise RuntimeError("Web package does not contain update-managed files")
    previous: dict[str, dict[str, object]] = {}
    if previous_zip is not None and previous_zip.is_file():
        with tempfile.TemporaryDirectory(prefix="bilipdj-prev-web-release-") as temp:
            previous_root = Path(temp)
            _safe_extract(previous_zip, previous_root)
            previous = _collect_files(previous_root)

    changed = sorted(
        (path for path, metadata in current.items() if path not in previous or previous[path]["sha256"] != metadata["sha256"]),
        key=str.casefold,
    )
    removed = sorted((path for path in previous if path not in current), key=str.casefold)
    _pack_files(package_dir, current, output_pack)

    manifest = {
        "schema": 2,
        "kind": "bilipdj-file-manifest",
        "version": str(version),
        "package": PACKAGE_KEY,
        "package_sha256": str(package_sha256).strip().lower(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "directories": _directories(current),
        "files": [current[path] for path in sorted(current, key=str.casefold)],
        "incremental": {
            "base_version": str(base_version or "").strip(),
            "changed_from_base": changed,
            "removed_from_base": removed,
        },
        "excluded": {
            "files": sorted(PRESERVED_FILES),
            "prefixes": list(PRESERVED_PREFIXES),
        },
    }
    output_manifest.parent.mkdir(parents=True, exist_ok=True)
    output_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    changed_bytes = sum(int(current[path]["size"]) for path in changed)
    total_bytes = sum(int(item["size"]) for item in current.values())
    print(
        "web incremental assets: "
        f"managed_files={len(current)} changed_from_base={len(changed)} removed={len(removed)} "
        f"changed_bytes={changed_bytes} total_bytes={total_bytes} "
        f"pack_bytes={output_pack.stat().st_size} base_version={base_version or 'none'}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Build BiliPDJ Web Portable per-file manifest and range pack")
    parser.add_argument("--package-dir", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--package-sha256", required=True)
    parser.add_argument("--output-manifest", type=Path, required=True)
    parser.add_argument("--output-pack", type=Path, required=True)
    parser.add_argument("--previous-zip", type=Path)
    parser.add_argument("--base-version", default="")
    args = parser.parse_args()
    build(
        package_dir=args.package_dir,
        version=args.version,
        package_sha256=args.package_sha256,
        output_manifest=args.output_manifest,
        output_pack=args.output_pack,
        previous_zip=args.previous_zip,
        base_version=args.base_version,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
