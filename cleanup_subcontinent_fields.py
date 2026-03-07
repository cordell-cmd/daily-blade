#!/usr/bin/env python3
"""cleanup_subcontinent_fields.py

One-off cleanup utility to remove legacy `subcontinent` fields (and the removed
`subcontinents` category) from stored JSON files.

This repo previously carried a `subcontinent` place field and a `subcontinents`
codex/lore category. The current location hierarchy no longer includes either.

By default this runs in DRY-RUN mode and prints what it would change.
Use --apply to write changes (with timestamped backups).
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from typing import Any


REMOVE_FIELD_KEY = "subcontinent"
REMOVE_CATEGORY_KEY = "subcontinents"


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _dump_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _backup_path(path: str) -> str:
    return f"{path}.bak.{_timestamp()}"


def _remove_key_recursive(obj: Any, key: str) -> int:
    """Remove dict entries matching `key` anywhere inside obj."""
    removed = 0
    if isinstance(obj, dict):
        if key in obj:
            obj.pop(key, None)
            removed += 1
        # Iterate over a snapshot since we may mutate nested objects.
        for v in list(obj.values()):
            removed += _remove_key_recursive(v, key)
    elif isinstance(obj, list):
        for it in obj:
            removed += _remove_key_recursive(it, key)
    return removed


def _remove_top_level_category(obj: Any, key: str) -> int:
    if isinstance(obj, dict) and key in obj:
        obj.pop(key, None)
        return 1
    return 0


def _process_file(path: str, apply: bool) -> tuple[bool, dict[str, int]]:
    if not os.path.exists(path):
        return False, {}

    with open(path, "r", encoding="utf-8") as f:
        original_text = f.read()

    data = json.loads(original_text)
    removed_category = _remove_top_level_category(data, REMOVE_CATEGORY_KEY)
    removed_fields = _remove_key_recursive(data, REMOVE_FIELD_KEY)

    changed = bool(removed_category or removed_fields)

    stats = {
        "removed_subcontinents_category": removed_category,
        "removed_subcontinent_fields": removed_fields,
    }

    if changed and apply:
        backup = _backup_path(path)
        # Write an exact copy of the original file first.
        with open(backup, "w", encoding="utf-8") as f:
            f.write(original_text)
        _dump_json(path, data)

    return changed, stats


def main() -> int:
    parser = argparse.ArgumentParser(description="Remove legacy subcontinent fields from stored JSON.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write changes to disk (creates .bak.<timestamp> backups).",
    )
    parser.add_argument(
        "--files",
        nargs="*",
        default=["codex.json", "lore.json", "characters.json"],
        help="Files to process (default: codex.json lore.json characters.json).",
    )
    args = parser.parse_args()

    any_changes = False
    for path in args.files:
        changed, stats = _process_file(path, apply=bool(args.apply))
        if not os.path.exists(path):
            print(f"SKIP  {path} (not found)")
            continue
        if changed:
            any_changes = True
            mode = "APPLIED" if args.apply else "DRYRUN"
            print(f"{mode} {path}: {stats}")
        else:
            print(f"OK    {path}: no legacy subcontinent data found")

    if not args.apply:
        print("\nDry run complete. Re-run with --apply to write changes.")

    return 0 if (args.apply or not any_changes) else 0


if __name__ == "__main__":
    raise SystemExit(main())
