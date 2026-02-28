#!/usr/bin/env python3

import glob
import json
import os
import re
from typing import Any, Dict, List, Optional, Set, Tuple


def normalize_title_key(value: str) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        return ""

    # Mirror the UI normalization in index.html
    raw = raw.replace("\u2019", "'").replace("\u2018", "'").replace("\u2011", "-")
    raw = raw.replace("’", "'").replace("'", "")
    raw = re.sub(r"[^a-z0-9]+", " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def iter_day_payloads() -> Dict[str, Dict[str, Any]]:
    by_date: Dict[str, Dict[str, Any]] = {}

    if os.path.exists("stories.json"):
        try:
            data = load_json("stories.json")
            d = str(data.get("date") or "").strip()
            if d and isinstance(data.get("stories"), list):
                by_date[d] = data
        except Exception:
            pass

    for path in glob.glob("archive/*.json"):
        if path.endswith("index.json"):
            continue
        try:
            data = load_json(path)
            d = str(data.get("date") or os.path.basename(path).replace(".json", "")).strip()
            if d and isinstance(data.get("stories"), list):
                by_date[d] = data
        except Exception:
            pass

    return by_date


def story_exists_on_date(by_date: Dict[str, Dict[str, Any]], date: str, title: str) -> Tuple[bool, Optional[str]]:
    day = by_date.get(date)
    if not day:
        return False, None

    want_raw = (title or "").strip().lower()
    want_key = normalize_title_key(title)

    for s in day.get("stories", []) or []:
        t = str((s or {}).get("title") or "").strip()
        if t.strip().lower() == want_raw:
            return True, t

    for s in day.get("stories", []) or []:
        t = str((s or {}).get("title") or "").strip()
        if normalize_title_key(t) == want_key:
            return True, t

    return False, None


def build_global_title_index(by_date: Dict[str, Dict[str, Any]]) -> Dict[str, Set[str]]:
    idx: Dict[str, Set[str]] = {}
    for d, payload in by_date.items():
        for s in payload.get("stories", []) or []:
            title = str((s or {}).get("title") or "").strip()
            if not title:
                continue
            key = normalize_title_key(title)
            idx.setdefault(key, set()).add(d)
    return idx


def main() -> int:
    codex = load_json("codex.json")
    categories = [k for k, v in codex.items() if isinstance(v, list)]

    by_date = iter_day_payloads()
    global_idx = build_global_title_index(by_date)

    broken: List[Dict[str, Any]] = []

    for cat in categories:
        for item in codex.get(cat, []) or []:
            entity = str((item or {}).get("name") or "").strip()
            if not entity:
                continue

            for a in (item.get("story_appearances") or []):
                d = str((a or {}).get("date") or "").strip()
                t = str((a or {}).get("title") or "").strip()
                if not d or not t:
                    continue
                ok, canon = story_exists_on_date(by_date, d, t)
                if not ok:
                    key = normalize_title_key(t)
                    suggestions = sorted(global_idx.get(key, set()))
                    broken.append(
                        {
                            "category": cat,
                            "entity": entity,
                            "link_type": "story_appearances",
                            "date": d,
                            "title": t,
                            "suggested_dates_for_title": suggestions,
                        }
                    )

            fd = str(item.get("first_date") or "").strip()
            fs = str(item.get("first_story") or "").strip()
            if fd and fs:
                ok, canon = story_exists_on_date(by_date, fd, fs)
                if not ok:
                    key = normalize_title_key(fs)
                    suggestions = sorted(global_idx.get(key, set()))
                    broken.append(
                        {
                            "category": cat,
                            "entity": entity,
                            "link_type": "first_story",
                            "date": fd,
                            "title": fs,
                            "suggested_dates_for_title": suggestions,
                        }
                    )

    out_path = "broken_story_links.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "dates_loaded": sorted(by_date.keys()),
                "broken_links": broken,
                "broken_count": len(broken),
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    print(f"dates_loaded={len(by_date)}")
    print(f"broken_links={len(broken)}")
    if broken:
        print(f"wrote {out_path}")
        for row in broken[:25]:
            sugg = row.get("suggested_dates_for_title") or []
            print(
                f"- {row['category']} | {row['entity']} | {row['link_type']} | {row['date']} | {row['title']} | sugg: {sugg[:5]}"
            )

    # Quick check for the user-reported entity name
    needle = "the shadow of the high magistrate"
    found = 0
    for cat in categories:
        for item in codex.get(cat, []) or []:
            if str((item or {}).get("name") or "").strip().lower() == needle:
                found += 1
    print(f"reported_entity_found={found}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
