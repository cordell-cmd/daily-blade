#!/usr/bin/env python3

from __future__ import annotations

import json
import re
from pathlib import Path

GEOGRAPHY_FILE = Path("geography.json")
LORE_FILE = Path("lore.json")
CODEX_FILE = Path("codex.json")
STORIES_FILE = Path("stories.json")


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _phrase_in(text: str, phrase: str) -> bool:
    t = (
        (text or "")
        .lower()
        .replace("\u2019", "'")
        .replace("\u2018", "'")
        .replace("\u2011", "-")
    )
    p = (phrase or "").strip().lower()
    if not p:
        return False
    return re.search(r"(?<![a-z0-9])" + re.escape(p) + r"(?![a-z0-9])", t) is not None


def _merge_list_by_name(lst: list, incoming: list, overwrite_keys: set[str] | None = None) -> None:
    overwrite_keys = overwrite_keys or set()
    by = {
        str(x.get("name") or "").strip().lower(): x
        for x in lst
        if isinstance(x, dict) and str(x.get("name") or "").strip()
    }

    for it in incoming:
        if not isinstance(it, dict):
            continue
        nm = str(it.get("name") or "").strip()
        if not nm:
            continue
        k = nm.lower()
        if k not in by:
            obj = {"name": nm}
            lst.append(obj)
            by[k] = obj
        tgt = by[k]

        for kk, vv in it.items():
            if kk == "name":
                continue
            if vv in (None, "", [], {}):
                continue
            if kk in overwrite_keys:
                tgt[kk] = vv
            elif tgt.get(kk) in (None, "", [], {}):
                tgt[kk] = vv


def _merge_codex_category(
    codex: dict,
    cat: str,
    incoming: list,
    date_key: str,
    stories: list,
    overwrite_keys: set[str] | None = None,
) -> None:
    overwrite_keys = overwrite_keys or set()

    lst = codex.get(cat)
    if not isinstance(lst, list):
        lst = []
        codex[cat] = lst

    by = {
        str(x.get("name") or "").strip().lower(): x
        for x in lst
        if isinstance(x, dict) and str(x.get("name") or "").strip()
    }

    story_blobs: list[tuple[str, str]] = []
    for s in stories:
        if not isinstance(s, dict):
            continue
        title = str(s.get("title") or "").strip()
        blob = f"{s.get('title','')}\n{s.get('text','')}"
        story_blobs.append((title, blob))

    for it in incoming:
        if not isinstance(it, dict):
            continue
        nm = str(it.get("name") or "").strip()
        if not nm:
            continue
        k = nm.lower()

        if k not in by:
            obj = {
                "name": nm,
                "tagline": "",
                "first_story": "",
                "first_date": date_key,
                "appearances": 0,
                "story_appearances": [],
            }
            lst.append(obj)
            by[k] = obj

        tgt = by[k]

        for kk, vv in it.items():
            if kk == "name":
                continue
            if vv in (None, "", [], {}):
                continue
            if kk in overwrite_keys:
                tgt[kk] = vv
            elif tgt.get(kk) in (None, "", [], {}):
                tgt[kk] = vv

        apps = []
        for title, blob in story_blobs:
            if title and _phrase_in(blob, nm):
                apps.append({"date": date_key, "title": title})

        prior = tgt.get("story_appearances")
        if not isinstance(prior, list):
            prior = []

        seen = {
            (str(a.get("date") or ""), str(a.get("title") or ""))
            for a in prior
            if isinstance(a, dict)
        }
        for a in apps:
            key = (a["date"], a["title"])
            if key in seen:
                continue
            prior.append(a)
            seen.add(key)

        tgt["story_appearances"] = prior
        tgt["appearances"] = len(prior)

        if prior and not str(tgt.get("first_story") or "").strip():
            tgt["first_story"] = str(prior[0].get("title") or "")
        if prior and not str(tgt.get("first_date") or "").strip():
            tgt["first_date"] = str(prior[0].get("date") or date_key)


def main() -> int:
    geo = _load_json(GEOGRAPHY_FILE)
    lore = _load_json(LORE_FILE)
    codex = _load_json(CODEX_FILE)
    day = _load_json(STORIES_FILE)

    if isinstance(lore, dict):
        lore.pop("subcontinents", None)
    if isinstance(codex, dict):
        codex.pop("subcontinents", None)

    date_key = str(day.get("date") or "").strip() or str(codex.get("last_updated") or "").strip() or "unknown"
    stories = day.get("stories") if isinstance(day.get("stories"), list) else []

    lore.setdefault("hemispheres", [])
    lore.setdefault("continents", [])
    codex.setdefault("hemispheres", [])
    codex.setdefault("continents", [])

    hemispheres = geo.get("hemispheres") if isinstance(geo.get("hemispheres"), list) else []
    continents = geo.get("continents") if isinstance(geo.get("continents"), list) else []

    hemi_name_by_id = {
        str(h.get("id") or "").strip(): str(h.get("name") or "").strip()
        for h in hemispheres
        if isinstance(h, dict) and str(h.get("id") or "").strip() and str(h.get("name") or "").strip()
    }

    hemi_in = []
    for h in hemispheres:
        if not isinstance(h, dict):
            continue
        name = str(h.get("name") or "").strip()
        if not name:
            continue
        climate_band = str(h.get("climate_band") or "").strip()
        hemi_in.append(
            {
                "name": name,
                "id": str(h.get("id") or "").strip(),
                "description": str(h.get("description") or "").strip(),
                "function": "Global climate/season logic.",
                "status": "known",
                "notes": f"Climate band: {climate_band}." if climate_band else "",
            }
        )

    cont_in = []
    for c in continents:
        if not isinstance(c, dict):
            continue
        name = str(c.get("name") or "").strip()
        if not name:
            continue
        hz = [
            hemi_name_by_id.get(str(x).strip(), str(x).strip())
            for x in (c.get("hemispheres") or [])
            if str(x or "").strip()
        ]
        cont_in.append(
            {
                "name": name,
                "id": str(c.get("id") or "").strip(),
                "description": str(c.get("description") or "").strip(),
                "hemispheres": hz,
                "climate_zones": c.get("climate_zones") if isinstance(c.get("climate_zones"), list) else [],
                "function": "Macro-biome and travel logic.",
                "status": str(c.get("status") or "unknown").strip() or "unknown",
                "notes": "",
            }
        )

    before_lore_hemi = len(lore.get("hemispheres") or [])
    before_lore_cont = len(lore.get("continents") or [])
    before_codex_hemi = len(codex.get("hemispheres") or [])
    before_codex_cont = len(codex.get("continents") or [])

    _merge_list_by_name(lore["hemispheres"], hemi_in, overwrite_keys={"status"})
    _merge_list_by_name(lore["continents"], cont_in, overwrite_keys={"status"})

    canonical_overwrite = {"id", "description", "function", "status", "notes", "hemispheres", "climate_zones"}
    _merge_codex_category(codex, "hemispheres", hemi_in, date_key, stories, overwrite_keys=canonical_overwrite)
    _merge_codex_category(codex, "continents", cont_in, date_key, stories, overwrite_keys=canonical_overwrite)

    lore["last_updated"] = date_key
    codex["last_updated"] = date_key

    LORE_FILE.write_text(json.dumps(lore, ensure_ascii=True, indent=2), encoding="utf-8")
    CODEX_FILE.write_text(json.dumps(codex, ensure_ascii=True, indent=2), encoding="utf-8")

    after_lore_hemi = len(lore.get("hemispheres") or [])
    after_lore_cont = len(lore.get("continents") or [])
    after_codex_hemi = len(codex.get("hemispheres") or [])
    after_codex_cont = len(codex.get("continents") or [])

    print(f"date_key={date_key}")
    print(f"geo hemispheres={len(hemispheres)} continents={len(continents)}")
    print(f"lore hemispheres {before_lore_hemi} -> {after_lore_hemi}; continents {before_lore_cont} -> {after_lore_cont}")
    print(f"codex hemispheres {before_codex_hemi} -> {after_codex_hemi}; continents {before_codex_cont} -> {after_codex_cont}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
