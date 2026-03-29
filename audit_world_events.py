#!/usr/bin/env python3
"""audit_world_events.py

Generate a concise machine-readable summary of the most active large-scale events
("world event arcs") from codex.json.

This is meant to be run in GitHub Actions (or locally) to produce a persistent
"what's happening in the world" register, akin to a novel's unfolding arc list.

Output: world-events.json
"""

from __future__ import annotations

import json
import os
import hashlib
from datetime import datetime, timezone

import anthropic


CODEX_FILE = "codex.json"
OUTPUT_FILE = "world-events.json"


def _norm(s: str) -> str:
    return str(s or "").strip().lower()


def _load_previous_summaries(path: str) -> dict[tuple[str, str], dict]:
    """Return a mapping (name_lc, event_type_lc) -> prior event dict."""
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        events = data.get("events", []) if isinstance(data, dict) else []
        if not isinstance(events, list):
            return {}
        out: dict[tuple[str, str], dict] = {}
        for e in events:
            if not isinstance(e, dict):
                continue
            k = (_norm(e.get("name")), _norm(e.get("event_type")))
            if not any(k):
                continue
            out[k] = e
        return out
    except Exception:
        return {}


def _pick_latest_seen(row: dict) -> str:
    """Pick the best 'last seen' marker for summary caching."""
    last_seen = str(row.get("last_seen") or "").strip()
    if last_seen:
        return last_seen
    apps = row.get("story_appearances") if isinstance(row.get("story_appearances"), list) else []
    best = ""
    for a in apps:
        if not isinstance(a, dict):
            continue
        d = str(a.get("date") or "").strip()
        if d and d > best:
            best = d
    return best


def _select_story_appearances_for_summary(apps: list, max_tales: int) -> list:
    """Select appearances for summarization.

    When capped, include both the beginning and the most recent chunk so the
    summary can cover "from the start" without requiring the full history.
    """
    if not isinstance(apps, list):
        return []
    max_tales = int(max_tales or 0)
    if max_tales <= 0 or len(apps) <= max_tales:
        return apps

    # Keep ~1/3 from the start, ~2/3 from the end.
    start_n = max(3, max_tales // 3)
    end_n = max(1, max_tales - start_n)
    head = apps[:start_n]
    tail = apps[-end_n:]

    # Avoid duplication if the arc is short.
    seen = set()
    out = []
    for a in head + tail:
        if not isinstance(a, dict):
            continue
        d = str(a.get("date") or "").strip()
        t = str(a.get("title") or "").strip()
        k = (d, t)
        if k in seen:
            continue
        seen.add(k)
        out.append(a)
    return out


def _appearance_fingerprint(apps: list) -> str:
    """Stable fingerprint of (date,title) pairs for cache invalidation."""
    if not isinstance(apps, list) or not apps:
        return ""
    parts: list[str] = []
    for a in apps:
        if not isinstance(a, dict):
            continue
        d = str(a.get("date") or "").strip()
        t = str(a.get("title") or "").strip()
        if not t:
            continue
        parts.append(f"{d}::{t}")
    raw = "\n".join(parts)
    return hashlib.sha1(raw.encode("utf-8"), usedforsecurity=False).hexdigest() if raw else ""


def _build_event_arc_summary_prompt(event_row: dict, prior_tales: list) -> str:
    """Reader-facing summary prompt for a world event arc."""
    name = str(event_row.get("name") or "unnamed event").strip()
    canon_json = json.dumps(event_row, ensure_ascii=False, sort_keys=True)
    tales_payload = json.dumps(prior_tales or [], ensure_ascii=False, indent=2)
    return f"""You are an archivist writing a reader-facing summary of a running world event in a sword-and-sorcery universe.

Event name: {name}

AUTHORITATIVE EVENT DATA (JSON):
{canon_json}

RELATED TALES IN CHRONOLOGICAL ORDER ({len(prior_tales or [])} tales):
{tales_payload}

Task: Summarize the event from its beginning to the current stage.

Constraints:
- Only include facts supported by the JSON and/or the tales.
- Preserve chronology (what happened first, then what, then what).
- Emphasize cause/effect, escalation, and turning points.
- Treat the authoritative stage/intensity in the JSON as canonical. Use this scale when describing the arc: 1=seed, 2=simmering, 3=rising, 4=crisis, 5=climax; resolved events should read as aftermath.
- Match the prose energy to the stage: simmering should feel early and partial, rising/crisis should feel increasingly disruptive, and climax should feel like a breaking point or peak pressure.
- Write the SUMMARY as a single narrative paragraph with a light, story-like chronicle tone (no purple prose).
- End with the current state and what remains unresolved.
- Keep it concise and readable for a casual reader.
- If helpful, weave in 1–3 tale titles as turning points (do not invent events outside the tales).

Return ONLY plain text in this format:
SUMMARY:
(4-10 sentences)

CURRENT STATE:
- (1-4 bullets)

OPEN THREADS:
- (bullets)
"""


def _build_fallback_arc_summary(event_row: dict) -> str:
    """Deterministic fallback summary when the LLM summary is unavailable.

    This keeps the UI usable even before the first LLM-enabled run.
    """
    name = str(event_row.get("name") or "(unnamed event)").strip()
    event_type = str(event_row.get("event_type") or "").strip() or "event"
    scope = str(event_row.get("scope") or "").strip()
    epicenter = str(event_row.get("epicenter") or "").strip()
    stage = str(event_row.get("stage") or "").strip()
    intensity = event_row.get("intensity")
    resolved = bool(event_row.get("resolved"))
    last_seen = str(event_row.get("last_seen") or "").strip()
    recent_issues = event_row.get("recent_issues")
    trend = event_row.get("trend")
    tagline = str(event_row.get("tagline") or "").strip()

    apps = event_row.get("story_appearances") if isinstance(event_row.get("story_appearances"), list) else []
    apps = [a for a in apps if isinstance(a, dict) and str(a.get("title") or "").strip()]
    apps_sorted = sorted(
        apps,
        key=lambda a: (
            str(a.get("date") or ""),
            str(a.get("title") or ""),
        ),
    )

    first_date = str(apps_sorted[0].get("date") or "").strip() if apps_sorted else ""
    last_date = str(apps_sorted[-1].get("date") or "").strip() if apps_sorted else ""
    first_title = str(apps_sorted[0].get("title") or "").strip() if apps_sorted else ""
    last_title = str(apps_sorted[-1].get("title") or "").strip() if apps_sorted else ""
    n_apps = len(apps_sorted)

    parts: list[str] = []
    parts.append("SUMMARY:")
    if tagline:
        parts.append(f"{name} — {tagline}")
    else:
        parts.append(f"{name} is a running {event_type}.")

    details: list[str] = []
    if scope:
        details.append(f"Scope: {scope}")
    if epicenter:
        details.append(f"Epicenter: {epicenter}")
    if stage:
        details.append(f"Stage: {stage}")
    if intensity is not None:
        details.append(f"Intensity: {intensity}")
    if details:
        parts.append(". ".join(details) + ".")

    if n_apps:
        if first_date and last_date and first_date != last_date:
            parts.append(f"Appears in {n_apps} recorded tales from {first_date} through {last_date}.")
        elif last_date:
            parts.append(f"Appears in {n_apps} recorded tales; last seen {last_date}.")
        else:
            parts.append(f"Appears in {n_apps} recorded tales.")

        if first_title and first_date:
            parts.append(f"First noted in: {first_title} ({first_date}).")
        if last_title and last_date:
            parts.append(f"Most recently: {last_title} ({last_date}).")
    elif last_seen:
        parts.append(f"Last seen: {last_seen}.")

    parts.append("")
    parts.append("CURRENT STATE:")
    parts.append(f"- Resolved: {'yes' if resolved else 'no'}")
    if stage:
        parts.append(f"- Stage: {stage}")
    if intensity is not None:
        parts.append(f"- Intensity: {intensity}")
    if last_seen:
        parts.append(f"- Last seen: {last_seen}")
    if recent_issues is not None:
        parts.append(f"- Recent issues: {recent_issues}")
    if trend is not None:
        parts.append(f"- Trend: {trend}")

    parts.append("")
    parts.append("OPEN THREADS:")
    if not resolved:
        parts.append("- The arc remains unresolved.")
    if epicenter.lower() in {"", "unknown"}:
        parts.append("- The epicenter is not yet identified.")
    if not n_apps:
        parts.append("- No recorded story appearances are linked yet.")

    return "\n".join(parts).strip() + "\n"


def _load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: str, data) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)


def main() -> int:
    if not os.path.exists(CODEX_FILE):
        raise SystemExit(f"ERROR: {CODEX_FILE} not found")

    # Import generator helpers (they already know how to infer geo + arc stage).
    import generate_stories as gs

    codex = _load_json(CODEX_FILE)
    events = codex.get("events", []) if isinstance(codex, dict) else []
    if not isinstance(events, list):
        events = []

    loc_names = gs._canon_loc_names_from_codex(codex)  # type: ignore[attr-defined]
    known_dates = gs._load_known_issue_dates()  # type: ignore[attr-defined]

    prev_by_key = _load_previous_summaries(OUTPUT_FILE)

    enable_summaries = (os.environ.get("ENABLE_WORLD_EVENT_SUMMARIES", "1").strip().lower() in {"1", "true", "yes", "y"})
    force_regen = (os.environ.get("WORLD_EVENT_SUMMARY_FORCE_REGEN", "0").strip().lower() in {"1", "true", "yes", "y"})
    max_updates = int(os.environ.get("WORLD_EVENT_SUMMARY_MAX_UPDATES", "50") or 50)
    max_tales = int(os.environ.get("WORLD_EVENT_SUMMARY_MAX_TALES", "24") or 24)
    max_chars_per_story = int(os.environ.get("WORLD_EVENT_SUMMARY_MAX_CHARS_PER_STORY", "2000") or 2000)
    max_total_chars = int(os.environ.get("WORLD_EVENT_SUMMARY_MAX_TOTAL_INPUT_CHARS", "80000") or 80000)
    max_tokens = int(os.environ.get("WORLD_EVENT_SUMMARY_MAX_TOKENS", "1200") or 1200)

    api_key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    client = None
    if enable_summaries and api_key:
        client = anthropic.Anthropic(api_key=api_key)

    rows = []
    for e in events:
        if not isinstance(e, dict):
            continue
        name = str(e.get("name") or "").strip()
        if not name:
            continue

        geo = gs._infer_event_geo_from_codex(e, loc_names)  # type: ignore[attr-defined]
        arc = gs._event_arc_metrics(e, known_dates)  # type: ignore[attr-defined]

        scope = str(geo.get("scope") or "regional")
        epicenter = str(geo.get("epicenter") or "unknown")
        mentions = geo.get("mentions") if isinstance(geo.get("mentions"), dict) else {}

        mentioned_places = mentions.get("places") if isinstance(mentions.get("places"), list) else []
        mentioned_regions = mentions.get("regions") if isinstance(mentions.get("regions"), list) else []
        mentioned_realms = mentions.get("realms") if isinstance(mentions.get("realms"), list) else []
        mentioned_continents = mentions.get("continents") if isinstance(mentions.get("continents"), list) else []

        story_apps = e.get("story_appearances") if isinstance(e.get("story_appearances"), list) else []

        rows.append({
            "name": name,
            "tagline": str(e.get("tagline") or "").strip(),
            "event_type": str(e.get("event_type") or "").strip(),
            "scope": scope,
            "epicenter": epicenter,
            "stage": str(arc.get("stage") or "seed"),
            "intensity": int(arc.get("intensity") or 1),
            "resolved": bool(arc.get("resolved")),
            "last_seen": str(arc.get("last_date") or "").strip() or None,
            "recent_issues": int(arc.get("recent_count") or 0),
            "trend": int(arc.get("trend") or 0),
            "referenced_locations": {
                "places": mentioned_places[:12],
                "regions": mentioned_regions[:12],
                "realms": mentioned_realms[:12],
                "continents": mentioned_continents[:8],
            },
            "story_appearances": story_apps[-12:],
            "story_appearances_all": story_apps,
        })

    # De-dupe by (name, event_type). This protects against codex merge mishaps
    # (e.g., whitespace variants) while preserving useful continuity signals.
    def _uniq_story_apps(apps):
        if not isinstance(apps, list):
            return []
        out = []
        seen = set()
        for a in apps:
            if not isinstance(a, dict):
                continue
            d = str(a.get("date") or "").strip()
            t = str(a.get("title") or "").strip()
            if not t:
                continue
            k = (d, t)
            if k in seen:
                continue
            seen.add(k)
            out.append({"date": d, "title": t})
        return out

    def _uniq_list(a, b, limit=None):
        a = a if isinstance(a, list) else []
        b = b if isinstance(b, list) else []
        out = list(a)
        seen = {str(x).strip().lower() for x in a if str(x).strip()}
        for x in b:
            k = str(x).strip().lower()
            if not k or k in seen:
                continue
            seen.add(k)
            out.append(x)
            if isinstance(limit, int) and len(out) >= limit:
                break
        return out

    merged: dict[tuple[str, str], dict] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        k = (_norm(r.get("name")), _norm(r.get("event_type")))
        if not any(k):
            continue

        if k not in merged:
            merged[k] = r
            continue

        ex = merged[k]
        # Prefer richer metadata.
        if not ex.get("tagline") and r.get("tagline"):
            ex["tagline"] = r.get("tagline")
        if not ex.get("epicenter") or str(ex.get("epicenter") or "").strip().lower() == "unknown":
            if r.get("epicenter") and str(r.get("epicenter") or "").strip().lower() != "unknown":
                ex["epicenter"] = r.get("epicenter")

        # Keep strongest arc signals.
        ex["resolved"] = bool(ex.get("resolved")) and bool(r.get("resolved"))
        ex["intensity"] = max(int(ex.get("intensity") or 1), int(r.get("intensity") or 1))
        ex["recent_issues"] = max(int(ex.get("recent_issues") or 0), int(r.get("recent_issues") or 0))
        ex["trend"] = max(int(ex.get("trend") or 0), int(r.get("trend") or 0), key=abs)
        ex["last_seen"] = max(str(ex.get("last_seen") or ""), str(r.get("last_seen") or "")) or None

        # Merge location references.
        ex_loc = ex.get("referenced_locations") if isinstance(ex.get("referenced_locations"), dict) else {}
        r_loc = r.get("referenced_locations") if isinstance(r.get("referenced_locations"), dict) else {}
        ex["referenced_locations"] = {
            "places": _uniq_list(ex_loc.get("places"), r_loc.get("places"), limit=12),
            "regions": _uniq_list(ex_loc.get("regions"), r_loc.get("regions"), limit=12),
            "realms": _uniq_list(ex_loc.get("realms"), r_loc.get("realms"), limit=12),
            "continents": _uniq_list(ex_loc.get("continents"), r_loc.get("continents"), limit=8),
        }

        # Merge story appearances and keep the last 12.
        ex_apps = _uniq_story_apps(ex.get("story_appearances"))
        r_apps = _uniq_story_apps(r.get("story_appearances"))
        ex["story_appearances"] = (ex_apps + [a for a in r_apps if a not in ex_apps])[-12:]

        # Also merge the full story history for summarization (not written to output).
        ex_all = _uniq_story_apps(ex.get("story_appearances_all"))
        r_all = _uniq_story_apps(r.get("story_appearances_all"))
        ex["story_appearances_all"] = (ex_all + [a for a in r_all if a not in ex_all])

    rows = list(merged.values())

    def _scope_rank(scope: str) -> int:
        s = (scope or "").strip().lower()
        return {"world": 4, "continental": 3, "regional": 2, "city": 1}.get(s, 2)

    # Prefer big, unresolved, high-intensity, recently-active arcs.
    rows.sort(
        key=lambda r: (
            0 if not r.get("resolved") else 1,
            -_scope_rank(str(r.get("scope") or "regional")),
            -int(r.get("intensity") or 1),
            -(int(r.get("recent_issues") or 0)),
            str(r.get("last_seen") or ""),
            str(r.get("name") or ""),
        )
    )

    # Keep the file compact but useful.
    top = rows[:50]

    # Attach (cached or freshly generated) reader-facing arc summaries.
    # We only regenerate when the event has new story appearances since the last summary.
    updates_done = 0
    for r in top:
        k = (_norm(r.get("name")), _norm(r.get("event_type")))
        prev = prev_by_key.get(k) or {}
        prev_summary = str(prev.get("arc_summary") or "").strip()
        prev_last = str(prev.get("arc_summary_last_seen") or "").strip()
        prev_fp = str(prev.get("arc_summary_fingerprint") or "").strip()
        prev_model = str(prev.get("arc_summary_model") or "").strip()
        prev_is_fallback = prev_model.startswith("fallback")
        want_last = _pick_latest_seen(r)

        full_apps = r.get("story_appearances_all") if isinstance(r.get("story_appearances_all"), list) else r.get("story_appearances")
        full_apps = full_apps if isinstance(full_apps, list) else []
        full_apps = _uniq_story_apps(full_apps)
        full_apps = sorted(
            full_apps,
            key=lambda a: (
                str(a.get("date") or ""),
                str(a.get("title") or ""),
            ),
        )
        want_fp = _appearance_fingerprint(full_apps)

        if (not force_regen) and prev_summary and prev_fp and want_fp and prev_fp == want_fp and (not prev_is_fallback or client is None):
            r["arc_summary"] = prev_summary
            r["arc_summary_last_seen"] = prev_last or want_last
            r["arc_summary_fingerprint"] = prev_fp
            if prev.get("arc_summary_updated_at"):
                r["arc_summary_updated_at"] = prev.get("arc_summary_updated_at")
            if prev.get("arc_summary_model"):
                r["arc_summary_model"] = prev.get("arc_summary_model")
            continue

        # Back-compat: older cached files may not have a fingerprint yet.
        if (not force_regen) and prev_summary and (not prev_fp) and prev_last and want_last and prev_last >= want_last:
            r["arc_summary"] = prev_summary
            r["arc_summary_last_seen"] = prev_last
            if prev.get("arc_summary_updated_at"):
                r["arc_summary_updated_at"] = prev.get("arc_summary_updated_at")
            if prev.get("arc_summary_model"):
                r["arc_summary_model"] = prev.get("arc_summary_model")
            continue

        # If we can't (or shouldn't) regenerate, keep the prior summary if present.
        if not enable_summaries or client is None or updates_done >= max_updates or not want_last:
            if prev_summary:
                r["arc_summary"] = prev_summary
                r["arc_summary_last_seen"] = prev_last or want_last
                if prev_fp:
                    r["arc_summary_fingerprint"] = prev_fp
                if prev.get("arc_summary_updated_at"):
                    r["arc_summary_updated_at"] = prev.get("arc_summary_updated_at")
                if prev.get("arc_summary_model"):
                    r["arc_summary_model"] = prev.get("arc_summary_model")
            else:
                # Ensure the UI has something to show even before LLM summaries exist.
                r["arc_summary"] = _build_fallback_arc_summary(r)
                r["arc_summary_last_seen"] = want_last
                # Deliberately avoid setting a fingerprint for fallback so an LLM
                # run can replace it later even if appearances haven't changed.
                r["arc_summary_updated_at"] = datetime.now(timezone.utc).isoformat()
                r["arc_summary_model"] = "fallback-v1"
            continue

        selected_apps = _select_story_appearances_for_summary(full_apps, max_tales)

        canon_for_prompt = dict(r)
        canon_for_prompt["story_appearances"] = selected_apps
        canon_for_prompt.pop("story_appearances_all", None)

        prior_tales = gs.gather_prior_tales_for_entity(
            canon_for_prompt,
            max_appearances=0,
            max_chars_per_story=max_chars_per_story,
            max_total_chars=max_total_chars,
        )
        if not prior_tales:
            # If we can't load tale texts (e.g., missing archive entries), keep
            # the prior summary if any; otherwise, fall back deterministically.
            if prev_summary:
                r["arc_summary"] = prev_summary
                r["arc_summary_last_seen"] = prev_last or want_last
                if prev_fp:
                    r["arc_summary_fingerprint"] = prev_fp
                if prev.get("arc_summary_updated_at"):
                    r["arc_summary_updated_at"] = prev.get("arc_summary_updated_at")
                if prev.get("arc_summary_model"):
                    r["arc_summary_model"] = prev.get("arc_summary_model")
            else:
                r["arc_summary"] = _build_fallback_arc_summary(r)
                r["arc_summary_last_seen"] = want_last
                r["arc_summary_updated_at"] = datetime.now(timezone.utc).isoformat()
                r["arc_summary_model"] = "fallback-v1"
            continue

        prompt = _build_event_arc_summary_prompt(canon_for_prompt, prior_tales)
        try:
            msg = client.messages.create(
                model=getattr(gs, "MODEL", "claude-haiku-4-5-20251001"),
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}],
            )
            text = (msg.content[0].text or "").strip()
            if text:
                r["arc_summary"] = text
                r["arc_summary_last_seen"] = want_last
                r["arc_summary_fingerprint"] = want_fp
                r["arc_summary_updated_at"] = datetime.now(timezone.utc).isoformat()
                r["arc_summary_model"] = getattr(gs, "MODEL", "claude-haiku-4-5-20251001")
                updates_done += 1
        except Exception as e:
            if prev_summary:
                r["arc_summary"] = prev_summary
                r["arc_summary_last_seen"] = prev_last or want_last
                if prev_fp:
                    r["arc_summary_fingerprint"] = prev_fp
                if prev.get("arc_summary_updated_at"):
                    r["arc_summary_updated_at"] = prev.get("arc_summary_updated_at")
                if prev.get("arc_summary_model"):
                    r["arc_summary_model"] = prev.get("arc_summary_model")
            else:
                r["arc_summary"] = _build_fallback_arc_summary(r)
                r["arc_summary_last_seen"] = want_last
                r["arc_summary_updated_at"] = datetime.now(timezone.utc).isoformat()
                r["arc_summary_model"] = "fallback-v1"
            print(f"WARNING: summary build failed for {r.get('name')}: {e}")

    # Strip internal-only fields before writing.
    for r in top:
        if isinstance(r, dict):
            r.pop("story_appearances_all", None)

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": CODEX_FILE,
        "intensity_min": 1,
        "intensity_max": int(getattr(gs, "WORLD_EVENT_ARC_INTENSITY_MAX", 5) or 5),
        "count": len(top),
        "events": top,
    }
    _write_json(OUTPUT_FILE, out)
    msg = f"✓ Wrote {OUTPUT_FILE} ({len(top)} events)"
    if enable_summaries:
        if not api_key:
            msg += " (summaries enabled, but ANTHROPIC_API_KEY not set — using cached summaries only)"
        else:
            msg += f" (summary updates: {updates_done})"
    print(msg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
