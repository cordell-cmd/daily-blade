#!/usr/bin/env python3

import json
import importlib
from datetime import datetime, timezone


def main() -> None:
    gs = importlib.import_module("generate_stories")
    lore = gs.load_lore()
    today_str = datetime.now(timezone.utc).strftime("%B %d, %Y")
    story_prompt = gs.build_prompt(today_str, lore)

    try:
        with open("stories.json", "r", encoding="utf-8") as handle:
            data = json.load(handle)
        stories = data.get("stories", [])[:10]
    except Exception:
        stories = [
            {"title": f"Story {index + 1}", "text": "", "subgenre": "Sword & Sorcery"}
            for index in range(10)
        ]

    extract_prompt = gs.build_lore_extraction_prompt(stories, lore)
    lore_context = gs.build_lore_context(lore)

    def approx(chars: int) -> int:
        return int(round(chars / 4))

    stories_payload_chars = (
        sum(
            len(story.get("title", ""))
            + len(story.get("subgenre", ""))
            + len(story.get("text", ""))
            for story in stories
        )
        + 600
        if stories and any(story.get("text") for story in stories)
        else 0
    )

    print("story_prompt_chars", len(story_prompt), "approx_tokens", approx(len(story_prompt)))
    print("extract_prompt_chars", len(extract_prompt), "approx_tokens", approx(len(extract_prompt)))
    print("lore_context_chars", len(lore_context), "approx_tokens", approx(len(lore_context)))
    print(
        "stories_payload_chars",
        stories_payload_chars,
        "approx_tokens",
        approx(stories_payload_chars) if stories_payload_chars else 0,
    )


if __name__ == "__main__":
    main()