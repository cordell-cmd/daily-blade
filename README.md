# The Daily Blade

Static pulp-fantasy “daily paper” site.

- Frontend: `index.html`
- Data: `stories.json`, `archive/`, `codex.json`, `lore.json`, `characters.json`
- Generator: `generate_stories.py` (Anthropic)

## Live, single-source setup

Recommended production setup is:

1. GitHub Actions generates and commits the daily edition to `main`
2. GitHub Pages serves the repo as a live site

See [AUTOMATION.md](AUTOMATION.md) for exact steps.

## Local viewing (optional)

Because the app uses `fetch()` for JSON, you need an HTTP server (not `file://`).

```sh
python3 -m http.server 8008
```

Then open `http://localhost:8008/`.
