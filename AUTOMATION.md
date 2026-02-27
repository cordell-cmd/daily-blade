# Daily Blade automation

This repo includes a generator that creates 10 new stories per day and writes:
- `stories.json`
- `archive/YYYY-MM-DD.json`
- `archive/index.json`
- plus lore outputs (`lore.json`, `characters.json`, `codex.json`)

This repo is designed to run automatically via GitHub Actions (recommended), keeping a single live source of truth.

## Option A: GitHub Actions (runs on GitHub)

A scheduled workflow exists at `.github/workflows/daily.yml`.

Requirements:
- The repo must be on GitHub with **Actions enabled**.
- Add a repository secret named `ANTHROPIC_API_KEY`.
- The schedule is **UTC** (GitHub cron is always UTC).

Notes:
- If the workflow fails (missing secret, API error), nothing is generated/pushed.
- If you want it to run at “dawn” in your local timezone, edit the cron expression.

## Make the site live (recommended: GitHub Pages)

This project is a static site (`index.html` + JSON files). The simplest “one source of truth” setup is:

- **GitHub Actions** generates daily JSON and commits it to `main`
- **GitHub Pages** serves the repo over HTTPS

Steps (one-time):
1. In your GitHub repo: **Settings → Pages**
2. **Build and deployment**
   - Source: **Deploy from a branch**
   - Branch: `main`
   - Folder: `/ (root)`
3. Wait for Pages to publish; your URL will look like:
   - `https://<username>.github.io/<repo>/`

Checklist:
- Ensure the repo secret `ANTHROPIC_API_KEY` is set (Settings → Secrets and variables → Actions).
- Check **Actions → Generate Daily Stories** for green runs.
- When Actions commits new JSON, the Pages site updates automatically.

Why this avoids “two versions”:
- You never run the generator locally.
- Everything (code + generated data) lives in one place: the repo.

If you want a custom domain later, add a `CNAME` file and set it in Pages.

## Safety / idempotency

`generate_stories.py` is idempotent: if `archive/YYYY-MM-DD.json` already exists and looks complete, it exits successfully without regenerating.

To force a regen (for manual runs only):

```sh
FORCE_REGENERATE=1 python generate_stories.py
```
