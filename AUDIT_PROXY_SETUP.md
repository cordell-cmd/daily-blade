# One-click Audit (No GitHub UI)

GitHub Pages is a static site, so it **cannot** safely store credentials needed to dispatch GitHub Actions (or call Anthropic) without user involvement.

To make “Audit Story” truly one-click (no GitHub UI, no PAT prompt), you need a tiny server-side proxy that holds a secret and triggers the workflow.

This repo includes a ready-to-deploy Cloudflare Worker that:
- accepts `{ date, title }` from the web UI
- dispatches `.github/workflows/audit.yml`
- returns a small JSON status

## Option A: Cloudflare Worker (recommended)

### 1) Create the Worker
- Create a new Worker in Cloudflare.
- Paste the code from `worker/audit-proxy.js`.

### 2) Set secrets / vars
In Cloudflare Worker settings → Variables:

**Secrets**
- `GH_TOKEN` — a GitHub fine-grained PAT owned by you (or a bot user), scoped to `cordell-cmd/daily-blade`.

**Plaintext variables**
- `GH_OWNER` = `cordell-cmd`
- `GH_REPO` = `daily-blade`
- `GH_WORKFLOW_FILE` = `audit.yml`
- `GH_REF` = `main`

### 3) Protect the endpoint (important)
If you deploy this without protection, anyone who finds the endpoint could trigger audits.

Recommended protections:
- Put the Worker behind **Cloudflare Access** (GitHub/Google login), OR
- Add a secret header check in the worker (see `DEV_AUTH_TOKEN` in code) and send it from a browser prompt stored in localStorage.

### 4) Point the UI at your proxy
On the site in developer mode (`?dev=1`), click Audit once; it will ask for an **Audit Proxy URL**.
Paste your Worker URL (e.g., `https://daily-blade-audit-proxy.yourname.workers.dev/audit`).

After that, auditing is one-click and the browser never needs a GitHub token.

## Notes
- Audit persistence still comes from the GitHub Action committing `codex.json`.
- You can rotate the GitHub token at any time in Cloudflare without touching the UI.
