/**
 * Cloudflare Worker: Audit proxy for Daily Blade
 *
 * POST /audit
 * Body: { "date": "YYYY-MM-DD", "title": "Story Title" }
 *
 * POST /extract
 * Body: { "date": "YYYY-MM-DD", "title": "Story Title", "text": "Highlighted text" }
 *
 * POST /audit-entity
 * Body: { "entity": { name, type, ...fields }, "stories": [ { title, date, text } ] }
 * Returns: { ok: true, findings: [ { field, issue, suggestion } ], summary: "..." }
 * Calls Anthropic Haiku directly to check if the codex card is complete vs story evidence.
 *
 * Required:
 *  - GH_TOKEN (secret)
 *  - GH_OWNER (var)
 *  - GH_REPO (var)
 *  - GH_WORKFLOW_FILE (var, default: audit.yml)
 *  - GH_REF (var, default: main)
 *
 * Required for /audit-entity:
 *  - ANTHROPIC_API_KEY (secret)
 *
 * Optional protection:
 *  - DEV_AUTH_TOKEN (secret)
 *    If set, caller must send header: X-Dev-Auth: <token>
 */

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    const cleanText = (value) => {
      if (value === undefined || value === null) return '';
      let s = String(value).trim();
      if (
        (s.startsWith('"') && s.endsWith('"') && s.length >= 2) ||
        (s.startsWith("'") && s.endsWith("'") && s.length >= 2)
      ) {
        s = s.slice(1, -1).trim();
      }
      return s;
    };

    // Basic CORS (tighten this for your domain if desired)
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type,X-Dev-Auth',
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders });
    }

    // Helpful root/health response (prevents "Not found" confusion in the browser)
    const normalizedPath = url.pathname.replace(/\/+$/, '') || '/';
    if (request.method === 'GET' && (normalizedPath === '/' || normalizedPath === '/health')) {
      const owner = cleanText(env.GH_OWNER);
      const repo = cleanText(env.GH_REPO);
      const workflowInput = cleanText(env.GH_WORKFLOW_FILE) || 'audit.yml';
      const workflow = workflowInput
        .replace(/^\s*\.?\/?\.github\/workflows\//, '')
        .replace(/^\/+/, '')
        .trim();
      const ref = cleanText(env.GH_REF) || 'main';

      return new Response(
        JSON.stringify({
          ok: true,
          service: 'daily-blade-audit-proxy',
          endpoints: { audit: 'POST /audit', extract: 'POST /extract', auditEntity: 'POST /audit-entity' },
          configured: {
            GH_TOKEN: !!env.GH_TOKEN,
            GH_OWNER: !!owner,
            GH_REPO: !!repo,
          },
          config: { owner: owner || null, repo: repo || null, workflow, ref },
          auth: env.DEV_AUTH_TOKEN ? 'X-Dev-Auth required' : 'none',
        }),
        { status: 200, headers: { 'Content-Type': 'application/json', ...corsHeaders } }
      );
    }

    // ── Supported POST routes ──────────────────────────────────────
    const isAudit       = request.method === 'POST' && normalizedPath === '/audit';
    const isExtract     = request.method === 'POST' && normalizedPath === '/extract';
    const isAuditEntity = request.method === 'POST' && normalizedPath === '/audit-entity';

    if (!isAudit && !isExtract && !isAuditEntity) {
      return new Response(JSON.stringify({ error: 'Not found' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json', ...corsHeaders },
      });
    }

    // ── Auth check ────────────────────────────────────────────────
    if (env.DEV_AUTH_TOKEN) {
      const got = request.headers.get('X-Dev-Auth') || '';
      if (got !== env.DEV_AUTH_TOKEN) {
        return new Response(JSON.stringify({ error: 'Unauthorized' }), {
          status: 401,
          headers: { 'Content-Type': 'application/json', ...corsHeaders },
        });
      }
    }

    let body;
    try {
      body = await request.json();
    } catch {
      body = null;
    }

    // ── /audit-entity: call Anthropic directly ────────────────────
    if (isAuditEntity) {
      if (!env.ANTHROPIC_API_KEY) {
        return new Response(JSON.stringify({ error: 'ANTHROPIC_API_KEY not configured on worker' }), {
          status: 500,
          headers: { 'Content-Type': 'application/json', ...corsHeaders },
        });
      }

      const entity = body && body.entity;
      const stories = body && Array.isArray(body.stories) ? body.stories : [];
      if (!entity || !entity.name) {
        return new Response(JSON.stringify({ error: 'Missing entity object with name' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json', ...corsHeaders },
        });
      }
      if (stories.length === 0) {
        return new Response(JSON.stringify({ error: 'No stories provided' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json', ...corsHeaders },
        });
      }

      // Build the prompt
      const entityJson = JSON.stringify(entity, null, 2);
      const storyBlock = stories.map((s, i) =>
        `--- Story ${i + 1}: "${s.title || 'Untitled'}" (${s.date || '?'}) ---\n${s.text || '(no text)'}`
      ).join('\n\n');

      const prompt = `You are a lore auditor for a sword-and-sorcery serial fiction project called "The Daily Blade."

Below is a CODEX ENTRY (the current record for an entity) and ALL STORIES where this entity appears.

Your job: Compare the codex entry against every detail mentioned in the stories. Identify anything that is:
1. MISSING from the codex — facts, relationships, locations, events, traits, or status changes mentioned in stories but not in the card
2. INCORRECT — details in the codex that contradict what the stories say
3. INCOMPLETE — fields that exist but are vague/placeholder ("unknown") when the stories provide specifics
4. STALE — status, location, or relationship info that was true once but has changed in later stories

Return your analysis as JSON (no markdown fences) with this exact structure:
{
  "findings": [
    {
      "field": "bio",
      "issue": "missing",
      "detail": "Story 'X' reveals they were exiled from Pelimor, not mentioned in bio"
    }
  ],
  "summary": "One-paragraph overall assessment of the card's completeness",
  "completeness_pct": 85
}

If the card is perfect, return an empty findings array and completeness_pct of 100.

CODEX ENTRY:
${entityJson}

STORIES:
${storyBlock}`;

      try {
        const anthropicRes = await fetch('https://api.anthropic.com/v1/messages', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'x-api-key': env.ANTHROPIC_API_KEY,
            'anthropic-version': '2023-06-01',
          },
          body: JSON.stringify({
            model: 'claude-3-5-haiku-latest',
            max_tokens: 2048,
            messages: [{ role: 'user', content: prompt }],
          }),
        });

        if (!anthropicRes.ok) {
          const errText = await anthropicRes.text();
          return new Response(JSON.stringify({ error: 'Anthropic API error', status: anthropicRes.status, detail: errText.slice(0, 1000) }), {
            status: 502,
            headers: { 'Content-Type': 'application/json', ...corsHeaders },
          });
        }

        const anthropicData = await anthropicRes.json();
        const rawContent = (anthropicData.content && anthropicData.content[0] && anthropicData.content[0].text) || '';

        // Try to parse JSON from the response
        let parsed;
        try {
          // Strip markdown fences if present
          const cleaned = rawContent.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/i, '').trim();
          parsed = JSON.parse(cleaned);
        } catch {
          parsed = { findings: [], summary: rawContent, completeness_pct: null, raw: true };
        }

        return new Response(JSON.stringify({ ok: true, ...parsed }), {
          status: 200,
          headers: { 'Content-Type': 'application/json', ...corsHeaders },
        });
      } catch (fetchErr) {
        return new Response(JSON.stringify({ error: 'Failed to call Anthropic API', detail: String(fetchErr) }), {
          status: 502,
          headers: { 'Content-Type': 'application/json', ...corsHeaders },
        });
      }
    }

    // ── /audit & /extract: dispatch GitHub Actions ────────────────
    const date  = String(body && body.date  ? body.date  : '').trim();
    const title = String(body && body.title ? body.title : '').trim();
    const text  = String(body && body.text  ? body.text  : '').trim();

    if (!date || !title) {
      return new Response(JSON.stringify({ error: 'Missing date/title' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders },
      });
    }

    if (isExtract && !text) {
      return new Response(JSON.stringify({ error: 'Missing text (highlighted text to categorize)' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders },
      });
    }

    // ── Resolve config ────────────────────────────────────────────
    const owner = cleanText(env.GH_OWNER);
    const repo = cleanText(env.GH_REPO);
    const ref = cleanText(env.GH_REF) || 'main';

    // Pick the workflow file: extract.yml for /extract, audit.yml for /audit.
    const workflowFile = isExtract
      ? 'extract.yml'
      : (cleanText(env.GH_WORKFLOW_FILE) || 'audit.yml')
          .replace(/^\s*\.?\/?\.github\/workflows\//, '')
          .replace(/^\/+/, '')
          .trim();

    if (!env.GH_TOKEN || !owner || !repo) {
      return new Response(
        JSON.stringify({
          error: 'Worker not configured',
          missing: {
            GH_TOKEN: !env.GH_TOKEN,
            GH_OWNER: !owner,
            GH_REPO: !repo,
          },
        }),
        {
        status: 500,
        headers: { 'Content-Type': 'application/json', ...corsHeaders },
        }
      );
    }

    // ── Build inputs ──────────────────────────────────────────────
    const inputs = isExtract
      ? { date, title, text }
      : { date, title };

    const apiUrl = `https://api.github.com/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/actions/workflows/${encodeURIComponent(workflowFile)}/dispatches`;

    const ghRes = await fetch(apiUrl, {
      method: 'POST',
      headers: {
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
        'User-Agent': 'daily-blade-audit-proxy (Cloudflare Worker)',
        'Authorization': `Bearer ${env.GH_TOKEN}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ ref, inputs }),
    });

    if (!ghRes.ok) {
      const requestId = ghRes.headers.get('x-github-request-id') || '';
      const raw = await ghRes.text();
      let msg = '';
      try {
        const j = JSON.parse(raw);
        msg = j && j.message ? j.message : '';
      } catch {
        msg = '';
      }

      return new Response(JSON.stringify({
        error: 'GitHub dispatch failed',
        status: ghRes.status,
        message: msg,
        request_id: requestId,
        raw: raw ? raw.slice(0, 2000) : '',
        config: { owner, repo, workflow: workflowFile, ref },
      }), {
        status: 502,
        headers: { 'Content-Type': 'application/json', ...corsHeaders },
      });
    }

    return new Response(JSON.stringify({ ok: true, queued: true }), {
      status: 200,
      headers: { 'Content-Type': 'application/json', ...corsHeaders },
    });
  }
};
