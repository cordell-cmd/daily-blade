/**
 * Cloudflare Worker: Audit proxy for Daily Blade
 *
 * POST /audit
 * Body: { "date": "YYYY-MM-DD", "title": "Story Title" }
 *
 * Required:
 *  - GH_TOKEN (secret)
 *  - GH_OWNER (var)
 *  - GH_REPO (var)
 *  - GH_WORKFLOW_FILE (var, default: audit.yml)
 *  - GH_REF (var, default: main)
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
      'Access-Control-Allow-Methods': 'POST,OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type,X-Dev-Auth',
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders });
    }

    if (request.method !== 'POST' || url.pathname.replace(/\/+$/, '') !== '/audit') {
      return new Response(JSON.stringify({ error: 'Not found' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json', ...corsHeaders },
      });
    }

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

    const date = String(body && body.date ? body.date : '').trim();
    const title = String(body && body.title ? body.title : '').trim();

    if (!date || !title) {
      return new Response(JSON.stringify({ error: 'Missing date/title' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders },
      });
    }

    const owner = cleanText(env.GH_OWNER);
    const repo = cleanText(env.GH_REPO);
    const workflowInput = cleanText(env.GH_WORKFLOW_FILE) || 'audit.yml';
    const workflow = workflowInput
      .replace(/^\s*\.?\/?\.github\/workflows\//, '')
      .replace(/^\/+/, '')
      .trim();
    const ref = cleanText(env.GH_REF) || 'main';

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

    const apiUrl = `https://api.github.com/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/actions/workflows/${encodeURIComponent(workflow)}/dispatches`;

    const ghRes = await fetch(apiUrl, {
      method: 'POST',
      headers: {
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
        'User-Agent': 'daily-blade-audit-proxy (Cloudflare Worker)',
        'Authorization': `Bearer ${env.GH_TOKEN}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        ref,
        inputs: { date, title },
      }),
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
        config: {
          owner,
          repo,
          workflow,
          ref,
        }
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
