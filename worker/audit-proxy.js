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

    const owner = env.GH_OWNER;
    const repo = env.GH_REPO;
    const workflow = env.GH_WORKFLOW_FILE || 'audit.yml';
    const ref = env.GH_REF || 'main';

    if (!env.GH_TOKEN || !owner || !repo) {
      return new Response(JSON.stringify({ error: 'Worker not configured' }), {
        status: 500,
        headers: { 'Content-Type': 'application/json', ...corsHeaders },
      });
    }

    const apiUrl = `https://api.github.com/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/actions/workflows/${encodeURIComponent(workflow)}/dispatches`;

    const ghRes = await fetch(apiUrl, {
      method: 'POST',
      headers: {
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
        'Authorization': `Bearer ${env.GH_TOKEN}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        ref,
        inputs: { date, title },
      }),
    });

    if (!ghRes.ok) {
      let msg = '';
      try {
        const j = await ghRes.json();
        msg = j && j.message ? j.message : '';
      } catch {
        msg = '';
      }
      return new Response(JSON.stringify({ error: `GitHub dispatch failed`, status: ghRes.status, message: msg }), {
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
