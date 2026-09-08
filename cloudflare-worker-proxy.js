// Cloudflare Worker: minimal CORS-adding proxy for stock-investment's TW
// price fetches. Only forwards to an allowlisted set of hosts (TWSE/TPEX/
// Yahoo) — not an open proxy. Deploy via the Cloudflare dashboard (free
// tier, no credit card): Workers & Pages -> Create -> paste this file's
// contents -> Deploy. Then use the resulting *.workers.dev URL as
// ?url=<encoded target> in the app's corsProxies list.

const ALLOWED_HOSTS = new Set([
  'mis.twse.com.tw',
  'openapi.twse.com.tw',
  'www.tpex.org.tw',
  'query1.finance.yahoo.com',
]);

export default {
  async fetch(request) {
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, OPTIONS',
          'Access-Control-Allow-Headers': '*',
          'Access-Control-Max-Age': '86400',
        },
      });
    }

    const reqUrl = new URL(request.url);
    const target = reqUrl.searchParams.get('url');
    if (!target) {
      return new Response('Missing ?url= param', { status: 400 });
    }

    let targetUrl;
    try {
      targetUrl = new URL(target);
    } catch (e) {
      return new Response('Invalid url', { status: 400 });
    }

    if (!ALLOWED_HOSTS.has(targetUrl.hostname)) {
      return new Response('Host not allowed: ' + targetUrl.hostname, { status: 403 });
    }

    let upstream;
    try {
      upstream = await fetch(targetUrl.toString(), {
        headers: { 'User-Agent': 'Mozilla/5.0 (compatible; stock-investment-proxy/1.0)' },
        cf: { cacheTtl: 15, cacheEverything: true },
      });
    } catch (e) {
      return new Response('Upstream fetch failed: ' + e.message, { status: 502 });
    }

    const body = await upstream.arrayBuffer();
    return new Response(body, {
      status: upstream.status,
      headers: {
        'Content-Type': upstream.headers.get('content-type') || 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'public, max-age=15',
      },
    });
  },
};
