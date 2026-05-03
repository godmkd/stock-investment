const CACHE_NAME = 'invest-tracker-v9';
const ASSETS = [
  './',
  './index.html',
  './manifest.json',
  'https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js'
];

// Install: cache core assets
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => cache.addAll(ASSETS))
  );
  self.skipWaiting();
});

// Activate: clean old caches
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

// Fetch: network first, fallback to cache
self.addEventListener('fetch', event => {
  // Skip non-GET and CORS proxy requests
  if (event.request.method !== 'GET') return;
  const url = event.request.url;
  if (url.includes('allorigins') || url.includes('corsproxy') || url.includes('codetabs') ||
      url.includes('twse.com') || url.includes('tpex.org') || url.includes('finnhub') ||
      url.includes('er-api.com') || url.includes('supabase.co') ||
      url.includes('query1.finance.yahoo.com')) {
    // API requests: network only, no cache (live data, auth, never stale)
    return;
  }

  event.respondWith(
    fetch(event.request)
      .then(response => {
        // Cache successful responses
        if (response.ok) {
          const clone = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
        }
        return response;
      })
      .catch(() => caches.match(event.request))
  );
});
