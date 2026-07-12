/* Minimal service worker: network-first with cache fallback for static
   assets, so the app shell opens even with flaky store wifi. API calls are
   never cached. Book covers (open library / google books) get their own
   cache-first store so the shelves view stays instant and works offline. */

const CACHE = "book-bot-v4";
const STATIC = [
  "/", "/style.css", "/app.js", "/scanner.js", "/shelf.js", "/shelf3d.js",
  "/vendor/zxing.min.js", "/vendor/gsap.min.js", "/vendor/Flip.min.js",
  "/vendor/three.module.min.js", "/vendor/three.core.min.js",
  "/manifest.webmanifest",
];

const COVER_CACHE = "book-bot-covers-v1";
const COVER_HOSTS = ["covers.openlibrary.org", "books.google.com", "books.googleusercontent.com"];
const COVER_MAX = 400;

self.addEventListener("install", (e) => {
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(STATIC)).then(() => self.skipWaiting()));
});

self.addEventListener("activate", (e) => {
  const keep = [CACHE, COVER_CACHE];
  e.waitUntil(
    caches.keys().then((keys) => Promise.all(keys.filter((k) => !keep.includes(k)).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (e) => {
  if (e.request.method !== "GET") return;
  const url = new URL(e.request.url);

  // cover images: cache-first (no-cors responses are opaque; that's fine)
  if (COVER_HOSTS.includes(url.hostname)) {
    e.respondWith(coverFetch(e.request));
    return;
  }

  // same-origin static: network-first with cache fallback
  if (url.origin !== self.location.origin || url.pathname.startsWith("/api/")) return;
  e.respondWith(
    fetch(e.request)
      .then((resp) => {
        const copy = resp.clone();
        caches.open(CACHE).then((c) => c.put(e.request, copy));
        return resp;
      })
      .catch(() => caches.match(e.request))
  );
});

async function coverFetch(req) {
  const cache = await caches.open(COVER_CACHE);
  const hit = await cache.match(req);
  if (hit) return hit;
  const resp = await fetch(req); // a network failure here surfaces as the fetch error
  cache.put(req, resp.clone()).then(() => trimCovers(cache)).catch(() => {});
  return resp;
}

// keep the cover cache bounded — drop oldest entries first
async function trimCovers(cache) {
  const keys = await cache.keys();
  const excess = keys.length - COVER_MAX;
  for (let i = 0; i < excess; i++) await cache.delete(keys[i]);
}
