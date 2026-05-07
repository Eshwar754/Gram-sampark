const CACHE_NAME = 'gram-sampark-v2';
const APP_SHELL_ASSETS = [
  './',
  './index.html',
  './style.css',
  './app.js',
  './canvas-bg.js',
  './manifest.json',
  './xicon.svg',
  './gram_sampark.svg',
  './survey_1.png',
  './survey_2.png',
  './survey_3.png',
  './survey_4.png',
  './antigravity_bg.png',
  './village_bg.png',
  './og_image.png'
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(APP_SHELL_ASSETS))
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((cacheNames) => Promise.all(
      cacheNames
        .filter((name) => name !== CACHE_NAME)
        .map((name) => caches.delete(name))
    )).then(() => self.clients.claim())
  );
});

function shouldBypassCache(request) {
  const url = new URL(request.url);
  return request.method !== 'GET'
    || url.origin !== self.location.origin && url.hostname.includes('googleapis.com')
    || url.href.includes('firestore.googleapis.com')
    || url.href.includes('identitytoolkit.googleapis.com')
    || url.href.includes('securetoken.googleapis.com');
}

self.addEventListener('fetch', (event) => {
  const { request } = event;

  if (shouldBypassCache(request)) return;

  if (request.mode === 'navigate') {
    event.respondWith(
      fetch(request)
        .then((response) => {
          const cloned = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put('./index.html', cloned));
          return response;
        })
        .catch(() => caches.match('./index.html'))
    );
    return;
  }

  event.respondWith(
    caches.match(request).then((cachedResponse) => {
      const networkFetch = fetch(request)
        .then((response) => {
          if (response && response.status === 200) {
            const cloned = response.clone();
            caches.open(CACHE_NAME).then((cache) => cache.put(request, cloned));
          }
          return response;
        })
        .catch(() => cachedResponse);

      return cachedResponse || networkFetch;
    })
  );
});

self.addEventListener('message', (event) => {
  if (event.data === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});
