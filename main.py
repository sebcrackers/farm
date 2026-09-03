import asyncio
import random
import os
import re
import time
import json
import psutil
import urllib.request
import itertools
from collections import defaultdict, deque
from playwright.async_api import async_playwright
from fake_useragent import UserAgent

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

class Logger:
    def __init__(self):
        self.enabled = True
        self.verbose = False

    def _ts(self):
        return time.strftime("%H:%M:%S", time.localtime())

    def log(self, tag, msg, worker=None):
        if not self.enabled:
            return
        prefix = f"[{self._ts()}]"
        if worker:
            prefix += f"[{worker}]"
        print(f"{prefix} [{tag}] {msg}")

    def debug(self, msg, worker=None):
        if self.verbose:
            self.log("DBG", msg, worker)

    def info(self, msg, worker=None):
        self.log("INF", msg, worker)

    def warn(self, msg, worker=None):
        self.log("WRN", msg, worker)

    def err(self, msg, worker=None):
        self.log("ERR", msg, worker)

    def ad(self, msg, worker=None):
        self.log("AD ", msg, worker)

LOG = Logger()

# ─────────────────────────────────────────────
# PROXY API
# ─────────────────────────────────────────────

PROXY_SOURCES = [
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&country=us",
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&country=gb",
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&country=de",
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&country=ca",
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=ipport&format=text&country=hk",
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=ipport&format=text&country=sg",
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=ipport&format=text&country=tw",
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=ipport&format=text&country=se",
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=ipport&format=text&country=mx",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
]

DEFAULT_TIMEZONES = [
    "America/New_York", "America/Chicago", "America/Denver",
    "America/Los_Angeles", "America/Phoenix", "America/Detroit",
    "America/Kentucky/Louisville", "America/Indiana/Indianapolis",
    "America/Toronto", "America/Vancouver", "America/Edmonton",
    "America/Halifax", "America/Winnipeg", "Europe/London",
    "Europe/Berlin", "Europe/Paris", "Europe/Dublin",
]

def fetch_proxies_from_api() -> list:
    proxies = []
    seen = set()

    for source_url in PROXY_SOURCES:
        try:
            req = urllib.request.Request(source_url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=20) as resp:
                text = resp.read().decode("utf-8", errors="ignore")

            count_before = len(proxies)
            for line in text.strip().split("\n"):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if not line.startswith(("socks4://", "socks5://", "http://", "https://")):
                    if "socks5" in source_url:
                        line = f"socks5://{line}"
                    elif "socks4" in source_url:
                        line = f"socks4://{line}"
                    else:
                        line = f"http://{line}"

                if line in seen:
                    continue
                seen.add(line)

                proxies.append({
                    "server": line,
                    "tz": random.choice(DEFAULT_TIMEZONES),
                    "fails": 0,
                    "uses": 0,
                    "last_used": 0,
                    "health": 50,
                    "score": 0,
                })

            fetched = len(proxies) - count_before
            if fetched > 0:
                LOG.info(f"Fetched {fetched} from {source_url.split('/')[-1][:40]}")
        except Exception as e:
            LOG.warn(f"Source failed {source_url[:50]}: {e}")

    random.shuffle(proxies)
    LOG.info(f"Total unique proxies (shuffled): {len(proxies)}")
    return proxies

def load_proxies_from_file(filepath: str) -> list:
    proxies = []
    if not os.path.exists(filepath):
        return proxies
    with open(filepath, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            server = line
            if not server.startswith(("socks4://", "socks5://", "http://", "https://")):
                server = f"http://{server}"
            proxies.append({
                "server": server,
                "tz": random.choice(DEFAULT_TIMEZONES),
                "fails": 0,
                "uses": 0,
                "last_used": 0,
                "health": 50,
                "score": 0,
            })
    random.shuffle(proxies)
    return proxies

# ─────────────────────────────────────────────
# PROXY PRE-VALIDATION
# ─────────────────────────────────────────────

async def validate_proxies(pw, proxies, concurrency=200, timeout=6000):
    """
    Uses Playwright's APIRequestContext to quickly test proxies
    without spinning up a full browser context.
    """
    sem = asyncio.Semaphore(concurrency)
    total = len(proxies)
    counter = {"checked": 0, "valid": 0}
    LOG.info(f"Validating {total} proxies (concurrency={concurrency}, timeout={timeout/1000:.0f}s)...")
    
    # Test against the target site itself to ensure it doesn't block the proxy
    test_url = "https://richoffsolana.lol/"

    async def check_one(proxy):
        req_ctx = None
        async with sem:
            try:
                req_ctx = await pw.request.new_context(
                    proxy={"server": proxy["server"]},
                    ignore_https_errors=True,
                )
                response = await req_ctx.head(test_url, timeout=timeout)
                # Accept any 2xx or 3xx (redirects are fine)
                if response.ok or (300 <= response.status < 400):
                    counter["valid"] += 1
                    proxy["health"] = 90  # Validated proxies start with high health
                    proxy["score"] = 1
                    return proxy
            except Exception:
                pass
            finally:
                if req_ctx:
                    try: await req_ctx.dispose()
                    except: pass
            
            counter["checked"] += 1
            if counter["checked"] % 500 == 0:
                LOG.info(f"  Checked {counter['checked']}/{total} — {counter['valid']} working so far")
            return None

    tasks = [check_one(p) for p in proxies]
    results = await asyncio.gather(*tasks)
    valid = [r for r in results if r is not None]

    LOG.info(f"Validation complete: {len(valid)}/{total} proxies are alive and can reach the target")
    return valid

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

CONFIG = {
    "target_urls": [
        "https://richoffsolana.lol/",
    ],
    "target_rotation": "random",

    # ── CONCURRENCY ──
    "browser_count": 10,
    "contexts_per_browser": 3,          # 15 × 4 = 60 workers
    "max_concurrent_visits": 60,
    "max_visits": 500000,

    # ── TIMING ──
    "dwell_time_range": (1.0, 2.5),
    "ad_dwell_time_range": (2, 4),
    "post_click_dwell": (0.5, 1.5),
    "inter_visit_delay": (0.1, 0.5),
    "context_close_delay": (0.1, 0.3),

    # ── PROXY ──
    "use_proxy_file": False,             # TRUE = Load from proxy.txt, FALSE = Fetch from API
    "proxy_file_path": "proxy.txt",
    "proxy_refresh_interval": 0,        # 0 = DISABLED. Refresher adds unvalidated garbage.
    "max_proxy_fails": 2,               # Drop after 2 fails in the bot
    "proxy_cooldown_seconds": 10,       # Reuse good proxies fast
    "min_proxies_for_scale": 5,

    # ── TIMEOUTS ──
    "goto_timeout": 10000,              # 10s. Since proxies are validated, 10s is plenty.
    "max_click_attempts": 2,
    "browser_launch_timeout": 30000,
    "ad_script_wait_timeout": 8,

    # ── RESOURCE LIMITS ──
    "max_ram_percent": 92,
    "max_cpu_percent": 98,
    "resource_check_interval": 20,
    "worker_pause_duration": 2,

    # ── FINGERPRINT ──
    "viewport_options": [
        {"width": 1920, "height": 1080},
        {"width": 1366, "height": 768},
        {"width": 1536, "height": 864},
        {"width": 1440, "height": 900},
        {"width": 1280, "height": 720},
        {"width": 1600, "height": 900},
    ],
    "languages": ["en-US", "en-US,en;q=0.9", "en-GB,en;q=0.8"],
    "color_schemes": ["light", "dark"],
    "device_scales": [1, 1.25, 1.5],
}

# ─────────────────────────────────────────────
# COMPONENTS
# ─────────────────────────────────────────────

class ProxyManager:
    def __init__(self, pool: list):
        self.pool = pool
        self._lock = asyncio.Lock()
        LOG.info(f"Proxy pool initialized: {len(self.pool)} proxies")

    async def get_random(self) -> dict:
        async with self._lock:
            if not self.pool:
                raise RuntimeError("Proxy pool is empty")

            now = time.time()
            cooldown = CONFIG["proxy_cooldown_seconds"]

            candidates = [p for p in self.pool if now - p["last_used"] >= cooldown]
            if not candidates:
                proxy = min(self.pool, key=lambda p: p["last_used"])
            else:
                weights = [max(p.get("health", 50), 10) for p in candidates]
                proxy = random.choices(candidates, weights=weights, k=1)[0]

            proxy["last_used"] = now
            proxy["uses"] = proxy.get("uses", 0) + 1
            return proxy

    async def mark_fail(self, proxy: dict):
        async with self._lock:
            proxy["fails"] = proxy.get("fails", 0) + 1
            proxy["health"] = max(0, proxy.get("health", 50) - 40)
            if proxy["fails"] >= CONFIG["max_proxy_fails"]:
                try:
                    self.pool.remove(proxy)
                except ValueError:
                    pass

    async def mark_success(self, proxy: dict):
        async with self._lock:
            proxy["health"] = min(100, proxy.get("health", 50) + 15)
            proxy["fails"] = 0  # Reset fails on success
            proxy["score"] = proxy.get("score", 0) + 1

    async def pool_size(self) -> int:
        async with self._lock:
            return len(self.pool)

    async def refresh(self):
        pass # Disabled

class FingerprintManager:
    def __init__(self):
        self.ua_gen = UserAgent()
        self._ua_cache = []
        self._cache_size = 100

    def _refresh_ua_cache(self):
        for _ in range(self._cache_size):
            try: self._ua_cache.append(self.ua_gen.random)
            except: pass

    def generate(self, timezone: str = None) -> dict:
        if not self._ua_cache: self._refresh_ua_cache()
        ua = random.choice(self._ua_cache) if self._ua_cache else self.ua_gen.random
        viewport = random.choice(CONFIG["viewport_options"])
        return {
            "user_agent": ua,
            "viewport": viewport,
            "timezone": timezone or random.choice(DEFAULT_TIMEZONES),
            "language": random.choice(CONFIG["languages"]),
            "locale": "en-US",
            "color_scheme": random.choice(CONFIG["color_schemes"]),
            "device_scale": random.choice(CONFIG["device_scales"]),
            "has_touch": random.choice([True, False]),
            "platform": self._platform_from_ua(ua),
        }

    @staticmethod
    def _platform_from_ua(ua: str) -> str:
        if "Macintosh" in ua or "Mac OS" in ua: return "MacIntel"
        elif "Windows" in ua: return "Win32"
        elif "Linux" in ua: return "Linux x86_64"
        return "Win32"

class Stats:
    def __init__(self):
        self.visits = 0
        self.ad_hits = 0
        self.failures = 0
        self.proxy_drops = 0
        self.fail_reasons = defaultdict(int)
        self.target_stats = defaultdict(lambda: {"visits": 0, "ads": 0})
        self._start_time = time.time()
        self._counter = itertools.count(1)

    def next_visit_num(self) -> int: return next(self._counter)
    def visit(self, target: str):
        self.visits += 1
        self.target_stats[target]["visits"] += 1
    def ad(self, target: str):
        self.ad_hits += 1
        self.target_stats[target]["ads"] += 1
    def fail(self, reason: str = "unknown"):
        self.failures += 1
        self.fail_reasons[reason] += 1
    def snapshot(self) -> str:
        elapsed = time.time() - self._start_time
        rate = f"{(self.ad_hits / max(self.visits, 1) * 100):.1f}%" if self.visits else "0%"
        vps = f"{self.visits / max(elapsed, 1):.1f}/s" if elapsed > 0 else "0/s"
        aps = f"{self.ad_hits / max(elapsed, 1):.2f}/s" if elapsed > 0 else "0/s"
        return (f"visits={self.visits} | ads={self.ad_hits} | fails={self.failures} | "
                f"rate={rate} | vps={vps} | aps={aps}")
    def fail_snapshot(self) -> str:
        if not self.fail_reasons: return "  (none)"
        return "\n".join(f"  {r}: {c}" for r, c in sorted(self.fail_reasons.items(), key=lambda x: -x[1]))

class ResourceManager:
    def __init__(self):
        self._paused = False
    def is_paused(self) -> bool: return self._paused
    def check(self):
        try:
            ram = psutil.virtual_memory().percent
            cpu = psutil.cpu_percent(interval=1)
            if ram > CONFIG["max_ram_percent"] or cpu > CONFIG["max_cpu_percent"]:
                if not self._paused: LOG.warn(f"Resource spike: RAM={ram:.1f}% CPU={cpu:.1f}% — pausing"); self._paused = True
            else:
                if self._paused: LOG.info(f"Resources OK: RAM={ram:.1f}% CPU={cpu:.1f}% — resuming"); self._paused = False
        except: pass
    async def wait_if_paused(self):
        while self._paused:
            await asyncio.sleep(CONFIG["worker_pause_duration"])
            self.check()

class TargetManager:
    def __init__(self, urls: list):
        self.urls = urls if urls else [CONFIG["target_urls"][0]]
    def get_target(self) -> str:
        if len(self.urls) == 1: return self.urls[0]
        return random.choice(self.urls)

# ─────────────────────────────────────────────
# STEALTH JS
# ─────────────────────────────────────────────
STEALTH_JS = """
() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    delete navigator.__proto__.webdriver;
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
    const fakePlugins = [
        { name: 'PDF Viewer', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
        { name: 'Chrome PDF Viewer', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
        { name: 'Chromium PDF Viewer', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
        { name: 'Microsoft Edge PDF Viewer', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
        { name: 'WebKit built-in PDF', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
    ];
    Object.defineProperty(navigator, 'plugins', {
        get: () => {
            const arr = fakePlugins.map(p => {
                const plugin = Object.create(Plugin.prototype);
                Object.defineProperty(plugin, 'name', { value: p.name });
                Object.defineProperty(plugin, 'filename', { value: p.filename });
                Object.defineProperty(plugin, 'description', { value: p.description });
                Object.defineProperty(plugin, 'length', { value: 1 });
                return plugin;
            });
            arr.namedItem = (name) => arr.find(p => p.name === name) || null;
            arr.refresh = () => {}; arr.item = (i) => arr[i] || null;
            return arr;
        }
    });
    const getParameter = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(param) {
        if (param === 37445) return 'Intel Inc.';
        if (param === 37446) return 'Intel(R) Iris(TM) Plus Graphics 640';
        if (param === 7937) return 'Intel(R) Iris(TM) Plus Graphics 640';
        return getParameter.call(this, param);
    };
    window.chrome = {
        runtime: { onConnect: { addListener: () => {}, removeListener: () => {} }, onMessage: { addListener: () => {}, removeListener: () => {} }, connect: () => {}, sendMessage: () => {} },
        loadTimes: () => ({ commitLoadTime: Date.now() / 1000 - Math.random() * 5, connectionInfo: 'h2', finishDocumentLoadTime: Date.now() / 1000 - Math.random() * 2, finishLoadTime: Date.now() / 1000 - Math.random() * 1, firstPaintAfterLoadTime: 0, firstPaintTime: Date.now() / 1000 - Math.random() * 4, navigationType: 'Other', npnNegotiatedProtocol: 'h2', requestTime: Date.now() / 1000 - Math.random() * 6, startLoadTime: Date.now() / 1000 - Math.random() * 5, wasAlternateProtocolAvailable: false, wasFetchedViaSpdy: true, wasNpnNegotiated: true }),
        csi: () => ({ startE: Date.now() - Math.random() * 3000, onloadT: Date.now() - Math.random() * 1000, pageT: Math.random() * 5000, tran: 15 }),
    };
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (parameters.name === 'notifications' ? Promise.resolve({ state: Notification.permission }) : originalQuery(parameters));
    Object.defineProperty(screen, 'colorDepth', { get: () => 24 });
    Object.defineProperty(screen, 'pixelDepth', { get: () => 24 });
    Object.defineProperty(screen, 'availWidth', { get: () => window.screen.width });
    Object.defineProperty(screen, 'availHeight', { get: () => window.screen.height - 40 });
    if (window.outerWidth === 0 || window.outerWidth === undefined) { Object.defineProperty(window, 'outerWidth', { get: () => window.innerWidth }); }
    if (window.outerHeight === 0 || window.outerHeight === undefined) { Object.defineProperty(window, 'outerHeight', { get: () => window.innerHeight + 80 }); }
    Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 4 });
    Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
    Object.defineProperty(navigator, 'platform', { get: () => '__PLATFORM__' });
    const originalDescriptor = Object.getOwnPropertyDescriptor(HTMLElement.prototype, 'offsetHeight');
    Object.defineProperty(HTMLElement.prototype, 'offsetHeight', { get() { if (this.id === 'modernizr' || this.className.includes('webdriver')) return 0; return originalDescriptor ? originalDescriptor.get.call(this) : 0; } });
    const originalOpen = window.open;
    window.__captured_ad_urls = [];
    window.open = function(url, target, features) { if (url && typeof url === 'string') { window.__captured_ad_urls.push(url); } return originalOpen.call(window, url, target, features); };
    const originalCreate = document.createElement;
    document.createElement = function(tag) {
        const el = originalCreate.call(document, tag);
        if (tag.toLowerCase() === 'script') {
            const originalSrc = Object.getOwnPropertyDescriptor(HTMLScriptElement.prototype, 'src');
            Object.defineProperty(el, 'src', {
                set: function(val) { if (val && typeof val === 'string' && (val.includes('popcash') || val.includes('popunder') || val.includes('juicyads'))) { window.__captured_ad_urls.push(val); } originalSrc.set.call(this, val); },
                get: function() { return originalSrc.get.call(this); }
            });
        }
        return el;
    };
    navigator.getBattery = () => Promise.resolve({ charging: true, chargingTime: 0, dischargingTime: Infinity, level: Math.random() * 0.5 + 0.5, addEventListener: () => {}, removeEventListener: () => {} });
    if (!navigator.mediaDevices) { navigator.mediaDevices = {}; }
    navigator.mediaDevices.enumerateDevices = () => Promise.resolve([{ kind: 'audioinput', deviceId: 'default', label: '', groupId: 'group1' }, { kind: 'audiooutput', deviceId: 'default', label: '', groupId: 'group1' }, { kind: 'videoinput', deviceId: 'default', label: '', groupId: 'group2' }]);
}
"""
def build_stealth_js(platform: str) -> str:
    return STEALTH_JS.replace("__PLATFORM__", platform)

# ─────────────────────────────────────────────
# BEHAVIOR ENGINE
# ─────────────────────────────────────────────

class BehaviorEngine:
    @staticmethod
    async def human_mouse_move(page, x, y, steps=None, worker_id=None):
        start_x, start_y = random.randint(0, 300), random.randint(0, 300)
        steps = steps or random.randint(4, 8)
        for i in range(steps):
            t = i / steps
            eased = t * t * (3 - 2 * t)
            jx = random.uniform(-4, 4) if i < steps - 1 else 0
            jy = random.uniform(-4, 4) if i < steps - 1 else 0
            await page.mouse.move(start_x + (x - start_x) * eased + jx, start_y + (y - start_y) * eased + jy)
            await asyncio.sleep(random.uniform(0.008, 0.025))

    @staticmethod
    async def human_scroll(page, iterations=2, worker_id=None):
        for _ in range(iterations):
            await page.mouse.wheel(0, random.randint(150, 500))
            await asyncio.sleep(random.uniform(0.2, 0.7))

    @staticmethod
    async def simulate_reading(page, viewport: dict, worker_id=None):
        await BehaviorEngine.human_scroll(page, random.randint(1, 2), worker_id=worker_id)
        await asyncio.sleep(random.uniform(0.3, 1.0))
        for _ in range(random.randint(1, 2)):
            x = random.randint(50, viewport["width"] - 50)
            y = random.randint(100, min(viewport["height"] - 100, 600))
            await BehaviorEngine.human_mouse_move(page, x, y, worker_id=worker_id)
            await asyncio.sleep(random.uniform(0.2, 0.8))

    @staticmethod
    async def check_page_loaded(page, worker_id=None) -> bool:
        try: return (await page.evaluate("document.body ? document.body.innerHTML.length : 0")) > 200
        except: return False

    @staticmethod
    async def wait_for_ad_scripts(page, timeout=8, worker_id=None) -> bool:
        try:
            await page.wait_for_function("""() => { if (window._pop || window.PopCash || window.popcash_url) return true; const s = document.querySelectorAll('script[src]'); for (let i=0;i<s.length;i++) { const u=s[i].src.toLowerCase(); if(u.includes('popcash')||u.includes('popunder')||u.includes('juicyads')||u.includes('exoclick')||u.includes('propeller')||u.includes('adcash')||u.includes('hilltop')||u.includes('adsterra')||u.includes('adskeeper')||u.includes('mgid')||u.includes('onclickperformance')) return true; } return !!(document.body && document.body.onclick); }""", timeout=timeout * 1000)
            return True
        except: return False

    @staticmethod
    async def try_extract_ad_url(page, worker_id=None) -> str:
        try:
            url = await page.evaluate(r"""() => { if (window.__captured_ad_urls && window.__captured_ad_urls.length > 0) return window.__captured_ad_urls[0]; if (window._pop) return window._pop.url || window._pop.clickUrl || window._pop.click || (window._pop.config && window._pop.config.url); if (window.PopCash) return window.PopCash.url || window.PopCash.clickUrl || (window.PopCash.config && window.PopCash.config.url); if (window.popcash_url) return window.popcash_url; if (window.popcash) return window.popcash; if (window.pcash) return window.pcash; if (window.juicy_ads && window.juicy_ads.url) return window.juicy_ads.url; if (window.juicyads_url) return window.juicyads_url; if (window.juicyads && window.juicyads.url) return window.juicyads.url; if (window.popunder_url) return window.popunder_url; if (window.ad_url) return window.ad_url; if (window._popunder) return window._popunder; if (window._ad_url) return window._ad_url; if (window.ExoLoader) return window.ExoLoader.serve || window.ExoLoader.zone; if (window.propellerAds && window.propellerAds.url) return window.propellerAds.url; let scripts = document.querySelectorAll('script:not([src])'); let patterns = [/https?:\/\/[^"'\s]+track\.popcash[^"'\s]+/i, /https?:\/\/[^"'\s]+popcash\.net\/click[^"'\s]+/i, /https?:\/\/[^"'\s]+popcash\.net\/popunder[^"'\s]+/i, /https?:\/\/[^"'\s]+click\.popcash[^"'\s]+/i, /https?:\/\/[^"'\s]+juicyads\.com\/click[^"'\s]+/i, /https?:\/\/[^"'\s]+exoclick[^"'\s]+/i, /https?:\/\/[^"'\s]+popunder[^"'\s]+/i, /https?:\/\/[^"'\s]+click[^"'\s]*(?:pop|ad|under)[^"'\s]+/i]; for (let s of scripts) { let text = s.textContent || s.innerText || ''; if (text.length < 10) continue; for (let p of patterns) { let m = text.match(p); if (m && m[0] && !m[0].endsWith('.js') && !m[0].endsWith('.css')) return m[0]; } } let srcScripts = document.querySelectorAll('script[src]'); for (let s of srcScripts) { let src = s.src; if (src.includes('popcash') && src.includes('click')) return src; if (src.includes('popunder') && !src.endsWith('.js')) return src; } return null; }""")
            if url and not (url.endswith('.js') or url.endswith('.css')): return url
        except: pass
        return None

    @staticmethod
    def is_valid_ad_url(url: str, target_url: str) -> bool:
        if not url or url == "about:blank" or url.endswith('.js') or url.endswith('.css') or target_url in url or url in target_url or url.startswith('data:') or not url.startswith('http'): return False
        return True

    @staticmethod
    async def trigger_popunder(page, context, viewport: dict, popup_list: list, target_url: str, worker_id=None) -> bool:
        if not viewport: return False
        await asyncio.sleep(random.uniform(1.0, 2.5))

        # Strategy 1: Body clicks
        for attempt in range(CONFIG["max_click_attempts"]):
            cx, cy = random.randint(100, max(200, viewport["width"] - 100)), random.randint(100, min(500, viewport["height"] - 100))
            await BehaviorEngine.human_mouse_move(page, cx, cy, steps=random.randint(4, 8), worker_id=worker_id)
            await asyncio.sleep(random.uniform(0.05, 0.2))
            await page.mouse.click(cx, cy)
            await asyncio.sleep(random.uniform(1.0, 2.5))

            real_popups = [p for p in popup_list if BehaviorEngine.is_valid_ad_url(p.url, target_url)]
            for p in popup_list:
                if p not in real_popups:
                    try: await p.close()
                    except: pass
            popup_list.clear(); popup_list.extend(real_popups)
            if popup_list: return True

            captured = await page.evaluate("window.__captured_ad_urls ? window.__captured_ad_urls.length : 0")
            if captured > 0:
                ad_url = await BehaviorEngine.try_extract_ad_url(page, worker_id)
                if ad_url and BehaviorEngine.is_valid_ad_url(ad_url, target_url):
                    try:
                        new_page = await context.new_page()
                        await new_page.goto(ad_url, wait_until="domcontentloaded", timeout=10000)
                        if BehaviorEngine.is_valid_ad_url(new_page.url, target_url): popup_list.append(new_page); return True
                        await new_page.close()
                    except: pass
            await asyncio.sleep(random.uniform(0.2, 0.8))

        # Strategy 2: Element clicks
        try:
            elements = await page.query_selector_all("a, button, div[onclick], span[onclick], [role='button'], [onclick]")
            for i, el in enumerate(elements[:5]):
                try:
                    box = await el.bounding_box()
                    if not box or box["width"] < 10 or box["height"] < 10: continue
                    cx, cy = box["x"] + box["width"] / 2 + random.uniform(-5, 5), box["y"] + box["height"] / 2 + random.uniform(-5, 5)
                    await BehaviorEngine.human_mouse_move(page, cx, cy, steps=random.randint(4, 8), worker_id=worker_id)
                    await asyncio.sleep(random.uniform(0.05, 0.2))
                    await el.click(timeout=2000)
                    await asyncio.sleep(random.uniform(1.0, 2.0))

                    real_popups = [p for p in popup_list if BehaviorEngine.is_valid_ad_url(p.url, target_url)]
                    for p in popup_list:
                        if p not in real_popups:
                            try: await p.close()
                            except: pass
                    popup_list.clear(); popup_list.extend(real_popups)
                    if popup_list: return True
                except: continue
        except: pass

        # Strategy 3: Keyboard
        try:
            await page.keyboard.press("Tab"); await asyncio.sleep(random.uniform(0.1, 0.3))
            await page.keyboard.press("Enter"); await asyncio.sleep(random.uniform(1.0, 2.5))
            real_popups = [p for p in popup_list if BehaviorEngine.is_valid_ad_url(p.url, target_url)]
            for p in popup_list:
                if p not in real_popups:
                    try: await p.close()
                    except: pass
            popup_list.clear(); popup_list.extend(real_popups)
            if popup_list: return True
        except: pass

        # Strategy 4: Direct extraction
        ad_url = await BehaviorEngine.try_extract_ad_url(page, worker_id)
        if ad_url and BehaviorEngine.is_valid_ad_url(ad_url, target_url):
            try:
                new_page = await context.new_page()
                await new_page.goto(ad_url, wait_until="domcontentloaded", timeout=10000)
                if BehaviorEngine.is_valid_ad_url(new_page.url, target_url): popup_list.append(new_page); return True
                await new_page.close()
            except: pass
        return False

    @staticmethod
    async def handle_popup_ad(popup_page, worker_id=None) -> bool:
        try:
            try: await popup_page.wait_for_url(lambda url: url != "about:blank" and not url.startswith("data:"), timeout=6000)
            except: pass
            current_url = popup_page.url
            if not current_url or current_url == "about:blank" or current_url.endswith('.js') or current_url.endswith('.css'):
                await popup_page.close(); return False
            await asyncio.sleep(random.uniform(*CONFIG["ad_dwell_time_range"]))
            try: await BehaviorEngine.human_scroll(popup_page, random.randint(1, 2), worker_id=worker_id)
            except: pass
            await asyncio.sleep(random.uniform(0.3, 1.5))
            await popup_page.close()
            return True
        except:
            try: await popup_page.close()
            except: pass
            return False

# ─────────────────────────────────────────────
# VISIT CYCLE
# ─────────────────────────────────────────────

async def visit_cycle(worker_id: str, browser, proxy_mgr, fp_mgr, stats: Stats, target_mgr: TargetManager, res_mgr: ResourceManager, visit_semaphore: asyncio.Semaphore, visit_num: int):
    await res_mgr.wait_if_paused()

    if await proxy_mgr.pool_size() < CONFIG["min_proxies_for_scale"]:
        stats.fail("proxy_pool_low"); return

    async with visit_semaphore:
        proxy = await proxy_mgr.get_random()
        fp = fp_mgr.generate(timezone=proxy["tz"])
        stealth_js = build_stealth_js(fp["platform"])
        target_url = target_mgr.get_target()

        context = await browser.new_context(
            user_agent=fp["user_agent"], viewport=fp["viewport"], locale=fp["locale"], timezone_id=fp["timezone"],
            java_script_enabled=True, proxy={"server": proxy["server"]}, device_scale_factor=fp["device_scale"],
            has_touch=fp["has_touch"], color_scheme=fp["color_scheme"], ignore_https_errors=True, bypass_csp=True,
        )
        await context.add_init_script(stealth_js)
        page = await context.new_page()
        page.set_default_timeout(15000)

        popup_pages = []
        def on_new_page(new_page):
            if new_page != page: popup_pages.append(new_page)
        context.on("page", on_new_page)

        try:
            referrer = random.choice(["https://www.google.com/search?q=crypto+memecoins", "https://www.reddit.com/r/CryptoCurrency/", "", ""])
            await page.goto(target_url, wait_until="domcontentloaded", referer=referrer if referrer else None, timeout=CONFIG["goto_timeout"])

            if not await BehaviorEngine.check_page_loaded(page, worker_id=worker_id):
                stats.fail("page_not_loaded"); await proxy_mgr.mark_fail(proxy); return

            # COUNT VISIT HERE
            stats.visit(target_url)
            await proxy_mgr.mark_success(proxy)

            if not await BehaviorEngine.wait_for_ad_scripts(page, timeout=CONFIG["ad_script_wait_timeout"], worker_id=worker_id):
                stats.fail("no_ad_scripts")

            await asyncio.sleep(random.uniform(*CONFIG["dwell_time_range"]))
            await BehaviorEngine.simulate_reading(page, fp["viewport"], worker_id=worker_id)
            await asyncio.sleep(random.uniform(0.5, 1.5))

            if await BehaviorEngine.trigger_popunder(page, context, fp["viewport"], popup_pages, target_url, worker_id=worker_id):
                for popup in popup_pages:
                    if await BehaviorEngine.handle_popup_ad(popup, worker_id=worker_id): stats.ad(target_url)
                LOG.info(f"✓ AD | {stats.snapshot()}", worker_id)
            else:
                stats.fail("no_popunder_triggered")

            await asyncio.sleep(random.uniform(*CONFIG["post_click_dwell"]))

        except Exception as e:
            err_str = str(e)
            if "Timeout" in err_str: stats.fail("timeout"); await proxy_mgr.mark_fail(proxy)
            elif "ERR_PROXY" in err_str or "ERR_TUNNEL" in err_str: stats.fail("proxy_error"); await proxy_mgr.mark_fail(proxy)
            elif "ERR_" in err_str: stats.fail("network_error"); await proxy_mgr.mark_fail(proxy)
            else: stats.fail("unexpected_error")
        finally:
            for popup in popup_pages:
                try: await popup.close()
                except: pass
            try: await context.clear_cookies()
            except: pass
            await asyncio.sleep(random.uniform(*CONFIG["context_close_delay"]))
            await context.close()

async def worker(worker_id: str, browser, proxy_mgr, fp_mgr, stats: Stats, target_mgr: TargetManager, res_mgr: ResourceManager, visit_semaphore: asyncio.Semaphore, max_visits: int):
    while True:
        if stats.visits + stats.failures >= max_visits: break
        visit_num = stats.next_visit_num()
        await visit_cycle(worker_id, browser, proxy_mgr, fp_mgr, stats, target_mgr, res_mgr, visit_semaphore, visit_num)
        await asyncio.sleep(random.uniform(*CONFIG["inter_visit_delay"]))

async def stats_printer(stats: Stats, proxy_mgr: ProxyManager, res_mgr: ResourceManager, target_mgr: TargetManager, interval: int = 30):
    while True:
        await asyncio.sleep(interval)
        pool_sz = await proxy_mgr.pool_size()
        paused = "PAUSED" if res_mgr.is_paused() else "RUNNING"
        print(f"\n{'─' * 70}")
        print(f"[STATS] {stats.snapshot()}")
        print(f"[POOL]  proxies={pool_sz} | workers={paused}")
        print(f"[FAILS]\n{stats.fail_snapshot()}")
        print(f"{'─' * 70}\n")

async def resource_monitor(res_mgr: ResourceManager, interval: int = 20):
    while True:
        await asyncio.sleep(interval)
        res_mgr.check()

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

async def main():
    LOG.info("=" * 60)
    LOG.info("  AD FARM v9 — THE FILTER + FILE TOGGLE")
    LOG.info("=" * 60)

    # ── PROXY SOURCE TOGGLE ──
    if CONFIG.get("use_proxy_file", False):
        LOG.info(f"USE_PROXY_FILE is ON. Loading from {CONFIG['proxy_file_path']}...")
        proxy_pool = load_proxies_from_file(CONFIG["proxy_file_path"])
    else:
        LOG.info("USE_PROXY_FILE is OFF. Fetching from API...")
        proxy_pool = fetch_proxies_from_api()
        # Fallback to file if API fails completely
        if not proxy_pool or len(proxy_pool) < 10:
            if os.path.exists(CONFIG["proxy_file_path"]):
                LOG.warn(f"API fetch failed or returned too few. Falling back to {CONFIG['proxy_file_path']}")
                proxy_pool = load_proxies_from_file(CONFIG["proxy_file_path"])

    if not proxy_pool or len(proxy_pool) < 5:
        LOG.err("No proxies loaded. Exiting.")
        return

    proxy_mgr = ProxyManager(proxy_pool)
    fp_mgr = FingerprintManager()
    stats = Stats()
    res_mgr = ResourceManager()
    target_mgr = TargetManager(CONFIG["target_urls"])
    visit_semaphore = asyncio.Semaphore(CONFIG["max_concurrent_visits"])

    total_workers = CONFIG["browser_count"] * CONFIG["contexts_per_browser"]
    print(f"\n{'=' * 60}")
    print(f"  CONFIG — v9 THE FILTER")
    print(f"{'=' * 60}")
    print(f"  Workers:          {CONFIG['browser_count']} × {CONFIG['contexts_per_browser']} = {total_workers}")
    print(f"  Max concurrent:   {CONFIG['max_concurrent_visits']}")
    print(f"  Proxy pool:       {len(proxy_pool)} (PRE-VALIDATION)")
    print(f"  Source:           {'proxy.txt' if CONFIG.get('use_proxy_file') else 'API'}")
    print(f"{'=' * 60}\n")

    asyncio.create_task(stats_printer(stats, proxy_mgr, res_mgr, target_mgr, interval=30))
    asyncio.create_task(resource_monitor(res_mgr, interval=CONFIG["resource_check_interval"]))

    async with async_playwright() as pw:
        # ── RUN PRE-VALIDATION ──
        LOG.info("Starting proxy validation phase...")
        validated_pool = await validate_proxies(pw, proxy_mgr.pool)
        
        if not validated_pool or len(validated_pool) < 5:
            LOG.err(f"Only {len(validated_pool)} valid proxies. Exiting.")
            return
            
        async with proxy_mgr._lock:
            proxy_mgr.pool = validated_pool
        LOG.info(f"Proxy pool updated: {len(validated_pool)} validated proxies. Starting workers.")

        launch_args = [
            "--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-setuid-sandbox", "--disable-infobars",
            "--disable-dev-shm-usage", "--disable-popup-blocking", "--disable-features=IsolateOrigins,site-per-process",
            "--disable-web-security", "--allow-running-insecure-content", "--ignore-certificate-errors", "--mute-audio",
            "--no-first-run", "--disable-extensions", "--blink-settings=imagesEnabled=false", "--disable-gpu",
            "--disable-software-rasterizer", "--disable-background-timer-throttling", "--disable-backgrounding-occluded-windows",
            "--disable-default-apps", "--disable-sync", "--disable-translate", "--no-default-browser-check", "--disable-renderer-backgrounding",
        ]

        async def launch_browser(idx: int):
            browser = await pw.chromium.launch(headless=True, args=launch_args)
            LOG.info(f"Browser {idx + 1}/{CONFIG['browser_count']} launched")
            return browser

        browsers = await asyncio.gather(*[launch_browser(i) for i in range(CONFIG["browser_count"])])
        LOG.info(f"All {len(browsers)} browsers launched — starting {total_workers} workers\n")

        workers = []
        for b_idx, browser in enumerate(browsers):
            for c_idx in range(CONFIG["contexts_per_browser"]):
                wid = f"b{b_idx}c{c_idx}"
                workers.append(worker(wid, browser, proxy_mgr, fp_mgr, stats, target_mgr, res_mgr, visit_semaphore, CONFIG["max_visits"]))

        await asyncio.gather(*workers)
        for browser in browsers: await browser.close()

    print(f"\n{'=' * 60}\n  FINAL STATS\n{'=' * 60}")
    print(f"[*] {stats.snapshot()}")
    print(f"[*] FAIL REASONS:\n{stats.fail_snapshot()}")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    asyncio.run(main())
