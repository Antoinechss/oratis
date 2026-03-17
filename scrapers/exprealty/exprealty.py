"""
exprealty.com — Florida agent scraper
Cloudflare bypass: Playwright + 2captcha TurnstileTaskProxyless
"""

import asyncio
import json
import re
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests
from playwright.async_api import BrowserContext, Page, async_playwright

# ── Config ──────────────────────────────────────────────────────────────────────

TWOCAPTCHA_API_KEY = "f55be8565862edb0dc57d7525b2b2427"

AGENTS_URL = (
    "https://www.exprealty.com/agents-search"
    "?page={page}&country=US&m=f&location=Florida+%28FL%29+-+State"
)

COOKIES_FILE = Path(__file__).parent / "cf_cookies.json"

# ── Cloudflare / Turnstile intercept scripts ─────────────────────────────────
# Served in place of Cloudflare's api.js so window.turnstile.render() is ours.

FAKE_TURNSTILE_JS = """
(function () {
    function interceptRender(container, options) {
        var params = {
            type:      'TurnstileTaskProxyless',
            websiteKey: options.sitekey,
            websiteURL: window.location.href,
            data:       options.cData       || undefined,
            pagedata:   options.chlPageData || undefined,
            action:     options.action      || undefined,
            userAgent:  navigator.userAgent,
        };
        window.__tsParams   = params;
        window.__tsCallback = options.callback;
        try {
            window.top.__tsParams   = params;
            window.top.__tsCallback = options.callback;
        } catch (_) {
            window.top.postMessage({ __cfIntercept: true, params: params }, '*');
        }
        return 'intercept-widget-id';
    }

    window.turnstile = {
        render:      interceptRender,
        execute:     interceptRender,
        ready:       function (cb) { if (cb) cb(); },
        remove:      function () {},
        reset:       function () {},
        getResponse: function () { return window.__tsToken || ''; },
        isExpired:   function () { return false; },
    };
})();
"""

# Fallback: intercepts window.turnstile via property setter (covers cached api.js).
DEFINE_PROPERTY_SCRIPT = """
(function () {
    var _t;
    Object.defineProperty(window, 'turnstile', {
        configurable: true,
        set: function (v) {
            _t = v;
            v.render = function (container, options) {
                var params = {
                    type:      'TurnstileTaskProxyless',
                    websiteKey: options.sitekey,
                    websiteURL: window.location.href,
                    data:       options.cData       || undefined,
                    pagedata:   options.chlPageData || undefined,
                    action:     options.action      || undefined,
                    userAgent:  navigator.userAgent,
                };
                window.__tsParams   = params;
                window.__tsCallback = options.callback;
                try {
                    window.top.__tsParams   = params;
                    window.top.__tsCallback = options.callback;
                } catch (_) {}
                return 'intercept-widget-id';
            };
        },
        get: function () { return _t; }
    });
})();
"""


# ── 2captcha ─────────────────────────────────────────────────────────────────

def solve_turnstile(params: dict) -> str:
    payload = {
        "clientKey": TWOCAPTCHA_API_KEY,
        "task": {"type": "TurnstileTaskProxyless", **params},
    }
    print(f"Submitting to 2captcha: sitekey={params.get('websiteKey')}")

    submit = requests.post("https://api.2captcha.com/createTask", json=payload).json()
    if submit.get("errorId") != 0:
        raise RuntimeError(f"2captcha createTask failed: {submit}")

    task_id = submit["taskId"]
    print(f"Task ID: {task_id}")

    for attempt in range(24):  # poll up to 2 minutes
        time.sleep(5)
        result = requests.post(
            "https://api.2captcha.com/getTaskResult",
            json={"clientKey": TWOCAPTCHA_API_KEY, "taskId": task_id},
        ).json()

        if result.get("errorId") != 0:
            raise RuntimeError(f"2captcha getTaskResult error: {result}")

        if result.get("status") == "ready":
            print("Token received.")
            return result["solution"]["token"]

        print(f"Waiting… ({attempt + 1}/24)")

    raise TimeoutError("2captcha did not return a token within 2 minutes.")


# ── Cloudflare bypass ────────────────────────────────────────────────────────

async def _fake_api_route(route, request):
    onload_fn = parse_qs(urlparse(request.url).query).get("onload", [""])[0]
    callback_call = (
        f"\nif (typeof window['{onload_fn}'] === 'function') window['{onload_fn}']();"
        if onload_fn else ""
    )
    await route.fulfill(
        content_type="application/javascript",
        body=FAKE_TURNSTILE_JS + callback_call,
    )


async def bypass_cloudflare(page: Page) -> None:
    print("Cloudflare challenge detected. Waiting for Turnstile intercept…")

    try:
        await page.wait_for_function("window.__tsParams !== undefined", timeout=30_000)
    except Exception:
        raise RuntimeError("window.__tsParams was never set — intercept did not fire.")

    params = await page.evaluate("window.__tsParams")
    print(f"Captured sitekey={params.get('websiteKey')} action={params.get('action')}")

    token = solve_turnstile({k: v for k, v in params.items() if v is not None})

    async with page.expect_navigation(timeout=30_000, wait_until="load"):
        await page.evaluate(f"window.__tsCallback('{token}')")
    print("Challenge passed.")


# ── Cookie persistence ────────────────────────────────────────────────────────
# cf_clearance is valid ~24h; reloading it on subsequent runs skips the challenge.

async def save_cookies(context: BrowserContext) -> None:
    cookies = await context.cookies()
    COOKIES_FILE.write_text(json.dumps(cookies, indent=2))
    print(f"Saved {len(cookies)} cookies.")


async def load_cookies(context: BrowserContext) -> None:
    if COOKIES_FILE.exists():
        cookies = json.loads(COOKIES_FILE.read_text())
        await context.add_cookies(cookies)
        print(f"Loaded {len(cookies)} saved cookies.")


# ── Data extraction ──────────────────────────────────────────────────────────

def _parse_agents(raw: list[dict]) -> list[dict]:
    return [
        {
            "name":  f"{a.get('firstName', '')} {a.get('lastName', '')}".strip(),
            "email": a.get("email") or "",
            "phone": a.get("phoneNumber") or "",
            "city":  a.get("city") or "",
            "state": a.get("state") or "",
            "photo": a.get("photo") or "",
        }
        for a in raw
        if a.get("firstName") or a.get("lastName")
    ]


# ── Main scraper ──────────────────────────────────────────────────────────────

async def scrape_agents(
    num_pages: int = 1,
    rate_limit_s: float = 2.0,
    headless: bool = False,
) -> list[dict]:
    all_agents: list[dict] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
            locale="en-US",
            timezone_id="America/New_York",
        )

        # Replace Cloudflare's turnstile api.js with our intercept version
        await context.route(re.compile(r"turnstile/v0/.+/api\.js"), _fake_api_route)
        await context.add_init_script(DEFINE_PROPERTY_SCRIPT)
        await load_cookies(context)

        try:
            page = await context.new_page()

            for page_num in range(1, num_pages + 1):
                url = AGENTS_URL.format(page=page_num)
                print(f"\n── Page {page_num} ──")

                captured: list[dict] = []

                async def handle_response(response, _captured=captured):
                    if "agentdir-api.expproptech.com/graphql" not in response.url:
                        return
                    try:
                        data = (await response.json()).get("data") or {}
                        agents = (
                            data.get("search", {}).get("agents")
                            or data.get("getAgentsByFilters", {}).get("agents")
                            or data.get("agents")
                            or []
                        )
                        if agents:
                            _captured.extend(agents)
                            print(f"GraphQL: {len(agents)} agents")
                    except Exception as e:
                        print(f"GraphQL parse error: {e}")

                page.on("response", handle_response)
                await page.goto(url, wait_until="load", timeout=60_000)

                if "Just a moment" in await page.title():
                    await bypass_cloudflare(page)

                # Wait up to 20s for the agents GraphQL response
                for _ in range(40):
                    if captured:
                        break
                    await asyncio.sleep(0.5)

                page.remove_listener("response", handle_response)

                if page_num == 1:
                    await save_cookies(context)

                agents = _parse_agents(captured)
                print(f"Extracted {len(agents)} agents from page {page_num}")
                all_agents.extend(agents)

                if page_num < num_pages:
                    await asyncio.sleep(rate_limit_s)

        finally:
            await browser.close()

    return all_agents


if __name__ == "__main__":
    import csv
    import sys

    pages = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    results = asyncio.run(scrape_agents(num_pages=pages))

    print(f"\nTotal: {len(results)} agents")

    out = Path(__file__).parent / "agents.csv"
    with out.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "email", "phone", "city", "state", "photo"])
        writer.writeheader()
        writer.writerows(results)
    print(f"Saved → {out}")
