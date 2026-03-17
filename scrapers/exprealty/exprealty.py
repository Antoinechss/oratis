"""
exprealty.com — Florida agent scraper
Cloudflare bypass: Playwright + 2captcha TurnstileTaskProxyless
Phase 1 (browser): scrape agent list pages → collect IDs + basic data
Phase 2 (HTTP):    enrich each agent via direct GraphQL calls (no browser needed)
"""

import asyncio
import json
import re
import time
from html import unescape
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
GRAPHQL_URL  = "https://agentdir-api.expproptech.com/graphql"
GETTOKEN_URL = "https://www.exprealty.com/api/gettoken?tenant=expRealtyUs"

COOKIES_FILE = Path(__file__).parent / "cf_cookies.json"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

DETAIL_QUERY = """
query GetAgent($id: ID!) {
  agent(id: $id) {
    id firstName lastName email phoneNumber photo
    bio languages specializations
    facebook instagram linkedIn twitter website youtube tiktok
    city state zipcode countryCode
  }
}
"""

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


# ── License extraction ────────────────────────────────────────────────────────

STATE_TO_TIMEZONE = {
    "AL": "Central", "AK": "Alaska", "AZ": "Mountain", "AR": "Central",
    "CA": "Pacific", "CO": "Mountain", "CT": "Eastern", "DE": "Eastern",
    "FL": "Eastern", "GA": "Eastern", "HI": "Hawaii", "ID": "Mountain",
    "IL": "Central", "IN": "Eastern", "IA": "Central", "KS": "Central",
    "KY": "Eastern", "LA": "Central", "ME": "Eastern", "MD": "Eastern",
    "MA": "Eastern", "MI": "Eastern", "MN": "Central", "MS": "Central",
    "MO": "Central", "MT": "Mountain", "NE": "Central", "NV": "Pacific",
    "NH": "Eastern", "NJ": "Eastern", "NM": "Mountain", "NY": "Eastern",
    "NC": "Eastern", "ND": "Central", "OH": "Eastern", "OK": "Central",
    "OR": "Pacific", "PA": "Eastern", "RI": "Eastern", "SC": "Eastern",
    "SD": "Central", "TN": "Central", "TX": "Central", "UT": "Mountain",
    "VT": "Eastern", "VA": "Eastern", "WA": "Pacific", "WV": "Eastern",
    "WI": "Central", "WY": "Mountain",
}

STATE_NAME_TO_CODE = {
    "florida": "FL", "california": "CA", "texas": "TX", "new york": "NY",
}


def extract_licenses(text: str) -> list[dict]:
    def clean_text(t: str) -> str:
        t = unescape(t)
        t = re.sub(r"<[^>]+>", " ", t)
        t = re.sub(r"\s+", " ", t)
        return t.strip()

    def normalize_state(raw: str):
        raw = raw.strip().lower()
        if len(raw) == 2:
            return raw.upper()
        return STATE_NAME_TO_CODE.get(raw)

    text = clean_text(text)
    raw_matches = []

    for state, number in re.findall(
        r"([A-Z]{2})\s*(?:RE|Real Estate)?\s*License[:#]?\s*([A-Z0-9]+)", text, re.IGNORECASE
    ):
        raw_matches.append((state.upper(), number))

    for state, number in re.findall(
        r"([A-Za-z\s]+?)\s*License[:#]?\s*([A-Z0-9]+)", text, re.IGNORECASE
    ):
        normalized = normalize_state(state)
        if normalized:
            raw_matches.append((normalized, number))

    for number, state in re.findall(
        r"Lic(?:ense)?\.?\s*#?\s*([A-Z0-9]+)\s*\(?([A-Z]{2})\)?", text, re.IGNORECASE
    ):
        raw_matches.append((state.upper(), number))

    for state, number in re.findall(
        r"([A-Z]{2})\s*(?:DRE|License|Lic\.?)#?\s*([A-Z0-9]+)", text, re.IGNORECASE
    ):
        raw_matches.append((state.upper(), number))

    seen = set()
    licenses = []
    for state, number in raw_matches:
        key = (state, number)
        if key in seen:
            continue
        seen.add(key)
        licenses.append({
            "locale": STATE_TO_TIMEZONE.get(state),
            "number": number,
            "state": state,
            "primary": False,
        })

    if licenses:
        licenses[0]["primary"] = True

    return licenses


# ── Phase 1: list scraper (browser) ─────────────────────────────────────────

def _parse_list_agents(raw: list[dict]) -> list[dict]:
    """Extract id + basic fields from the search list GraphQL response."""
    return [
        {
            "id":    a.get("id") or "",
            "firstName": a.get("firstName") or "",
            "lastName":  a.get("lastName") or "",
            "email": a.get("email") or "",
            "phone": a.get("phoneNumber") or "",
            "city":  a.get("city") or "",
            "state": a.get("state") or "",
            "photo": a.get("photo") or "",
        }
        for a in raw
        if (a.get("firstName") or a.get("lastName")) and a.get("id")
    ]


async def scrape_agent_list(
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
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 800},
            locale="en-US",
            timezone_id="America/New_York",
        )

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

                for _ in range(40):
                    if captured:
                        break
                    await asyncio.sleep(0.5)

                page.remove_listener("response", handle_response)

                if page_num == 1:
                    await save_cookies(context)

                agents = _parse_list_agents(captured)
                print(f"Extracted {len(agents)} agents from page {page_num}")
                all_agents.extend(agents)

                if page_num < num_pages:
                    await asyncio.sleep(rate_limit_s)

        finally:
            await browser.close()

    return all_agents


# ── Phase 2: detail enrichment (via browser fetch) ───────────────────────────
# Direct HTTP to agentdir-api.expproptech.com is blocked at TLS level outside a
# browser. We reuse the existing Playwright page to make fetch() calls from
# within the browser — same TLS fingerprint, same session cookies.

async def get_auth_token_from_page(page) -> str:
    """Fetch the bearer token using the browser's existing session."""
    token = await page.evaluate(
        f"() => fetch('{GETTOKEN_URL}').then(r => r.text())"
    )
    token = (token or "").strip()
    if not token:
        raise RuntimeError("gettoken returned empty response")
    print(f"Auth token obtained ({token[:20]}…)")
    return token


async def fetch_agent_detail_from_page(page, agent_id: str, auth_token: str) -> dict:
    """Make the GraphQL detail call from within the browser."""
    query = DETAIL_QUERY.replace("`", "\\`").replace("${", "\\${")
    result = await page.evaluate(f"""
        async () => {{
            const resp = await fetch('{GRAPHQL_URL}', {{
                method: 'POST',
                headers: {{
                    'Authorization': 'Bearer {auth_token}',
                    'Content-Type': 'application/json',
                }},
                body: JSON.stringify({{
                    query: `{query}`,
                    variables: {{ id: '{agent_id}' }}
                }})
            }});
            return await resp.json();
        }}
    """)
    return (result.get("data") or {}).get("agent") or {}


def build_agent_record(detail: dict) -> dict:
    """Map raw GraphQL detail response to our output schema."""
    bio = detail.get("bio") or ""
    return {
        "id":          detail.get("id") or "",
        "firstName":   detail.get("firstName") or "",
        "lastName":    detail.get("lastName") or "",
        "email":       detail.get("email") or "",
        "phoneNumber": detail.get("phoneNumber") or "",
        "photo":       detail.get("photo") or "",
        "languages":   detail.get("languages") or [],
        "specializations": detail.get("specializations") or [],
        "facebook":    detail.get("facebook") or "",
        "instagram":   detail.get("instagram") or "",
        "linkedIn":    detail.get("linkedIn") or "",
        "twitter":     detail.get("twitter") or "",
        "website":     detail.get("website") or "",
        "youtube":     detail.get("youtube") or "",
        "tiktok":      detail.get("tiktok") or "",
        "city":        detail.get("city") or "",
        "state":       detail.get("state") or "",
        "zipcode":     detail.get("zipcode") or "",
        "countryCode": detail.get("countryCode") or "",
        "license":     extract_licenses(bio),
    }


async def scrape_all(
    num_pages: int = 1,
    list_rate_limit_s: float = 2.0,
    detail_rate_limit_s: float = 0.5,
    headless: bool = False,
) -> list[dict]:
    """Single browser session: Phase 1 (list) + Phase 2 (detail enrichment)."""
    all_agents: list[dict] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 800},
            locale="en-US",
            timezone_id="America/New_York",
        )
        await context.route(re.compile(r"turnstile/v0/.+/api\.js"), _fake_api_route)
        await context.add_init_script(DEFINE_PROPERTY_SCRIPT)
        await load_cookies(context)

        try:
            page = await context.new_page()

            # ── Phase 1: collect agent IDs from listing pages ──
            list_agents: list[dict] = []
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
                            or data.get("agents") or []
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

                for _ in range(40):
                    if captured:
                        break
                    await asyncio.sleep(0.5)

                page.remove_listener("response", handle_response)

                if page_num == 1:
                    await save_cookies(context)

                batch = _parse_list_agents(captured)
                print(f"Extracted {len(batch)} agents from page {page_num}")
                list_agents.extend(batch)

                if page_num < num_pages:
                    await asyncio.sleep(list_rate_limit_s)

            print(f"\nPhase 1 complete: {len(list_agents)} agents")

            # ── Phase 2: enrich each agent via browser fetch ──
            print("\nPhase 2: fetching agent details…")
            auth_token = await get_auth_token_from_page(page)

            for i, agent in enumerate(list_agents, 1):
                agent_id = agent.get("id")
                if not agent_id:
                    continue
                try:
                    detail = await fetch_agent_detail_from_page(page, agent_id, auth_token)
                    record = build_agent_record(detail)
                    all_agents.append(record)
                    print(f"[{i}/{len(list_agents)}] {record['firstName']} {record['lastName']}")
                except Exception as e:
                    print(f"[{i}/{len(list_agents)}] Error for {agent_id}: {e}")

                if i < len(list_agents):
                    await asyncio.sleep(detail_rate_limit_s)

        finally:
            await browser.close()

    return all_agents


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import csv
    import sys

    pages = int(sys.argv[1]) if len(sys.argv) > 1 else 1

    enriched = asyncio.run(scrape_all(num_pages=pages))
    print(f"\nTotal enriched: {len(enriched)} agents")

    out = Path(__file__).parent / "agents.json"
    out.write_text(json.dumps(enriched, indent=2))
    print(f"Saved → {out}")

    # Also save as CSV (flatten list fields)
    csv_out = Path(__file__).parent / "agents.csv"
    fieldnames = [
        "id", "firstName", "lastName", "email", "phoneNumber", "photo",
        "languages", "specializations",
        "facebook", "instagram", "linkedIn", "twitter", "website", "youtube", "tiktok",
        "city", "state", "zipcode", "countryCode", "license",
    ]
    with csv_out.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for a in enriched:
            row = {**a}
            row["languages"] = ", ".join(a.get("languages") or [])
            row["specializations"] = ", ".join(a.get("specializations") or [])
            row["license"] = json.dumps(a.get("license") or [])
            writer.writerow(row)
    print(f"Saved → {csv_out}")
