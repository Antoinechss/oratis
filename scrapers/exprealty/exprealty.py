import asyncio
import csv
import json
import re
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests
from playwright.async_api import BrowserContext, Page, async_playwright

# ── Config ────────────────────────────────────────────────────────────────────

TWOCAPTCHA_API_KEY = "f55be8565862edb0dc57d7525b2b2427"

START_URL = (
    "https://www.exprealty.com/agents-search"
    "?page=1&country=US&m=f&location=Florida+%28FL%29+-+State"
)
AGENTS_URL = (
    "https://www.exprealty.com/agents-search"
    "?page={page}&country=US&m=f&location=Florida+%28FL%29+-+State"
)

COOKIES_FILE = Path(__file__).parent / "cf_cookies.json"
CSV_OUT      = Path(__file__).parent / "agents.csv"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

CSV_FIELDS = [
    "id", "firstName", "lastName", "email", "phoneNumber", "photo",
    "languages", "specializations",
    "facebook", "instagram", "linkedIn", "twitter", "website", "youtube",
    "city", "state", "zipcode", "countryCode", "license", "memberSince",
]

# ── Turnstile intercept scripts ───────────────────────────────────────────────

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


# ── 2captcha ──────────────────────────────────────────────────────────────────

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


# ── Cloudflare bypass ─────────────────────────────────────────────────────────

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

async def save_cookies(context: BrowserContext) -> None:
    cookies = await context.cookies()
    COOKIES_FILE.write_text(json.dumps(cookies, indent=2))
    print(f"Saved {len(cookies)} cookies.")


async def load_cookies(context: BrowserContext) -> None:
    if COOKIES_FILE.exists():
        cookies = json.loads(COOKIES_FILE.read_text())
        await context.add_cookies(cookies)
        print(f"Loaded {len(cookies)} saved cookies.")


# ── License / date parsing ────────────────────────────────────────────────────

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


def extract_licenses(text: str) -> list[dict]:
    text = re.sub(r"<[^>]+>", " ", unescape(text))
    text = re.sub(r"\s+", " ", text).strip()

    matches = re.findall(
        r"\b([A-Z]{2})\s+(?:RE\s+)?License\s*[:#]\s*([A-Z]{1,3}\d{4,10})\b",
        text,
        re.IGNORECASE,
    )

    seen = set()
    licenses = []
    for state, number in matches:
        key = (state.upper(), number.upper())
        if key in seen:
            continue
        seen.add(key)
        licenses.append({
            "locale":  STATE_TO_TIMEZONE.get(state.upper()),
            "number":  number.upper(),
            "state":   state.upper(),
            "primary": False,
        })

    if licenses:
        licenses[0]["primary"] = True

    return licenses


def extract_member_since_from_uuid(agent_id: str) -> str:
    """UUID v1 embeds a 60-bit timestamp; decode it to an ISO date."""
    try:
        u = uuid.UUID(agent_id)
        if u.version == 1:
            ts = (u.time - 0x01B21DD213814000) / 1e7
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        pass
    return ""


# ── CSV helpers ───────────────────────────────────────────────────────────────

def _open_csv_writer():
    """Open agents.csv in append mode; write header only if file is new/empty."""
    is_new = not CSV_OUT.exists() or CSV_OUT.stat().st_size == 0
    f = CSV_OUT.open("a", newline="")
    writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
    if is_new:
        writer.writeheader()
    return f, writer


def _csv_row(record: dict) -> dict:
    row = {**record}
    row["languages"]       = ", ".join(record.get("languages") or [])
    row["specializations"] = ", ".join(record.get("specializations") or [])
    licenses = record.get("license") or []
    row["license"] = "; ".join(
        f"{lic['number']} ({lic['state']})" for lic in licenses
    )
    return row


# ── Search trigger ────────────────────────────────────────────────────────────

async def dismiss_overlays(page: Page) -> None:
    for sel in [
        "button#truste-consent-button",
        "button[id*='consent' i]",
        "button[class*='consent' i]",
        "button:has-text('Accept')",
        "button:has-text('OK')",
        "button:has-text('Got it')",
        "a:has-text('Accept')",
    ]:
        try:
            btn = page.locator(sel).first
            await btn.wait_for(state="visible", timeout=2_000)
            await btn.click()
            print(f"Dismissed consent overlay via: {sel}")
            await asyncio.sleep(0.5)
            break
        except Exception:
            continue

    await page.evaluate("""() => {
        const overlay = document.querySelector('#pop-div205792745910362476, .truste_box_overlay, [id^="pop-div"]');
        if (overlay) overlay.remove();
        const frame = document.querySelector('.truste_popframe, [id^="pop-frame"]');
        if (frame) frame.remove();
    }""")


async def trigger_search(page: Page) -> None:
    """Type 'Florida' in the location input and click the first suggestion or press Enter."""
    location_inp = None
    for sel in [
        "input[placeholder*='location' i]",
        "input[data-testid='locationInput']",
        "input[placeholder*='city' i]",
        "input[placeholder*='search' i]",
        "input[name*='location' i]",
        "input[type='text']",
    ]:
        try:
            loc = page.locator(sel).first
            await loc.wait_for(state="visible", timeout=2_000)
            location_inp = loc
            break
        except Exception:
            continue

    if location_inp is None:
        print("Could not find location input — page may not have loaded correctly.")
        return

    await page.evaluate("""(sel) => {
        const inp = document.querySelector(sel);
        if (!inp) return;
        const nativeInputValueSetter = Object.getOwnPropertyDescriptor(
            window.HTMLInputElement.prototype, 'value').set;
        nativeInputValueSetter.call(inp, '');
        inp.dispatchEvent(new Event('input', { bubbles: true }));
    }""", "input[placeholder='Location'], input[data-testid='locationInput']")
    await location_inp.click(force=True)
    await location_inp.fill("")
    await location_inp.type("Florida", delay=80)
    await asyncio.sleep(2.0)

    clicked_suggestion = False
    for sel in [
        "[data-testid*='suggestion']",
        "li[role='option']",
        "ul[role='listbox'] li",
        ".autocomplete-suggestion, .suggestion-item",
    ]:
        try:
            loc = page.locator(sel).first
            await loc.wait_for(state="visible", timeout=3_000)
            await loc.click()
            print(f"Clicked suggestion via selector: {sel}")
            clicked_suggestion = True
            break
        except Exception:
            continue

    if not clicked_suggestion:
        await location_inp.press("Enter")
        print("Pressed Enter in location input to trigger search.")


# ── Phase 1: list scraping ────────────────────────────────────────────────────

def _parse_list_agents(raw: list[dict]) -> list[dict]:
    return [
        {
            "id":        a.get("id") or "",
            "firstName": a.get("firstName") or "",
            "lastName":  a.get("lastName") or "",
            "email":     a.get("email") or "",
            "phone":     a.get("phoneNumber") or "",
            "city":      a.get("city") or "",
            "state":     a.get("state") or "",
            "photo":     a.get("photo") or "",
        }
        for a in raw
        if (a.get("firstName") or a.get("lastName")) and a.get("id")
    ]


async def _collect_page(page: Page, page_num: int) -> list[dict]:
    """Navigate to one listing page and return parsed agents."""
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
                existing_ids = {a.get("id") for a in _captured}
                new = [a for a in agents if a.get("id") not in existing_ids]
                _captured.extend(new)
                print(f"GraphQL: {len(new)} new agents (total: {len(_captured)})")
        except Exception as e:
            print(f"GraphQL parse error: {e}")

    page.on("response", handle_response)

    if page_num == 1:
        await page.goto(START_URL, wait_until="domcontentloaded", timeout=60_000)

        if "Just a moment" in await page.title():
            await bypass_cloudflare(page)

        await asyncio.sleep(3.0)
        await dismiss_overlays(page)

        # If no agents captured within 3s, trigger search manually
        if not captured:
            await trigger_search(page)
    else:
        # Try clicking the Next page button — avoids retyping Florida (which resets to page 1)
        clicked_next = False
        for sel in [
            "button[aria-label*='Next' i]",
            "a[aria-label*='Next' i]",
            "button:has-text('Next')",
            "[class*='agination'] button:last-child",
        ]:
            try:
                btn = page.locator(sel).first
                await btn.wait_for(state="visible", timeout=3_000)
                await btn.click()
                print(f"Clicked Next page via: {sel}")
                clicked_next = True
                break
            except Exception:
                continue

        if clicked_next:
            # Responses trickle in one-by-one after Next click; wait a fixed window.
            await asyncio.sleep(15.0)
        else:
            print("Next button not found — falling back to URL navigation + trigger_search")
            url = AGENTS_URL.format(page=page_num)
            await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            if "Just a moment" in await page.title():
                await bypass_cloudflare(page)
            await asyncio.sleep(3.0)
            await dismiss_overlays(page)
            if not captured:
                await trigger_search(page)

    # Wait up to 20s for GraphQL response
    for _ in range(40):
        if captured:
            break
        await asyncio.sleep(0.5)

    if captured:
        try:
            await page.wait_for_load_state("networkidle", timeout=10_000)
        except Exception:
            pass

    page.remove_listener("response", handle_response)
    return _parse_list_agents(captured)


# ── Phase 2: detail scraping ──────────────────────────────────────────────────

async def fetch_agent_detail_via_navigation(page: Page, agent: dict) -> dict:
    """
    Navigate to the agent profile page and intercept the GraphQL response.
    Direct fetch() calls return fake data; only the React-initiated call is real.
    cf_clearance from Phase 1 is still active, so no new CF challenge fires.
    """
    agent_id = agent["id"]
    first = (agent.get("firstName") or "").replace(" ", "-")
    last  = (agent.get("lastName")  or "").replace(" ", "-")
    profile_url = f"https://www.exprealty.com/agents-search/{first}-{last}_{agent_id}"

    async def _attempt(captured: dict) -> None:
        async def capture_detail(response):
            if "agentdir-api.expproptech.com/graphql" not in response.url:
                return
            try:
                # Read body bytes eagerly before Playwright GC's the resource
                raw = await response.body()
                agent_data = (json.loads(raw).get("data") or {}).get("agent")
                if agent_data and isinstance(agent_data, dict):
                    if not agent_data.get("id"):
                        agent_data["id"] = agent_id
                    captured.update(agent_data)
            except Exception:
                pass

        page.on("response", capture_detail)
        try:
            await page.goto(profile_url, wait_until="domcontentloaded", timeout=30_000)

            if "Just a moment" in await page.title():
                await bypass_cloudflare(page)

            for _ in range(40):
                if captured:
                    break
                await asyncio.sleep(0.5)

            if not captured:
                await page.evaluate("window.scrollTo(0, 500)")
                await asyncio.sleep(3.0)
        finally:
            page.remove_listener("response", capture_detail)

    captured: dict = {}
    await _attempt(captured)
    if not captured:
        await asyncio.sleep(1.0)
        await _attempt(captured)  # one retry on a fresh navigation

    return captured


def build_agent_record(detail: dict) -> dict:
    bio      = detail.get("bio") or ""
    agent_id = detail.get("id") or ""
    member_since = detail.get("memberSince") or extract_member_since_from_uuid(agent_id)
    return {
        "id":              agent_id,
        "firstName":       detail.get("firstName") or "",
        "lastName":        detail.get("lastName") or "",
        "email":           detail.get("email") or "",
        "phoneNumber":     detail.get("phoneNumber") or "",
        "photo":           detail.get("photo") or "",
        "languages":       detail.get("languages") or [],
        "specializations": detail.get("specializations") or [],
        "facebook":        detail.get("facebook") or "",
        "instagram":       detail.get("instagram") or "",
        "linkedIn":        detail.get("linkedIn") or "",
        "twitter":         detail.get("twitter") or "",
        "website":         detail.get("website") or "",
        "youtube":         detail.get("youtube") or "",
        "city":            detail.get("city") or "",
        "state":           detail.get("state") or "",
        "zipcode":         detail.get("zipcode") or "",
        "countryCode":     detail.get("countryCode") or "",
        "license":         extract_licenses(bio),
        "memberSince":     member_since,
    }


# ── Entry point ───────────────────────────────────────────────────────────────

async def scrape_all(num_pages: int = 1, detail_rate_limit_s: float = 0.5) -> None:
    _stop = False

    def _handle_sigint(sig, frame):
        nonlocal _stop
        _stop = True

    signal.signal(signal.SIGINT, _handle_sigint)

    csv_file, csv_writer = _open_csv_writer()
    saved_count = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
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

            # ── Phase 1 ──
            list_agents: list[dict] = []
            for page_num in range(1, num_pages + 1):
                print(f"\n── Page {page_num} ──")
                batch = await _collect_page(page, page_num)
                print(f"Extracted {len(batch)} agents from page {page_num}")
                list_agents.extend(batch)

                if page_num == 1:
                    await save_cookies(context)

                if page_num < num_pages:
                    await asyncio.sleep(2.0)

            # Deduplicate across pages by id
            seen_ids: set = set()
            unique_agents = []
            for a in list_agents:
                if a["id"] not in seen_ids:
                    seen_ids.add(a["id"])
                    unique_agents.append(a)

            print(f"\nPhase 1 complete: {len(unique_agents)} unique agents")

            # ── Phase 2 ──
            print("\nPhase 2: fetching agent details…")

            for i, agent in enumerate(unique_agents, 1):
                if _stop:
                    break
                if not agent.get("id"):
                    continue
                try:
                    detail = await fetch_agent_detail_via_navigation(page, agent)
                    if not detail.get("id"):
                        print(f"[{i}/{len(unique_agents)}] No detail for {agent.get('id')} — skipping")
                        continue
                    record = build_agent_record(detail)
                    csv_writer.writerow(_csv_row(record))
                    csv_file.flush()
                    saved_count += 1
                    print(f"[{i}/{len(unique_agents)}] {record['firstName']} {record['lastName']}")
                except Exception as e:
                    print(f"[{i}/{len(unique_agents)}] Error for {agent.get('id')}: {e}")

                if i < len(unique_agents):
                    await asyncio.sleep(detail_rate_limit_s)

        finally:
            csv_file.close()
            await browser.close()

    if _stop:
        print(f"\nInterrupted — saved {saved_count} agents so far.")
    else:
        print(f"\nDone — saved {saved_count} agents to {CSV_OUT}")


if __name__ == "__main__":
    pages = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    asyncio.run(scrape_all(num_pages=pages))
