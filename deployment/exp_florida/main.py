"""
Actor Apify pour scraper les agents ExP Florida
1. Phase 1 : collecte la liste des agents via interception GraphQL (pagination)
2. Phase 2 : récupère les détails de chaque agent via navigation profil
3. Push les données dans staging_scrapes sur Supabase
"""

import asyncio
import json
import os
import random
import re
import time
import uuid
from datetime import datetime, timezone
from html import unescape
from urllib.parse import parse_qs, urlparse

import requests
from apify import Actor, Event
from playwright.async_api import BrowserContext, Page, async_playwright


# ── Config ────────────────────────────────────────────────────────────────────

START_URL = (
    "https://www.exprealty.com/agents-search"
    "?page=1&country=US&m=f&location=Florida+%28FL%29+-+State"
)
AGENTS_URL = (
    "https://www.exprealty.com/agents-search"
    "?page={page}&country=US&m=f&location=Florida+%28FL%29+-+State"
)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

NETWORK_NAME = "ExP Florida"

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

def solve_turnstile(params: dict, api_key: str) -> str:
    payload = {
        "clientKey": api_key,
        "task": {"type": "TurnstileTaskProxyless", **params},
    }
    Actor.log.info(f"Submitting to 2captcha: sitekey={params.get('websiteKey')}")

    submit = requests.post("https://api.2captcha.com/createTask", json=payload).json()
    if submit.get("errorId") != 0:
        raise RuntimeError(f"2captcha createTask failed: {submit}")

    task_id = submit["taskId"]
    Actor.log.info(f"2captcha task ID: {task_id}")

    for attempt in range(24):  # poll up to 2 minutes
        time.sleep(5)
        result = requests.post(
            "https://api.2captcha.com/getTaskResult",
            json={"clientKey": api_key, "taskId": task_id},
        ).json()

        if result.get("errorId") != 0:
            raise RuntimeError(f"2captcha getTaskResult error: {result}")

        if result.get("status") == "ready":
            Actor.log.info("2captcha token received.")
            return result["solution"]["token"]

        Actor.log.info(f"Waiting for 2captcha… ({attempt + 1}/24)")

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


async def bypass_cloudflare(page: Page, captcha_api_key: str) -> None:
    Actor.log.info("Cloudflare challenge detected. Waiting for Turnstile intercept…")

    try:
        await page.wait_for_function("window.__tsParams !== undefined", timeout=30_000)
    except Exception:
        raise RuntimeError("window.__tsParams was never set — intercept did not fire.")

    params = await page.evaluate("window.__tsParams")
    Actor.log.info(f"Captured sitekey={params.get('websiteKey')} action={params.get('action')}")

    token = solve_turnstile({k: v for k, v in params.items() if v is not None}, captcha_api_key)

    async with page.expect_navigation(timeout=30_000, wait_until="load"):
        await page.evaluate(f"window.__tsCallback('{token}')")
    Actor.log.info("Cloudflare challenge passed.")


# ── Cookie persistence (via Apify KV store) ───────────────────────────────────

async def save_cookies(context: BrowserContext) -> None:
    cookies = await context.cookies()
    await Actor.set_value("cf_cookies", cookies)
    Actor.log.info(f"Saved {len(cookies)} cookies to KV store.")


async def load_cookies(context: BrowserContext) -> None:
    cookies = await Actor.get_value("cf_cookies")
    if cookies:
        await context.add_cookies(cookies)
        Actor.log.info(f"Loaded {len(cookies)} saved cookies from KV store.")


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


def extract_licenses(text: str) -> list:
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


# ── Search / overlay helpers ──────────────────────────────────────────────────

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
            Actor.log.info(f"Dismissed consent overlay via: {sel}")
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
        Actor.log.warning("Could not find location input — page may not have loaded correctly.")
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

    # Wait for autocomplete suggestions to appear (longer on headless)
    await asyncio.sleep(4.0)

    # 1. Try clicking a suggestion by selector
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
            Actor.log.info(f"Clicked suggestion via selector: {sel}")
            clicked_suggestion = True
            break
        except Exception:
            continue

    if not clicked_suggestion:
        # 2. ArrowDown selects the first suggestion in the dropdown, then Enter confirms it.
        #    This works on headless where the suggestion may render but not be "visible"
        #    to Playwright's click detection.
        Actor.log.info("Suggestion click failed — trying ArrowDown + Enter.")
        await location_inp.press("ArrowDown")
        await asyncio.sleep(0.5)
        await location_inp.press("Enter")
        Actor.log.info("ArrowDown + Enter pressed to select first suggestion.")


# ── Phase 1: list scraping ────────────────────────────────────────────────────

def _parse_list_agents(raw: list) -> list:
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


def _extract_graphql_agents(data: dict) -> list:
    return (
        data.get("search", {}).get("agents")
        or data.get("getAgentsByFilters", {}).get("agents")
        or data.get("agents")
        or []
    )


async def _collect_page1_via_browser(page: Page, captcha_api_key: str) -> tuple:
    """
    Page 1 only: navigate via browser, bypass CF if needed, trigger search.
    Returns (list_agents, graphql_request_info).

    Key constraint: on Apify (headless Linux), typing 'Florida' fires an
    autocomplete GraphQL stub (1 agent). The real full-search response (12+ agents)
    only arrives after Enter/suggestion click. We must NOT break on the first
    agent — we wait a fixed window so both responses are collected, and we only
    capture the request metadata from the full-search response (≥5 agents).
    """
    captured_agents: list = []
    captured_request: dict = {}

    async def handle_response(response):
        if "agentdir-api.expproptech.com/graphql" not in response.url:
            return
        try:
            data = (await response.json()).get("data") or {}
            agents = _extract_graphql_agents(data)
            if agents:
                # Only capture request metadata from full-search responses (not autocomplete stubs)
                if len(agents) >= 5 and not captured_request:
                    try:
                        req = response.request
                        post_data = req.post_data
                        if post_data:
                            captured_request['url'] = req.url
                            captured_request['headers'] = dict(req.headers)
                            captured_request['post_data'] = post_data
                            Actor.log.info(f"GraphQL full-search request captured ({len(agents)} agents)")
                    except Exception as e:
                        Actor.log.debug(f"Request capture error: {e}")
                existing_ids = {a.get("id") for a in captured_agents}
                new = [a for a in agents if a.get("id") not in existing_ids]
                captured_agents.extend(new)
                Actor.log.info(f"GraphQL: {len(new)} new agents (total: {len(captured_agents)})")
        except Exception as e:
            Actor.log.debug(f"GraphQL parse error: {e}")

    page.on("response", handle_response)

    await page.goto(START_URL, wait_until="domcontentloaded", timeout=60_000)

    if "Just a moment" in await page.title():
        await bypass_cloudflare(page, captcha_api_key)

    await asyncio.sleep(3.0)
    await dismiss_overlays(page)

    # Wait up to 15s for automatic GraphQL from URL load
    for _ in range(30):
        if len(captured_agents) >= 5:
            break
        await asyncio.sleep(0.5)

    # If URL load didn't produce full results, trigger search manually then wait a
    # FIXED window — do not break early, the autocomplete fires first (1 agent)
    # and the full search fires a few seconds later (12+ agents).
    if len(captured_agents) < 5:
        Actor.log.info("Full search results not detected via URL — triggering search manually.")
        await trigger_search(page)
        Actor.log.info("Waiting 20s for full search GraphQL response…")
        await asyncio.sleep(20.0)

    if not captured_agents:
        Actor.log.warning("No agents captured on page 1.")
    else:
        Actor.log.info(f"Page 1 final: {len(captured_agents)} agents, request captured: {bool(captured_request)}")

    page.remove_listener("response", handle_response)
    return _parse_list_agents(captured_agents), captured_request


def _fetch_page_via_graphql(page_num: int, request_info: dict) -> list:
    """
    Pages 2+: replay the captured GraphQL request directly via HTTP,
    modifying only the page number in the variables. No browser needed.
    """
    try:
        body = json.loads(request_info['post_data'])

        # Update the page variable — try common field names
        if 'variables' in body and isinstance(body['variables'], dict):
            variables = body['variables']
            updated = False
            for key in ['page', 'pageNumber', 'pageNum', 'currentPage', 'offset']:
                if key in variables:
                    if key == 'offset':
                        # offset-based: infer page size from existing value
                        page_size = variables.get('limit') or variables.get('pageSize') or 12
                        variables[key] = (page_num - 1) * page_size
                    else:
                        variables[key] = page_num
                    updated = True
                    break
            if not updated:
                variables['page'] = page_num

        # Strip headers that would cause issues server-side
        headers = {
            k: v for k, v in request_info['headers'].items()
            if k.lower() not in ('content-length', 'host', ':method', ':path', ':scheme', ':authority')
        }

        response = requests.post(
            request_info['url'],
            headers=headers,
            json=body,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json().get("data") or {}
        agents = _extract_graphql_agents(data)
        Actor.log.info(f"GraphQL API page {page_num}: {len(agents)} agents")
        return _parse_list_agents(agents)
    except Exception as e:
        Actor.log.error(f"GraphQL API request failed for page {page_num}: {e}")
        return []


# ── Phase 2: detail scraping ──────────────────────────────────────────────────

async def fetch_agent_detail_via_navigation(page: Page, agent: dict, captcha_api_key: str) -> dict:
    agent_id = agent["id"]
    first = (agent.get("firstName") or "").replace(" ", "-")
    last  = (agent.get("lastName")  or "").replace(" ", "-")
    profile_url = f"https://www.exprealty.com/agents-search/{first}-{last}_{agent_id}"

    async def _attempt(captured: dict) -> None:
        async def capture_detail(response):
            if "agentdir-api.expproptech.com/graphql" not in response.url:
                return
            try:
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
            nav_ok = True
            try:
                await page.goto(profile_url, wait_until="domcontentloaded", timeout=30_000)
            except Exception:
                nav_ok = False

            if not nav_ok:
                Actor.log.warning("Navigation timed out — CF rate-limit suspected. Cooling down 60s…")
                await asyncio.sleep(60.0)
                try:
                    await page.goto(profile_url, wait_until="domcontentloaded", timeout=30_000)
                except Exception:
                    pass

            if "Just a moment" in await page.title():
                await bypass_cloudflare(page, captcha_api_key)
                await page.goto(profile_url, wait_until="domcontentloaded", timeout=30_000)

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
        await _attempt(captured)

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


# ── Supabase push ─────────────────────────────────────────────────────────────

def push_to_supabase(supabase_client, batch_id, items):
    """Push les données dans staging_scrapes sur Supabase."""
    if not items:
        return 0

    rows = [
        {"batch_id": str(batch_id), "network": NETWORK_NAME, "raw_data": item, "status": "PENDING"}
        for item in items
    ]

    batch_size = 100
    inserted = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        try:
            supabase_client.table("staging_scrapes").insert(batch).execute()
            inserted += len(batch)
        except Exception as e:
            Actor.log.error(f"Erreur Supabase insert batch {i}: {e}")

    return inserted


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    async with Actor:
        input_data = await Actor.get_input() or {}

        captcha_api_key = input_data.get("captchaApiKey") or os.environ.get("TWOCAPTCHA_API_KEY")
        num_pages = input_data.get("numPages", 1)
        detail_rate_limit_s = input_data.get("detailRateLimit", 3.0)
        supabase_url = input_data.get("supabaseUrl") or os.environ.get("SUPABASE_URL")
        supabase_key = input_data.get("supabaseKey") or os.environ.get("SUPABASE_KEY")

        batch_id = uuid.uuid4()

        Actor.log.info("=" * 50)
        Actor.log.info("=== ExP Florida Scraper ===")
        Actor.log.info(f"Batch ID: {batch_id}")
        Actor.log.info(f"Pages à scraper: {num_pages}")
        Actor.log.info("=" * 50)

        if not captcha_api_key:
            Actor.log.error("captchaApiKey est requis (clé 2captcha).")
            return

        # Supabase staging
        supabase_client = None
        if supabase_url and supabase_key:
            try:
                from supabase import create_client
                supabase_client = create_client(supabase_url, supabase_key)
                Actor.log.info("Supabase staging connecté")
            except ImportError:
                Actor.log.warning("Module supabase non installé")
            except Exception as e:
                Actor.log.warning(f"Erreur connexion Supabase: {e}")
        else:
            Actor.log.info("Supabase non configuré - export dataset uniquement")

        _stop = False

        async def handle_abort():
            nonlocal _stop
            _stop = True
            Actor.log.info("Abort reçu — arrêt propre après l'agent en cours.")

        Actor.on(Event.ABORTING, handle_abort)

        all_records = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
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

                # ── Phase 1: collect agent list ──
                Actor.log.info(f"\n--- PHASE 1: Collecte des listes ({num_pages} page(s)) ---")
                list_agents: list = []

                # Page 1: browser (CF bypass + search trigger + capture GraphQL request)
                Actor.log.info(f"  Page 1/{num_pages}…")
                page1_agents, graphql_request_info = await _collect_page1_via_browser(page, captcha_api_key)
                Actor.log.info(f"  {len(page1_agents)} agents extraits de la page 1")
                list_agents.extend(page1_agents)
                await save_cookies(context)

                if graphql_request_info:
                    Actor.log.info("  GraphQL request capturé — pagination via API directe")
                else:
                    Actor.log.warning("  GraphQL request non capturé — pages 2+ impossible")

                # Pages 2+: direct GraphQL API (no browser navigation)
                for page_num in range(2, num_pages + 1):
                    if _stop:
                        break
                    if not graphql_request_info:
                        break
                    Actor.log.info(f"  Page {page_num}/{num_pages}…")
                    batch = _fetch_page_via_graphql(page_num, graphql_request_info)
                    Actor.log.info(f"  {len(batch)} agents extraits de la page {page_num}")
                    list_agents.extend(batch)
                    await asyncio.sleep(1.0)

                # Deduplicate
                seen_ids: set = set()
                unique_agents = []
                for a in list_agents:
                    if a["id"] not in seen_ids:
                        seen_ids.add(a["id"])
                        unique_agents.append(a)

                Actor.log.info(f"\nPhase 1 terminée: {len(unique_agents)} agents uniques")

                # ── Phase 2: fetch agent details ──
                Actor.log.info(f"\n--- PHASE 2: Détails des agents ({len(unique_agents)}) ---")

                for i, agent in enumerate(unique_agents, 1):
                    if _stop:
                        break
                    if not agent.get("id"):
                        continue
                    try:
                        detail = await fetch_agent_detail_via_navigation(page, agent, captcha_api_key)
                        if not detail.get("id"):
                            Actor.log.warning(f"  [{i}/{len(unique_agents)}] Pas de détail pour {agent.get('id')} — skip")
                            continue
                        record = build_agent_record(detail)
                        all_records.append(record)
                        Actor.log.info(f"  [{i}/{len(unique_agents)}] {record['firstName']} {record['lastName']}")
                    except Exception as e:
                        Actor.log.warning(f"  [{i}/{len(unique_agents)}] Erreur pour {agent.get('id')}: {e}")

                    if i < len(unique_agents) and not _stop:
                        if i % 25 == 0:
                            Actor.log.info(f"  Pause 90s à l'agent {i} pour reset CF rate window…")
                            await asyncio.sleep(90.0)
                        else:
                            jitter = random.uniform(0.0, 2.0)
                            await asyncio.sleep(detail_rate_limit_s + jitter)

            finally:
                await browser.close()

        Actor.log.info(f"\n{len(all_records)} agents collectés")

        # ── Push vers Supabase staging ──
        if supabase_client and all_records:
            Actor.log.info(f"\n--- PUSH Supabase ---")
            inserted = push_to_supabase(supabase_client, batch_id, all_records)
            Actor.log.info(f"Lignes insérées: {inserted}")

        # ── Push vers Apify dataset ──
        Actor.log.info(f"\n--- Sauvegarde dataset Apify ---")

        output_items = [
            {
                "batch_id":        str(batch_id),
                "network":         NETWORK_NAME,
                "id":              r.get("id"),
                "firstName":       r.get("firstName"),
                "lastName":        r.get("lastName"),
                "email":           r.get("email"),
                "phoneNumber":     r.get("phoneNumber"),
                "photo":           r.get("photo"),
                "languages":       r.get("languages"),
                "specializations": r.get("specializations"),
                "facebook":        r.get("facebook"),
                "instagram":       r.get("instagram"),
                "linkedIn":        r.get("linkedIn"),
                "twitter":         r.get("twitter"),
                "website":         r.get("website"),
                "youtube":         r.get("youtube"),
                "city":            r.get("city"),
                "state":           r.get("state"),
                "zipcode":         r.get("zipcode"),
                "countryCode":     r.get("countryCode"),
                "license":         r.get("license"),
                "memberSince":     r.get("memberSince"),
                "raw_data":        r,
            }
            for r in all_records
        ]

        await Actor.push_data(output_items)
        Actor.log.info(f"Données sauvegardées: {len(output_items)} agents")

        Actor.log.info(f"\n{'='*50}")
        Actor.log.info("=== SCRAPING TERMINÉ ===")
        Actor.log.info(f"Batch ID: {batch_id}")
        Actor.log.info(f"Network: {NETWORK_NAME}")
        Actor.log.info(f"Total agents: {len(all_records)}")
        Actor.log.info(f"{'='*50}")

        await Actor.set_value("batch_id", str(batch_id))
        await Actor.set_value("stats", {
            "batch_id":     str(batch_id),
            "network":      NETWORK_NAME,
            "total_agents": len(all_records),
            "supabase_enabled": supabase_client is not None,
        })


if __name__ == "__main__":
    asyncio.run(main())
