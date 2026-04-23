"""
Actor Apify pour scraper les agents RealAdvisor Spain
1. Phase 1 : collecte les URLs agents depuis les pages listing (requests)
2. Phase 2 : scrape chaque profil via Playwright (phone, mandates, sales, price, linkedin)
3. Push les données dans staging_scrapes sur Supabase
"""

import asyncio
import json
import os
import re
import time
import uuid
import requests
from datetime import datetime
from apify import Actor
from playwright.async_api import async_playwright


BASE_URL = "https://realadvisor.es"
LISTING_URL = BASE_URL + "/es/agentes-inmobiliarios"
NETWORK_NAME = "RealAdvisor Spain"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9",
}


#### PHASE 1: LISTING PAGE SCRAPING (requests) ####

def fetch_listing_page(page_num):
    """Fetches one listing page and extracts agent URLs from JSON-LD."""
    if page_num == 1:
        url = LISTING_URL
    else:
        url = f"{LISTING_URL}/pagina-{page_num}"

    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    html = resp.text

    match = re.search(
        r'<script[^>]*id="agent-locality-schema"[^>]*>(.*?)</script>',
        html, re.DOTALL
    )
    if not match:
        return []

    data = json.loads(match.group(1))
    if isinstance(data, list) and len(data) > 0:
        data = data[0] if isinstance(data[0], dict) and data[0].get("itemListElement") else data
    if isinstance(data, list):
        return []

    agents = []
    for item in data.get("itemListElement", []):
        agent_item = item.get("item", {})
        rel_url = agent_item.get("url", "")
        full_url = BASE_URL + rel_url if rel_url.startswith("/") else rel_url

        agents.append({
            "name": agent_item.get("name", ""),
            "agency": agent_item.get("description", ""),
            "address": agent_item.get("address", ""),
            "url": full_url,
        })

    return agents


def fetch_all_agent_urls(max_pages=None):
    """Fetches all agent basic info from listing pages."""
    all_agents = []
    page = 1

    while True:
        if max_pages and page > max_pages:
            break

        try:
            agents = fetch_listing_page(page)
        except Exception as e:
            Actor.log.warning(f"Error on page {page}: {e}")
            break

        if not agents:
            break

        all_agents.extend(agents)
        if page % 10 == 0:
            Actor.log.info(f"  Page {page}: {len(all_agents)} agents collected...")

        page += 1
        time.sleep(0.5)

    return all_agents


#### PHASE 2: PROFILE PAGE SCRAPING (Playwright) ####

def parse_address(address_str):
    """Extracts postal code and city from address like 'Calle X, 07010 Palma'."""
    if not address_str:
        return None, None
    match = re.search(r'(\d{5})\s+(.+)$', address_str)
    if match:
        return match.group(1), match.group(2).strip()
    return None, address_str.strip()


def parse_price_k(text):
    """Parses '280k' or '1.2M' format to integer."""
    if not text:
        return None
    text = text.strip().replace('\xa0', ' ')
    match = re.search(r'([\d.,]+)\s*(k|M)', text)
    if match:
        num_str = match.group(1).replace(',', '.')
        multiplier = 1000 if match.group(2) == 'k' else 1000000
        try:
            return int(float(num_str) * multiplier)
        except ValueError:
            return None
    return None


async def fetch_agent_profile(page, url):
    """Scrapes an individual agent profile page using Playwright."""
    result = {
        "phone_number": None,
        "arrival_date": None,
        "linkedin_url": None,
        "nb_mandates": None,
        "nb_sales": None,
        "avg_mandate_price": None,
    }

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(2)
    except Exception as e:
        Actor.log.debug(f"Error navigating to {url}: {e}")
        return result

    # Click "mostrar numero" button to reveal phone
    try:
        show_btn = page.locator("button:has-text('mostrar'), button:has-text('Mostrar'), button:has-text('número'), button:has-text('teléfono')")
        if await show_btn.count() > 0:
            await show_btn.first.click()
            await asyncio.sleep(1.5)
    except Exception:
        pass

    html = await page.content()

    # Phone from tel: link (visible after click)
    phone_match = re.search(r'href="tel:([^"]+)"', html)
    if phone_match:
        phone = phone_match.group(1).strip()
        result["phone_number"] = re.sub(r'\s+', '', phone)

    # Arrival date: infer from "X años en el negocio"
    years_match = re.search(r'(\d+)\s*años?\s+en\s+el\s+negocio', html, re.IGNORECASE)
    if years_match:
        years = int(years_match.group(1))
        arrival_year = datetime.now().year - years
        result["arrival_date"] = f"{arrival_year}"

    # LinkedIn URL
    linkedin_match = re.search(r'href="(https?://(?:www\.)?linkedin\.com/[^"]+)"', html)
    if linkedin_match:
        result["linkedin_url"] = linkedin_match.group(1)

    # Active mandates: number before "contratos en RealAdvisor"
    mandates_match = re.search(r'>(\d+)</div>\s*[Cc]ontratos?\s+en\s+RealAdvisor', html)
    if not mandates_match:
        mandates_match = re.search(r'(\d+)\s*contratos?\s+en\s+RealAdvisor', html, re.IGNORECASE)
    if mandates_match:
        result["nb_mandates"] = int(mandates_match.group(1))

    # Sales: number before "Propiedades vendidas"
    sales_match = re.search(r'>(\d+)</div>\s*Propiedades\s+vendidas', html)
    if not sales_match:
        sales_match = re.search(r'(\d+)\s*Propiedades\s+vendidas', html, re.IGNORECASE)
    if sales_match:
        result["nb_sales"] = int(sales_match.group(1))

    # Median sale price: "Xk EUR" before "Precio de venta mediano"
    price_match = re.search(
        r'>([\d.,]+[kM])\s*(?:&nbsp;|\xa0|\s)*EUR</div>\s*Precio\s+de\s+venta\s+median[oa]',
        html, re.IGNORECASE
    )
    if not price_match:
        price_match = re.search(
            r'([\d.,]+[kM])\s*(?:&nbsp;|\xa0|\s)*EUR.*?Precio\s+de\s+venta\s+median[oa]',
            html, re.IGNORECASE
        )
    if price_match:
        result["avg_mandate_price"] = parse_price_k(price_match.group(1))

    return result


#### SUPABASE PUSH ####

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


#### MAIN ####

async def main():
    async with Actor:
        input_data = await Actor.get_input() or {}

        max_pages = input_data.get("maxPages", None)
        supabase_url = input_data.get("supabaseUrl") or os.environ.get("SUPABASE_URL")
        supabase_key = input_data.get("supabaseKey") or os.environ.get("SUPABASE_KEY")

        batch_id = uuid.uuid4()

        Actor.log.info("=" * 50)
        Actor.log.info("=== RealAdvisor Spain Scraper ===")
        Actor.log.info(f"Batch ID: {batch_id}")
        Actor.log.info("=" * 50)

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

        # ============================================
        # PHASE 1: Collect agent URLs (requests)
        # ============================================
        Actor.log.info("\n--- PHASE 1: Collecte des URLs agents ---")

        agents_listing = fetch_all_agent_urls(max_pages=max_pages)
        Actor.log.info(f"Total agents collectés: {len(agents_listing)}")

        if not agents_listing:
            Actor.log.error("Aucun agent trouvé. Arrêt.")
            return

        # ============================================
        # PHASE 2: Scrape profiles (Playwright)
        # ============================================
        Actor.log.info(f"\n--- PHASE 2: Scraping profils ({len(agents_listing)}) ---")

        all_records = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            pw_page = await browser.new_page()

            for i, agent in enumerate(agents_listing, 1):
                url = agent.get("url")
                if not url:
                    continue

                profile = await fetch_agent_profile(pw_page, url)

                name = agent.get("name", "")
                parts = name.split(" ", 1)
                first_name = parts[0] if parts else ""
                last_name = parts[1] if len(parts) > 1 else ""
                postal_code, city = parse_address(agent.get("address"))

                record = {
                    "id": url,
                    "first_name": first_name,
                    "last_name": last_name,
                    "postal_code": postal_code,
                    "city": city,
                    "phone_number": profile.get("phone_number"),
                    "arrival_date": profile.get("arrival_date"),
                    "email": None,
                    "linkedin_url": profile.get("linkedin_url"),
                    "nb_mandates": profile.get("nb_mandates"),
                    "avg_mandate_price": profile.get("avg_mandate_price"),
                    "nb_sales": profile.get("nb_sales"),
                    "url_website": url,
                    "network": NETWORK_NAME,
                }
                all_records.append(record)

                if i % 10 == 0:
                    Actor.log.info(f"  [{i}/{len(agents_listing)}] {first_name} {last_name}: "
                                   f"phone={record['phone_number']}, mandates={record['nb_mandates']}, "
                                   f"sales={record['nb_sales']}")

                await asyncio.sleep(1.0)

            await browser.close()

        Actor.log.info(f"\n{len(all_records)} agents scrapés")

        with_phone = sum(1 for a in all_records if a.get("phone_number"))
        with_city = sum(1 for a in all_records if a.get("city"))
        with_mandates = sum(1 for a in all_records if a.get("nb_mandates"))
        Actor.log.info(f"  Avec téléphone: {with_phone}")
        Actor.log.info(f"  Avec ville: {with_city}")
        Actor.log.info(f"  Avec mandats: {with_mandates}")

        # ============================================
        # PHASE 3: Push vers Supabase staging
        # ============================================
        if supabase_client and all_records:
            Actor.log.info(f"\n--- PHASE 3: Push vers Supabase ---")
            inserted = push_to_supabase(supabase_client, batch_id, all_records)
            Actor.log.info(f"Lignes insérées: {inserted}")

        # ============================================
        # PHASE 4: Sauvegarde dataset Apify
        # ============================================
        Actor.log.info(f"\n--- PHASE 4: Sauvegarde dataset Apify ---")

        output_items = [
            {
                "batch_id": str(batch_id),
                "network": NETWORK_NAME,
                "id": a.get("id"),
                "first_name": a.get("first_name"),
                "last_name": a.get("last_name"),
                "postal_code": a.get("postal_code"),
                "city": a.get("city"),
                "phone_number": a.get("phone_number"),
                "arrival_date": a.get("arrival_date"),
                "email": a.get("email"),
                "linkedin_url": a.get("linkedin_url"),
                "nb_mandates": a.get("nb_mandates"),
                "avg_mandate_price": a.get("avg_mandate_price"),
                "nb_sales": a.get("nb_sales"),
                "url_website": a.get("url_website"),
                "raw_data": a,
            }
            for a in all_records
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
            "batch_id": str(batch_id),
            "network": NETWORK_NAME,
            "total_agents": len(all_records),
            "with_phone": with_phone,
            "with_city": with_city,
            "with_mandates": with_mandates,
            "supabase_enabled": supabase_client is not None,
        })


if __name__ == "__main__":
    asyncio.run(main())
