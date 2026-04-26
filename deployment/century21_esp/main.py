"""
Actor Apify pour scraper les agents Century21 Spain
1. Liste tous les agents via /api/agents (paginé)
2. Pour chaque agent, récupère ses propriétés via /api/properties?agent_handler=X
3. Calcule nb_mandates, avg_mandate_price, ville
4. Push les données dans staging_scrapes sur Supabase
"""

import asyncio
import os
import re
import time
import uuid
import requests
from apify import Actor


BASE_URL = "https://century21.es"
AGENTS_URL = BASE_URL + "/api/agents?page={page}"
PROPERTIES_URL = BASE_URL + "/api/properties?agent_handler={handler}&order_by=entered_market_desc"
NETWORK_NAME = "Century21 Spain"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "es-ES,es;q=0.9",
}


#### API CALLS ####

def fetch_agents_page(page=1):
    resp = requests.get(AGENTS_URL.format(page=page), headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_all_agents(max_agents=None):
    all_agents = []
    first = fetch_agents_page(1)
    total = first.get("total", 0)
    all_agents.extend(first.get("data", []))

    per_page = max(1, len(first.get("data", [])))
    if total <= per_page:
        return all_agents[:max_agents] if max_agents else all_agents

    total_pages = (total + per_page - 1) // per_page
    for page in range(2, total_pages + 1):
        try:
            data = fetch_agents_page(page)
            all_agents.extend(data.get("data", []))
            if max_agents and len(all_agents) >= max_agents:
                return all_agents[:max_agents]
            if page % 10 == 0:
                Actor.log.info(f"  Page {page}/{total_pages}: {len(all_agents)} agents")
            time.sleep(0.3)
        except Exception as e:
            Actor.log.warning(f"  Error on page {page}: {e}")
            break

    return all_agents


def fetch_agent_properties(handler):
    try:
        resp = requests.get(
            PROPERTIES_URL.format(handler=handler), headers=HEADERS, timeout=30
        )
        resp.raise_for_status()
        return resp.json().get("data", [])
    except Exception as e:
        Actor.log.debug(f"  Error fetching properties for {handler}: {e}")
        return []


def fetch_agent_linkedin(profile_url):
    """Fetches the agent's profile page and extracts personal LinkedIn URL (/in/ path)."""
    if not profile_url:
        return None
    try:
        resp = requests.get(profile_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        match = re.search(r'https?://(?:www\.)?linkedin\.com/in/[^"\s<>\\]+', resp.text)
        if match:
            return match.group(0).rstrip("/")
    except Exception:
        pass
    return None


#### HELPERS ####

def split_name(full_name):
    if not full_name:
        return "", ""
    parts = full_name.strip().split(" ", 1)
    first = parts[0]
    last = parts[1] if len(parts) > 1 else ""
    return first, last


def normalize_phone(phone):
    if not phone:
        return None
    digits = "".join(c for c in str(phone) if c.isdigit() or c == "+")
    digits = digits.replace("+", "")
    if digits.startswith("34") and len(digits) == 11:
        return f"+{digits}"
    if len(digits) == 9 and digits[0] in "6789":
        return f"+34{digits}"
    return phone.strip() if phone else None


def parse_city_from_address(address):
    if not address:
        return None
    if "," in address:
        parts = [p.strip() for p in address.split(",")]
        if len(parts) >= 3:
            candidate = parts[-2].strip()
            if candidate and not candidate.isdigit():
                return candidate.title()
    paren_match = re.search(r'\(([^)]+)\)', address)
    if paren_match:
        return paren_match.group(1).strip().title()
    return None


def aggregate_listings(properties):
    if not properties:
        return None, None, None

    sale_prices = []
    cities = []
    for p in properties:
        if p.get("ad_type") == "sell":
            price = p.get("price")
            if price and price > 0:
                sale_prices.append(price)
        city = parse_city_from_address(p.get("address"))
        if city:
            cities.append(city)

    nb_mandates = len(properties) if properties else None
    avg_price = int(sum(sale_prices) / len(sale_prices)) if sale_prices else None
    top_city = max(set(cities), key=cities.count) if cities else None

    return nb_mandates, avg_price, top_city


#### PARSING ####

def build_agent_record(agent_raw):
    name = agent_raw.get("name") or agent_raw.get("display_name") or ""
    first_name, last_name = split_name(name.strip())

    handler = agent_raw.get("handler") or ""
    properties = fetch_agent_properties(handler) if handler else []

    nb_mandates, avg_price, city = aggregate_listings(properties)
    linkedin = fetch_agent_linkedin(agent_raw.get("link"))

    return {
        "id": agent_raw.get("id"),
        "first_name": first_name,
        "last_name": last_name,
        "postal_code": None,
        "city": city,
        "phone_number": normalize_phone(agent_raw.get("phone")),
        "arrival_date": None,
        "email": agent_raw.get("email"),
        "linkedin_url": linkedin,
        "nb_mandates": nb_mandates,
        "avg_mandate_price": avg_price,
        "nb_sales": None,
        "url_website": agent_raw.get("link"),
        "network": NETWORK_NAME,
    }


#### SUPABASE PUSH ####

def push_to_supabase(supabase_client, batch_id, items):
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

        max_agents = input_data.get("maxAgents", None)
        supabase_url = input_data.get("supabaseUrl") or os.environ.get("SUPABASE_URL")
        supabase_key = input_data.get("supabaseKey") or os.environ.get("SUPABASE_KEY")

        batch_id = uuid.uuid4()

        Actor.log.info("=" * 50)
        Actor.log.info("=== Century21 Spain Scraper ===")
        Actor.log.info(f"Batch ID: {batch_id}")
        Actor.log.info("=" * 50)

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
        # PHASE 1: Fetch all agents
        # ============================================
        Actor.log.info("\n--- PHASE 1: Liste des agents ---")
        all_agents_raw = fetch_all_agents(max_agents=max_agents)
        Actor.log.info(f"Total agents: {len(all_agents_raw)}")

        # ============================================
        # PHASE 2: Enrich with properties
        # ============================================
        Actor.log.info(f"\n--- PHASE 2: Enrichissement propriétés ---")

        all_records = []
        for i, agent in enumerate(all_agents_raw, 1):
            record = build_agent_record(agent)
            all_records.append(record)

            if i % 50 == 0:
                Actor.log.info(f"  {i}/{len(all_agents_raw)} agents enrichis...")

            time.sleep(0.3)

        with_phone = sum(1 for a in all_records if a.get("phone_number"))
        with_email = sum(1 for a in all_records if a.get("email"))
        with_city = sum(1 for a in all_records if a.get("city"))
        with_mandates = sum(1 for a in all_records if a.get("nb_mandates"))
        Actor.log.info(f"  Avec téléphone: {with_phone}")
        Actor.log.info(f"  Avec email: {with_email}")
        Actor.log.info(f"  Avec ville: {with_city}")
        Actor.log.info(f"  Avec mandats: {with_mandates}")

        # ============================================
        # PHASE 3: Push to Supabase
        # ============================================
        if supabase_client and all_records:
            Actor.log.info(f"\n--- PHASE 3: Push vers Supabase ---")
            inserted = push_to_supabase(supabase_client, batch_id, all_records)
            Actor.log.info(f"Lignes insérées: {inserted}")

        # ============================================
        # PHASE 4: Save to Apify dataset
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
            "supabase_enabled": supabase_client is not None,
        })


if __name__ == "__main__":
    asyncio.run(main())
