"""
Actor Apify pour scraper les agents ExP France
1. Récupère tous les agents via l'API Supabase ExP France (pagination)
2. Enrichit avec les données de sites web (LinkedIn)
3. Push les données dans staging_scrapes sur Supabase
"""

import os
import uuid
import asyncio
import requests
from datetime import datetime
from apify import Actor


AGENTS_URL = "https://ywzpnbmomlzkcbzzkaqr.supabase.co/rest/v1/agents"
LISTINGS_URL = "https://ywzpnbmomlzkcbzzkaqr.supabase.co/rest/v1/listings"
WEBSITES_URL = "https://nhkxpqunzawllesgatth.supabase.co/rest/v1/websites"
NETWORK_NAME = "ExP France"


#### API CALL FUNCTIONS ####

def fetch_agents_page(offset: int, limit: int, api_key: str):
    """Fetches a paginated batch of active French agents from the ExP France Supabase."""
    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
    }
    params = {
        "select": "id,first_name,middle_family_name,last_name,email,phone,picture,licence_number,full_payload",
        "country_code": "eq.FR",
        "secret_agent": "eq.false",
        "order": "first_name.asc,last_name.asc,id.asc",
        "offset": offset,
        "limit": limit,
        "source_system": "eq.modelo_france",
        "status": "eq.Active",
    }
    response = requests.get(url=AGENTS_URL, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json(), response.headers


def fetch_agent_listings(agent_email, agent_id, api_key):
    """
    Fetches active sale listings for a given agent.
    Returns (nb_mandates, avg_mandate_price, most_common_city, most_common_zipcode).
    """
    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
    }
    or_filter = f"agent_email.eq.{agent_email},agent_id.eq.{agent_id},secondary_agent_id.eq.{agent_id}"
    params = {
        "select": "price,city,zipcode",
        "listing_type": "in.(1,5,7)",
        "country_code": "eq.FR",
        "or": f"({or_filter})",
        "status": "in.(1,2,3,4)",
        "offset": 0,
        "limit": 100,
    }
    try:
        response = requests.get(url=LISTINGS_URL, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        listings = response.json()
        if not listings:
            return 0, None, None, None
        prices = [l["price"] for l in listings if l.get("price") and l["price"] > 0]
        nb_mandates = len(listings)
        avg_price = int(sum(prices) / len(prices)) if prices else None
        cities = [l["city"] for l in listings if l.get("city")]
        zipcodes = [l["zipcode"] for l in listings if l.get("zipcode")]
        top_city = max(set(cities), key=cities.count) if cities else None
        top_zipcode = max(set(zipcodes), key=zipcodes.count) if zipcodes else None
        return nb_mandates, avg_price, top_city, top_zipcode
    except Exception as e:
        Actor.log.warning(f"Error fetching listings for {agent_email}: {e}")
        return None, None, None, None


def fetch_websites_page(api_key: str):
    """Fetches all website records for LinkedIn enrichment."""
    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
    }
    response = requests.get(url=WEBSITES_URL, headers=headers, params={"select": "*"}, timeout=30)
    response.raise_for_status()
    return response.json()


#### HELPER & FORMATTING FUNCTIONS ####

def extract_arrival_date(date_str):
    """Extracts arrival date from the time_created timestamp."""
    if not date_str:
        return None
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def format_phone(phone_str):
    """Formats a French phone number to +33XXXXXXXXX format."""
    if not phone_str:
        return None
    digits = "".join(c for c in str(phone_str) if c.isdigit() or c == "+")
    digits = digits.replace("+", "")
    if digits.startswith("33") and len(digits) == 11:
        return f"+{digits}"
    if digits.startswith("0") and len(digits) == 10:
        return f"+33{digits[1:]}"
    return phone_str


def parse_linkedin(website_response):
    """Extracts LinkedIn URL from custom footer links."""
    if not website_response:
        return None
    footer = website_response.get("footer", {}) or {}
    custom_links = footer.get("customLinks", [])
    for item in custom_links or []:
        url = item.get("url", "").lower()
        if "linkedin.com" in url:
            return url
    return None


def build_website_map(website_response):
    """Builds an email -> website record lookup map."""
    website_map = {}
    for w in website_response:
        email = w.get("email")
        if email:
            website_map[email.lower()] = w
    return website_map


#### PARSING & ENRICHMENT ####

def parse_agent_data(response):
    """Extracts and formats agent data from the raw API response."""
    full_payload = response.get("full_payload") or {}
    agent = {}
    agent["id"] = full_payload.get("user_uuid") or response.get("id")
    agent["first_name"] = response.get("first_name")
    agent["last_name"] = response.get("last_name")
    agent["postal_code"] = full_payload.get("postal_code")
    agent["city"] = full_payload.get("city")
    agent["phone_number"] = format_phone(response.get("phone"))
    agent["arrival_date"] = extract_arrival_date(full_payload.get("time_created"))
    agent["email"] = response.get("email")
    agent["linkedin_url"] = None  # filled by enrich_agent_with_website
    agent["nb_mandates"] = None
    agent["avg_mandate_price"] = None
    agent["nb_sales"] = None
    agent["url_website"] = None
    agent["network"] = NETWORK_NAME
    return agent


def parse_website_url(website_response):
    """Builds the agent's personal website URL from the subdomain field."""
    if not website_response:
        return None
    subdomain = website_response.get("subdomain")
    if not subdomain:
        return None
    return f"https://{subdomain}.expfrance.fr/"


def enrich_agent_with_website(agent, website_map, agents_api_key):
    """Adds LinkedIn URL, website URL, listings data, and city fallback."""
    email = (agent.get("email") or "").lower()
    website = website_map.get(email)
    agent["linkedin_url"] = parse_linkedin(website)
    agent["url_website"] = parse_website_url(website)
    # Enrich with listings data
    agent_id = agent.get("id") or ""
    agent_email = agent.get("email") or ""
    nb_mandates, avg_price, listing_city, listing_zip = fetch_agent_listings(
        agent_email, agent_id, agents_api_key
    )
    agent["nb_mandates"] = nb_mandates if nb_mandates else None
    agent["avg_mandate_price"] = avg_price
    # Fill city/postal_code from listings if missing in agent profile
    if not agent.get("city") and listing_city:
        agent["city"] = listing_city
    if not agent.get("postal_code") and listing_zip:
        agent["postal_code"] = listing_zip
    return agent


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
    """Fonction principale de l'actor."""
    async with Actor:
        input_data = await Actor.get_input() or {}

        agents_api_key = input_data.get("agentsApiKey") or os.environ.get("EXP_AGENTS_API_KEY")
        websites_api_key = input_data.get("websitesApiKey") or os.environ.get("EXP_WEBSITES_API_KEY")
        max_agents = input_data.get("maxAgents", None)
        supabase_url = input_data.get("supabaseUrl") or os.environ.get("SUPABASE_URL")
        supabase_key = input_data.get("supabaseKey") or os.environ.get("SUPABASE_KEY")

        batch_id = uuid.uuid4()

        Actor.log.info("=" * 50)
        Actor.log.info("=== ExP France Scraper ===")
        Actor.log.info(f"Batch ID: {batch_id}")
        Actor.log.info("=" * 50)

        if not agents_api_key or not websites_api_key:
            Actor.log.error("agentsApiKey et websitesApiKey sont requis.")
            return

        # Supabase (staging)
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
        # ÉTAPE 1: Récupérer tous les agents (pagination)
        # ============================================
        Actor.log.info("\n--- ÉTAPE 1: Récupération des agents ---")

        offset = 0
        limit = 100
        all_agents_raw = []

        while True:
            try:
                batch, _ = fetch_agents_page(offset, limit, agents_api_key)
            except Exception as e:
                Actor.log.error(f"Erreur fetch agents (offset={offset}): {e}")
                break

            if not batch:
                break

            all_agents_raw.extend(batch)
            Actor.log.info(f"  {len(all_agents_raw)} agents récupérés...")

            if max_agents and len(all_agents_raw) >= max_agents:
                all_agents_raw = all_agents_raw[:max_agents]
                Actor.log.info(f"  Limité à {max_agents} agents")
                break

            if len(batch) < limit:
                break

            offset += limit

        Actor.log.info(f"Total agents récupérés: {len(all_agents_raw)}")

        # ============================================
        # ÉTAPE 2: Enrichissement avec sites web
        # ============================================
        Actor.log.info("\n--- ÉTAPE 2: Enrichissement LinkedIn ---")

        try:
            websites = fetch_websites_page(websites_api_key)
            website_map = build_website_map(websites)
            Actor.log.info(f"  {len(website_map)} sites web chargés")
        except Exception as e:
            Actor.log.warning(f"Erreur fetch websites: {e} — LinkedIn sera vide")
            website_map = {}

        agents = []
        for i, a in enumerate(all_agents_raw):
            parsed = parse_agent_data(a)
            enriched = enrich_agent_with_website(parsed, website_map, agents_api_key)
            agents.append(enriched)
            if (i + 1) % 50 == 0:
                Actor.log.info(f"  {i + 1}/{len(all_agents_raw)} agents enrichis...")

        with_phone = sum(1 for a in agents if a.get("phone_number"))
        with_linkedin = sum(1 for a in agents if a.get("linkedin_url"))
        with_city = sum(1 for a in agents if a.get("city"))
        with_mandates = sum(1 for a in agents if a.get("nb_mandates"))
        Actor.log.info(f"  Agents avec téléphone: {with_phone}")
        Actor.log.info(f"  Agents avec LinkedIn: {with_linkedin}")
        Actor.log.info(f"  Agents avec ville: {with_city}")
        Actor.log.info(f"  Agents avec mandats: {with_mandates}")

        # ============================================
        # ÉTAPE 3: Push vers Supabase staging
        # ============================================
        if supabase_client and agents:
            Actor.log.info(f"\n--- ÉTAPE 3: Push vers Supabase ---")
            inserted = push_to_supabase(supabase_client, batch_id, agents)
            Actor.log.info(f"Lignes insérées: {inserted}")

        # ============================================
        # ÉTAPE 4: Sauvegarde dataset Apify
        # ============================================
        Actor.log.info(f"\n--- ÉTAPE 4: Sauvegarde dataset Apify ---")

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
            for a in agents
        ]

        await Actor.push_data(output_items)
        Actor.log.info(f"Données sauvegardées: {len(output_items)} agents")

        Actor.log.info(f"\n{'='*50}")
        Actor.log.info("=== SCRAPING TERMINÉ ===")
        Actor.log.info(f"Batch ID: {batch_id}")
        Actor.log.info(f"Network: {NETWORK_NAME}")
        Actor.log.info(f"Total agents: {len(agents)}")
        Actor.log.info(f"Avec téléphone: {with_phone}")
        Actor.log.info(f"Avec LinkedIn: {with_linkedin}")
        Actor.log.info(f"{'='*50}")

        await Actor.set_value("batch_id", str(batch_id))
        await Actor.set_value("stats", {
            "batch_id": str(batch_id),
            "network": NETWORK_NAME,
            "total_agents": len(agents),
            "with_phone": with_phone,
            "with_linkedin": with_linkedin,
            "supabase_enabled": supabase_client is not None,
        })


if __name__ == "__main__":
    asyncio.run(main())
