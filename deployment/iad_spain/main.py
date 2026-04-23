"""
Actor Apify pour scraper les conseillers iad Spain
1. Itère sur les 52 provinces espagnoles via l'API publique iad
2. Déduplique par agentId, puis fetch le profil complet de chaque conseiller
3. Push les données dans staging_scrapes sur Supabase
"""

import asyncio
import base64
import os
import re
import time
import uuid
import requests
from apify import Actor


BASE_URL = "https://www.iadespana.es"
SECTOR_URL = BASE_URL + "/api/agents/sector/{slug}?page={page}&locale=fr"
AGENT_URL = BASE_URL + "/api/agents/{username}?locale=fr"
PROFILE_PAGE_URL = BASE_URL + "/fr/conseiller/{username}"
NETWORK_NAME = "iad Spain"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "fr-FR,fr;q=0.9",
}

PROVINCE_SLUGS = [
    "alava-01", "albacete-02", "alicante-03", "almeria-04", "avila-05",
    "badajoz-06", "illes-balears-07", "barcelona-08", "burgos-09", "caceres-10",
    "cadiz-11", "castellon-12", "ciudad-real-13", "cordoba-14", "a-coruna-15",
    "cuenca-16", "girona-17", "granada-18", "guadalajara-19", "gipuzkoa-20",
    "huelva-21", "huesca-22", "jaen-23", "leon-24", "lleida-25",
    "la-rioja-26", "lugo-27", "madrid-28", "malaga-29", "murcia-30",
    "navarra-31", "ourense-32", "asturias-33", "palencia-34", "las-palmas-35",
    "pontevedra-36", "salamanca-37", "santa-cruz-de-tenerife-38", "cantabria-39",
    "segovia-40", "sevilla-41", "soria-42", "tarragona-43", "teruel-44",
    "toledo-45", "valencia-46", "valladolid-47", "bizkaia-48", "zamora-49",
    "zaragoza-50", "ceuta-51", "melilla-52",
]


#### API CALLS ####

def fetch_sector_page(slug, page=1):
    url = SECTOR_URL.format(slug=slug, page=page)
    resp = requests.get(url, headers=HEADERS, timeout=30)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def fetch_all_agents_by_sector(slug):
    agents = []
    first = fetch_sector_page(slug, page=1)
    if first is None:
        return []

    total = first.get("totalItems", 0)
    per_page = first.get("itemsPerPage", 12)
    agents.extend(first.get("items", []))

    if total <= per_page:
        return agents

    total_pages = (total + per_page - 1) // per_page
    for page in range(2, total_pages + 1):
        try:
            data = fetch_sector_page(slug, page=page)
            if data is None:
                break
            agents.extend(data.get("items", []))
            time.sleep(0.3)
        except Exception as e:
            Actor.log.warning(f"  Error on {slug} page {page}: {e}")
            break

    return agents


def fetch_agent_profile(username):
    url = AGENT_URL.format(username=username)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        Actor.log.debug(f"  Error fetching profile for {username}: {e}")
        return None


#### HELPERS ####

def decode_phone(hashed_phone):
    if not hashed_phone:
        return None
    try:
        return base64.b64decode(hashed_phone).decode("utf-8").replace(" ", "")
    except Exception:
        return None


def split_name(full_name):
    if not full_name:
        return "", ""
    parts = full_name.strip().split(" ", 1)
    first = parts[0]
    last = parts[1] if len(parts) > 1 else ""
    return first, last


def parse_status_or_sector(text):
    if not text:
        return None, None
    match = re.search(r'^(.+?)\s*\((\d{4,5})\)', text.strip())
    if match:
        return match.group(1).strip(), match.group(2)
    return text.strip(), None


def extract_linkedin(social_networks):
    if not social_networks:
        return None
    for s in social_networks:
        if isinstance(s, dict):
            url = s.get("url") or s.get("link") or ""
            if "linkedin.com" in url.lower():
                return url
        elif isinstance(s, str) and "linkedin.com" in s.lower():
            return s
    return None


def avg_price_from_properties(properties):
    if not properties:
        return None
    prices = []
    for p in properties:
        if p.get("transactionType") == "sale":
            price = (p.get("price") or {}).get("main")
            if price and price > 0:
                prices.append(price)
    if not prices:
        return None
    return int(sum(prices) / len(prices))


#### PARSING ####

def build_agent_record(sector_item, profile):
    first_name, last_name = split_name(sector_item.get("fullName") or "")
    phone = decode_phone((sector_item.get("directContact") or {}).get("hashedPhone"))

    city, postal = None, None
    if profile:
        loc = profile.get("location") or {}
        city = loc.get("place")
        postal = loc.get("postcode")
    if not city or not postal:
        sec_city, sec_postal = parse_status_or_sector(sector_item.get("statusOrSector"))
        city = city or sec_city
        postal = postal or sec_postal

    nb_mandates = None
    nb_sales = None
    avg_price = None
    linkedin = None
    if profile:
        nb_mandates = profile.get("propertyCount") or None
        latest = profile.get("latestTransactions") or {}
        nb_sales = latest.get("totalSold") or None
        avg_price = avg_price_from_properties(profile.get("properties") or [])
        linkedin = extract_linkedin(profile.get("socialNetworks"))

    username = sector_item.get("userName", "")
    profile_url = PROFILE_PAGE_URL.format(username=username) if username else None

    return {
        "id": sector_item.get("agentId"),
        "first_name": first_name,
        "last_name": last_name,
        "postal_code": postal,
        "city": city,
        "phone_number": phone,
        "arrival_date": None,
        "email": None,
        "linkedin_url": linkedin,
        "nb_mandates": nb_mandates,
        "avg_mandate_price": avg_price,
        "nb_sales": nb_sales,
        "url_website": profile_url,
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

        max_provinces = input_data.get("maxProvinces", None)
        supabase_url = input_data.get("supabaseUrl") or os.environ.get("SUPABASE_URL")
        supabase_key = input_data.get("supabaseKey") or os.environ.get("SUPABASE_KEY")

        batch_id = uuid.uuid4()

        Actor.log.info("=" * 50)
        Actor.log.info("=== iad Spain Scraper ===")
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

        provinces = PROVINCE_SLUGS[:max_provinces] if max_provinces else PROVINCE_SLUGS

        # ============================================
        # PHASE 1: Collect agents per province (dedupe)
        # ============================================
        Actor.log.info(f"\n--- PHASE 1: Collecte sur {len(provinces)} provinces ---")

        all_agents_raw = {}  # agentId -> sector_item

        for i, slug in enumerate(provinces, 1):
            try:
                items = fetch_all_agents_by_sector(slug)
            except Exception as e:
                Actor.log.warning(f"  [{i}/{len(provinces)}] {slug}: error {e}")
                continue

            new_count = 0
            for item in items:
                aid = item.get("agentId")
                if aid and aid not in all_agents_raw:
                    all_agents_raw[aid] = item
                    new_count += 1

            Actor.log.info(f"  [{i}/{len(provinces)}] {slug}: +{new_count} "
                           f"(total unique: {len(all_agents_raw)})")
            time.sleep(0.3)

        Actor.log.info(f"\nPhase 1 done: {len(all_agents_raw)} unique agents")

        # ============================================
        # PHASE 2: Fetch full profile per agent
        # ============================================
        Actor.log.info(f"\n--- PHASE 2: Fetch profils ({len(all_agents_raw)}) ---")

        all_records = []
        for i, (aid, sector_item) in enumerate(all_agents_raw.items(), 1):
            username = sector_item.get("userName")
            profile = fetch_agent_profile(username) if username else None
            record = build_agent_record(sector_item, profile)
            all_records.append(record)

            if i % 50 == 0:
                Actor.log.info(f"  {i}/{len(all_agents_raw)} profils fetchés...")

            time.sleep(0.2)

        with_phone = sum(1 for a in all_records if a.get("phone_number"))
        with_linkedin = sum(1 for a in all_records if a.get("linkedin_url"))
        with_city = sum(1 for a in all_records if a.get("city"))
        with_mandates = sum(1 for a in all_records if a.get("nb_mandates"))
        Actor.log.info(f"  Avec téléphone: {with_phone}")
        Actor.log.info(f"  Avec LinkedIn: {with_linkedin}")
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
            "with_linkedin": with_linkedin,
            "supabase_enabled": supabase_client is not None,
        })


if __name__ == "__main__":
    asyncio.run(main())
