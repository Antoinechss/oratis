import base64
import csv
import os
import re
import time
import requests

CSV_PATH = os.path.join(os.path.dirname(__file__), "iad_spain_agents.csv")
CSV_FIELDS = [
    "id", "first_name", "last_name", "postal_code", "city", "phone_number",
    "arrival_date", "email", "linkedin_url", "nb_mandates", "avg_mandate_price",
    "nb_sales", "url_website", "network"
]

#### Configs ####

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

# Spanish province slugs (iad format: {name}-{code} with dash)
PROVINCE_SLUGS = [
    "alava-01", "albacete-02", "alicante-03", "almeria-04", "avila-05",
    "badajoz-06", "illes-balears-07", "barcelona-08", "burgos-09", "caceres-10",
    "cadiz-11", "castellon-12", "ciudad-real-13", "cordoba-14", "a-coruna-15",
    "cuenca-16", "girona-17", "granada-18", "guadalajara-19", "gipuzkoa-20",
    "huelva-21", "huesca-22", "jaen-23", "leon-24", "lleida-25",
    "la-rioja-26", "lugo-27", "madrid-28", "malaga-29", "murcia-30",
    "navarra-31", "ourense-32", "asturias-33", "palencia-34", "las-palmas-35",
    "pontevedra-36", "salamanca-37", "santa-cruz-de-tenerife-38", "cantabria-39", "segovia-40",
    "sevilla-41", "soria-42", "tarragona-43", "teruel-44", "toledo-45",
    "valencia-46", "valladolid-47", "bizkaia-48", "zamora-49", "zaragoza-50",
    "ceuta-51", "melilla-52",
]


#### API CALLS ####

def fetch_sector_page(slug, page=1):
    """Fetches one page of agents for a given sector/province slug."""
    url = SECTOR_URL.format(slug=slug, page=page)
    resp = requests.get(url, headers=HEADERS, timeout=30)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def fetch_all_agents_by_sector(slug):
    """Paginates through all agents for a given sector. Returns list."""
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
            print(f"  Error on {slug} page {page}: {e}")
            break

    return agents


def fetch_agent_profile(username):
    """Fetches the full profile of one agent by userName."""
    url = AGENT_URL.format(username=username)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"  Error fetching profile for {username}: {e}")
        return None


#### HELPERS ####

def decode_phone(hashed_phone):
    """Decodes a base64-encoded phone like 'KzM0NjMzMzQ0OTAx' to '+34633334901'."""
    if not hashed_phone:
        return None
    try:
        return base64.b64decode(hashed_phone).decode("utf-8").replace(" ", "")
    except Exception:
        return None


def split_name(full_name):
    """Splits a full name into first and last name."""
    if not full_name:
        return "", ""
    parts = full_name.strip().split(" ", 1)
    first = parts[0]
    last = parts[1] if len(parts) > 1 else ""
    return first, last


def parse_status_or_sector(text):
    """Parses 'Barcelona (08028)' into (city, postal_code)."""
    if not text:
        return None, None
    match = re.search(r'^(.+?)\s*\((\d{4,5})\)', text.strip())
    if match:
        return match.group(1).strip(), match.group(2)
    return text.strip(), None


def extract_linkedin(social_networks):
    """Extracts LinkedIn URL from socialNetworks list."""
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
    """Computes average sale price from the agent's active properties list."""
    if not properties:
        return None
    prices = []
    for p in properties:
        # Only count sales, not rentals
        if p.get("transactionType") == "sale":
            price = (p.get("price") or {}).get("main")
            if price and price > 0:
                prices.append(price)
    if not prices:
        return None
    return int(sum(prices) / len(prices))


#### PARSING ####

def build_agent_record(sector_item, profile):
    """Combines sector listing + full profile into a CSV record."""
    first_name, last_name = split_name(sector_item.get("fullName") or "")

    phone = decode_phone((sector_item.get("directContact") or {}).get("hashedPhone"))

    # Prefer profile data for location; fallback to sector 'statusOrSector'
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


#### CSV ####

def load_existing_ids(filepath):
    if not os.path.exists(filepath):
        return set()
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return {row["id"] for row in reader}


def write_agents_to_csv(agents, filepath=CSV_PATH):
    existing_ids = load_existing_ids(filepath)
    new_agents = [a for a in agents if str(a.get("id")) not in existing_ids]

    if not new_agents:
        return 0

    file_exists = os.path.exists(filepath)
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerows(new_agents)

    return len(new_agents)


#### MAIN ####

if __name__ == "__main__":
    import sys

    # Optional arg: limit number of provinces to test
    max_provinces = int(sys.argv[1]) if len(sys.argv) > 1 else None

    print("=== iad Spain Scraper ===")
    provinces = PROVINCE_SLUGS[:max_provinces] if max_provinces else PROVINCE_SLUGS
    print(f"Scraping {len(provinces)} provinces...")

    # Phase 1: collect all agents (deduped) from all provinces
    all_agents_raw = {}  # agentId -> sector_item

    for i, slug in enumerate(provinces, 1):
        print(f"[{i}/{len(provinces)}] Province: {slug}")
        try:
            items = fetch_all_agents_by_sector(slug)
        except Exception as e:
            print(f"  Error: {e}")
            continue

        new_count = 0
        for item in items:
            aid = item.get("agentId")
            if aid and aid not in all_agents_raw:
                all_agents_raw[aid] = item
                new_count += 1

        print(f"  +{new_count} new (total unique: {len(all_agents_raw)})")
        time.sleep(0.5)

    print(f"\nPhase 1 done: {len(all_agents_raw)} unique agents")

    # Phase 2: fetch profile for each and build record
    print(f"\nPhase 2: fetching profiles...")
    all_records = []

    for i, (aid, sector_item) in enumerate(all_agents_raw.items(), 1):
        username = sector_item.get("userName")
        profile = fetch_agent_profile(username) if username else None
        record = build_agent_record(sector_item, profile)
        all_records.append(record)

        if i % 20 == 0:
            print(f"  [{i}/{len(all_agents_raw)}] {record['first_name']} {record['last_name']}: "
                  f"city={record['city']}, phone={record['phone_number']}, "
                  f"mandates={record['nb_mandates']}, sales={record['nb_sales']}")

        time.sleep(0.3)

    # Phase 3: write CSV
    written = write_agents_to_csv(all_records)
    print(f"\n{written} new agents written to {CSV_PATH}")
    print(f"Total records: {len(all_records)}")
