import csv
import os
import re
import time
import requests

CSV_PATH = os.path.join(os.path.dirname(__file__), "century21_esp_agents.csv")
CSV_FIELDS = [
    "id", "first_name", "last_name", "postal_code", "city", "phone_number",
    "arrival_date", "email", "linkedin_url", "nb_mandates", "avg_mandate_price",
    "nb_sales", "url_website", "network"
]

#### Configs ####

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
    """Fetches one page of agents."""
    resp = requests.get(AGENTS_URL.format(page=page), headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_all_agents():
    """Paginates through all agents."""
    all_agents = []
    first = fetch_agents_page(1)
    total = first.get("total", 0)
    all_agents.extend(first.get("data", []))

    # Default page size = 12 (observed)
    per_page = max(1, len(first.get("data", [])))
    if total <= per_page:
        return all_agents

    total_pages = (total + per_page - 1) // per_page
    for page in range(2, total_pages + 1):
        try:
            data = fetch_agents_page(page)
            all_agents.extend(data.get("data", []))
            if page % 10 == 0:
                print(f"  Page {page}/{total_pages}: {len(all_agents)} agents collected")
            time.sleep(0.3)
        except Exception as e:
            print(f"  Error on page {page}: {e}")
            break

    return all_agents


def fetch_agent_properties(handler):
    """Fetches all properties for an agent."""
    try:
        resp = requests.get(PROPERTIES_URL.format(handler=handler), headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.json().get("data", [])
    except Exception as e:
        print(f"  Error fetching properties for {handler}: {e}")
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


# Per-run cache: agency_handler -> (city, postal_code)
_AGENCY_CACHE = {}


def fetch_agency_location(agency_handler, agency_link):
    """
    Scrapes the agency page for its address and extracts (city, postal_code).
    Format expected: 'Street, 28660, Madrid'.
    Cached per handler so each agency is fetched once.
    """
    if not agency_handler or agency_handler in _AGENCY_CACHE:
        return _AGENCY_CACHE.get(agency_handler, (None, None))

    if not agency_link:
        _AGENCY_CACHE[agency_handler] = (None, None)
        return None, None

    try:
        resp = requests.get(agency_link, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        html = resp.text
        # Match any 'Street X, 28660, City' pattern in the HTML
        # The 5-digit postal code in the middle disambiguates from other text
        match = re.search(
            r'>([^<>]*?,\s*(\d{5})\s*,\s*[^<>,]+?)<',
            html
        )
        if match:
            address = match.group(1).strip()
            parts = [p.strip() for p in address.split(",")]
            postal = next((p for p in parts if re.match(r'^\d{5}$', p)), None)
            city = parts[-1] if parts and not parts[-1].isdigit() else None
            _AGENCY_CACHE[agency_handler] = (city, postal)
            return city, postal
    except Exception:
        pass

    _AGENCY_CACHE[agency_handler] = (None, None)
    return None, None


#### HELPERS ####

def split_name(full_name):
    if not full_name:
        return "", ""
    parts = full_name.strip().split(" ", 1)
    first = parts[0]
    last = parts[1] if len(parts) > 1 else ""
    return first, last


def normalize_phone(phone):
    """Normalizes Spanish phone to +34XXXXXXXXX format."""
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
    """
    Tries to extract city from address strings.
    Comma format: 'Calle X, 39, Arafo, Spain' -> 'Arafo'
    Paren format: 'CL X 13 EDIF Y SAN CRISTOBAL (LA LAGUNA) (S.C. TENERIFE)' -> 'LA LAGUNA'
    """
    if not address:
        return None
    # Comma format: take second to last segment
    if "," in address:
        parts = [p.strip() for p in address.split(",")]
        if len(parts) >= 3:
            candidate = parts[-2].strip()
            # Avoid postcodes / pure numbers
            if candidate and not candidate.isdigit():
                return candidate.title()
    # Paren format: first parenthesized group is usually the city
    paren_match = re.search(r'\(([^)]+)\)', address)
    if paren_match:
        return paren_match.group(1).strip().title()
    return None


def aggregate_listings(properties):
    """Computes nb_mandates, avg_mandate_price, most common city from properties."""
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
    """Combines listing API data + properties enrichment into a CSV record."""
    name = agent_raw.get("name") or agent_raw.get("display_name") or ""
    first_name, last_name = split_name(name.strip())

    handler = agent_raw.get("handler") or ""
    properties = fetch_agent_properties(handler) if handler else []

    nb_mandates, avg_price, city = aggregate_listings(properties)
    linkedin = fetch_agent_linkedin(agent_raw.get("link"))

    # Agency-derived city/postal (cached per agency)
    agency = agent_raw.get("agency") or {}
    agency_city, agency_postal = fetch_agency_location(
        agency.get("handler"), agency.get("link")
    )
    # Prefer agency location since it's stable; fallback to listings city
    city = agency_city or city
    postal_code = agency_postal

    return {
        "id": agent_raw.get("id"),
        "first_name": first_name,
        "last_name": last_name,
        "postal_code": postal_code,
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

    max_agents = int(sys.argv[1]) if len(sys.argv) > 1 else None

    print("=== Century21 Spain Scraper ===")
    print("Phase 1: fetching all agents...")

    all_agents_raw = fetch_all_agents()
    print(f"\nPhase 1 done: {len(all_agents_raw)} agents")

    if max_agents:
        all_agents_raw = all_agents_raw[:max_agents]
        print(f"Limited to {max_agents} agents")

    print(f"\nPhase 2: enriching with properties...")
    all_records = []

    for i, agent in enumerate(all_agents_raw, 1):
        record = build_agent_record(agent)
        all_records.append(record)

        if i % 25 == 0:
            print(f"  [{i}/{len(all_agents_raw)}] {record['first_name']} {record['last_name']}: "
                  f"city={record['city']}, mandates={record['nb_mandates']}, "
                  f"avg_price={record['avg_mandate_price']}")

        time.sleep(0.3)

    written = write_agents_to_csv(all_records)
    print(f"\n{written} new agents written to {CSV_PATH}")
    print(f"Total records: {len(all_records)}")
