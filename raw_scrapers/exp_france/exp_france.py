import csv
import os
import requests
from datetime import datetime

CSV_PATH = os.path.join(os.path.dirname(__file__), "exp_france_agents.csv")
CSV_FIELDS = [
    "id", "first_name", "last_name", "postal_code", "city", "phone_number",
    "arrival_date", "email", "linkedin_url", "nb_mandates", "avg_mandate_price",
    "nb_sales", "url_website", "network"
]

#### Configs ####

ORIGIN_URL = "https://www.expfrance.fr/findanagent"
AGENTS_URL = "https://ywzpnbmomlzkcbzzkaqr.supabase.co/rest/v1/agents"
LISTINGS_URL = "https://ywzpnbmomlzkcbzzkaqr.supabase.co/rest/v1/listings"
WEBSITES_URL = "https://nhkxpqunzawllesgatth.supabase.co/rest/v1/websites"
WEBSITES_API_KEY = os.environ.get("EXP_WEBSITES_API_KEY", "")
AGENTS_API_KEY = os.environ.get("EXP_AGENTS_API_KEY", "")


agents_headers = {
    "apikey": AGENTS_API_KEY,
    "Authorization": f"Bearer {AGENTS_API_KEY}",
}

websites_headers = {
    "apikey": WEBSITES_API_KEY,
    "Authorization": f"Bearer {WEBSITES_API_KEY}",
}

#### API CALLS FUNCTIONS ####

def fetch_websites_page(): 
    """Fetches information available on website page"""
    params = {
        "select": "*"
        }
    
    response = requests.get(url=WEBSITES_URL, headers=websites_headers, params=params)
    response.raise_for_status()
    return response.json()


def fetch_agents_page(offset: int, limit: int): 
    """
    Fetches batch of agents raw json data from offset to limit
    """
    params = {
        "select": "id,first_name,middle_family_name,last_name,email,phone,picture,licence_number,full_payload",
        "country_code": "eq.FR",
        "secret_agent": "eq.false", 
        "order": "first_name.asc,last_name.asc,id.asc",
        "offset": offset,
        "limit": limit,
        "source_system": "eq.modelo_france",
        "status": "eq.Active"
    }
    
    response = requests.get(url=AGENTS_URL, params=params, headers=agents_headers, timeout=30)
    response.raise_for_status()
    return response.json(), response.headers


def fetch_agent_listings(agent_email, agent_id):
    """
    Fetches active sale listings for a given agent from the listings table.
    Returns (nb_mandates, avg_mandate_price, most_common_city, most_common_zipcode).
    City/zipcode are derived from the most frequent values across listings.
    """
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
        response = requests.get(url=LISTINGS_URL, params=params, headers=agents_headers, timeout=30)
        response.raise_for_status()
        listings = response.json()
        if not listings:
            return 0, None, None, None
        prices = [l["price"] for l in listings if l.get("price") and l["price"] > 0]
        nb_mandates = len(listings)
        avg_price = int(sum(prices) / len(prices)) if prices else None
        # Most frequent city/zipcode from listings
        cities = [l["city"] for l in listings if l.get("city")]
        zipcodes = [l["zipcode"] for l in listings if l.get("zipcode")]
        top_city = max(set(cities), key=cities.count) if cities else None
        top_zipcode = max(set(zipcodes), key=zipcodes.count) if zipcodes else None
        return nb_mandates, avg_price, top_city, top_zipcode
    except Exception as e:
        print(f"Error fetching listings for {agent_email}: {e}")
        return None, None, None, None


#### Helper & Formatting functions ####

def extract_arrival_date(date_str):
    """
    Helper function that extracts arrival date from the
    time_created timestamp in the json response
    """
    if not date_str:
        return None
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def format_phone(phone_str):
    """
    Formats a French phone number to +33XXXXXXXXX format.
    Handles inputs like 0612345678, +33612345678, 33612345678.
    """
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
    """Fetches LinkedIn profile link in the custom links added manually by the real estate agents"""
    
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
    website_map = {}

    for w in website_response:
        email = w.get("email")
        if email:
            website_map[email.lower()] = w
    return website_map


#### CSV FUNCTIONS ####

def load_existing_ids(filepath):
    """Returns a set of agent IDs already written in the CSV."""
    if not os.path.exists(filepath):
        return set()
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return {row["id"] for row in reader}


def write_agents_to_csv(agents, filepath=CSV_PATH):
    """Appends agents to CSV, skipping any whose ID is already present."""
    existing_ids = load_existing_ids(filepath)
    new_agents = [a for a in agents if a.get("id") not in existing_ids]

    if not new_agents:
        return 0

    file_exists = os.path.exists(filepath)
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerows(new_agents)

    return len(new_agents)


#### JSON PARSING FUNCTIONS & ENRICHMENT ###

def parse_agent_data(response):
    """
    Extracts and formats data of interest for a given agent json response
    """
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
    agent["network"] = "ExP France"

    return agent


def parse_website_url(website_response):
    """Builds the agent's personal website URL from the subdomain field."""
    if not website_response:
        return None
    subdomain = website_response.get("subdomain")
    if not subdomain:
        return None
    return f"https://{subdomain}.expfrance.fr/"


def enrich_agent_with_website(agent, website_map):
    email = (agent.get("email") or "").lower()
    website = website_map.get(email)
    agent["linkedin_url"] = parse_linkedin(website)
    agent["url_website"] = parse_website_url(website)
    # Enrich with listings data
    agent_id = agent.get("id") or ""
    agent_email = agent.get("email") or ""
    nb_mandates, avg_price, listing_city, listing_zip = fetch_agent_listings(agent_email, agent_id)
    agent["nb_mandates"] = nb_mandates if nb_mandates else None
    agent["avg_mandate_price"] = avg_price
    # Fill city/postal_code from listings if missing in agent profile
    if not agent.get("city") and listing_city:
        agent["city"] = listing_city
    if not agent.get("postal_code") and listing_zip:
        agent["postal_code"] = listing_zip
    return agent


if __name__ == "__main__":
    agent_batch = fetch_agents_page(offset=0, limit=100)
    websites = fetch_websites_page()
    website_map = build_website_map(websites)
    agents = agent_batch[0]

    agents_parsed = [enrich_agent_with_website(parse_agent_data(a), website_map) for a in agents]
    written = write_agents_to_csv(agents_parsed)
    print(f"{written} new agents written.")

    