"""
Supabase REST API
"""

import csv
import os
import requests
from pprint import pprint
from datetime import datetime

CSV_PATH = os.path.join(os.path.dirname(__file__), "exp_france_agents.csv")
CSV_FIELDS = ["id", "first_name", "last_name", "email", "phone", "picture",
              "address", "location", "licence_number", "RSAC_identifier",
              "member_since", "cities_covered", "linkedin"]

#### Configs ####

ORIGIN_URL = "https://www.expfrance.fr/findanagent"
AGENTS_URL = "https://ywzpnbmomlzkcbzzkaqr.supabase.co/rest/v1/agents"
WEBSITES_URL = "https://nhkxpqunzawllesgatth.supabase.co/rest/v1/websites"
WEBSITES_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5oa3hwcXVuemF3bGxlc2dhdHRoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDIzOTkzNDIsImV4cCI6MjA1Nzk3NTM0Mn0.0oKdjpmGHuSoD-4DGnl6LNkrVw2uv15Yl3vPig0BSbY"
AGENTS_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inl3enBuYm1vbWx6a2NienprYXFyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDQ2NDkyMzMsImV4cCI6MjA2MDIyNTIzM30.6b8PT7DMzY2jnRgglammdCpqsT6EKR1_Na2T7djGb9A"


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


#### Helper & Formatting functions ####

def extract_arrival_date(date_str):
    """
    Helper function that extracts year and month of arrival from the 
    time_created timestamp in the json response
    """
    if not date_str: 
        return None 
    try: 
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m")
    except Exception: 
        return None


def extract_cities_covered(cities_dict): 
    """
    extracts the city only from the dict of cities covered 
    """ 
    if not cities_dict: 
        return None 
    try:
        cities = [city['name'] for city in cities_dict]
        return cities 
    except Exception: 
        return None


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
        user_id = w.get("user_id")
        if user_id:
            website_map[user_id] = w
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

    # Personal Details
    agent["id"] = full_payload.get("user_uuid") or response.get("id") # Fallback on generic ID is uuid is missing
    agent["address"] = full_payload.get("address")
    agent['picture'] = response.get("picture")
    agent["first_name"] = response.get("first_name")
    agent["last_name"] = response.get("last_name")
    agent["email"] = response.get("email")
    agent["phone"] = response.get("phone")
    agent["location"] = full_payload.get("city")

    # Work details
    agent["licence_number"] = response.get("licence_number")
    agent["RSAC_identifier"] = full_payload.get("legal_rsac_number")
    agent["member_since"] = extract_arrival_date(full_payload.get("time_created"))
    agent["cities_covered"] = extract_cities_covered(full_payload.get("cities_covered"))

    return agent


def enrich_agent_with_website(agent, website_map):
    user_id = agent.get("id")
    website = website_map.get(user_id)
    linkedin_url = parse_linkedin(website)
    agent["linkedin"] = linkedin_url
    return agent


if __name__ == "__main__": 
    agent_batch = fetch_agents_page(offset=0, limit=100)
    websites = fetch_websites_page()
    website_map = build_website_map(websites)
    agents = agent_batch[0]
    agents_parsed = [enrich_agent_with_website(parse_agent_data(a), website_map) for a in agents]
    written = write_agents_to_csv(agents_parsed)
    print(f"{written} new agents written.")