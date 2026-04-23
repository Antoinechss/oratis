import asyncio
import csv
import json
import os
import re
import sys
import time
import requests
from playwright.async_api import async_playwright

CSV_PATH = os.path.join(os.path.dirname(__file__), "real_advisor_esp_agents.csv")
CSV_FIELDS = [
    "id", "first_name", "last_name", "postal_code", "city", "phone_number",
    "arrival_date", "email", "linkedin_url", "nb_mandates", "avg_mandate_price",
    "nb_sales", "url_website", "network"
]

#### Configs ####

BASE_URL = "https://realadvisor.es"
LISTING_URL = BASE_URL + "/es/agentes-inmobiliarios"
NETWORK_NAME = "RealAdvisor Spain"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9",
}


#### PHASE 1: LISTING PAGE SCRAPING (requests — fast) ####

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
            print(f"Error on page {page}: {e}")
            break

        if not agents:
            break

        all_agents.extend(agents)
        if page % 10 == 0:
            print(f"  Page {page}: {len(all_agents)} agents collected...")

        page += 1
        time.sleep(0.5)

    return all_agents


#### PHASE 2: PROFILE PAGE SCRAPING (Playwright — for phone) ####

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
        print(f"  Error navigating to {url}: {e}")
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

    # Phone from tel: link (now visible after click)
    phone_match = re.search(r'href="tel:([^"]+)"', html)
    if phone_match:
        phone = phone_match.group(1).strip()
        result["phone_number"] = re.sub(r'\s+', '', phone)

    # Arrival date: infer from "X años en el negocio"
    years_match = re.search(r'(\d+)\s*años?\s+en\s+el\s+negocio', html, re.IGNORECASE)
    if years_match:
        from datetime import datetime
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


#### BUILD RECORD ####

def build_agent_record(listing_info, profile_info):
    """Combines listing page info and profile page info into a final record."""
    name = listing_info.get("name", "")
    parts = name.split(" ", 1)
    first_name = parts[0] if parts else ""
    last_name = parts[1] if len(parts) > 1 else ""

    postal_code, city = parse_address(listing_info.get("address"))

    return {
        "id": listing_info.get("url", ""),
        "first_name": first_name,
        "last_name": last_name,
        "postal_code": postal_code,
        "city": city,
        "phone_number": profile_info.get("phone_number"),
        "arrival_date": profile_info.get("arrival_date"),
        "email": None,
        "linkedin_url": profile_info.get("linkedin_url"),
        "nb_mandates": profile_info.get("nb_mandates"),
        "avg_mandate_price": profile_info.get("avg_mandate_price"),
        "nb_sales": profile_info.get("nb_sales"),
        "url_website": listing_info.get("url"),
        "network": NETWORK_NAME,
    }


#### MAIN ####

async def main():
    max_pages = int(sys.argv[1]) if len(sys.argv) > 1 else 3

    print("=== RealAdvisor Spain Scraper ===")
    print(f"Scraping {max_pages} listing pages...")

    # Phase 1: collect agent URLs (requests — fast)
    agents_listing = fetch_all_agent_urls(max_pages=max_pages)
    print(f"\nPhase 1 done: {len(agents_listing)} agents collected")

    # Phase 2: scrape each profile (Playwright — for phone)
    print(f"\nPhase 2: scraping profiles with Playwright...")
    all_records = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        for i, agent in enumerate(agents_listing, 1):
            url = agent.get("url")
            if not url:
                continue

            profile = await fetch_agent_profile(page, url)
            record = build_agent_record(agent, profile)
            all_records.append(record)

            if i % 10 == 0:
                print(f"  [{i}/{len(agents_listing)}] {record['first_name']} {record['last_name']}: "
                      f"mandates={record['nb_mandates']}, sales={record['nb_sales']}, "
                      f"price={record['avg_mandate_price']}, phone={record['phone_number']}")

            await asyncio.sleep(1.0)

        await browser.close()

    # Phase 3: write CSV
    written = write_agents_to_csv(all_records)
    print(f"\n{written} new agents written to {CSV_PATH}")
    print(f"Total records: {len(all_records)}")


if __name__ == "__main__":
    asyncio.run(main())
