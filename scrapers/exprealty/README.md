# exprealty.com agent scraper

Scrapes Florida agent listings from exprealty.com with full profile data.
Handles Cloudflare managed challenge via Playwright + [2captcha](https://2captcha.com).

## Output fields

| Field | Source |
|-------|--------|
| id, firstName, lastName, email, phoneNumber, photo | GraphQL list |
| languages, specializations | GraphQL detail |
| facebook, instagram, linkedIn, twitter, website, youtube, tiktok | GraphQL detail |
| city, state, zipcode, countryCode | GraphQL detail |
| license (number, state, locale, primary) | Parsed from bio HTML |

## Setup

```bash
pip install -r requirements.txt
playwright install chromium
```

Set your 2captcha API key in `exprealty.py`:
```python
TWOCAPTCHA_API_KEY = "your_key_here"
```

## Usage

```bash
# Scrape 1 page (~12 agents) with full details
python exprealty.py

# Scrape N pages
python exprealty.py 10
```

Results are saved to `agents.json` and `agents.csv`.

## How it works

**Phase 1 (browser):** Playwright navigates the search listing pages. On first run, Cloudflare's managed challenge is solved via 2captcha (~30s). The `cf_clearance` cookie is cached in `cf_cookies.json` (~24h) — subsequent runs skip the challenge.

**Phase 2 (HTTP):** For each agent ID collected in Phase 1, a direct GraphQL call is made to `agentdir-api.expproptech.com` using a bearer token fetched from `exprealty.com/api/gettoken`. No browser needed — fast and lightweight.

## Notes

- The site returns ~12 agents per page; ~7000 agents total for Florida.
- `cf_cookies.json` and output files are gitignored.
