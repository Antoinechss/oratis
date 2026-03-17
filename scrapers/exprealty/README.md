# exprealty.com agent scraper

Scrapes Florida agent listings (name, email, phone, city, state) from exprealty.com.
Handles Cloudflare managed challenge via Playwright + [2captcha](https://2captcha.com).

## Setup

```bash
pip install -r requirements.txt
playwright install chromium
```

Set your 2captcha API key in `scraper.py`:
```python
TWOCAPTCHA_API_KEY = "your_key_here"
```

## Usage

```bash
# Scrape 1 page (~12 agents)
python scraper.py

# Scrape N pages
python scraper.py 10
```

Results are saved to `agents.csv`.

## Notes

- The first run opens a visible browser window to solve the Cloudflare challenge (~30s).
- The `cf_clearance` cookie is cached in `cf_cookies.json` (~24h validity) — subsequent runs skip the challenge entirely.
- The site returns ~12 agents per page; there are ~7000 agents total for Florida.
- Add a delay between pages with `rate_limit_s` if scraping at scale.
