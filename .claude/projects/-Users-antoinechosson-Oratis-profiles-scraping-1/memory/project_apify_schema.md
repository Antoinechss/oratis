---
name: Apify dataset schema
description: Standard field names required for all Apify actor dataset outputs (staging_scrapes)
type: project
---

All Apify actors must output these exact fields (lowercase):

- batch_id
- network
- id
- first_name
- last_name
- postal_code
- city
- phone_number
- arrival_date
- email
- linkedin_url
- nb_mandates
- avg_mandate_price
- nb_sales
- url_website
- raw_data

**Why:** These map to the `staging_scrapes` Supabase table columns. Missing or misnamed fields break the downstream pipeline.

**How to apply:** Every new Apify actor's `output_items` dict and every raw scraper's `CSV_FIELDS` must use exactly these field names (lowercase). Use `None` for unavailable fields, never `0` or empty string.
