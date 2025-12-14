# CareerBuilder Job Scraper

Fast, production-ready Apify actor that pulls CareerBuilder jobs using an API-first strategy with HTML/JSON-LD fallback for resilience. Designed to run through Apify Residential proxies with optional cookies to stay stealthy and reduce blocking.

## How it works
- Tries CareerBuilder JSON search endpoints first (HTTP call + JSON parse) using built-in candidates.
- If API results are insufficient, falls back to HTML/JSON-LD parsing of listing and detail pages.
- If HTML gets blocked (403/Cloudflare), automatically falls back to a Playwright browser crawl.
- Deduplicates by job id/url, cleans descriptions (HTML + text), and stops once `results_wanted` is reached or `max_pages` is hit.

## Inputs (key fields)
- `startUrl` (string, optional): Direct search URL from your browser. Recommended for best relevancy.
- `keyword` / `location` / `posted_date`: Build the search when `startUrl` is empty. `posted_date`: `anytime | 24h | 7d | 30d`.
- `results_wanted` (int): Max jobs to collect (default 100).
- `max_pages` (int): Max listing pages to visit in HTML fallback (default 20).
- `cookies` or `cookiesJson`: Optional cookies to bypass blocks.
- `proxyConfiguration`: Use Apify Residential proxy; datacenter is frequently blocked.

## Output fields
Each dataset item:
- `title`, `company`, `location`, `date_posted`, `salary`, `job_type`
- `description_html`, `description_text`
- `url`, `scraped_at`, `source` (`api`, `json-ld-list`, `json-ld-detail`, `html-detail`, etc.)
- `raw` (only on API results) for debugging payload mappings

## Usage tips
- Prefer providing `startUrl` copied from your browser search.
- Always run with Residential proxy; add browser cookies if you see blocking.
- Start with smaller `results_wanted` to validate, then scale.

## Troubleshooting
- **0 jobs**: Enable RESIDENTIAL proxy, add cookies, and re-run.
- **Blocked/403**: Rotate sessions (re-run), reduce concurrency (already low), provide fresh cookies.

## Running locally
```bash
npm install
APIFY_PROXY_PASSWORD=YOUR_TOKEN npm start
```

> Respect CareerBuilder's terms of service. Use responsibly.
