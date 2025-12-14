# CareerBuilder Job Scraper

Fast, production-ready Apify actor that pulls CareerBuilder jobs using an API-first strategy with HTML/JSON-LD fallback for resilience. Designed to run through Apify Residential proxies with optional cookies to stay stealthy and reduce blocking.

## How it works
- Tries CareerBuilder JSON search endpoints first (HTTP call + JSON parse). You can override the API URL and headers; otherwise it auto-tests common endpoints.
- If API results are insufficient (or in `preferApi` mode), falls back to HTML/JSON-LD parsing of listing and detail pages.
- Deduplicates by job id/url, cleans descriptions (HTML + text), and stops once `results_wanted` is reached or `max_pages` is hit.

## Inputs (key fields)
- `startUrl` (string, optional): Direct search URL from your browser. Recommended for best relevancy.
- `keyword` / `location` / `posted_date`: Build the search when `startUrl` is empty. `posted_date`: `anytime | 24h | 7d | 30d`.
- `results_wanted` (int): Max jobs to collect (default 100).
- `max_pages` (int): Max listing pages to visit in HTML fallback (default 20).
- `mode` (select): `preferApi` (default), `apiOnly`, `htmlOnly`.
- `searchApiUrl` (string, optional): Custom API endpoint if you sniffed a working one in DevTools.
- `apiPageSize` (int): Jobs per API page (default 50).
- `apiHeaders` (JSON string): Extra headers for API calls (auth/cookies, etc.).
- `apiExtraParams` (JSON string): Extra query params for API calls (sorting, radius, etc.).
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
- For stubborn searches, run `apiOnly` with a sniffed `searchApiUrl`/`apiHeaders` from DevTools; otherwise use `preferApi` to let HTML fallback.
- Start with smaller `results_wanted` to validate, then scale.

## Troubleshooting
- **0 jobs**: Enable RESIDENTIAL proxy, add cookies, and try `preferApi` or `htmlOnly`.
- **Blocked/403**: Rotate sessions (re-run), reduce concurrency (already low), provide fresh cookies.
- **Wrong API mapping**: Supply `searchApiUrl`/`apiHeaders`/`apiExtraParams` to mirror the network call you see in DevTools.

## Running locally
```bash
npm install
APIFY_PROXY_PASSWORD=YOUR_TOKEN npm start
```

> Respect CareerBuilder's terms of service. Use responsibly.
