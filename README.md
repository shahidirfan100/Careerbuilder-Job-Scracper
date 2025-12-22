# CareerBuilder Job Scraper

Production-ready Apify actor that scrapes CareerBuilder.com jobs using a **3-tier extraction strategy** for maximum speed, reliability, and stealth.

## 🚀 How It Works

The actor automatically tries extraction methods in order of efficiency:

1. **Tier 1 - JSON API** (Fastest ⚡)
   - Tries GraphQL endpoint at `/graphql`
   - Falls back to REST API if available
   - ~100ms per request
   - No browser overhead

2. **Tier 2 - HTML Parsing** (Fast 🔄)
   - Uses `got-scraping` with stealth headers
   - Extracts JSON-LD structured data
   - Cheerio HTML parsing as fallback
   - ~200ms per request

3. **Tier 3 - Browser (Camoufox)** (Stealth 🛡️)
   - Full Playwright + Camoufox browser
   - Bypasses Cloudflare & anti-bot systems
   - USA residential proxy required
   - ~5-10s per request (last resort only)

Each tier automatically falls back to the next if blocked or unsuccessful.

## 🌍 Important: Geographic Restrictions

CareerBuilder uses **geo-blocking** (MCB Bermuda Ltd) and requires:
- ✅ **USA Residential Proxy** (configured by default)
- ✅ Proper stealth headers and fingerprinting
- ❌ NOT accessible from outside supported regions

## 📥 Inputs

### Required
- **Start URLs** or **Keyword + Location**: Build search or paste direct URL

### Search Parameters
- `keyword` (string): Job title/keywords (e.g., "Software Engineer")
- `location` (string): City, state, or zip (e.g., "New York, NY")
- `posted_date` (enum): `anytime` | `24h` | `7d` | `30d`
- `radius` (int): Search radius in miles (default: 50)

### Extraction Control
- `results_wanted` (int): Max jobs to scrape (default: 20)
- `max_pages` (int): Max listing pages (default: 10)
- `extractionMethod` (enum): 
  - `auto` ⭐ Recommended - tries all tiers
  - `api` - API only (fastest)
  - `html` - HTML parsing only
  - `browser` - Browser only (slowest)

### Advanced
- `cookiesJson` (JSON): Optional cookies for bypass
- `proxyConfiguration` (object): **REQUIRED** - USA RESIDENTIAL proxy

## 📤 Output Fields

Each job contains:
- `title`, `company`, `location`, `date_posted`
- `salary`, `job_type`
- `description_html`, `description_text`
- `url`, `scraped_at`
- `source` (e.g., `api-graphql`, `html-json-ld`, `browser-detail`)
- `raw` (API responses only, for debugging)

## 💡 Usage Tips

1. **Always use RESIDENTIAL proxy with countryCode: "US"** (default)
2. Start with `extractionMethod: "auto"` for best results
3. Lower `results_wanted` for testing (default 20)
4. Provide `cookiesJson` if you encounter persistent blocking
5. Check logs to see which extraction tier succeeded

## 🔧 Troubleshooting

| Issue | Solution |
|-------|----------|
| **0 jobs extracted** | Check proxy is set to RESIDENTIAL + USA |
| **Geo-blocking errors** | Verify proxy country code is "US" |
| **403 / Cloudflare** | Try adding cookies from a real browser session |
| **All tiers failing** | Site may be down or structure changed |

## 🏃 Running Locally

```bash
npm install
APIFY_PROXY_PASSWORD=your_token npm start
```

## 📊 Performance Benchmarks

| Method | Speed | Cost | Success Rate |
|--------|-------|------|--------------|
| API | ⚡⚡⚡ Fast | 💰 Cheap | High (if available) |
| HTML | ⚡⚡ Medium | 💰💰 Low | High |
| Browser | ⚡ Slow | 💰💰💰 High | Very High |

## ⚖️ Legal Notice

Respect CareerBuilder's Terms of Service. This tool is for educational and authorized use only. Always verify you have permission to scrape target websites.

