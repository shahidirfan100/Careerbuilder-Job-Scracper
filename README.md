# CareerBuilder Job Scraper

Fast and reliable scraper for CareerBuilder job listings with advanced anti-blocking features.

## 🚀 Quick Start

### Method 1: Direct URL (Recommended)
1. Go to CareerBuilder.com and search for jobs
2. Copy the URL from your browser
3. Paste it in the "Direct Search URL" field
4. Enable Apify Proxy (RESIDENTIAL)
5. Run the scraper

### Method 2: Keyword Search
1. Enter job keywords (e.g., "Software Engineer")
2. Enter location (e.g., "New York, NY")
3. Enable Apify Proxy (RESIDENTIAL)
4. Run the scraper

## 🔒 Important: Anti-Blocking

**REQUIRED:**
- ✅ Enable Apify Proxy with RESIDENTIAL group
- ✅ Use Direct URL method for best results

**Optional but helps:**
- 🍪 Add browser cookies if getting blocked
- ⏱️ Use lower job counts (50-200) per run
- 🔄 Run multiple times for large datasets

## 📊 Output Fields

- Job title
- Company name
- Location
- Posted date
- Salary (if available)
- Job type
- Full description (text + HTML)
- Job URL
- Timestamp

## 💡 Tips

1. **Use RESIDENTIAL proxy** - Datacenter proxies get blocked
2. **Copy cookies from browser** if you see 403 errors
3. **Use Direct URL** - More reliable than keyword search
4. **Start with small numbers** - Test with 20-50 jobs first
5. **Check CareerBuilder manually** - Make sure your search returns results

## 🐛 Troubleshooting

**403 Error / Access Denied:**
- Enable RESIDENTIAL proxy (not datacenter)
- Add cookies from your browser
- Use Direct URL method
- Try different search terms

**No Results:**
- Verify search works on CareerBuilder website
- Check spelling of keywords/location
- Try broader search terms
- Use Direct URL from working search

**Actor Stops Early:**
- This is normal - CareerBuilder has rate limits
- Run multiple times to collect more data
- Reduce results_wanted to 50-100 per run
