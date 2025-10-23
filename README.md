# CareerBuilder Job Scraper

Fast and reliable scraper for CareerBuilder job listings. Extract job titles, companies, locations, salaries, descriptions, and more with advanced filtering options.

## Features

✅ Search by keywords and location  
✅ Filter by posting date (24h, 7d, 30d)  
✅ Extract complete job details including descriptions  
✅ Stealth mode with proxy support  
✅ Clean, structured data output  
✅ Handles pagination automatically

## Input Parameters

- **Job Keywords**: Search term (e.g., "Software Engineer")  
- **Location**: City, state, or zip code (e.g., "New York, NY")  
- **Posted Date**: Filter by when job was posted  
- **Number of Jobs**: Maximum jobs to scrape (default: 100)  
- **Maximum Pages**: Maximum listing pages to process (default: 20)  
- **Custom Start URL**: Direct CareerBuilder search URL (optional)  
- **Proxy Configuration**: Highly recommended to avoid blocking

## Output

Each job listing includes:  
- Job title  
- Company name  
- Location  
- Posting date  
- Salary (if available)  
- Job type  
- Full description (text and HTML)  
- Job URL  
- Scraping timestamp

## Tips

- Enable Apify Proxy for best results  
- Use specific keywords for better matches  
- Adjust max_pages if you need more results
