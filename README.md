# CareerBuilder Job Scraper

This Apify actor is a powerful and reliable tool for scraping job listings from CareerBuilder.com. It is designed with advanced anti-blocking features to ensure a high success rate. You can search for jobs using either a direct URL from a CareerBuilder search or by specifying keywords and location.

## Key Features

- **Dual Scraping Modes:** Use a direct search URL for precision or keywords and location for broader searches.
- **Advanced Anti-Blocking:** Utilizes residential proxies and other techniques to minimize the risk of being blocked.
- **Comprehensive Data Extraction:** Scrapes a wide range of data points for each job, including title, company, location, salary, and full job description.
- **Robust and Resilient:** Includes features like automatic retries and error handling to manage the complexities of web scraping.

## Input Configuration

The actor's behavior is configured through a set of input fields. Here’s a detailed explanation of each:

| Field | Type | Description |
| --- | --- | --- |
| **Direct Search URL (Recommended)** | `string` | The most reliable method. Paste a CareerBuilder search URL directly from your browser. Example: `https://www.careerbuilder.com/jobs?keywords=developer&location=remote` |
| **OR Search by Keywords** | `string` | Job titles or keywords to search for (e.g., 'Software Engineer'). Leave this empty if you are using the Direct Search URL. |
| **Location** | `string` | The city, state, or zip code for the job search (e.g., 'New York, NY'). |
| **Posted Date** | `string` | Filter jobs by their posting date. Options: `Anytime`, `Last 24 hours`, `Last 7 days`, `Last 30 days`. |
| **Number of Jobs** | `integer` | The maximum number of jobs you want to scrape. |
| **Maximum Pages** | `integer` | The maximum number of listing pages to process. |
| **Browser Cookies (Optional)** | `string` | You can add your browser's cookies to help bypass blocking. Format: 'name1=value1; name2=value2'. |
| **OR Cookies as JSON (Optional)** | `string` | An alternative way to provide cookies, in JSON format. |
| **Proxy Configuration** | `object` | **REQUIRED.** It is essential to use a proxy to avoid being blocked. Residential proxies are highly recommended. |

## Output Data Structure

The scraper returns a dataset of job listings with the following fields for each job:

| Field | Type | Description |
| --- | --- | --- |
| `title` | `string` | The title of the job. |
| `company` | `string` | The name of the company that posted the job. |
| `location` | `string` | The location of the job. |
| `date_posted` | `string` | The date when the job was posted. |
| `salary` | `string` | The salary information, if available. |
| `job_type` | `string` | The type of employment (e.g., full-time, part-time). |
| `description_text` | `string` | The job description in plain text. |
| `description_html` | `string` | The job description in HTML format. |
| `url` | `string` | The URL of the job posting. |
| `scraped_at` | `string` | The timestamp of when the job was scraped. |
| `source` | `string` | The method used for data extraction. |

## Usage Guide

Here’s how to get the best results with the CareerBuilder Job Scraper:

### Method 1: Direct Search URL (Recommended)

1.  Go to [CareerBuilder.com](https://www.careerbuilder.com) and perform a job search.
2.  Copy the URL from your browser's address bar.
3.  Paste the URL into the **Direct Search URL** input field.
4.  Ensure that **Apify Proxy** with a **RESIDENTIAL** group is enabled in the Proxy Configuration.
5.  Run the actor.

### Method 2: Keyword Search

1.  Enter your desired job keywords (e.g., "Software Engineer") in the **OR Search by Keywords** field.
2.  Enter the location for your search (e.g., "New York, NY") in the **Location** field.
3.  Ensure that **Apify Proxy** with a **RESIDENTIAL** group is enabled in the Proxy Configuration.
4.  Run the actor.

## Anti-Blocking Strategy

CareerBuilder has measures to prevent scraping. To ensure the scraper works reliably, please follow these recommendations:

- **Use Residential Proxies:** This is the most critical step. Datacenter proxies are easily detected and blocked.
- **Use the Direct URL Method:** This method is more reliable and less prone to being blocked than keyword-based searches.
- **Add Browser Cookies:** If you encounter "Access Denied" errors, copying cookies from your browser can help.
- **Start with Smaller Scrapes:** Test with a smaller number of jobs (e.g., 50-100) to ensure your configuration is working correctly.

## Troubleshooting

- **403 Error / Access Denied:** This usually means you are being blocked. The best solution is to enable a **RESIDENTIAL** proxy. Adding browser cookies can also help.
- **No Results:**
    - Double-check that your search query works on the CareerBuilder website.
    - Verify the spelling of your keywords and location.
    - Try broader search terms.
- **Actor Stops Early:** This can happen due to rate limits. Try running the actor again to collect more data, or reduce the number of jobs requested per run.

## Contact & Support

If you have any questions, need assistance, or have suggestions for improving this actor, please feel free to reach out.