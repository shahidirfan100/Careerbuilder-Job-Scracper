# CareerBuilder.com Jobs Scraper

This Apify actor scrapes job listings from CareerBuilder.com.

## Features

- Scrapes CareerBuilder.com job search results.
- Extracts detailed job information including title, company, location, date posted, and full description.
- Handles pagination to collect multiple pages of results.
- Saves results to a dataset.

## Input

The actor accepts the following input fields:

- `keyword`: The job title or keywords to search for (e.g., "Software Developer").
- `location`: The geographic location to filter jobs by (e.g., "New York, NY").
- `posted_date`: Filter jobs by when they were posted ("24h", "7d", "30d", "anytime").
- `results_wanted`: The maximum number of jobs to scrape.

## Output

The actor outputs a dataset of job listings with the following fields:

- `title`: The job title.
- `company`: The company name.
- `location`: The job location.
- `date_posted`: When the job was posted.
- `description_html`: The job description in HTML format.
- `description_text`: The job description in plain text.
- `url`: The URL of the job posting.
