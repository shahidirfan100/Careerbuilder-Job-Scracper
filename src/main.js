import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';

// ------------------------- INITIALIZATION -------------------------
await Actor.init();

// ------------------------- INPUT -------------------------
const input = await Actor.getInput() ?? {};
const {
    keyword = '',
    location = '',
    posted_date = 'anytime',
    results_wanted: RESULTS_WANTED_RAW = 100,
    max_pages: MAX_PAGES_RAW = 20,
    startUrl,
    cookies,
    cookiesJson,
    proxyConfiguration,
} = input;

const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : Number.MAX_SAFE_INTEGER;
const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW) ? Math.max(1, +MAX_PAGES_RAW) : 20;

// ------------------------- HELPERS -------------------------
const buildStartUrl = (kw, loc, date) => {
    const url = new URL('https://www.careerbuilder.com/jobs');
    if (kw) {
        url.searchParams.set('keywords', kw);
    }
    if (loc) {
        url.searchParams.set('location', loc);
    }
    if (date && date !== 'anytime') {
        const dateMap = {
            '24h': '1',
            '7d': '7',
            '30d': '30',
        };
        if (dateMap[date]) {
            url.searchParams.set('posted', dateMap[date]);
        }
    }
    // Add default parameters for better results
    url.searchParams.set('cb_apply', 'false');
    url.searchParams.set('radius', '50');
    
    return url.href;
};

const normalizeCookieHeader = ({ cookies: rawCookies, cookiesJson: jsonCookies }) => {
    if (rawCookies && typeof rawCookies === 'string' && rawCookies.trim()) return rawCookies.trim();
    if (jsonCookies && typeof jsonCookies === 'string') {
        try {
            const parsed = JSON.parse(jsonCookies);
            const parts = [];
            if (Array.isArray(parsed)) {
                for (const item of parsed) {
                    if (typeof item === 'string') parts.push(item.trim());
                    else if (item && typeof item === 'object' && item.name) {
                        parts.push(`${item.name}=${item.value ?? ''}`);
                    }
                }
            } else if (parsed && typeof parsed === 'object') {
                for (const [k, v] of Object.entries(parsed)) parts.push(`${k}=${v ?? ''}`);
            }
            if (parts.length) return parts.join('; ');
        } catch (e) {
            log.warning(`Could not parse cookiesJson: ${e.message}`);
        }
    }
    return '';
};

// ------------------------- PROXY & START URLS -------------------------
const proxyConf = proxyConfiguration
    ? await Actor.createProxyConfiguration(proxyConfiguration)
    : undefined;

let jobsScraped = 0;

const crawler = new CheerioCrawler({
    proxyConfiguration: proxyConf,
    maxRequestsPerMinute: 120, // Reduced to avoid rate limiting
    requestHandlerTimeoutSecs: 90,
    navigationTimeoutSecs: 90,
    maxConcurrency: 5, // More conservative concurrency
    useSessionPool: true,
    persistCookiesPerSession: true,
    sessionPoolOptions: {
        maxPoolSize: 30,
        sessionOptions: {
            maxUsageCount: 20,
            maxErrorScore: 2,
        },
    },
    preNavigationHooks: [
        ({ request }) => {
            // Enhanced anti-blocking headers
            request.headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Cache-Control': 'max-age=0',
                'Sec-Ch-Ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            };
            const cookieHeader = normalizeCookieHeader({ cookies, cookiesJson });
            if (cookieHeader) {
                request.headers.Cookie = cookieHeader;
            }
        },
    ],

    async requestHandler({ request, $, enqueueLinks, log: crawlerLog, session }) {
        const { label = 'LIST', page = 1 } = request.userData;

        if (jobsScraped >= RESULTS_WANTED) {
            crawlerLog.info(`Target number of jobs (${RESULTS_WANTED}) reached, skipping further requests.`);
            return;
        }

        if (label === 'LIST' || !label) {
            crawlerLog.info(`Scraping LIST page ${page}: ${request.url}`);
            
            // Log the page title to verify we're getting the right page
            const pageTitle = $('title').text();
            crawlerLog.info(`Page title: ${pageTitle}`);
            
            // Multiple selector strategies for job listings
            let jobCards = [];
            
            // Try different possible selectors
            const selectors = [
                'div[data-template-id="job-search-result"]',
                'div.job-listing-item',
                'article.job-result',
                'div[class*="job-search-result"]',
                'div[data-cy="job-card"]',
                '.data-results-content a[href*="/job/"]',
                'a.data-results-title',
            ];
            
            for (const selector of selectors) {
                const elements = $(selector).toArray();
                if (elements.length > 0) {
                    jobCards = elements;
                    crawlerLog.info(`Found ${elements.length} job cards using selector: ${selector}`);
                    break;
                }
            }
            
            // If still no results, try to find any links with /job/ in them
            if (jobCards.length === 0) {
                jobCards = $('a[href*="/job/"]').toArray();
                crawlerLog.info(`Using fallback: found ${jobCards.length} job links`);
            }

            if (jobCards.length === 0) {
                crawlerLog.warning(`No job cards found on page ${page}.`);
                crawlerLog.info(`HTML snippet: ${$('body').html()?.substring(0, 500)}`);
                
                // Check if we're being blocked
                const bodyText = $('body').text().toLowerCase();
                if (bodyText.includes('access denied') || 
                    bodyText.includes('blocked') || 
                    bodyText.includes('captcha') ||
                    bodyText.includes('security check')) {
                    crawlerLog.error('Possible blocking detected. Try using proxy configuration.');
                    session.retire();
                }
                return;
            }

            const linksToEnqueue = [];
            const seenUrls = new Set();
            
            for (const card of jobCards) {
                if (jobsScraped + linksToEnqueue.length >= RESULTS_WANTED) {
                    break;
                }
                
                // Try different ways to extract job URL
                let jobLink = $(card).attr('href') || 
                             $(card).find('a[href*="/job/"]').first().attr('href') ||
                             $(card).find('a.data-results-title').first().attr('href') ||
                             $(card).find('a').first().attr('href');
                
                if (jobLink && jobLink.includes('/job/')) {
                    const absoluteUrl = new URL(jobLink, 'https://www.careerbuilder.com').href;
                    
                    // Avoid duplicates
                    if (!seenUrls.has(absoluteUrl)) {
                        seenUrls.add(absoluteUrl);
                        linksToEnqueue.push(absoluteUrl);
                    }
                }
            }

            if (linksToEnqueue.length > 0) {
                crawlerLog.info(`Enqueuing ${linksToEnqueue.length} detail pages.`);
                await enqueueLinks({
                    urls: linksToEnqueue,
                    userData: { label: 'DETAIL' },
                });
            } else {
                crawlerLog.warning('No valid job links found to enqueue.');
            }

            // Pagination logic - try multiple selectors
            if (jobsScraped + linksToEnqueue.length < RESULTS_WANTED && page < MAX_PAGES) {
                let nextPageUrl = null;
                
                // Try different pagination selectors
                const paginationSelectors = [
                    'a.next-page',
                    'a[aria-label="Next"]',
                    'a[rel="next"]',
                    'li.next a',
                    'a:contains("Next")',
                    'button:contains("Next")',
                ];
                
                for (const selector of paginationSelectors) {
                    const url = $(selector).first().attr('href');
                    if (url) {
                        nextPageUrl = url;
                        crawlerLog.info(`Found next page using selector: ${selector}`);
                        break;
                    }
                }
                
                // Alternative: construct URL manually
                if (!nextPageUrl) {
                    const currentUrl = new URL(request.url);
                    const currentPage = parseInt(currentUrl.searchParams.get('page_number') || '1');
                    currentUrl.searchParams.set('page_number', String(currentPage + 1));
                    nextPageUrl = currentUrl.href;
                    crawlerLog.info(`Constructed next page URL manually: page ${currentPage + 1}`);
                }
                
                if (nextPageUrl) {
                    const absoluteNextUrl = new URL(nextPageUrl, 'https://www.careerbuilder.com').href;
                    crawlerLog.info(`Enqueuing next list page: ${page + 1}`);
                    await enqueueLinks({
                        urls: [absoluteNextUrl],
                        userData: { label: 'LIST', page: page + 1 },
                    });
                } else {
                    crawlerLog.info('No "next page" link found. Reached the end of pagination.');
                }
            } else if (page >= MAX_PAGES) {
                crawlerLog.info(`Max pages limit (${MAX_PAGES}) reached. Stopping pagination.`);
            }
        }

        if (label === 'DETAIL') {
            if (jobsScraped >= RESULTS_WANTED) {
                crawlerLog.info(`Skipping detail page as limit is reached: ${request.url}`);
                return;
            }

            crawlerLog.info(`Scraping job detail: ${request.url}`);
            
            // Multiple selectors for title
            let title = $('h1.h2').text().trim() ||
                       $('h1[data-testid="job-title"]').text().trim() ||
                       $('h1').first().text().trim() ||
                       $('[class*="job-title"]').first().text().trim();

            // If title is missing, try other strategies
            if (!title) {
                crawlerLog.warning(`Could not extract title from ${request.url}. Trying alternative selectors.`);
                title = $('h1').text().trim();
            }
            
            if (!title) {
                crawlerLog.error(`Failed to extract title from ${request.url}. Page might be blocked.`);
                session.retire();
                return;
            }

            // Multiple selectors for company, location, date
            const company = $('div.data-details > span:nth-child(1)').text().trim() ||
                           $('[data-testid="job-company"]').text().trim() ||
                           $('span[class*="company"]').first().text().trim() ||
                           $('div.company-name').text().trim();
            
            const jobLocation = $('div.data-details > span:nth-child(2)').text().trim() ||
                               $('[data-testid="job-location"]').text().trim() ||
                               $('span[class*="location"]').first().text().trim() ||
                               $('div.location').text().trim();
            
            const date_posted = $('div.data-details > span:nth-child(3)').text().trim() ||
                               $('[data-testid="job-posted"]').text().trim() ||
                               $('span[class*="posted"]').first().text().trim() ||
                               $('time').text().trim();

            // Multiple selectors for description
            let description_html = $('#jdp_description > .jdp-description-details').html() ||
                                  $('[data-testid="job-description"]').html() ||
                                  $('div[class*="job-description"]').html() ||
                                  $('div.description').html() ||
                                  '';
            
            let description_text = $('#jdp_description > .jdp-description-details').text().trim() ||
                                  $('[data-testid="job-description"]').text().trim() ||
                                  $('div[class*="job-description"]').text().trim() ||
                                  $('div.description').text().trim();

            // Extract additional details
            const salary = $('[class*="salary"]').text().trim() || '';
            const jobType = $('[class*="job-type"]').text().trim() || '';

            const jobData = {
                title,
                company: company || 'Not specified',
                location: jobLocation || 'Not specified',
                date_posted: date_posted || 'Not specified',
                salary: salary || 'Not specified',
                job_type: jobType || 'Not specified',
                description_html,
                description_text,
                url: request.url,
                scraped_at: new Date().toISOString(),
            };

            await Dataset.pushData(jobData);

            jobsScraped++;
            crawlerLog.info(`✅ Scraped job ${jobsScraped}/${RESULTS_WANTED}: ${title}`);
        }
    },

    failedRequestHandler: async ({ request }, error) => {
        log.error(`Request ${request.url} failed: ${error.message}`);
        log.error(`Error stack: ${error.stack}`);
    },
});

const initialUrl = startUrl || buildStartUrl(keyword, location, posted_date);

if (!keyword && !location && !startUrl) {
    log.warning('No keyword, location, or startUrl provided. Using default CareerBuilder jobs page.');
}

log.info('🚀 Starting CareerBuilder scraper...');
log.info(`- Target jobs: ${RESULTS_WANTED}`);
log.info(`- Max pages: ${MAX_PAGES}`);
log.info(`- Start URL: ${initialUrl}`);
log.info(`- Proxy enabled: ${proxyConf ? 'Yes' : 'No (RECOMMENDED to enable)'}`);

await crawler.run([{ url: initialUrl, userData: { label: 'LIST', page: 1 } }]);

log.info(`🏁 Scraping finished. Total jobs scraped: ${jobsScraped}.`);

if (jobsScraped === 0) {
    log.warning('⚠️ No jobs were scraped. Possible issues:');
    log.warning('  1. The website structure may have changed');
    log.warning('  2. You may be getting blocked - try enabling proxy configuration');
    log.warning('  3. The search parameters may not return any results');
    log.warning('  4. Custom cookies may be needed');
}

await Actor.exit();