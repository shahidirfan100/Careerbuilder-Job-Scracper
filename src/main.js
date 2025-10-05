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
    
    // Add keywords and location if provided
    if (kw) url.searchParams.set('keywords', kw);
    if (loc) url.searchParams.set('location', loc);
    
    // Add date filter
    if (date && date !== 'anytime') {
        const dateMap = { '24h': '1', '7d': '7', '30d': '30' };
        if (dateMap[date]) url.searchParams.set('posted', dateMap[date]);
    }
    
    // Add standard parameters
    url.searchParams.set('cb_apply', 'false');
    url.searchParams.set('radius', '50');
    url.searchParams.set('cb_veterans', 'false');
    url.searchParams.set('cb_workhome', 'all');
    
    return url.href;
};

// Parse and validate the start URL
const normalizeStartUrl = (urlString) => {
    try {
        const url = new URL(urlString);
        // Ensure it's a CareerBuilder URL
        if (!url.hostname.includes('careerbuilder.com')) {
            log.warning(`URL is not from careerbuilder.com: ${url.hostname}`);
            return null;
        }
        return url.href;
    } catch (e) {
        log.error(`Invalid URL provided: ${urlString}`);
        return null;
    }
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

// NEW: Clean description text - removes HTML and unwanted sections
const cleanDescriptionText = ($element) => {
    if (!$element || $element.length === 0) return '';
    
    const $clone = $element.clone();
    
    // Remove unwanted sections
    $clone.find('.jdp-required-skills, #apply-bottom-content, #apply-bottom, #cb-tip').remove();
    $clone.find('#ads-mobile-placeholder, #ads-desktop-placeholder, #col-right').remove();
    $clone.find('.seperate-top-border-mobile, .site-tip, .report-job-link').remove();
    $clone.find('button, script, style, noscript').remove();
    
    // Get text and format it
    let text = $clone.text();
    text = text.replace(/\s+/g, ' ');  // Multiple spaces to single
    text = text.replace(/\n\s*\n\s*\n+/g, '\n\n');  // Clean newlines
    
    return text.trim();
};

// NEW: Clean description HTML - keeps only job content, removes links
const cleanDescriptionHtml = ($element) => {
    if (!$element || $element.length === 0) return '';
    
    const $clone = $element.clone();
    
    // Remove unwanted sections
    $clone.find('.jdp-required-skills, #apply-bottom-content, #apply-bottom, #cb-tip').remove();
    $clone.find('#ads-mobile-placeholder, #ads-desktop-placeholder, #col-right').remove();
    $clone.find('.seperate-top-border-mobile, .site-tip').remove();
    $clone.find('button, script, style, noscript').remove();
    
    // Remove all <a> tags but keep text content
    $clone.find('a').each(function() {
        const $this = $clone.constructor(this);
        $this.replaceWith($this.text());
    });
    
    // Remove empty elements
    $clone.find('p:empty, div:empty').remove();
    
    let html = $clone.html() || '';
    html = html.replace(/\s+/g, ' ').replace(/>\s+</g, '><');
    
    return html.trim();
};

// Extract JSON-LD structured data from page
const extractJsonLd = ($, crawlerLog) => {
    const jsonLdScripts = $('script[type="application/ld+json"]').toArray();
    const jobPostings = [];
    
    for (const script of jsonLdScripts) {
        try {
            const content = $(script).html();
            if (!content) continue;
            
            const parsed = JSON.parse(content);
            
            // Handle single JobPosting
            if (parsed['@type'] === 'JobPosting') {
                jobPostings.push(parsed);
            }
            // Handle array of JobPostings
            else if (Array.isArray(parsed)) {
                for (const item of parsed) {
                    if (item['@type'] === 'JobPosting') {
                        jobPostings.push(item);
                    }
                }
            }
            // Handle nested structures
            else if (parsed['@graph']) {
                for (const item of parsed['@graph']) {
                    if (item['@type'] === 'JobPosting') {
                        jobPostings.push(item);
                    }
                }
            }
        } catch (e) {
            crawlerLog.debug(`Failed to parse JSON-LD: ${e.message}`);
        }
    }
    
    return jobPostings;
};

// Extract job URLs from listing page
const extractJobUrls = ($, crawlerLog) => {
    const urls = new Set();
    
    // Try multiple approaches to find job links
    const strategies = [
        // Strategy 1: Direct job links
        () => {
            $('a[href*="/job/"]').each((_, el) => {
                const href = $(el).attr('href');
                if (href && href.includes('/job/') && !href.includes('?') && href.length > 20) {
                    urls.add(href);
                }
            });
        },
        // Strategy 2: Data attributes
        () => {
            $('[data-job-did], [data-job-id]').each((_, el) => {
                const href = $(el).find('a').first().attr('href') || $(el).attr('href');
                if (href && href.includes('/job/')) {
                    urls.add(href);
                }
            });
        },
        // Strategy 3: Job card containers
        () => {
            $('div[class*="job"], article[class*="job"], li[class*="job"]').each((_, el) => {
                const href = $(el).find('a[href*="/job/"]').first().attr('href');
                if (href) {
                    urls.add(href);
                }
            });
        },
        // Strategy 4: Search for job IDs in onclick or data attributes
        () => {
            $('[onclick*="job"], [data-gtm*="job"]').each((_, el) => {
                const href = $(el).attr('href') || $(el).find('a').first().attr('href');
                if (href && href.includes('/job/')) {
                    urls.add(href);
                }
            });
        }
    ];
    
    for (const strategy of strategies) {
        try {
            strategy();
            if (urls.size > 0) break;
        } catch (e) {
            crawlerLog.debug(`Strategy failed: ${e.message}`);
        }
    }
    
    // Convert relative URLs to absolute
    const absoluteUrls = [];
    for (const url of urls) {
        try {
            const absolute = new URL(url, 'https://www.careerbuilder.com').href;
            absoluteUrls.push(absolute);
        } catch (e) {
            crawlerLog.debug(`Invalid URL: ${url}`);
        }
    }
    
    return absoluteUrls;
};

// Find next page URL - ENHANCED VERSION
const findNextPageUrl = ($, currentUrl, currentPage, crawlerLog) => {
    crawlerLog.info(`Looking for next page. Current page: ${currentPage}, URL: ${currentUrl}`);
    
    // Strategy 1: Look for explicit next page links
    const nextSelectors = [
        'a[aria-label*="Next"]',
        'a[aria-label*="next"]', 
        'a.next',
        'a[rel="next"]',
        'button[aria-label*="Next"]',
        '.pagination a:contains("Next")',
        'a.pagination-next',
        'a[data-page="next"]',
        '.pager-next a',
        '.next-page a',
        'a[title*="Next"]',
        'a[title*="next"]'
    ];
    
    let nextUrl = null;
    for (const selector of nextSelectors) {
        const $element = $(selector).first();
        if ($element.length > 0) {
            const href = $element.attr('href');
            const isDisabled = $element.hasClass('disabled') || $element.attr('disabled') || 
                              $element.closest('li').hasClass('disabled');
            
            if (href && !isDisabled) {
                nextUrl = href;
                crawlerLog.info(`Found next page with selector: ${selector} -> ${href}`);
                break;
            }
        }
    }
    
    // Strategy 2: Look for numbered pagination - next number
    if (!nextUrl) {
        crawlerLog.info('Trying numbered pagination strategy...');
        const pageLinks = $('a[href*="page"], a[href*="Page"]').toArray();
        
        for (const link of pageLinks) {
            const href = $(link).attr('href');
            const text = $(link).text().trim();
            const pageNum = parseInt(text);
            
            if (!isNaN(pageNum) && pageNum === currentPage + 1 && href) {
                nextUrl = href;
                crawlerLog.info(`Found next page by number: ${pageNum} -> ${href}`);
                break;
            }
        }
    }
    
    // Strategy 3: URL parameter manipulation - multiple patterns
    if (!nextUrl) {
        crawlerLog.info('Trying URL parameter manipulation...');
        try {
            const url = new URL(currentUrl);
            
            // Check different parameter names CareerBuilder might use
            const pageParams = ['page_number', 'page', 'p', 'pagenum', 'pg'];
            let paramFound = false;
            
            for (const param of pageParams) {
                if (url.searchParams.has(param)) {
                    const currentPageNum = parseInt(url.searchParams.get(param) || '1');
                    url.searchParams.set(param, String(currentPageNum + 1));
                    nextUrl = url.href;
                    paramFound = true;
                    crawlerLog.info(`Constructed next page URL using ${param}: page ${currentPageNum + 1}`);
                    break;
                }
            }
            
            // If no page parameter exists, try adding one
            if (!paramFound) {
                // Try the most common parameter name
                url.searchParams.set('page_number', String(currentPage + 1));
                nextUrl = url.href;
                crawlerLog.info(`Added page_number parameter: page ${currentPage + 1}`);
            }
        } catch (e) {
            crawlerLog.debug(`Failed to construct next URL: ${e.message}`);
        }
    }
    
    // Strategy 4: Path-based pagination
    if (!nextUrl) {
        crawlerLog.info('Trying path-based pagination...');
        try {
            const url = new URL(currentUrl);
            
            // Pattern: /page/1, /page/2, etc.
            if (url.pathname.includes('/page/')) {
                const newPath = url.pathname.replace(/\/page\/\d+/, `/page/${currentPage + 1}`);
                url.pathname = newPath;
                nextUrl = url.href;
                crawlerLog.info(`Constructed next page URL from path: page ${currentPage + 1}`);
            }
            // Pattern: /jobs/page-2, /jobs/page-3, etc.
            else if (url.pathname.includes('page-')) {
                const newPath = url.pathname.replace(/page-\d+/, `page-${currentPage + 1}`);
                url.pathname = newPath;
                nextUrl = url.href;
                crawlerLog.info(`Constructed next page URL from dash notation: page ${currentPage + 1}`);
            }
        } catch (e) {
            crawlerLog.debug(`Failed to construct path-based URL: ${e.message}`);
        }
    }
    
    // Strategy 5: Offset-based pagination (if CareerBuilder uses it)
    if (!nextUrl) {
        crawlerLog.info('Trying offset-based pagination...');
        try {
            const url = new URL(currentUrl);
            const offsetParams = ['offset', 'start', 'from'];
            
            for (const param of offsetParams) {
                if (url.searchParams.has(param)) {
                    const currentOffset = parseInt(url.searchParams.get(param) || '0');
                    const jobsPerPage = 25; // Common CareerBuilder page size
                    url.searchParams.set(param, String(currentOffset + jobsPerPage));
                    nextUrl = url.href;
                    crawlerLog.info(`Constructed next page URL using offset ${param}: ${currentOffset + jobsPerPage}`);
                    break;
                }
            }
        } catch (e) {
            crawlerLog.debug(`Failed to construct offset-based URL: ${e.message}`);
        }
    }
    
    if (nextUrl) {
        try {
            const absoluteUrl = new URL(nextUrl, 'https://www.careerbuilder.com').href;
            crawlerLog.info(`Final next page URL: ${absoluteUrl}`);
            return absoluteUrl;
        } catch (e) {
            crawlerLog.error(`Invalid next URL constructed: ${nextUrl}`);
            return null;
        }
    }
    
    crawlerLog.warning(`No next page URL found for page ${currentPage}`);
    return null;
};

// ------------------------- PROXY & CRAWLER -------------------------
const proxyConf = proxyConfiguration
    ? await Actor.createProxyConfiguration(proxyConfiguration)
    : undefined;

let jobsScraped = 0;
const scrapedUrls = new Set();
const processedPages = new Set(); // NEW: Track processed pages to avoid duplicates

const crawler = new CheerioCrawler({
    proxyConfiguration: proxyConf,
    maxRequestsPerMinute: 60,
    requestHandlerTimeoutSecs: 120,
    navigationTimeoutSecs: 120,
    maxConcurrency: 3,
    useSessionPool: true,
    persistCookiesPerSession: true,
    sessionPoolOptions: {
        maxPoolSize: 20,
        sessionOptions: {
            maxUsageCount: 15,
            maxErrorScore: 1,
        },
    },
    
    preNavigationHooks: [
        ({ request }) => {
            request.headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Sec-Ch-Ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
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
            crawlerLog.info(`Target reached: ${RESULTS_WANTED} jobs scraped`);
            return;
        }

        // ============= LIST PAGE HANDLER =============
        if (label === 'LIST') {
            // NEW: Check if we've already processed this exact URL
            const urlKey = `${request.url}_${page}`;
            if (processedPages.has(urlKey)) {
                crawlerLog.warning(`Already processed this page: ${urlKey}`);
                return;
            }
            processedPages.add(urlKey);
            
            crawlerLog.info(`Scraping listing page ${page}: ${request.url}`);
            
            const pageTitle = $('title').text();
            crawlerLog.info(`Page title: "${pageTitle}"`);
            
            // Check for blocking
            const bodyText = $('body').text().toLowerCase();
            if (bodyText.includes('access denied') || bodyText.includes('captcha') || 
                bodyText.includes('security check') || bodyText.includes('blocked')) {
                crawlerLog.error('BLOCKED! Enable proxy configuration or add cookies');
                session.retire();
                return;
            }

            // METHOD 1: Try to extract JSON-LD first (most reliable)
            const jsonLdJobs = extractJsonLd($, crawlerLog);
            let newJobsFromJsonLd = 0;
            
            if (jsonLdJobs.length > 0) {
                crawlerLog.info(`Found ${jsonLdJobs.length} jobs in JSON-LD data`);
                
                for (const job of jsonLdJobs) {
                    if (jobsScraped >= RESULTS_WANTED) break;
                    
                    const jobUrl = job.url || job['@id'] || job.identifier?.value;
                    if (!jobUrl || scrapedUrls.has(jobUrl)) {
                        crawlerLog.debug(`Skipping duplicate job: ${jobUrl}`);
                        continue;
                    }
                    
                    scrapedUrls.add(jobUrl);
                    
                    // NEW: Clean the description if it contains HTML
                    let descriptionHtml = job.description || '';
                    let descriptionText = job.description || '';
                    
                    if (descriptionHtml.includes('<')) {
                        const $tempDiv = $('<div>').html(descriptionHtml);
                        descriptionHtml = cleanDescriptionHtml($tempDiv);
                        descriptionText = cleanDescriptionText($tempDiv);
                    }
                    
                    const jobData = {
                        title: job.title || job.name || 'Not specified',
                        company: job.hiringOrganization?.name || 'Not specified',
                        location: typeof job.jobLocation === 'string' 
                            ? job.jobLocation 
                            : job.jobLocation?.address?.addressLocality || 'Not specified',
                        date_posted: job.datePosted || 'Not specified',
                        salary: job.baseSalary?.value || job.estimatedSalary || 'Not specified',
                        job_type: job.employmentType || 'Not specified',
                        description_text: descriptionText,
                        description_html: descriptionHtml,
                        url: jobUrl,
                        scraped_at: new Date().toISOString(),
                        source: 'json-ld'
                    };
                    
                    await Dataset.pushData(jobData);
                    jobsScraped++;
                    newJobsFromJsonLd++;
                    crawlerLog.info(`[${jobsScraped}/${RESULTS_WANTED}] ${jobData.title}`);
                }
                
                crawlerLog.info(`Added ${newJobsFromJsonLd} new jobs from JSON-LD on page ${page}`);
            }

            // METHOD 2: Extract job URLs and enqueue detail pages
            const jobUrls = extractJobUrls($, crawlerLog);
            crawlerLog.info(`Found ${jobUrls.length} job URLs on page ${page}`);
            
            // NEW: More detailed logging about job discovery
            if (jobUrls.length === 0 && jsonLdJobs.length === 0) {
                crawlerLog.warning(`No jobs found on page ${page}! Possible causes:`);
                crawlerLog.warning('  - Reached end of results');
                crawlerLog.warning('  - Site structure changed');
                crawlerLog.warning('  - Being blocked (try proxy)');
                
                // Check if this looks like an empty results page
                const noResultsIndicators = [
                    'no jobs found', 'no results', '0 jobs', 'try different', 
                    'broaden your search', 'no matches'
                ];
                const lowerBodyText = bodyText.substring(0, 2000);
                const seemsEmpty = noResultsIndicators.some(indicator => 
                    lowerBodyText.includes(indicator)
                );
                
                if (seemsEmpty) {
                    crawlerLog.info('This appears to be an empty results page - stopping pagination');
                    return;
                }
            }

            // Enqueue job detail pages (up to our limit)
            const urlsToEnqueue = [];
            for (const url of jobUrls) {
                if (jobsScraped + urlsToEnqueue.length >= RESULTS_WANTED) break;
                if (!scrapedUrls.has(url)) {
                    urlsToEnqueue.push(url);
                    scrapedUrls.add(url); // NEW: Add to scraped URLs immediately to prevent duplicates
                }
            }

            if (urlsToEnqueue.length > 0) {
                crawlerLog.info(`Enqueueing ${urlsToEnqueue.length} detail pages from page ${page}`);
                await enqueueLinks({
                    urls: urlsToEnqueue,
                    userData: { label: 'DETAIL' },
                });
            }

            // PAGINATION - ENHANCED LOGIC
            const totalJobsOnPage = newJobsFromJsonLd + urlsToEnqueue.length;
            crawlerLog.info(`Total jobs found on page ${page}: ${totalJobsOnPage} (${newJobsFromJsonLd} from JSON-LD, ${urlsToEnqueue.length} for detail scraping)`);
            
            const shouldContinue = jobsScraped < RESULTS_WANTED && 
                                 page < MAX_PAGES && 
                                 totalJobsOnPage > 0; // NEW: Only continue if we found jobs
            
            if (shouldContinue) {
                const nextUrl = findNextPageUrl($, request.url, page, crawlerLog);
                
                if (nextUrl && nextUrl !== request.url) {
                    // NEW: Additional validation to ensure next URL is different
                    const currentUrlBase = request.url.split('?')[0];
                    const nextUrlBase = nextUrl.split('?')[0];
                    const urlsAreDifferent = nextUrl !== request.url && 
                                           !processedPages.has(`${nextUrl}_${page + 1}`);
                    
                    if (urlsAreDifferent) {
                        crawlerLog.info(`Proceeding to page ${page + 1}: ${nextUrl}`);
                        await enqueueLinks({
                            urls: [nextUrl],
                            userData: { label: 'LIST', page: page + 1 },
                        });
                    } else {
                        crawlerLog.warning(`Next URL is same as current or already processed: ${nextUrl}`);
                    }
                } else {
                    crawlerLog.info(`No more pages found after page ${page} (or reached end of results)`);
                }
            } else {
                if (jobsScraped >= RESULTS_WANTED) {
                    crawlerLog.info(`Target reached: ${RESULTS_WANTED} jobs`);
                } else if (page >= MAX_PAGES) {
                    crawlerLog.info(`Max pages limit reached: ${MAX_PAGES}`);
                } else if (totalJobsOnPage === 0) {
                    crawlerLog.info('No more jobs found - reached end of results');
                }
            }
        }

        // ============= DETAIL PAGE HANDLER =============
        if (label === 'DETAIL') {
            if (jobsScraped >= RESULTS_WANTED) {
                crawlerLog.info(`Skipping (limit reached): ${request.url}`);
                return;
            }

            if (scrapedUrls.has(request.url)) {
                crawlerLog.info(`Already scraped: ${request.url}`);
                return;
            }

            crawlerLog.info(`Scraping job detail: ${request.url}`);
            
            // Try JSON-LD first
            const jsonLdJobs = extractJsonLd($, crawlerLog);
            if (jsonLdJobs.length > 0) {
                const job = jsonLdJobs[0];
                
                // NEW: Clean the description if it contains HTML
                let descriptionHtml = job.description || '';
                let descriptionText = job.description || '';
                
                if (descriptionHtml.includes('<')) {
                    const $tempDiv = $('<div>').html(descriptionHtml);
                    descriptionHtml = cleanDescriptionHtml($tempDiv);
                    descriptionText = cleanDescriptionText($tempDiv);
                }
                
                const jobData = {
                    title: job.title || job.name || 'Not specified',
                    company: job.hiringOrganization?.name || 'Not specified',
                    location: typeof job.jobLocation === 'string' 
                        ? job.jobLocation 
                        : job.jobLocation?.address?.addressLocality || 'Not specified',
                    date_posted: job.datePosted || 'Not specified',
                    salary: job.baseSalary?.value || job.estimatedSalary || 'Not specified',
                    job_type: job.employmentType || 'Not specified',
                    description_text: descriptionText,
                    description_html: descriptionHtml,
                    url: request.url,
                    scraped_at: new Date().toISOString(),
                    source: 'json-ld-detail'
                };
                
                scrapedUrls.add(request.url);
                await Dataset.pushData(jobData);
                jobsScraped++;
                crawlerLog.info(`[${jobsScraped}/${RESULTS_WANTED}] ${jobData.title}`);
                return;
            }

            // Fallback to HTML scraping if JSON-LD not available
            const title = $('h1').first().text().trim() ||
                         $('[class*="title"]').first().text().trim();

            if (!title) {
                crawlerLog.warning(`Could not extract title from ${request.url}`);
                session.retire();
                return;
            }

            const company = $('[class*="company"]').first().text().trim() ||
                           $('[data-testid*="company"]').first().text().trim() ||
                           'Not specified';
            
            const jobLocation = $('[class*="location"]').first().text().trim() ||
                               $('[data-testid*="location"]').first().text().trim() ||
                               'Not specified';
            
            const datePosted = $('time').first().text().trim() ||
                              $('[class*="posted"]').first().text().trim() ||
                              'Not specified';

            // NEW: Use cleaning functions for HTML fallback
            let $descElement = $('#jdp_description').first();
            if ($descElement.length === 0) {
                $descElement = $('[class*="description"]').first();
            }
            if ($descElement.length === 0) {
                $descElement = $('.jdp-left-content').first();
            }

            const description = cleanDescriptionHtml($descElement);
            const descriptionText = cleanDescriptionText($descElement);

            const jobData = {
                title,
                company,
                location: jobLocation,
                date_posted: datePosted,
                salary: 'Not specified',
                job_type: 'Not specified',
                description_html: description,
                description_text: descriptionText,
                url: request.url,
                scraped_at: new Date().toISOString(),
                source: 'html-scraping'
            };

            scrapedUrls.add(request.url);
            await Dataset.pushData(jobData);
            jobsScraped++;
            crawlerLog.info(`[${jobsScraped}/${RESULTS_WANTED}] ${title}`);
        }
    },

    failedRequestHandler: async ({ request }, error) => {
        log.error(`Request failed: ${request.url}`);
        log.error(`Error: ${error.message}`);
    },
});

// ============= START SCRAPING =============
let initialUrl;

if (startUrl) {
    initialUrl = normalizeStartUrl(startUrl);
    if (!initialUrl) {
        log.error('Invalid startUrl provided. Please provide a valid CareerBuilder URL.');
        await Actor.exit();
    }
    log.info('Using provided start URL (ignoring keyword/location parameters)');
} else if (keyword || location) {
    initialUrl = buildStartUrl(keyword, location, posted_date);
    log.info('Building URL from keyword/location parameters');
} else {
    log.warning('No startUrl, keyword, or location provided. Using default CareerBuilder jobs page.');
    initialUrl = 'https://www.careerbuilder.com/jobs?cb_apply=false&radius=50&cb_veterans=false&cb_workhome=all';
}

log.info('==========================================');
log.info('CareerBuilder Scraper Starting');
log.info('==========================================');
log.info(`Target: ${RESULTS_WANTED} jobs`);
log.info(`Max pages: ${MAX_PAGES}`);
log.info(`Start URL: ${initialUrl}`);
log.info(`Proxy: ${proxyConf ? 'ENABLED' : 'DISABLED (may cause blocking)'}`);
log.info(`Cookies: ${cookies || cookiesJson ? 'PROVIDED' : 'NOT PROVIDED'}`);
log.info('==========================================');

if (!proxyConf) {
    log.warning('WARNING: No proxy configured! You may get blocked.');
    log.warning('Enable "Apify Proxy" in input for best results.');
}

await crawler.run([{ url: initialUrl, userData: { label: 'LIST', page: 1 } }]);

log.info('==========================================');
log.info(`Scraping Complete`);
log.info(`Successfully scraped: ${jobsScraped} jobs`);
log.info('==========================================');

if (jobsScraped === 0) {
    log.error('==========================================');
    log.error('NO JOBS SCRAPED - TROUBLESHOOTING:');
    log.error('==========================================');
    log.error('1. Enable "Apify Proxy" in actor input');
    log.error('2. Try adding custom cookies from your browser');
    log.error('3. Check if your search parameters return results on website');
    log.error('4. Try different keywords or locations');
    log.error('==========================================');
}

await Actor.exit();