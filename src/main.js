import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';

// ------------------------- INITIALIZATION -------------------------
try {
    await Actor.init();
    log.info('Actor initialized successfully');
} catch (error) {
    log.error('Failed to initialize Actor', { error: error.message });
    process.exit(1);
}

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
    proxyConfiguration = { useApifyProxy: true },
} = input;

const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : Number.MAX_SAFE_INTEGER;
const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW) ? Math.max(1, +MAX_PAGES_RAW) : 20;

// Validate input
if (!startUrl && !keyword && !location) {
    log.warning('No search parameters provided. Using default job search.');
}

// ------------------------- HELPERS -------------------------
// Human-like delay with random variation
const humanDelay = async (min = 1000, max = 3000) => {
    const delay = Math.floor(Math.random() * (max - min + 1)) + min;
    await new Promise(resolve => setTimeout(resolve, delay));
};

// Exponential backoff with jitter
const exponentialBackoff = (attempt) => {
    const base = 1000;
    const maxDelay = 30000;
    const jitter = Math.random() * 1000;
    return Math.min(base * Math.pow(2, attempt) + jitter, maxDelay);
};

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

// Find next page URL
const findNextPageUrl = ($, currentUrl, currentPage, crawlerLog) => {
    const nextSelectors = [
        'a[aria-label*="Next"]',
        'a.next',
        'a[rel="next"]',
        'button[aria-label*="Next"]',
        '.pagination a:contains("Next")',
        'a.pagination-next',
        'a[data-page="next"]'
    ];
    
    let nextUrl = null;
    for (const selector of nextSelectors) {
        const href = $(selector).first().attr('href');
        if (href) {
            nextUrl = href;
            crawlerLog.debug(`Found next page with selector: ${selector}`);
            break;
        }
    }
    
    if (!nextUrl) {
        const pageLinks = $('a[href*="page"]').toArray();
        for (const link of pageLinks) {
            const href = $(link).attr('href');
            const text = $(link).text().trim();
            if (text === String(currentPage + 1)) {
                nextUrl = href;
                crawlerLog.debug('Found next page by page number');
                break;
            }
        }
    }
    
    if (!nextUrl) {
        try {
            const url = new URL(currentUrl);
            const currentPageNum = parseInt(url.searchParams.get('page_number') || '1');
            url.searchParams.set('page_number', String(currentPageNum + 1));
            nextUrl = url.href;
            crawlerLog.info(`Constructed next page URL: page ${currentPageNum + 1}`);
        } catch (e) {
            crawlerLog.debug(`Failed to construct next URL: ${e.message}`);
        }
    }
    
    if (!nextUrl) {
        try {
            const url = new URL(currentUrl);
            if (url.pathname.includes('/page/')) {
                const newPath = url.pathname.replace(/\/page\/\d+/, `/page/${currentPage + 1}`);
                url.pathname = newPath;
                nextUrl = url.href;
                crawlerLog.info(`Constructed next page URL from path: page ${currentPage + 1}`);
            }
        } catch (e) {
            crawlerLog.debug(`Failed to construct path-based URL: ${e.message}`);
        }
    }
    
    return nextUrl ? new URL(nextUrl, 'https://www.careerbuilder.com').href : null;
};

// ------------------------- PROXY & CRAWLER -------------------------
const proxyConf = await Actor.createProxyConfiguration(proxyConfiguration);

let jobsScraped = 0;
const scrapedUrls = new Set();
let requestCount = 0;
let pageCount = 0;

// Generate realistic user agents
const userAgents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
];

const getRandomUserAgent = () => userAgents[Math.floor(Math.random() * userAgents.length)];

const crawler = new CheerioCrawler({
    proxyConfiguration: proxyConf,
    maxRequestsPerMinute: 30, // Lower for more natural pacing
    requestHandlerTimeoutSecs: 180,
    navigationTimeoutSecs: 180,
    maxConcurrency: 2, // Lower concurrency for stealth
    maxRequestRetries: 5,
    useSessionPool: true,
    persistCookiesPerSession: true,
    sessionPoolOptions: {
        maxPoolSize: 10,
        sessionOptions: {
            maxUsageCount: 10,
            maxErrorScore: 2,
        },
    },
    
    preNavigationHooks: [
        async ({ request, session }) => {
            // Increment request counter
            requestCount++;
            
            // Add human-like delay between requests
            if (requestCount > 1) {
                await humanDelay(2000, 5000);
            }
            
            const ua = getRandomUserAgent();
            const chromeVersion = ua.match(/Chrome\/(\d+)/)?.[1] || '131';
            
            request.headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br, zstd',
                'Cache-Control': 'max-age=0',
                'Sec-Ch-Ua': `"Chromium";v="${chromeVersion}", "Google Chrome";v="${chromeVersion}", "Not?A_Brand";v="99"`,
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': request.userData.referer ? 'same-origin' : 'none',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': ua,
            };
            
            // Add referer for natural navigation
            if (request.userData.referer) {
                request.headers.Referer = request.userData.referer;
            }
            
            const cookieHeader = normalizeCookieHeader({ cookies, cookiesJson });
            if (cookieHeader) {
                request.headers.Cookie = cookieHeader;
            }
        },
    ],

    async requestHandler({ request, $, enqueueLinks, log: crawlerLog, session }) {
        const { label = 'LIST', page = 1 } = request.userData;

        // Add reading delay to simulate human behavior
        await humanDelay(500, 1500);

        if (jobsScraped >= RESULTS_WANTED) {
            crawlerLog.info(`✅ Target reached: ${RESULTS_WANTED} jobs scraped`);
            return;
        }

        // ============= LIST PAGE HANDLER =============
        if (label === 'LIST') {
            pageCount++;
            crawlerLog.info(`📄 Scraping listing page ${page} (Page count: ${pageCount}/${MAX_PAGES}): ${request.url}`);
            
            const pageTitle = $('title').text();
            crawlerLog.debug(`Page title: "${pageTitle}"`);
            
            // Check for blocking
            const bodyText = $('body').text().toLowerCase();
            if (bodyText.includes('access denied') || bodyText.includes('captcha') || 
                bodyText.includes('security check') || bodyText.includes('blocked')) {
                crawlerLog.error('🚫 BLOCKED! Rotating session...');
                session.retire();
                throw new Error('Access blocked - session retired');
            }

            // METHOD 1: Try to extract JSON-LD first (most reliable)
            const jsonLdJobs = extractJsonLd($, crawlerLog);
            if (jsonLdJobs.length > 0) {
                crawlerLog.info(`📊 Found ${jsonLdJobs.length} jobs in JSON-LD data`);
                
                let jobsToSave = 0;
                for (const job of jsonLdJobs) {
                    if (jobsScraped >= RESULTS_WANTED) break;
                    
                    const jobUrl = job.url || job['@id'] || job.identifier?.value;
                    if (!jobUrl || scrapedUrls.has(jobUrl)) continue;
                    
                    scrapedUrls.add(jobUrl);
                    
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
                    jobsToSave++;
                    crawlerLog.info(`✅ [${jobsScraped}/${RESULTS_WANTED}] ${jobData.title} at ${jobData.company}`);
                }
                
                if (jobsToSave > 0) {
                    crawlerLog.info(`💾 Saved ${jobsToSave} jobs from JSON-LD`);
                }
            }

            // METHOD 2: Extract job URLs and enqueue detail pages
            const jobUrls = extractJobUrls($, crawlerLog);
            crawlerLog.info(`🔗 Found ${jobUrls.length} job URLs on page`);
            
            if (jobUrls.length === 0 && jsonLdJobs.length === 0) {
                crawlerLog.warning('⚠️ No jobs found on this page!');
                return;
            }

            // Enqueue job detail pages (up to our limit)
            const urlsToEnqueue = [];
            for (const url of jobUrls) {
                if (jobsScraped + urlsToEnqueue.length >= RESULTS_WANTED) break;
                if (!scrapedUrls.has(url)) {
                    urlsToEnqueue.push(url);
                }
            }

            if (urlsToEnqueue.length > 0) {
                crawlerLog.info(`➕ Enqueueing ${urlsToEnqueue.length} detail pages`);
                await enqueueLinks({
                    urls: urlsToEnqueue,
                    userData: { 
                        label: 'DETAIL',
                        referer: request.url 
                    },
                });
            }

            // PAGINATION
            if (jobsScraped < RESULTS_WANTED && pageCount < MAX_PAGES) {
                const nextUrl = findNextPageUrl($, request.url, page, crawlerLog);
                
                if (nextUrl && nextUrl !== request.url) {
                    crawlerLog.info(`➡️ Next page found: ${page + 1}`);
                    await humanDelay(3000, 6000); // Longer delay before next page
                    await enqueueLinks({
                        urls: [nextUrl],
                        userData: { 
                            label: 'LIST', 
                            page: page + 1,
                            referer: request.url 
                        },
                    });
                } else {
                    crawlerLog.info('🏁 No more pages to scrape');
                }
            } else if (pageCount >= MAX_PAGES) {
                crawlerLog.info(`🛑 Max pages limit reached: ${MAX_PAGES}`);
            }
        }

        // ============= DETAIL PAGE HANDLER =============
        if (label === 'DETAIL') {
            if (jobsScraped >= RESULTS_WANTED) {
                crawlerLog.debug(`Skipping (limit reached): ${request.url}`);
                return;
            }

            if (scrapedUrls.has(request.url)) {
                crawlerLog.debug(`Already scraped: ${request.url}`);
                return;
            }

            crawlerLog.info(`📝 Scraping job detail: ${request.url}`);
            
            // Try JSON-LD first
            const jsonLdJobs = extractJsonLd($, crawlerLog);
            if (jsonLdJobs.length > 0) {
                const job = jsonLdJobs[0];
                
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
                crawlerLog.info(`✅ [${jobsScraped}/${RESULTS_WANTED}] ${jobData.title} at ${jobData.company}`);
                return;
            }

            // Fallback to HTML scraping if JSON-LD not available
            const title = $('h1').first().text().trim() ||
                         $('[class*="title"]').first().text().trim();

            if (!title) {
                crawlerLog.warning(`⚠️ Could not extract title from ${request.url}`);
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
            crawlerLog.info(`✅ [${jobsScraped}/${RESULTS_WANTED}] ${title} at ${company}`);
        }
    },

    failedRequestHandler: async ({ request }, error) => {
        log.error(`❌ Request failed: ${request.url}`, { error: error.message });
        
        // Don't count failed requests towards our limit
        if (requestCount > 0) requestCount--;
    },
});

// ============= START SCRAPING =============
let initialUrl;

if (startUrl) {
    initialUrl = normalizeStartUrl(startUrl);
    if (!initialUrl) {
        log.error('❌ Invalid startUrl provided. Please provide a valid CareerBuilder URL.');
        await Actor.exit();
    }
    log.info('🔗 Using provided start URL');
} else if (keyword || location) {
    initialUrl = buildStartUrl(keyword, location, posted_date);
    log.info('🏗️ Building URL from keyword/location parameters');
} else {
    log.info('🌐 Using default CareerBuilder jobs page');
    initialUrl = 'https://www.careerbuilder.com/jobs?cb_apply=false&radius=50&cb_veterans=false&cb_workhome=all';
}

log.info('==========================================');
log.info('🚀 CareerBuilder Scraper Starting');
log.info('==========================================');
log.info(`🎯 Target: ${RESULTS_WANTED} jobs`);
log.info(`📄 Max pages: ${MAX_PAGES}`);
log.info(`🔗 Start URL: ${initialUrl}`);
log.info(`🔒 Proxy: ${proxyConf ? 'ENABLED ✅' : 'DISABLED ❌'}`);
log.info(`🍪 Cookies: ${cookies || cookiesJson ? 'PROVIDED ✅' : 'NOT PROVIDED'}`);
log.info('==========================================');

try {
    await crawler.run([{ 
        url: initialUrl, 
        userData: { 
            label: 'LIST', 
            page: 1 
        } 
    }]);

    log.info('==========================================');
    log.info('✅ Scraping Complete');
    log.info(`📊 Successfully scraped: ${jobsScraped} jobs`);
    log.info(`📄 Pages processed: ${pageCount}`);
    log.info(`🔢 Total requests: ${requestCount}`);
    log.info('==========================================');

    if (jobsScraped === 0) {
        log.error('==========================================');
        log.error('❌ NO JOBS SCRAPED - TROUBLESHOOTING:');
        log.error('==========================================');
        log.error('1. ✓ Check if search parameters return results on website');
        log.error('2. ✓ Try different keywords or locations');
        log.error('3. ✓ Verify proxy configuration is working');
        log.error('==========================================');
    }
} catch (error) {
    log.error('Fatal error during scraping', { error: error.message, stack: error.stack });
    throw error;
} finally {
    await Actor.exit();
}