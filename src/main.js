import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
import * as cheerio from 'cheerio';

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

// ------------------------- JSON-LD EXTRACTOR -------------------------
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

// ------------------------- DESCRIPTION CLEANER -------------------------
function cleanDescription(rawHtml) {
    // Accepts a fragment (e.g., innerHTML of the description container) OR a string from JSON-LD
    if (rawHtml == null) return { html: '', text: '' };

    // Load as fragment; DO NOT assume <body> exists
    const $ = cheerio.load(String(rawHtml));
    const root = $.root();

    // Unwrap <a> (remove tag, keep anchor text)
    root.find('a').each((_, el) => {
        const $el = $(el);
        $el.replaceWith($el.text());
    });

    // Remove non-text/interactive tags that sometimes leak in
    root.find('script, style, noscript, iframe, button').remove();

    // Keep only text-related formatting tags; unwrap everything else into text
    const allowed = new Set(['p', 'ul', 'ol', 'li', 'strong', 'em', 'br']);
    root.find('*').each((_, el) => {
        const tag = el.tagName ? el.tagName.toLowerCase() : '';
        if (tag && !allowed.has(tag)) {
            const $el = $(el);
            $el.replaceWith($el.text());
        }
    });

    // HTML result (fragment)
    const html = root.html() || '';

    // Text result (normalized)
    const text = root.text()
        .replace(/\u00a0/g, ' ')     // nbsp -> space
        .replace(/\s*\n\s*/g, '\n')  // tidy newlines
        .replace(/[ \t]{2,}/g, ' ')  // collapse spaces
        .replace(/\n{3,}/g, '\n\n')  // limit blank lines
        .trim();

    return { html: html.trim(), text };
}

// ------------------------- PROXY & CRAWLER -------------------------
const proxyConf = proxyConfiguration
    ? await Actor.createProxyConfiguration(proxyConfiguration)
    : undefined;

let jobsScraped = 0;
const scrapedUrls = new Set(); // Track scraped URLs to avoid duplicates

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
            crawlerLog.info(`✓ Target reached: ${RESULTS_WANTED} jobs scraped`);
            return;
        }

        // ============= LIST PAGE HANDLER =============
        if (label === 'LIST') {
            crawlerLog.info(`📄 Scraping listing page ${page}: ${request.url}`);
            
            const pageTitle = $('title').text();
            crawlerLog.info(`Page title: "${pageTitle}"`);
            
            // Check for blocking
            const bodyText = $('body').text().toLowerCase();
            if (bodyText.includes('access denied') || bodyText.includes('captcha') || 
                bodyText.includes('security check') || bodyText.includes('blocked')) {
                crawlerLog.error('🚫 BLOCKED! Enable proxy configuration or add cookies');
                session.retire();
                return;
            }

            // METHOD 1: Try to extract JSON-LD first (most reliable)
            const jsonLdJobs = extractJsonLd($, crawlerLog);
            if (jsonLdJobs.length > 0) {
                crawlerLog.info(`✨ Found ${jsonLdJobs.length} jobs in JSON-LD data`);
                
                let jobsToSave = 0;
                for (const job of jsonLdJobs) {
                    if (jobsScraped >= RESULTS_WANTED) break;
                    
                    const jobUrl = job.url || job['@id'] || job.identifier?.value;
                    if (!jobUrl || scrapedUrls.has(jobUrl)) continue;
                    
                    scrapedUrls.add(jobUrl);

                    // CLEAN description coming from JSON-LD string
                    const { html: descHtml, text: descText } = cleanDescription(job.description || '');

                    const jobData = {
                        title: job.title || job.name || 'Not specified',
                        company: job.hiringOrganization?.name || 'Not specified',
                        location: typeof job.jobLocation === 'string' 
                            ? job.jobLocation 
                            : job.jobLocation?.address?.addressLocality || 'Not specified',
                        date_posted: job.datePosted || 'Not specified',
                        salary: job.baseSalary?.value || job.estimatedSalary || 'Not specified',
                        job_type: job.employmentType || 'Not specified',
                        description_html: descHtml,
                        description_text: descText,
                        url: jobUrl,
                        scraped_at: new Date().toISOString(),
                        source: 'json-ld'
                    };
                    
                    await Dataset.pushData(jobData);
                    jobsScraped++;
                    jobsToSave++;
                    crawlerLog.info(`✅ [${jobsScraped}/${RESULTS_WANTED}] ${jobData.title}`);
                }
                
                if (jobsToSave > 0) {
                    crawlerLog.info(`💾 Saved ${jobsToSave} jobs from JSON-LD`);
                }
            }

            // METHOD 2: Extract job URLs and enqueue detail pages
            const jobUrls = extractJobUrls($, crawlerLog);
            crawlerLog.info(`🔗 Found ${jobUrls.length} job URLs on page`);
            
            if (jobUrls.length === 0 && jsonLdJobs.length === 0) {
                crawlerLog.warning('⚠️ No jobs found! Possible causes:');
                crawlerLog.warning('  - Site structure changed');
                crawlerLog.warning('  - Being blocked (try proxy)');
                crawlerLog.warning('  - Search returned no results');
                crawlerLog.info(`HTML sample: ${$('body').html()?.substring(0, 500)}`);
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
                    userData: { label: 'DETAIL' },
                });
            }

            // PAGINATION
            if (jobsScraped < RESULTS_WANTED && page < MAX_PAGES) {
                const nextUrl = findNextPageUrl($, request.url, page, crawlerLog);
                
                if (nextUrl && nextUrl !== request.url) {
                    crawlerLog.info(`📄 Next page found: ${page + 1}`);
                    await enqueueLinks({
                        urls: [nextUrl],
                        userData: { label: 'LIST', page: page + 1 },
                    });
                } else {
                    crawlerLog.info('🏁 No more pages to scrape');
                }
            } else if (page >= MAX_PAGES) {
                crawlerLog.info(`🛑 Max pages limit reached: ${MAX_PAGES}`);
            }
        }

        // ============= DETAIL PAGE HANDLER =============
        if (label === 'DETAIL') {
            if (jobsScraped >= RESULTS_WANTED) {
                crawlerLog.info(`⏭️ Skipping (limit reached): ${request.url}`);
                return;
            }

            if (scrapedUrls.has(request.url)) {
                crawlerLog.info(`⏭️ Already scraped: ${request.url}`);
                return;
            }

            crawlerLog.info(`🔍 Scraping job detail: ${request.url}`);
            
            // Try JSON-LD first
            const jsonLdJobs = extractJsonLd($, crawlerLog);
            if (jsonLdJobs.length > 0) {
                const job = jsonLdJobs[0];

                // CLEAN description from JSON-LD
                const { html: descHtml, text: descText } = cleanDescription(job.description || '');

                const jobData = {
                    title: job.title || job.name || 'Not specified',
                    company: job.hiringOrganization?.name || 'Not specified',
                    location: typeof job.jobLocation === 'string' 
                        ? job.jobLocation 
                        : job.jobLocation?.address?.addressLocality || 'Not specified',
                    date_posted: job.datePosted || 'Not specified',
                    salary: job.baseSalary?.value || job.estimatedSalary || 'Not specified',
                    job_type: job.employmentType || 'Not specified',
                    description_html: descHtml,
                    description_text: descText,
                    url: request.url,
                    scraped_at: new Date().toISOString(),
                    source: 'json-ld-detail'
                };
                
                scrapedUrls.add(request.url);
                await Dataset.pushData(jobData);
                jobsScraped++;
                crawlerLog.info(`✅ [${jobsScraped}/${RESULTS_WANTED}] ${jobData.title}`);
                return;
            }

            // Fallback to HTML scraping if JSON-LD not available
            const title = $('h1').first().text().trim() ||
                         $('[class*="title"]').first().text().trim();

            if (!title) {
                crawlerLog.warning(`❌ Could not extract title from ${request.url}`);
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

            // Your original selector for description (kept), just cleaned below
            const rawDescription = $('[class*="description"]').first().html() || '';

            const { html: description, text: descriptionText } = cleanDescription(rawDescription);

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
            crawlerLog.info(`✅ [${jobsScraped}/${RESULTS_WANTED}] ${title}`);
        }
    },

    failedRequestHandler: async ({ request }, error) => {
        log.error(`❌ Request failed: ${request.url}`);
        log.error(`Error: ${error.message}`);
    },
});

// ============= START SCRAPING =============
let initialUrl;

if (startUrl) {
    // Use provided URL
    initialUrl = normalizeStartUrl(startUrl);
    if (!initialUrl) {
        log.error('Invalid startUrl provided. Please provide a valid CareerBuilder URL.');
        await Actor.exit();
    }
    log.info('Using provided start URL (ignoring keyword/location parameters)');
} else if (keyword || location) {
    // Build URL from keyword/location
    initialUrl = buildStartUrl(keyword, location, posted_date);
    log.info('Building URL from keyword/location parameters');
} else {
    // No parameters provided - use a default search
    log.warning('No startUrl, keyword, or location provided. Using default CareerBuilder jobs page.');
    initialUrl = 'https://www.careerbuilder.com/jobs?cb_apply=false&radius=50&cb_veterans=false&cb_workhome=all';
}

log.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
log.info('🚀 CareerBuilder Scraper Starting');
log.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
log.info(`📊 Target: ${RESULTS_WANTED} jobs`);
log.info(`📄 Max pages: ${MAX_PAGES}`);
log.info(`🔗 Start URL: ${initialUrl}`);
log.info(`🛡️ Proxy: ${proxyConf ? '✓ ENABLED' : '✗ DISABLED (may cause blocking)'}`);
log.info(`🍪 Cookies: ${cookies || cookiesJson ? '✓ PROVIDED' : '✗ NOT PROVIDED'}`);
log.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

if (!proxyConf) {
    log.warning('⚠️ WARNING: No proxy configured! You may get blocked.');
    log.warning('⚠️ Enable "Apify Proxy" in input for best results.');
}

await crawler.run([{ url: initialUrl, userData: { label: 'LIST', page: 1 } }]);

log.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
log.info(`🏁 Scraping Complete`);
log.info(`✅ Successfully scraped: ${jobsScraped} jobs`);
log.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

if (jobsScraped === 0) {
    log.error('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    log.error('❌ NO JOBS SCRAPED - TROUBLESHOOTING:');
    log.error('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    log.error('1. ✓ Enable "Apify Proxy" in actor input');
    log.error('2. ✓ Try adding custom cookies from your browser');
    log.error('3. ✓ Check if your search parameters return results on website');
    log.error('4. ✓ Try different keywords or locations');
    log.error('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
}

await Actor.exit();