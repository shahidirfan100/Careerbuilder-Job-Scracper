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
    if (kw) url.searchParams.set('keywords', kw);
    if (loc) url.searchParams.set('location', loc);
    if (date && date !== 'anytime') {
        const dateMap = { '24h': '1', '7d': '7', '30d': '30' };
        if (dateMap[date]) url.searchParams.set('posted', dateMap[date]);
    }
    url.searchParams.set('cb_apply', 'false');
    url.searchParams.set('radius', '50');
    url.searchParams.set('cb_veterans', 'false');
    url.searchParams.set('cb_workhome', 'all');
    return url.href;
};

const normalizeStartUrl = (urlString) => {
    try {
        const url = new URL(urlString);
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

// Clean description text - removes HTML and unwanted sections
const cleanDescriptionText = ($element) => {
    if (!$element || $element.length === 0) return '';
    const $clone = $element.clone();
    $clone.find('.jdp-required-skills, #apply-bottom-content, #apply-bottom, #cb-tip').remove();
    $clone.find('#ads-mobile-placeholder, #ads-desktop-placeholder, #col-right').remove();
    $clone.find('.seperate-top-border-mobile, .site-tip, .report-job-link').remove();
    $clone.find('button, script, style, noscript').remove();
    let text = $clone.text();
    text = text.replace(/\s+/g, ' ').replace(/\n\s*\n\s*\n+/g, '\n\n');
    return text.trim();
};

// Clean description HTML - keeps only job content, removes links
const cleanDescriptionHtml = ($element) => {
    if (!$element || $element.length === 0) return '';
    const $clone = $element.clone();
    $clone.find('.jdp-required-skills, #apply-bottom-content, #apply-bottom, #cb-tip').remove();
    $clone.find('#ads-mobile-placeholder, #ads-desktop-placeholder, #col-right').remove();
    $clone.find('.seperate-top-border-mobile, .site-tip').remove();
    $clone.find('button, script, style, noscript').remove();
    $clone.find('a').each(function() {
        const $this = $clone.constructor(this);
        $this.replaceWith($this.text());
    });
    $clone.find('p:empty, div:empty').remove();
    let html = $clone.html() || '';
    html = html.replace(/\s+/g, ' ').replace(/>\s+</g, '><');
    return html.trim();
};

const extractJsonLd = ($, crawlerLog) => {
    const jsonLdScripts = $('script[type="application/ld+json"]').toArray();
    const jobPostings = [];
    for (const script of jsonLdScripts) {
        try {
            const content = $(script).html();
            if (!content) continue;
            const parsed = JSON.parse(content);
            if (parsed['@type'] === 'JobPosting') {
                jobPostings.push(parsed);
            } else if (Array.isArray(parsed)) {
                for (const item of parsed) {
                    if (item['@type'] === 'JobPosting') jobPostings.push(item);
                }
            } else if (parsed['@graph']) {
                for (const item of parsed['@graph']) {
                    if (item['@type'] === 'JobPosting') jobPostings.push(item);
                }
            }
        } catch (e) {
            crawlerLog.debug(`Failed to parse JSON-LD: ${e.message}`);
        }
    }
    return jobPostings;
};

const extractJobUrls = ($, crawlerLog) => {
    const urls = new Set();
    const strategies = [
        () => {
            $('a[href*="/job/"]').each((_, el) => {
                const href = $(el).attr('href');
                if (href && href.includes('/job/') && !href.includes('?') && href.length > 20) {
                    urls.add(href);
                }
            });
        },
        () => {
            $('[data-job-did], [data-job-id]').each((_, el) => {
                const href = $(el).find('a').first().attr('href') || $(el).attr('href');
                if (href && href.includes('/job/')) urls.add(href);
            });
        },
        () => {
            $('div[class*="job"], article[class*="job"], li[class*="job"]').each((_, el) => {
                const href = $(el).find('a[href*="/job/"]').first().attr('href');
                if (href) urls.add(href);
            });
        },
        () => {
            $('[onclick*="job"], [data-gtm*="job"]').each((_, el) => {
                const href = $(el).attr('href') || $(el).find('a').first().attr('href');
                if (href && href.includes('/job/')) urls.add(href);
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
    const absoluteUrls = [];
    for (const url of urls) {
        try {
            absoluteUrls.push(new URL(url, 'https://www.careerbuilder.com').href);
        } catch (e) {
            crawlerLog.debug(`Invalid URL: ${url}`);
        }
    }
    return absoluteUrls;
};

const findNextPageUrl = ($, currentUrl, currentPage, crawlerLog) => {
    const nextSelectors = ['a[aria-label*="Next"]', 'a.next', 'a[rel="next"]'];
    let nextUrl = null;
    for (const selector of nextSelectors) {
        const href = $(selector).first().attr('href');
        if (href) {
            nextUrl = href;
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
        } catch (e) {
            crawlerLog.debug(`Failed to construct next URL: ${e.message}`);
        }
    }
    return nextUrl ? new URL(nextUrl, 'https://www.careerbuilder.com').href : null;
};

// ------------------------- PROXY & CRAWLER -------------------------
const proxyConf = proxyConfiguration
    ? await Actor.createProxyConfiguration(proxyConfiguration)
    : undefined;

let jobsScraped = 0;
const scrapedUrls = new Set();

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

        if (label === 'LIST') {
            crawlerLog.info(`Scraping listing page ${page}: ${request.url}`);
            const pageTitle = $('title').text();
            crawlerLog.info(`Page title: "${pageTitle}"`);
            
            const bodyText = $('body').text().toLowerCase();
            if (bodyText.includes('access denied') || bodyText.includes('captcha') || 
                bodyText.includes('security check') || bodyText.includes('blocked')) {
                crawlerLog.error('BLOCKED! Enable proxy configuration or add cookies');
                session.retire();
                return;
            }

            const jsonLdJobs = extractJsonLd($, crawlerLog);
            if (jsonLdJobs.length > 0) {
                crawlerLog.info(`Found ${jsonLdJobs.length} jobs in JSON-LD data`);
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
                        location: typeof job.jobLocation === 'string' ? job.jobLocation : job.jobLocation?.address?.addressLocality || 'Not specified',
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
                    crawlerLog.info(`[${jobsScraped}/${RESULTS_WANTED}] ${jobData.title}`);
                }
                if (jobsToSave > 0) {
                    crawlerLog.info(`Saved ${jobsToSave} jobs from JSON-LD`);
                }
            }

            const jobUrls = extractJobUrls($, crawlerLog);
            crawlerLog.info(`Found ${jobUrls.length} job URLs on page`);
            
            if (jobUrls.length === 0 && jsonLdJobs.length === 0) {
                crawlerLog.warning('No jobs found!');
                return;
            }

            const urlsToEnqueue = [];
            for (const url of jobUrls) {
                if (jobsScraped + urlsToEnqueue.length >= RESULTS_WANTED) break;
                if (!scrapedUrls.has(url)) {
                    urlsToEnqueue.push(url);
                }
            }

            if (urlsToEnqueue.length > 0) {
                crawlerLog.info(`Enqueueing ${urlsToEnqueue.length} detail pages`);
                await enqueueLinks({
                    urls: urlsToEnqueue,
                    userData: { label: 'DETAIL' },
                });
            }

            if (jobsScraped < RESULTS_WANTED && page < MAX_PAGES) {
                const nextUrl = findNextPageUrl($, request.url, page, crawlerLog);
                if (nextUrl && nextUrl !== request.url) {
                    crawlerLog.info(`Next page found: ${page + 1}`);
                    await enqueueLinks({
                        urls: [nextUrl],
                        userData: { label: 'LIST', page: page + 1 },
                    });
                }
            }
        }

        if (label === 'DETAIL') {
            if (jobsScraped >= RESULTS_WANTED) return;
            if (scrapedUrls.has(request.url)) return;

            crawlerLog.info(`Scraping job detail: ${request.url}`);
            
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
                    location: typeof job.jobLocation === 'string' ? job.jobLocation : job.jobLocation?.address?.addressLocality || 'Not specified',
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

            const title = $('h1').first().text().trim() || $('[class*="title"]').first().text().trim();
            if (!title) {
                crawlerLog.warning(`Could not extract title from ${request.url}`);
                session.retire();
                return;
            }

            const company = $('[class*="company"]').first().text().trim() || 'Not specified';
            const jobLocation = $('[class*="location"]').first().text().trim() || 'Not specified';
            const datePosted = $('time').first().text().trim() || 'Not specified';

            let $descElement = $('#jdp_description').first();
            if ($descElement.length === 0) $descElement = $('[class*="description"]').first();
            if ($descElement.length === 0) $descElement = $('.jdp-left-content').first();

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
        log.error('Invalid startUrl provided.');
        await Actor.exit();
    }
} else if (keyword || location) {
    initialUrl = buildStartUrl(keyword, location, posted_date);
} else {
    initialUrl = 'https://www.careerbuilder.com/jobs?cb_apply=false&radius=50&cb_veterans=false&cb_workhome=all';
}

log.info('CareerBuilder Scraper Starting');
log.info(`Target: ${RESULTS_WANTED} jobs | Max pages: ${MAX_PAGES}`);
log.info(`Proxy: ${proxyConf ? 'ENABLED' : 'DISABLED'}  | Cookies: ${cookies || cookiesJson ? 'PROVIDED' : 'NOT PROVIDED'}`);
log.info(`URL: ${initialUrl}`);

await crawler.run([{ url: initialUrl, userData: { label: 'LIST', page: 1 } }]);

log.info(`Scraping Complete - Successfully scraped: ${jobsScraped} jobs`);
await Actor.exit();