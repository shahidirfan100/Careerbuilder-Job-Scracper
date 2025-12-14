import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset, PlaywrightCrawler, RequestQueue, SessionPool } from 'crawlee';
import { gotScraping } from 'got-scraping';
import { load } from 'cheerio';

// -------------- CONSTANTS --------------
const API_CANDIDATES = [
    {
        name: 'cb-api-v3',
        baseUrl: 'https://www.careerbuilder.com/api/v3/search',
        pageParam: 'page_number',
        pageSizeParam: 'page_size',
        defaultPageSize: 50,
    },
    {
        name: 'cb-api-v2',
        baseUrl: 'https://www.careerbuilder.com/api/v2/search',
        pageParam: 'page_number',
        pageSizeParam: 'page_size',
        defaultPageSize: 50,
    },
    {
        name: 'cb-rest-jobsearch',
        baseUrl: 'https://www.careerbuilder.com/api/rest/jobsearch',
        pageParam: 'page',
        pageSizeParam: 'perPage',
        defaultPageSize: 50,
    },
];

const JSON_ARRAY_CANDIDATES = [
    ['data', 'results'],
    ['data', 'jobs'],
    ['data', 'items'],
    ['results'],
    ['jobs'],
    ['items'],
];

const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
];

// -------------- HELPERS --------------
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const randomBetween = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
const humanDelay = async (min = 1200, max = 2800) => sleep(randomBetween(min, max));
const getRandomUserAgent = () => USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];

const parseJSON = (maybeJson) => {
    if (!maybeJson) return null;
    try {
        return typeof maybeJson === 'string' ? JSON.parse(maybeJson) : maybeJson;
    } catch (err) {
        log.warning(`Could not parse JSON input: ${err.message}`);
        return null;
    }
};

const normalizeCookieHeader = ({ cookies: rawCookies, cookiesJson: jsonCookies }) => {
    if (rawCookies && typeof rawCookies === 'string' && rawCookies.trim()) return rawCookies.trim();
    const parsed = parseJSON(jsonCookies);
    if (!parsed) return '';
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
    return parts.join('; ');
};

const cookieHeaderToPlaywrightCookies = (headerValue) => {
    if (!headerValue) return [];
    const pairs = headerValue
        .split(';')
        .map((p) => p.trim())
        .filter(Boolean)
        .map((p) => {
            const idx = p.indexOf('=');
            if (idx <= 0) return null;
            return { name: p.slice(0, idx).trim(), value: p.slice(idx + 1).trim() };
        })
        .filter(Boolean);

    return pairs.map(({ name, value }) => ({
        name,
        value,
        url: 'https://www.careerbuilder.com/',
    }));
};

const cleanDescriptionText = ($element) => {
    if (!$element || $element.length === 0) return '';
    const $clone = $element.clone();
    $clone.find('.jdp-required-skills, #apply-bottom-content, #apply-bottom, #cb-tip').remove();
    $clone.find('#ads-mobile-placeholder, #ads-desktop-placeholder, #col-right').remove();
    $clone.find('.seperate-top-border-mobile, .site-tip, .report-job-link').remove();
    $clone.find('button, script, style, noscript').remove();
    let text = $clone.text();
    text = text.replace(/\s+/g, ' ');
    text = text.replace(/\n\s*\n\s*\n+/g, '\n\n');
    return text.trim();
};

const cleanDescriptionHtml = ($element) => {
    if (!$element || $element.length === 0) return '';
    const $clone = $element.clone();
    $clone.find('.jdp-required-skills, #apply-bottom-content, #apply-bottom, #cb-tip').remove();
    $clone.find('#ads-mobile-placeholder, #ads-desktop-placeholder, #col-right').remove();
    $clone.find('.seperate-top-border-mobile, .site-tip').remove();
    $clone.find('button, script, style, noscript').remove();
    $clone.find('a').each(function () {
        const $this = $clone.constructor(this);
        $this.replaceWith($this.text());
    });
    $clone.find('p:empty, div:empty').remove();
    let html = $clone.html() || '';
    html = html.replace(/\s+/g, ' ').replace(/>\s+</g, '><');
    return html.trim();
};

const cleanFromHtmlString = (html = '') => {
    if (!html) return { description_html: '', description_text: '' };
    const $ = load('<div id="desc"></div>');
    $('#desc').html(html);
    const $node = $('#desc');
    return {
        description_html: cleanDescriptionHtml($node),
        description_text: cleanDescriptionText($node),
    };
};

const buildStartUrl = (kw, loc, date) => {
    const url = new URL('https://www.careerbuilder.com/jobs');
    if (kw) url.searchParams.set('keywords', kw);
    if (loc) url.searchParams.set('location', loc);
    const dateMap = { '24h': '1', '7d': '7', '30d': '30' };
    if (date && date !== 'anytime' && dateMap[date]) url.searchParams.set('posted', dateMap[date]);
    url.searchParams.set('cb_apply', 'false');
    url.searchParams.set('radius', '50');
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

const pickValue = (obj, keys) => {
    for (const key of keys) {
        if (obj?.[key]) return obj[key];
    }
    return null;
};

const buildLocationString = (job) => {
    if (!job) return 'Not specified';
    if (typeof job === 'string') return job;
    if (job.address) {
        const parts = [job.address.addressLocality, job.address.addressRegion, job.address.addressCountry].filter(Boolean);
        if (parts.length) return parts.join(', ');
    }
    const city = pickValue(job, ['city', 'City']);
    const state = pickValue(job, ['state', 'State', 'province', 'Province', 'stateProvince']);
    const country = pickValue(job, ['country', 'Country']);
    const parts = [city, state, country].filter(Boolean);
    return parts.length ? parts.join(', ') : 'Not specified';
};

const normalizeApiJob = (job, { source = 'api', searchUrl, page }) => {
    const url = pickValue(job, ['url', 'job_url', 'jobUrl', 'jobURL', 'apply_url', 'applyUrl', 'applyLink']);
    const id = pickValue(job, ['id', 'job_id', 'jobId', 'job_id_local', 'job_did', 'jobDID', 'jobdid', 'did']) || url;
    const descriptionRaw = pickValue(job, ['description', 'job_description', 'jobDescription']) || '';
    const { description_html, description_text } = cleanFromHtmlString(descriptionRaw);

    return {
        title: pickValue(job, ['title', 'job_title', 'jobTitle', 'name']) || 'Not specified',
        company: pickValue(job, ['company', 'company_name', 'companyName']) || job?.hiringOrganization?.name || 'Not specified',
        location: buildLocationString(job.jobLocation || job.location || job.job_location || job.jobLocationAddress || job.address),
        date_posted: pickValue(job, ['datePosted', 'posted_date', 'postedDate', 'posted']) || job?.postedAt || 'Not specified',
        salary: pickValue(job, ['salary', 'salary_info', 'salaryInfo', 'baseSalary']) || 'Not specified',
        job_type: pickValue(job, ['employmentType', 'job_type', 'jobType']) || 'Not specified',
        description_html,
        description_text,
        url: url || searchUrl,
        scraped_at: new Date().toISOString(),
        source,
        page_hint: page,
        raw: job,
    };
};

const extractJsonLd = ($, crawlerLog) => {
    const jsonLdScripts = $('script[type="application/ld+json"]').toArray();
    const jobPostings = [];
    for (const script of jsonLdScripts) {
        try {
            const content = $(script).html();
            if (!content) continue;
            const parsed = JSON.parse(content);
            const inspectNode = (node) => {
                if (!node) return;
                if (Array.isArray(node)) {
                    for (const item of node) inspectNode(item);
                    return;
                }
                if (node['@type'] === 'JobPosting') jobPostings.push(node);
                if (node['@graph']) inspectNode(node['@graph']);
            };
            inspectNode(parsed);
        } catch (e) {
            crawlerLog.debug(`Failed to parse JSON-LD: ${e.message}`);
        }
    }
    return jobPostings;
};

const deepFindJobArrays = (node, results, max = 200) => {
    if (!node || results.length >= max) return;
    if (Array.isArray(node)) {
        const looksLikeJobArray =
            node.length > 0 &&
            node.every((item) => item && typeof item === 'object') &&
            node.some((item) => pickValue(item, ['title', 'jobTitle', 'job_title', 'name'])) &&
            node.some((item) => pickValue(item, ['company', 'companyName', 'company_name']) || item?.hiringOrganization?.name) &&
            node.some((item) => pickValue(item, ['url', 'jobUrl', 'job_url', 'applyUrl', 'apply_url']) || pickValue(item, ['id', 'jobId', 'job_id']));

        if (looksLikeJobArray) {
            results.push(node);
            return;
        }
        for (const item of node) deepFindJobArrays(item, results, max);
        return;
    }
    if (typeof node === 'object') {
        for (const value of Object.values(node)) {
            deepFindJobArrays(value, results, max);
            if (results.length >= max) return;
        }
    }
};

const extractEmbeddedJsonJobs = ($, crawlerLog) => {
    const scripts = $('script').toArray();
    const payloads = [];

    for (const script of scripts) {
        const id = $(script).attr('id') || '';
        const type = ($(script).attr('type') || '').toLowerCase();
        const text = $(script).html() || '';
        if (!text || text.length < 20) continue;

        // High-signal candidates first.
        if (id === '__NEXT_DATA__' || type.includes('application/json')) {
            payloads.push(text);
        } else if (text.includes('window.__') && text.includes('{') && text.length < 2_000_000) {
            // Potential inline state blobs (keep conservative)
            payloads.push(text);
        }
    }

    for (const payload of payloads) {
        let json = null;
        if (payload.trim().startsWith('{') || payload.trim().startsWith('[')) {
            try {
                json = JSON.parse(payload);
            } catch {
                json = null;
            }
        }

        // Support common inline assignment patterns: window.__STATE__=...
        if (!json && payload.includes('=')) {
            const match = payload.match(/=\s*({[\s\S]+});?\s*$/);
            if (match?.[1]) {
                try {
                    json = JSON.parse(match[1]);
                } catch {
                    json = null;
                }
            }
        }

        if (!json) continue;
        const arrays = [];
        deepFindJobArrays(json, arrays, 10);
        for (const arr of arrays) {
            if (arr.length) {
                crawlerLog.debug(`Found embedded JSON job array with ${arr.length} items`);
                return arr;
            }
        }
    }

    return [];
};

const extractJobUrls = ($, crawlerLog) => {
    const urls = new Set();
    const strategies = [
        () => {
            $('a[href*="/job/"]').each((_, el) => {
                const href = $(el).attr('href');
                if (href && href.includes('/job/') && href.length > 20) urls.add(href);
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
            const absolute = new URL(url, 'https://www.careerbuilder.com').href;
            absoluteUrls.push(absolute);
        } catch (e) {
            crawlerLog.debug(`Invalid URL: ${url}`);
        }
    }
    return absoluteUrls;
};

const findNextPageUrl = ($, currentUrl, currentPage, crawlerLog) => {
    const nextSelectors = [
        'a[aria-label*="Next"]',
        'a.next',
        'a[rel="next"]',
        'button[aria-label*="Next"]',
        '.pagination a:contains("Next")',
        'a.pagination-next',
        'a[data-page="next"]',
    ];
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
            const currentPageNum = parseInt(url.searchParams.get('page_number') || url.searchParams.get('page') || '1', 10);
            url.searchParams.set('page_number', String(currentPageNum + 1));
            nextUrl = url.href;
        } catch (e) {
            crawlerLog.debug(`Failed to construct next URL: ${e.message}`);
        }
    }
    return nextUrl ? new URL(nextUrl, 'https://www.careerbuilder.com').href : null;
};

const getJobArrayFromPayload = (payload) => {
    if (!payload) return [];
    if (Array.isArray(payload)) return payload;
    for (const path of JSON_ARRAY_CANDIDATES) {
        let node = payload;
        let valid = true;
        for (const key of path) {
            if (node && typeof node === 'object' && key in node) {
                node = node[key];
            } else {
                valid = false;
                break;
            }
        }
        if (valid && Array.isArray(node)) return node;
    }
    // Sometimes payload is an object with numeric keys
    const values = Object.values(payload);
    if (values.length && values.every((v) => typeof v === 'object')) return values;
    return [];
};

const baseHeaders = (referer) => {
    const userAgent = getRandomUserAgent();
    const chromeVersion = userAgent.match(/Chrome\/(\d+)/)?.[1] || '131';
    return {
        Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        // Note: `zstd` can increase suspicion and is not consistently supported by all stacks.
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control': 'max-age=0',
        Connection: 'keep-alive',
        Referer: referer || 'https://www.careerbuilder.com/',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Ch-Ua': `"Chromium";v="${chromeVersion}", "Google Chrome";v="${chromeVersion}", "Not?A_Brand";v="99"`,
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': referer ? 'same-origin' : 'none',
        'Sec-Fetch-User': '?1',
        'User-Agent': userAgent,
    };
};

const apiHeaders = (referer) => {
    const userAgent = getRandomUserAgent();
    const chromeVersion = userAgent.match(/Chrome\/(\d+)/)?.[1] || '131';
    return {
        Accept: 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        Connection: 'keep-alive',
        Origin: 'https://www.careerbuilder.com',
        Referer: referer || 'https://www.careerbuilder.com/',
        'Sec-Ch-Ua': `"Chromium";v="${chromeVersion}", "Google Chrome";v="${chromeVersion}", "Not?A_Brand";v="99"`,
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': userAgent,
    };
};

// -------------- MAIN --------------
await Actor.init();
log.info('Actor initialized');

const input = (await Actor.getInput()) ?? {};
const {
    keyword = '',
    location = '',
    posted_date = 'anytime',
    results_wanted: RESULTS_WANTED_RAW = 100,
    max_pages: MAX_PAGES_RAW = 20,
    startUrl,
    cookies,
    cookiesJson,
    proxyConfiguration = { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'] },
} = input;

const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : 100;
const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW) ? Math.max(1, +MAX_PAGES_RAW) : 20;

const cookieHeader = normalizeCookieHeader({ cookies, cookiesJson });
// Internal defaults (no user input)
const mode = 'preferApi'; // preferApi | apiOnly | htmlOnly
const searchApiUrl = null;
const apiPageSize = 50;
const parsedApiHeaders = {};
const parsedApiParams = {};

// CareerBuilder is US-centric; defaulting to US routing improves success rates (users can still override in the proxy editor).
const effectiveProxyConfiguration = { ...proxyConfiguration };
if (!('countryCode' in effectiveProxyConfiguration) && !('apifyProxyCountry' in effectiveProxyConfiguration)) {
    effectiveProxyConfiguration.countryCode = 'US';
}

const proxyConf = await Actor.createProxyConfiguration(effectiveProxyConfiguration);
if (!proxyConf) {
    log.error('No proxy configured. CareerBuilder blocks direct traffic. Enable Apify RESIDENTIAL proxy.');
    await Actor.exit();
}

let jobsScraped = 0;
const scrapedUrls = new Set();
const scrapedIds = new Set();
let pageCount = 0;

const pushJob = async (job) => {
    const uniqueKey = job?.raw?.id || job?.url;
    if (!uniqueKey) return false;
    const urlToStore = job.url || String(uniqueKey);
    if (scrapedIds.has(uniqueKey) || scrapedUrls.has(urlToStore)) return false;
    scrapedIds.add(uniqueKey);
    scrapedUrls.add(urlToStore);
    await Dataset.pushData(job);
    jobsScraped += 1;
    return true;
};

const resolveInitialUrl = () => {
    if (startUrl) {
        const normalized = normalizeStartUrl(startUrl);
        if (normalized) return normalized;
        log.error('Invalid startUrl provided. Please provide a valid CareerBuilder URL.');
        return null;
    }
    if (keyword || location) return buildStartUrl(keyword, location, posted_date);
    return 'https://www.careerbuilder.com/jobs?cb_apply=false&radius=50';
};

const initialUrl = resolveInitialUrl();
if (!initialUrl) {
    await Actor.exit();
    throw new Error('Invalid start URL provided.');
}

log.info('==========================================');
log.info('CareerBuilder Scraper Starting');
log.info('==========================================');
log.info(`Target jobs: ${RESULTS_WANTED}`);
log.info(`Max pages: ${MAX_PAGES}`);
log.info(`Start URL: ${initialUrl}`);
log.info(`Mode: ${mode}`);
log.info(`Proxy: ${proxyConf ? 'ENABLED (RESIDENTIAL recommended)' : 'DISABLED'}`);
log.info(`Proxy country: ${effectiveProxyConfiguration.countryCode || effectiveProxyConfiguration.apifyProxyCountry || 'AUTO'}`);
log.info(`Cookies provided: ${cookieHeader ? 'YES' : 'NO'}`);
log.info('==========================================');

// -------------- API PHASE --------------
const runApiPhase = async () => {
    if (mode === 'htmlOnly') return { jobs: 0, pages: 0, used: null };
    const candidates = [];
    if (searchApiUrl) {
        candidates.push({
            name: 'custom-api',
            baseUrl: searchApiUrl,
            pageParam: 'page_number',
            pageSizeParam: 'page_size',
            defaultPageSize: apiPageSize,
        });
    }
    candidates.push(...API_CANDIDATES);

    let apiJobs = 0;
    let apiPages = 0;
    let usedCandidate = null;

    const apiSessionPool = await SessionPool.open({
        maxPoolSize: 4,
        sessionOptions: {
            maxUsageCount: 10,
            maxErrorScore: 3,
        },
    });

    const warmUpSession = async (session, referer) => {
        if (session.userData?.warmedUp) return true;
        try {
            const proxyUrl = await proxyConf.newUrl(session.id);
            const res = await gotScraping({
                url: initialUrl,
                proxyUrl,
                headers: baseHeaders(referer),
                timeout: { request: 45000 },
                throwHttpErrors: false,
                retry: { limit: 0 },
                responseType: 'text',
                cookieJar: session.cookieJar,
            });
            const lower = (res.body || '').toLowerCase();
            const looksBlocked = lower.includes('cloudflare') || lower.includes('captcha') || lower.includes('sorry, you have been blocked');
            if (res.statusCode === 403 || looksBlocked) {
                session.markBad();
                return false;
            }
            session.userData = { ...(session.userData || {}), warmedUp: true };
            return true;
        } catch {
            session.markBad();
            return false;
        }
    };

    for (const candidate of candidates) {
        if (jobsScraped >= RESULTS_WANTED) break;
        let page = 1;
        let consecutiveEmpty = 0;
        const pageSize = candidate.defaultPageSize || apiPageSize || 50;
        const referer = startUrl || initialUrl;

        while (page <= MAX_PAGES && jobsScraped < RESULTS_WANTED) {
            const session = await apiSessionPool.getSession();
            // Warm up to establish cookies/region before hitting JSON endpoints.
            await warmUpSession(session, referer);
            const url = new URL(candidate.baseUrl);
            if (keyword) url.searchParams.set('keywords', keyword);
            if (location) url.searchParams.set('location', location);
            if (posted_date && posted_date !== 'anytime') {
                const map = { '24h': '1', '7d': '7', '30d': '30' };
                if (map[posted_date]) url.searchParams.set('posted', map[posted_date]);
            }
            url.searchParams.set(candidate.pageParam || 'page_number', String(page));
            url.searchParams.set(candidate.pageSizeParam || 'page_size', String(pageSize));
            for (const [k, v] of Object.entries(parsedApiParams)) url.searchParams.set(k, v);

            const headers = {
                ...apiHeaders(referer),
                ...parsedApiHeaders,
            };
            if (cookieHeader) headers.Cookie = cookieHeader;

            let responseBody;
            let status;
            let bodyText;
            try {
                const proxyUrl = await proxyConf.newUrl(session.id);
                const res = await gotScraping({
                    url: url.href,
                    proxyUrl,
                    headers,
                    timeout: { request: 45000 },
                    throwHttpErrors: false,
                    retry: { limit: 0 },
                    responseType: 'text',
                    cookieJar: session.cookieJar,
                });
                status = res.statusCode;
                bodyText = res.body;
                try {
                    responseBody = JSON.parse(res.body);
                } catch (err) {
                    log.debug(`API ${candidate.name} page ${page} returned non-JSON: ${err.message}`);
                }
            } catch (error) {
                log.warning(`API request failed (${candidate.name} p${page}): ${error.message}`);
                session.markBad();
                consecutiveEmpty += 1;
                if (consecutiveEmpty >= 2) break;
                await humanDelay(2000, 5000);
                page += 1;
                continue;
            }

            apiPages += 1;
            if (status && status >= 400) {
                log.warning(`API ${candidate.name} returned status ${status}, switching candidate`);
                session.markBad();
                break;
            }

            const lower = (bodyText || '').toLowerCase();
            const looksBlocked = lower.includes('cloudflare') || lower.includes('captcha') || lower.includes('access denied') || lower.includes('sorry, you have been blocked');
            if (looksBlocked) {
                log.warning(`API ${candidate.name} looks blocked, rotating session`);
                session.retire();
                consecutiveEmpty += 1;
                if (consecutiveEmpty >= 2) break;
                await humanDelay(4000, 9000);
                page += 1;
                continue;
            }

            const jobsArray = getJobArrayFromPayload(responseBody);
            if (!jobsArray.length) {
                log.debug(`API ${candidate.name} page ${page} returned empty payload`);
                consecutiveEmpty += 1;
                if (consecutiveEmpty >= 2) break;
                page += 1;
                continue;
            }

            consecutiveEmpty = 0;
            usedCandidate = candidate.name;

            for (const job of jobsArray) {
                if (jobsScraped >= RESULTS_WANTED) break;
                const normalized = normalizeApiJob(job, { source: candidate.name, searchUrl: url.href, page });
                const pushed = await pushJob(normalized);
                if (pushed) {
                    apiJobs += 1;
                    log.info(`[API] ${jobsScraped}/${RESULTS_WANTED}: ${normalized.title} @ ${normalized.company}`);
                }
            }

            if (jobsArray.length < pageSize) break;
            page += 1;
            await humanDelay(800, 1500);
        }

        if (apiJobs > 0 || jobsScraped >= RESULTS_WANTED) break;
    }

    return { jobs: apiJobs, pages: apiPages, used: usedCandidate };
};

// -------------- HTML FALLBACK PHASE --------------
const runHtmlPhase = async () => {
    if (mode === 'apiOnly' && jobsScraped >= RESULTS_WANTED) return;
    if (mode === 'apiOnly' && jobsScraped === 0) {
        log.warning('API-only mode requested but no jobs scraped via API. HTML crawler skipped.');
        return;
    }
    if (mode === 'apiOnly') return;

    let requestCount = 0;
    let failedRequests = 0;

    // Use a dedicated queue so later fallbacks can re-enqueue the same URLs.
    const requestQueue = await RequestQueue.open('html-queue');

    const crawler = new CheerioCrawler({
        proxyConfiguration: proxyConf,
        requestQueue,
        maxRequestsPerMinute: 60,
        maxRequestsPerCrawl: RESULTS_WANTED * 3,
        requestHandlerTimeoutSecs: 120,
        navigationTimeoutSecs: 120,
        maxConcurrency: 2,
        maxRequestRetries: 5,
        useSessionPool: true,
        persistCookiesPerSession: true,
        sessionPoolOptions: {
            maxPoolSize: 4,
            sessionOptions: { maxUsageCount: 10, maxErrorScore: 3 },
        },
        preNavigationHooks: [
            async ({ request, session }) => {
                requestCount += 1;
                if (requestCount > 1) await humanDelay();
                const headers = baseHeaders(request.userData.referer);
                if (cookieHeader) headers.Cookie = cookieHeader;
                request.headers = headers;
                if (request.userData.referer) request.headers.Referer = request.userData.referer;
                log.debug(`Request #${requestCount}: ${request.url}`);
            },
        ],
        async requestHandler({ request, $, enqueueLinks, log: crawlerLog, session }) {
            const { label = 'LIST', page = 1 } = request.userData;
            await humanDelay(1200, 2200);

            if (jobsScraped >= RESULTS_WANTED) return;

            if (label === 'LIST') {
                pageCount += 1;
                crawlerLog.info(`LIST page ${page} (pages: ${pageCount}/${MAX_PAGES}, jobs: ${jobsScraped}/${RESULTS_WANTED})`);

                const bodyText = $('body').text().toLowerCase();
                const bodyHtml = $('body').html() || '';
                const blockingIndicators = ['access denied', 'captcha', 'security check', 'blocked', 'cloudflare', 'ray id', 'checking your browser', 'enable javascript', 'enable cookies', 'unusual traffic', 'automated', 'bot'];
                const isBlocked = blockingIndicators.some((indicator) => bodyText.includes(indicator) || bodyHtml.toLowerCase().includes(indicator));
                if (isBlocked || bodyHtml.length < 1000) {
                    crawlerLog.error('Blocking detected or insufficient HTML, retiring session');
                    session.retire();
                    throw new Error('Blocked');
                }

                // JSON-LD on listing page
                const jsonLdJobs = extractJsonLd($, crawlerLog);
                for (const job of jsonLdJobs) {
                    if (jobsScraped >= RESULTS_WANTED) break;
                    const desc = cleanFromHtmlString(job.description || '');
                    const jobData = {
                        title: job.title || job.name || 'Not specified',
                        company: job.hiringOrganization?.name || 'Not specified',
                        location: buildLocationString(job.jobLocation),
                        date_posted: job.datePosted || 'Not specified',
                        salary: job.baseSalary?.value || job.estimatedSalary || 'Not specified',
                        job_type: job.employmentType || 'Not specified',
                        description_html: desc.description_html,
                        description_text: desc.description_text,
                        url: job.url || job['@id'] || job.identifier?.value,
                        scraped_at: new Date().toISOString(),
                        source: 'json-ld-list',
                    };
                    if (await pushJob(jobData)) {
                        crawlerLog.info(`[LD] ${jobsScraped}/${RESULTS_WANTED}: ${jobData.title} @ ${jobData.company}`);
                    }
                }

                // Embedded JSON (Next.js/app state) fallback
                if (jobsScraped < RESULTS_WANTED) {
                    const embeddedJobs = extractEmbeddedJsonJobs($, crawlerLog);
                    for (const job of embeddedJobs) {
                        if (jobsScraped >= RESULTS_WANTED) break;
                        const normalized = normalizeApiJob(job, { source: 'embedded-json-list', searchUrl: request.url, page });
                        if (await pushJob(normalized)) {
                            crawlerLog.info(`[EMBED] ${jobsScraped}/${RESULTS_WANTED}: ${normalized.title} @ ${normalized.company}`);
                        }
                    }
                }

                const jobUrls = extractJobUrls($, crawlerLog);
                crawlerLog.info(`Found ${jobUrls.length} job URLs on page`);

                const urlsToEnqueue = [];
                for (const url of jobUrls) {
                    if (jobsScraped + urlsToEnqueue.length >= RESULTS_WANTED) break;
                    if (!scrapedUrls.has(url)) urlsToEnqueue.push(url);
                }
                if (urlsToEnqueue.length) {
                    await enqueueLinks({
                        urls: urlsToEnqueue,
                        userData: { label: 'DETAIL', referer: request.url },
                    });
                }

                if (jobsScraped < RESULTS_WANTED && pageCount < MAX_PAGES) {
                    const nextUrl = findNextPageUrl($, request.url, page, crawlerLog);
                    if (nextUrl && nextUrl !== request.url) {
                        await enqueueLinks({
                            urls: [nextUrl],
                            userData: { label: 'LIST', page: page + 1, referer: request.url },
                        });
                    } else {
                        crawlerLog.info('No more pages discovered');
                    }
                }
            }

            if (label === 'DETAIL') {
                if (scrapedUrls.has(request.url)) return;
                const bodyHtml = $('body').html() || '';
                if (bodyHtml.length < 500) {
                    crawlerLog.warning('Detail page too short');
                    return;
                }

                const jsonLdJobs = extractJsonLd($, crawlerLog);
                if (jsonLdJobs.length) {
                    const job = jsonLdJobs[0];
                    const desc = cleanFromHtmlString(job.description || '');
                    const jobData = {
                        title: job.title || job.name || 'Not specified',
                        company: job.hiringOrganization?.name || 'Not specified',
                        location: buildLocationString(job.jobLocation),
                        date_posted: job.datePosted || 'Not specified',
                        salary: job.baseSalary?.value || job.estimatedSalary || 'Not specified',
                        job_type: job.employmentType || 'Not specified',
                        description_html: desc.description_html,
                        description_text: desc.description_text,
                        url: job.url || request.url,
                        scraped_at: new Date().toISOString(),
                        source: 'json-ld-detail',
                    };
                    if (await pushJob(jobData)) {
                        crawlerLog.info(`[LD] ${jobsScraped}/${RESULTS_WANTED}: ${jobData.title} @ ${jobData.company}`);
                    }
                    return;
                }

                const title = $('h1').first().text().trim() || $('[class*="title"]').first().text().trim();
                if (!title) {
                    crawlerLog.warning(`Could not extract title from ${request.url}`);
                    return;
                }
                const company = $('[class*="company"]').first().text().trim() || $('[data-testid*="company"]').first().text().trim() || 'Not specified';
                const jobLocation = $('[class*="location"]').first().text().trim() || $('[data-testid*="location"]').first().text().trim() || 'Not specified';
                const datePosted = $('time').first().text().trim() || $('[class*="posted"]').first().text().trim() || 'Not specified';

                let $descElement = $('#jdp_description').first();
                if ($descElement.length === 0) $descElement = $('[class*="description"]').first();
                if ($descElement.length === 0) $descElement = $('.jdp-left-content').first();

                const description_html = cleanDescriptionHtml($descElement);
                const description_text = cleanDescriptionText($descElement);

                const jobData = {
                    title,
                    company,
                    location: jobLocation,
                    date_posted: datePosted,
                    salary: 'Not specified',
                    job_type: 'Not specified',
                    description_html,
                    description_text,
                    url: request.url,
                    scraped_at: new Date().toISOString(),
                    source: 'html-detail',
                };
                if (await pushJob(jobData)) {
                    crawlerLog.info(`[HTML] ${jobsScraped}/${RESULTS_WANTED}: ${title} @ ${company}`);
                }
            }
        },
        failedRequestHandler: async ({ request }, error) => {
            failedRequests += 1;
            log.error(`Request failed: ${request.url}`, { error: error.message, failedRequests });
            const msg = (error?.message || '').toLowerCase();
            const isBlocked = msg.includes('blocked') || msg.includes('status code') && msg.includes('403');
            if (isBlocked) throw new Error('HTML blocked (403)');
            if (failedRequests > 10) throw new Error('Too many failed requests');
            await humanDelay(2000, 5000);
        },
    });

    await requestQueue.addRequest({ url: initialUrl, userData: { label: 'LIST', page: 1 } }, { forefront: true });
    await crawler.run();
};

// -------------- PLAYWRIGHT FALLBACK PHASE --------------
const runBrowserPhase = async () => {
    log.warning('Starting Playwright fallback (browser mode)...');

    let requestCount = 0;
    let failedRequests = 0;

    // Use a dedicated queue; the HTML phase may have already "handled" the start URL in the default queue.
    const requestQueue = await RequestQueue.open('browser-queue');

    const crawler = new PlaywrightCrawler({
        proxyConfiguration: proxyConf,
        requestQueue,
        maxRequestsPerCrawl: RESULTS_WANTED * 4,
        requestHandlerTimeoutSecs: 240,
        navigationTimeoutSecs: 240,
        maxConcurrency: 1,
        maxRequestRetries: 3,
        useSessionPool: true,
        persistCookiesPerSession: true,
        sessionPoolOptions: {
            maxPoolSize: 3,
            sessionOptions: {
                maxUsageCount: 8,
                maxErrorScore: 2,
            },
        },
        launchContext: {
            launchOptions: {
                headless: false,
                args: [
                    '--disable-blink-features=AutomationControlled',
                    '--no-sandbox',
                ],
            },
        },
        preNavigationHooks: [
            async ({ page, request }) => {
                requestCount += 1;
                // Keep headers minimal in a real browser; let Chromium generate most fingerprinting headers.
                await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-US,en;q=0.9' });
                if (!page.__cbInitScriptsApplied) {
                    page.__cbInitScriptsApplied = true;
                    await page.addInitScript(() => {
                        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                    });
                }

                const cookiesToSet = cookieHeaderToPlaywrightCookies(cookieHeader);
                if (cookiesToSet.length) await page.context().addCookies(cookiesToSet);

                // Reduce bandwidth / speed up: block heavy asset types.
                if (!page.__cbRoutesApplied) {
                    page.__cbRoutesApplied = true;
                    await page.route('**/*', async (route) => {
                        const type = route.request().resourceType();
                        if (type === 'image' || type === 'media' || type === 'font') return route.abort();
                        return route.continue();
                    });
                }
            },
        ],
        async requestHandler({ request, page, enqueueLinks, log: crawlerLog, session }) {
            const { label = 'LIST', page: pageNo = 1 } = request.userData;

            if (jobsScraped >= RESULTS_WANTED) return;

            const capturedJson = [];
            const responseListener = async (response) => {
                try {
                    if (capturedJson.length >= 25) return;
                    const url = response.url();
                    const headers = response.headers();
                    const contentType = (headers['content-type'] || '').toLowerCase();
                    if (!contentType.includes('application/json')) return;
                    const parsedUrl = new URL(url);
                    if (!parsedUrl.hostname.includes('careerbuilder.com')) return;
                    if (!/api|search|job/i.test(url)) return;
                    const data = await response.json();
                    capturedJson.push({ url, data });
                } catch {
                    // ignore
                }
            };
            page.on('response', responseListener);

            await page.waitForLoadState('domcontentloaded');
            await page.waitForTimeout(randomBetween(1200, 2200));

            if (label === 'LIST') {
                // Trigger lazy-loaded results
                for (let i = 0; i < 2; i++) {
                    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
                    await page.waitForTimeout(900);
                }
            }

            // Give XHR a moment to complete, then stop capturing.
            await page.waitForTimeout(2500);
            page.off('response', responseListener);

            const html = await page.content();
            const $ = load(html);

            const title = await page.title().catch(() => '');
            const bodyText = $('body').text();
            const lower = `${title}\n${bodyText}`.toLowerCase();
            const blockingIndicators = [
                'sorry, you have been blocked',
                'attention required',
                'cloudflare',
                'captcha',
                'access denied',
                'checking your browser',
                'enable cookies',
            ];
            const isBlocked = blockingIndicators.some((x) => lower.includes(x));
            if (isBlocked) {
                // Some CF checks resolve after a short wait + reload on Residential IPs.
                await page.waitForTimeout(10_000);
                await page.reload({ waitUntil: 'domcontentloaded' });
                const html2 = await page.content();
                const $2 = load(html2);
                const title2 = await page.title().catch(() => '');
                const lower2 = `${title2}\n${$2('body').text()}`.toLowerCase();
                const stillBlocked = blockingIndicators.some((x) => lower2.includes(x));
                if (stillBlocked) {
                    crawlerLog.error('Blocking detected in browser phase, rotating session');
                    session.retire();
                    throw new Error('Blocked in browser phase');
                }
            }

            if (label === 'LIST') {
                pageCount += 1;
                crawlerLog.info(`BROWSER LIST page ${pageNo} (pages: ${pageCount}/${MAX_PAGES}, jobs: ${jobsScraped}/${RESULTS_WANTED})`);

                if (capturedJson.length) {
                    const urls = [...new Set(capturedJson.map((x) => x.url))].slice(0, 5);
                    crawlerLog.info(`Captured ${capturedJson.length} JSON responses (sample): ${urls.join(' | ')}`);
                } else {
                    crawlerLog.debug('Captured 0 JSON responses on this page');
                }

                // First: try to extract jobs directly from captured JSON API responses.
                for (const { url, data } of capturedJson.slice(0, 10)) {
                    if (jobsScraped >= RESULTS_WANTED) break;
                    const arrays = [];
                    deepFindJobArrays(data, arrays, 5);
                    if (!arrays.length) continue;
                    crawlerLog.info(`Captured internal API jobs from ${url}`);
                    for (const job of arrays[0]) {
                        if (jobsScraped >= RESULTS_WANTED) break;
                        const normalized = normalizeApiJob(job, { source: 'playwright-api-capture', searchUrl: url, page: pageNo });
                        if (await pushJob(normalized)) {
                            crawlerLog.info(`[PW+API] ${jobsScraped}/${RESULTS_WANTED}: ${normalized.title} @ ${normalized.company}`);
                        }
                    }
                }

                const jsonLdJobs = extractJsonLd($, crawlerLog);
                for (const job of jsonLdJobs) {
                    if (jobsScraped >= RESULTS_WANTED) break;
                    const desc = cleanFromHtmlString(job.description || '');
                    const jobData = {
                        title: job.title || job.name || 'Not specified',
                        company: job.hiringOrganization?.name || 'Not specified',
                        location: buildLocationString(job.jobLocation),
                        date_posted: job.datePosted || 'Not specified',
                        salary: job.baseSalary?.value || job.estimatedSalary || 'Not specified',
                        job_type: job.employmentType || 'Not specified',
                        description_html: desc.description_html,
                        description_text: desc.description_text,
                        url: job.url || job['@id'] || job.identifier?.value,
                        scraped_at: new Date().toISOString(),
                        source: 'playwright-json-ld-list',
                    };
                    if (await pushJob(jobData)) {
                        crawlerLog.info(`[PW+LD] ${jobsScraped}/${RESULTS_WANTED}: ${jobData.title} @ ${jobData.company}`);
                    }
                }

                // Embedded JSON (Next.js/app state) fallback
                if (jobsScraped < RESULTS_WANTED) {
                    const embeddedJobs = extractEmbeddedJsonJobs($, crawlerLog);
                    for (const job of embeddedJobs) {
                        if (jobsScraped >= RESULTS_WANTED) break;
                        const normalized = normalizeApiJob(job, { source: 'playwright-embedded-json-list', searchUrl: request.url, page: pageNo });
                        if (await pushJob(normalized)) {
                            crawlerLog.info(`[PW+EMBED] ${jobsScraped}/${RESULTS_WANTED}: ${normalized.title} @ ${normalized.company}`);
                        }
                    }
                }

                const jobUrls = extractJobUrls($, crawlerLog);
                const urlsToEnqueue = [];
                for (const url of jobUrls) {
                    if (jobsScraped + urlsToEnqueue.length >= RESULTS_WANTED) break;
                    if (!scrapedUrls.has(url)) urlsToEnqueue.push(url);
                }

                if (urlsToEnqueue.length) {
                    await enqueueLinks({
                        urls: urlsToEnqueue,
                        userData: { label: 'DETAIL', referer: request.url },
                    });
                }

                if (jobsScraped < RESULTS_WANTED && pageCount < MAX_PAGES) {
                    const nextUrl = findNextPageUrl($, request.url, pageNo, crawlerLog);
                    if (nextUrl && nextUrl !== request.url) {
                        await enqueueLinks({
                            urls: [nextUrl],
                            userData: { label: 'LIST', page: pageNo + 1, referer: request.url },
                        });
                    }
                }
            }

            if (label === 'DETAIL') {
                if (scrapedUrls.has(request.url)) return;

                const jsonLdJobs = extractJsonLd($, crawlerLog);
                if (jsonLdJobs.length) {
                    const job = jsonLdJobs[0];
                    const desc = cleanFromHtmlString(job.description || '');
                    const jobData = {
                        title: job.title || job.name || 'Not specified',
                        company: job.hiringOrganization?.name || 'Not specified',
                        location: buildLocationString(job.jobLocation),
                        date_posted: job.datePosted || 'Not specified',
                        salary: job.baseSalary?.value || job.estimatedSalary || 'Not specified',
                        job_type: job.employmentType || 'Not specified',
                        description_html: desc.description_html,
                        description_text: desc.description_text,
                        url: job.url || request.url,
                        scraped_at: new Date().toISOString(),
                        source: 'playwright-json-ld-detail',
                    };
                    if (await pushJob(jobData)) {
                        crawlerLog.info(`[PW+LD] ${jobsScraped}/${RESULTS_WANTED}: ${jobData.title} @ ${jobData.company}`);
                    }
                    return;
                }

                const titleText = $('h1').first().text().trim() || $('[class*="title"]').first().text().trim();
                if (!titleText) {
                    crawlerLog.warning(`Could not extract title from ${request.url}`);
                    return;
                }

                const company = $('[class*="company"]').first().text().trim() || $('[data-testid*="company"]').first().text().trim() || 'Not specified';
                const jobLocation = $('[class*="location"]').first().text().trim() || $('[data-testid*="location"]').first().text().trim() || 'Not specified';
                const datePosted = $('time').first().text().trim() || $('[class*="posted"]').first().text().trim() || 'Not specified';

                let $descElement = $('#jdp_description').first();
                if ($descElement.length === 0) $descElement = $('[class*="description"]').first();
                if ($descElement.length === 0) $descElement = $('.jdp-left-content').first();

                const description_html = cleanDescriptionHtml($descElement);
                const description_text = cleanDescriptionText($descElement);

                const jobData = {
                    title: titleText,
                    company,
                    location: jobLocation,
                    date_posted: datePosted,
                    salary: 'Not specified',
                    job_type: 'Not specified',
                    description_html,
                    description_text,
                    url: request.url,
                    scraped_at: new Date().toISOString(),
                    source: 'playwright-html-detail',
                };

                if (await pushJob(jobData)) {
                    crawlerLog.info(`[PW] ${jobsScraped}/${RESULTS_WANTED}: ${titleText} @ ${company}`);
                }
            }
        },
        failedRequestHandler: async ({ request }, error) => {
            failedRequests += 1;
            log.error(`Playwright request failed: ${request.url}`, { error: error.message, failedRequests });
            if (failedRequests > 6) throw new Error('Too many failed Playwright requests');
            await humanDelay(3000, 7000);
        },
    });

    await requestQueue.addRequest({ url: initialUrl, userData: { label: 'LIST', page: 1 } }, { forefront: true });
    await crawler.run();
};

try {
    let browserTried = false;
    const apiResult = await runApiPhase();
    log.info(`API phase: jobs=${apiResult.jobs}, pages=${apiResult.pages}, candidate=${apiResult.used || 'none'}`);

    if (jobsScraped < RESULTS_WANTED) {
        log.info('Switching to HTML fallback...');
        try {
            await runHtmlPhase();
        } catch (error) {
            log.warning(`HTML fallback failed (${error.message}). Trying Playwright fallback...`);
            await runBrowserPhase();
            browserTried = true;
        }
        if (jobsScraped === 0) {
            if (!browserTried) {
                log.warning('No jobs scraped after HTML fallback; starting Playwright fallback...');
                await runBrowserPhase();
            }
        }
    } else {
        log.info('Target reached with API phase; HTML fallback skipped.');
    }

    log.info('==========================================');
    log.info('Scraping complete');
    log.info(`Jobs scraped: ${jobsScraped}`);
    log.info(`Pages processed: ${pageCount}`);
    log.info('==========================================');

    if (jobsScraped === 0) {
        throw new Error(
            'No jobs scraped. CareerBuilder is blocking the actor (403/Cloudflare). Use Apify RESIDENTIAL proxy (US) and provide fresh browser cookies in the input.',
        );
    }
} catch (error) {
    log.error('Fatal error during scraping', {
        error: error.message,
        stack: error.stack,
        jobsScraped,
        pagesProcessed: pageCount,
    });
    throw error;
} finally {
    await Actor.exit();
}
