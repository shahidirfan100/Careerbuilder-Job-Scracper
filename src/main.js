import { Actor, Dataset, log } from 'apify';
import { PlaywrightCrawler, RequestQueue } from 'crawlee';
import { chromium, launchOptions as camoufoxLaunchOptions } from 'camoufox-js';

// ---------- Helpers ----------
const sleep = (ms) => new Promise((res) => setTimeout(res, ms));
const randomBetween = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
const humanDelay = async (min = 800, max = 1800) => sleep(randomBetween(min, max));

const POSTED_DATE_MAP = { '24h': '1', '7d': '7', '30d': '30' };
const CHROME_UA =
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';
const DEFAULT_VIEWPORT = { width: 1366, height: 768 };

const parseJSON = (maybeJson) => {
    if (!maybeJson) return null;
    try {
        return typeof maybeJson === 'string' ? JSON.parse(maybeJson) : maybeJson;
    } catch (err) {
        log.warning(`Could not parse JSON input: ${err.message}`);
        return null;
    }
};

const normalizeCookieHeader = ({ cookies: rawCookies, cookiesJson }) => {
    if (rawCookies && typeof rawCookies === 'string' && rawCookies.trim()) return rawCookies.trim();
    const parsed = parseJSON(cookiesJson);
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

const buildStartUrl = (keyword, location, posted_date, radius) => {
    const url = new URL('https://www.careerbuilder.com/jobs');
    if (keyword) url.searchParams.set('keywords', keyword);
    if (location) url.searchParams.set('location', location);
    if (posted_date && posted_date !== 'anytime' && POSTED_DATE_MAP[posted_date]) {
        url.searchParams.set('posted', POSTED_DATE_MAP[posted_date]);
    }
    url.searchParams.set('cb_apply', 'false');
    url.searchParams.set('radius', String(radius || 50));
    return url.toString();
};

const normalizeStartUrl = (urlString) => {
    try {
        const url = new URL(urlString);
        if (!url.hostname.includes('careerbuilder.com')) return null;
        return url.toString();
    } catch {
        return null;
    }
};

const buildLocationString = (job) => {
    if (!job) return 'Not specified';
    const parts = [job.city, job.state, job.country].filter(Boolean);
    return parts.length ? parts.join(', ') : 'Not specified';
};

const cleanText = (value) => (value || '').replace(/\s+/g, ' ').trim();

const normalizeJob = (job, { source, url, page }) => {
    if (!job) return null;
    const title = job.title || job.job_title || job.name;
    const company = job.company || job.company_name || job.companyName || job.hiringOrganization?.name;
    const jobUrl = job.url || job.jobUrl || job.job_url || url;
    if (!title || !jobUrl) return null;

    const descriptionRaw = job.description || job.job_description || '';
    return {
        title: cleanText(title) || 'Not specified',
        company: cleanText(company) || 'Not specified',
        location: buildLocationString(job.jobLocation || job.location),
        date_posted: job.datePosted || job.posted_date || job.postedDate || 'Not specified',
        salary: job.salary || job.estimatedSalary || job.baseSalary || 'Not specified',
        job_type: job.employmentType || job.job_type || 'Not specified',
        description_html: descriptionRaw,
        description_text: cleanText(descriptionRaw),
        url: jobUrl,
        scraped_at: new Date().toISOString(),
        source,
        page_hint: page,
        raw: job,
    };
};

const extractJsonLdJobs = async (page) => {
    const payloads = await page.$$eval('script[type="application/ld+json"]', (nodes) =>
        nodes.map((n) => n.textContent).filter(Boolean),
    );
    const jobs = [];
    for (const payload of payloads) {
        try {
            const json = JSON.parse(payload);
            const inspect = (node) => {
                if (!node) return;
                if (Array.isArray(node)) return node.forEach(inspect);
                if (node['@type'] === 'JobPosting') jobs.push(node);
                if (node['@graph']) inspect(node['@graph']);
            };
            inspect(json);
        } catch {
            // ignore parse errors
        }
    }
    return jobs;
};

const extractJobLinks = async (page) => {
    const links = await page.$$eval('a[href*="/job/"]', (as) =>
        Array.from(new Set(as.map((a) => a.href).filter((href) => href && href.includes('/job/')))),
    );
    return links;
};

const extractNextPage = async (page, currentUrl, currentPage) => {
    const href = await page.$$eval(
        'a[aria-label*="Next"], a.next, a[rel="next"], .pagination a',
        (as) => {
            for (const a of as) {
                const text = (a.textContent || '').trim().toLowerCase();
                if (text === 'next' || text === '»' || text === '›') return a.href;
                if (text === String((window.__cbPage || 1) + 1)) return a.href;
            }
            return null;
        },
    );
    if (href) return href;
    try {
        const url = new URL(currentUrl);
        const pageNum = Number(url.searchParams.get('page_number') || url.searchParams.get('page') || currentPage || 1) + 1;
        url.searchParams.set('page_number', String(pageNum));
        return url.toString();
    } catch {
        return null;
    }
};

// ---------- Main ----------
await Actor.init();
log.info('Actor initialized');

const input = (await Actor.getInput()) ?? {};
const keyword = (input.keyword || '').trim();
const location = (input.location || '').trim();
const posted_date = (input.posted_date || 'anytime').trim().toLowerCase();
const radius = Math.min(250, Number(input.radius) || 50);
const RESULTS_WANTED = Number.isFinite(+input.results_wanted) ? Math.min(10000, Math.max(1, +input.results_wanted)) : 100;
const MAX_PAGES = Number.isFinite(+input.max_pages) ? Math.min(100, Math.max(1, +input.max_pages)) : 20;
const proxyConfiguration = input.proxyConfiguration ?? { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'] };
const cookieHeader = normalizeCookieHeader({ cookies: input.cookies, cookiesJson: input.cookiesJson });

const initialUrl = normalizeStartUrl(input.startUrl) || buildStartUrl(keyword, location, posted_date, radius);
const proxyConf = await Actor.createProxyConfiguration(proxyConfiguration);
if (!proxyConf) {
    throw new Error('Proxy configuration is required to avoid blocking.');
}

log.info('==========================================');
log.info('CareerBuilder Scraper (Playwright + Camoufox)');
log.info('==========================================');
log.info(`Target jobs: ${RESULTS_WANTED}`);
log.info(`Max pages: ${MAX_PAGES}`);
log.info(`Start URL: ${initialUrl}`);
log.info(`Keyword: ${keyword || 'N/A'}`);
log.info(`Location: ${location || 'N/A'}`);
log.info(`Posted date: ${posted_date}`);
log.info(`Radius: ${radius}`);
log.info('==========================================');

let jobsScraped = 0;
const scrapedUrls = new Set();
const scrapedIds = new Set();
let pageCount = 0;

const pushJob = async (job) => {
    if (!job) return false;
    const key = job.url || job.raw?.id;
    if (!key || scrapedUrls.has(key) || scrapedIds.has(key)) return false;
    scrapedUrls.add(key);
    if (job.raw?.id) scrapedIds.add(job.raw.id);
    await Dataset.pushData(job);
    jobsScraped += 1;
    return true;
};

const requestQueue = await RequestQueue.open(`pw-cb-${Date.now()}`);
await requestQueue.addRequest({ url: initialUrl, userData: { label: 'LIST', page: 1 } }, { forefront: true });

const camoufoxOptions = await camoufoxLaunchOptions({
    headless: false,
    block_webrtc: false,
    block_webgl: false,
    block_images: false,
    iKnowWhatImDoing: true,
    args: ['--window-size=1366,768', '--disable-blink-features=AutomationControlled', `--user-agent=${CHROME_UA}`],
    firefoxUserPrefs: {
        'general.appversion': '5.0 (Windows)',
        'general.platform': 'Win64',
        'media.navigator.enabled': false,
    },
    locale: 'en-US',
    timezoneId: 'America/New_York',
}).catch((err) => {
    log.warning(`Camoufox options failed, using defaults: ${err.message}`);
    return {};
});

const crawler = new PlaywrightCrawler({
    proxyConfiguration: proxyConf,
    requestQueue,
    maxConcurrency: 2,
    maxRequestRetries: 5,
    requestHandlerTimeoutSecs: 240,
    navigationTimeoutSecs: 180,
    maxRequestsPerCrawl: RESULTS_WANTED * 4,
    useSessionPool: true,
    persistCookiesPerSession: true,
    sessionPoolOptions: {
        maxPoolSize: 12,
        sessionOptions: { maxUsageCount: 4, maxErrorScore: 1 },
    },
    launchContext: {
        launcher: chromium,
        launchOptions: {
            ...camoufoxOptions,
            headless: false,
            viewport: DEFAULT_VIEWPORT,
        },
    },
    preNavigationHooks: [
        async ({ page, session, request }) => {
            const width = DEFAULT_VIEWPORT.width + randomBetween(-60, 60);
            const height = DEFAULT_VIEWPORT.height + randomBetween(-40, 60);
            await page.setViewportSize({ width, height });
            await page.setExtraHTTPHeaders({
                'Accept-Language': 'en-US,en;q=0.9',
                'User-Agent': CHROME_UA,
                Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Ch-Ua': '"Chromium";v="131", "Google Chrome";v="131", "Not?A_Brand";v="99"',
                'Sec-Ch-Ua-Platform': '"Windows"',
                'Sec-Ch-Ua-Mobile': '?0',
                ...(cookieHeader ? { Cookie: cookieHeader } : {}),
            });
            await page.context().addInitScript(() => {
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            });

            // Set cookies once per session
            if (cookieHeader && !session.userData?.cookiesSet) {
                try {
                    await page.context().addCookies(cookieHeaderToPlaywrightCookies(cookieHeader));
                    session.userData = { ...(session.userData || {}), cookiesSet: true };
                } catch (e) {
                    log.debug(`Failed to set cookies: ${e.message}`);
                }
            }

            // Warm-up hit to homepage once per session to establish cookies/clearance.
            if (!session.userData?.warmedUp) {
                try {
                    await page.goto('https://www.careerbuilder.com/?cb_apply=false', { waitUntil: 'domcontentloaded', timeout: 45000 });
                    await humanDelay(800, 1500);
                    session.userData = { ...(session.userData || {}), warmedUp: true };
                } catch (e) {
                    log.debug(`Warm-up failed: ${e.message}`);
                }
            }
            await humanDelay(400, 900);
        },
    ],
    async requestHandler({ request, page, enqueueLinks, log: crawlerLog, session }) {
        const { label = 'LIST', page: pageNo = 1 } = request.userData;
        if (jobsScraped >= RESULTS_WANTED) return;

        const capturedJson = [];
        const responseListener = async (response) => {
            try {
                const ct = (response.headers()['content-type'] || '').toLowerCase();
                if (!ct.includes('application/json')) return;
                const url = response.url();
                if (!url.includes('careerbuilder.com')) return;
                if (!/api|search|job/i.test(url)) return;
                const data = await response.json();
                capturedJson.push({ url, data });
            } catch {
                // ignore
            }
        };
        page.on('response', responseListener);

        const viewport = page.viewportSize() || DEFAULT_VIEWPORT;
        let response;
        try {
            response = await page.goto(request.url, { waitUntil: 'domcontentloaded', timeout: 45000 });
        } catch (err) {
            const msg = String(err?.message || '').toLowerCase();
            if (msg.includes('proxy') || msg.includes('connection refused')) {
                session?.retire();
            }
            throw err;
        }
        const status = response?.status() ?? 200;
        if (status === 403) {
            crawlerLog.warning(`Navigation returned status ${status}, retiring session and retrying with new session`);
            session?.retire();
            throw new Error(`Blocked (${status})`);
        }
        if (status >= 400) {
            crawlerLog.warning(`Navigation returned status ${status}`);
            throw new Error(`Blocked (${status})`);
        }

        await page.waitForTimeout(randomBetween(1400, 2600));
        await page.waitForLoadState('networkidle').catch(() => {});
        // Mimic human interaction: slight mouse move, scrolls.
        const mouseX = randomBetween(200, Math.max(400, viewport.width - 200));
        const mouseY = randomBetween(150, Math.max(300, viewport.height - 150));
        await page.mouse.move(mouseX, mouseY, { steps: 20 }).catch(() => {});
        await page.evaluate(() => {
            const distance = Math.floor(document.body.scrollHeight * (0.2 + Math.random() * 0.3));
            window.scrollBy(0, distance);
        }).catch(() => {});
        page.off('response', responseListener);

        const bodyText = await page.$eval('body', (b) => b.innerText || '').catch(() => '');
        const lower = bodyText.toLowerCase();
        const blocked = ['access denied', 'captcha', 'cloudflare', 'blocked', 'verify you are human'].some((s) => lower.includes(s));
        if (blocked) {
            crawlerLog.warning('Blocking detected, retiring session');
            session?.retire();
            throw new Error('Blocked');
        }

        if (label === 'LIST') {
            pageCount += 1;
            crawlerLog.info(`LIST page ${pageNo} (pages: ${pageCount}/${MAX_PAGES}, jobs: ${jobsScraped}/${RESULTS_WANTED})`);

            // Use captured API JSON first
            for (const { url, data } of capturedJson) {
                if (jobsScraped >= RESULTS_WANTED) break;
                const arrays = [];
                const findArrays = (node) => {
                    if (!node || arrays.length) return;
                    if (Array.isArray(node)) {
                        const looksJob = node.some((i) => i?.title || i?.job_title || i?.name);
                        if (looksJob) arrays.push(node);
                        return;
                    }
                    if (typeof node === 'object') Object.values(node).forEach(findArrays);
                };
                findArrays(data);
                if (!arrays.length) continue;
                crawlerLog.info(`Captured internal API jobs from ${url}`);
                for (const job of arrays[0]) {
                    if (jobsScraped >= RESULTS_WANTED) break;
                    const normalized = normalizeJob(job, { source: 'playwright-api', url, page: pageNo });
                    if (await pushJob(normalized)) {
                        crawlerLog.info(`[API] ${jobsScraped}/${RESULTS_WANTED}: ${normalized.title} @ ${normalized.company}`);
                    }
                }
            }

            if (jobsScraped >= RESULTS_WANTED) return;

            const jsonLdJobs = await extractJsonLdJobs(page);
            for (const job of jsonLdJobs) {
                if (jobsScraped >= RESULTS_WANTED) break;
                const normalized = normalizeJob(job, { source: 'json-ld-list', url: request.url, page: pageNo });
                if (await pushJob(normalized)) {
                    crawlerLog.info(`[LD] ${jobsScraped}/${RESULTS_WANTED}: ${normalized.title} @ ${normalized.company}`);
                }
            }

            if (jobsScraped < RESULTS_WANTED) {
                const jobLinks = await extractJobLinks(page);
                const toEnqueue = jobLinks.filter((url) => !scrapedUrls.has(url)).slice(0, RESULTS_WANTED - jobsScraped);
                if (toEnqueue.length) {
                    await enqueueLinks({ urls: toEnqueue, userData: { label: 'DETAIL', referer: request.url } });
                }
            }

            if (jobsScraped < RESULTS_WANTED && pageCount < MAX_PAGES) {
                const nextUrl = await extractNextPage(page, request.url, pageNo);
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
            const jsonLdJobs = await extractJsonLdJobs(page);
            if (jsonLdJobs.length) {
                const normalized = normalizeJob(jsonLdJobs[0], { source: 'json-ld-detail', url: request.url });
                if (await pushJob(normalized)) {
                    crawlerLog.info(`[LD] ${jobsScraped}/${RESULTS_WANTED}: ${normalized.title} @ ${normalized.company}`);
                }
                return;
            }

            const detail = await page.evaluate(() => {
                const pick = (sel) => (document.querySelector(sel)?.textContent || '').trim();
                const title = pick('h1') || pick('[class*="title"]');
                const company = pick('[class*="company"]') || pick('[data-testid*="company"]');
                const location = pick('[class*="location"]') || pick('[data-testid*="location"]');
                const date_posted = pick('time') || pick('[class*="posted"]');
                const descEl =
                    document.querySelector('#jdp_description') ||
                    document.querySelector('[class*="description"]') ||
                    document.querySelector('.jdp-left-content');
                const description_html = descEl ? descEl.innerHTML : '';
                const description_text = descEl ? descEl.innerText : '';
                return { title, company, location, date_posted, description_html, description_text };
            });

            const jobData = {
                title: detail.title || 'Not specified',
                company: detail.company || 'Not specified',
                location: detail.location || 'Not specified',
                date_posted: detail.date_posted || 'Not specified',
                salary: 'Not specified',
                job_type: 'Not specified',
                description_html: detail.description_html || '',
                description_text: cleanText(detail.description_text),
                url: request.url,
                scraped_at: new Date().toISOString(),
                source: 'html-detail',
            };
            if (await pushJob(jobData)) {
                crawlerLog.info(`[HTML] ${jobsScraped}/${RESULTS_WANTED}: ${jobData.title} @ ${jobData.company}`);
            }
        }
    },
    failedRequestHandler: async ({ request }, error) => {
        log.error(`Request failed: ${request.url}`, { error: error.message });
        const msg = (error?.message || '').toLowerCase();
        if (msg.includes('proxy') || msg.includes('connection refused') || msg.includes('blocked')) {
            // Give crawler chance to rotate session/proxy.
        }
        await humanDelay(2500, 5000);
    },
});

await crawler.run();

log.info('==========================================');
log.info('Scraping complete');
log.info(`Jobs scraped: ${jobsScraped}`);
log.info(`Pages processed: ${pageCount}`);
log.info('==========================================');

if (jobsScraped === 0) {
    throw new Error('No jobs scraped. CareerBuilder is blocking the actor. Use Apify RESIDENTIAL proxy and rerun.');
}

await Actor.exit();
