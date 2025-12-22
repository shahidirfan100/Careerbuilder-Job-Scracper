// src/main.js - Complete CareerBuilder Camoufox Crawler for Apify
import { Actor } from 'apify';
import { PlaywrightCrawler, createPlaywrightRouter } from 'crawlee';
import { firefox } from 'playwright';
import { launchOptions as camoufoxLaunchOptions } from 'camoufox-js';

// Helper functions
const cleanText = (value) => (value || '').replace(/\s+/g, ' ').trim();

const normalizeJob = (job, { source, url }) => {
    if (!job) return null;
    const title = job.title || job.job_title || job.name;
    const company = job.company || job.company_name || job.companyName || job.hiringOrganization?.name;
    const jobUrl = job.url || job.jobUrl || job.job_url || url;
    if (!title || !jobUrl) return null;

    const descriptionRaw = job.description || job.job_description || '';
    return {
        title: cleanText(title) || 'Not specified',
        company: cleanText(company) || 'Not specified',
        location: job.location || 'Not specified',
        date_posted: job.datePosted || job.posted_date || job.postedDate || 'Not specified',
        salary: job.salary || job.estimatedSalary || job.baseSalary || 'Not specified',
        job_type: job.employmentType || job.job_type || 'Not specified',
        description_html: descriptionRaw,
        description_text: cleanText(descriptionRaw),
        url: jobUrl,
        scraped_at: new Date().toISOString(),
        source,
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

const extractNextPage = async (page, currentUrl) => {
    const href = await page.$$eval(
        'a[aria-label*="Next"], a.next, a[rel="next"], .pagination a',
        (as) => {
            for (const a of as) {
                const text = (a.textContent || '').trim().toLowerCase();
                if (text === 'next' || text === '»' || text === '›') return a.href;
            }
            return null;
        },
    );
    if (href) return href;
    try {
        const url = new URL(currentUrl);
        const pageNum = Number(url.searchParams.get('page_number') || url.searchParams.get('page') || 1) + 1;
        url.searchParams.set('page_number', String(pageNum));
        return url.toString();
    } catch {
        return null;
    }
};

const router = createPlaywrightRouter();

// Handler for job search/list pages
router.addHandler('LIST', async ({ request, page, enqueueLinks, log }) => {
    log.info(`Processing list page: ${request.url}`);

    // Wait for content to load
    await page.waitForLoadState('networkidle');

    // Human-like interactions
    const viewport = await page.viewportSize();
    const mouseX = Math.floor(Math.random() * (viewport.width - 200)) + 100;
    const mouseY = Math.floor(Math.random() * (viewport.height - 200)) + 100;
    await page.mouse.move(mouseX, mouseY, { steps: 20 });
    await page.evaluate(() => {
        const distance = Math.floor(document.body.scrollHeight * (0.2 + Math.random() * 0.3));
        window.scrollBy(0, distance);
    });
    await page.waitForTimeout(Math.floor(Math.random() * 2000) + 1000);

    // Extract jobs from JSON-LD
    const jsonLdJobs = await extractJsonLdJobs(page);
    for (const job of jsonLdJobs) {
        const normalized = normalizeJob(job, { source: 'json-ld-list', url: request.url });
        if (normalized) {
            await Actor.pushData(normalized);
            log.info(`Pushed job: ${normalized.title} @ ${normalized.company}`);
        }
    }

    // Extract job links and enqueue detail pages
    const jobLinks = await extractJobLinks(page);
    if (jobLinks.length) {
        await enqueueLinks({
            urls: jobLinks,
            label: 'DETAIL',
        });
    }

    // Enqueue next page
    const nextUrl = await extractNextPage(page, request.url);
    if (nextUrl) {
        await enqueueLinks({
            urls: [nextUrl],
            label: 'LIST',
        });
    }
});

// Handler for job detail pages
router.addHandler('DETAIL', async ({ request, page, log }) => {
    log.info(`Processing detail page: ${request.url}`);

    // Wait for content to load
    await page.waitForLoadState('networkidle');

    // Human-like interactions
    const viewport = await page.viewportSize();
    const mouseX = Math.floor(Math.random() * (viewport.width - 200)) + 100;
    const mouseY = Math.floor(Math.random() * (viewport.height - 200)) + 100;
    await page.mouse.move(mouseX, mouseY, { steps: 20 });
    await page.evaluate(() => {
        const distance = Math.floor(document.body.scrollHeight * 0.3);
        window.scrollBy(0, distance);
    });
    await page.waitForTimeout(Math.floor(Math.random() * 1500) + 500);

    // Try JSON-LD first
    const jsonLdJobs = await extractJsonLdJobs(page);
    if (jsonLdJobs.length) {
        const normalized = normalizeJob(jsonLdJobs[0], { source: 'json-ld-detail', url: request.url });
        if (normalized) {
            await Actor.pushData(normalized);
            log.info(`Pushed job: ${normalized.title} @ ${normalized.company}`);
        }
        return;
    }

    // Fallback to HTML scraping
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

    await Actor.pushData(jobData);
    log.info(`Pushed job: ${jobData.title} @ ${jobData.company}`);
});

// Default handler for any unmatched routes
router.addDefaultHandler(async ({ request, page, enqueueLinks, log }) => {
    log.info(`Default handler for: ${request.url}`);

    // Check if it's a job search page
    if (request.url.includes('/jobs') || request.url.includes('keywords=')) {
        await enqueueLinks({
            urls: [request.url],
            label: 'LIST',
        });
    } else if (request.url.includes('/job/')) {
        await enqueueLinks({
            urls: [request.url],
            label: 'DETAIL',
        });
    }
});

(async () => {
    // Initialize Apify SDK
    await Actor.init();

    // Get input schema
    const input = await Actor.getInput();
    const { 
        startUrls = 'https://www.careerbuilder.com/jobs',
        keyword = '',
        location = '',
        posted_date = 'anytime',
        radius = 50,
        results_wanted = 100,
        max_pages = 20,
        cookiesJson,
        proxyConfiguration
    } = input;

    // Parse startUrls as newline-separated list
    let initialUrlsToCrawl = startUrls.split('\n').map(url => url.trim()).filter(url => url);

    // Build start URLs if keyword/location provided
    if (keyword.trim() || location.trim()) {
        const baseUrl = 'https://www.careerbuilder.com/jobs';
        const url = new URL(baseUrl);
        if (keyword.trim()) url.searchParams.set('keywords', keyword.trim());
        if (location.trim()) url.searchParams.set('location', location.trim());
        if (posted_date && posted_date !== 'anytime') {
            const POSTED_DATE_MAP = { '24h': '1', '7d': '7', '30d': '30' };
            if (POSTED_DATE_MAP[posted_date]) {
                url.searchParams.set('posted', POSTED_DATE_MAP[posted_date]);
            }
        }
        url.searchParams.set('cb_apply', 'false');
        url.searchParams.set('radius', String(radius)); // Corrected variable name
        initialUrlsToCrawl = [url.toString()];
    }

    // Normalize cookie header
    let cookieHeader = '';
    if (cookiesJson) {
        const parseJSON = (maybeJson) => {
            if (!maybeJson) return null;
            try {
                return typeof maybeJson === 'string' ? JSON.parse(maybeJson) : maybeJson;
            } catch (err) {
                log.warning(`Could not parse JSON input: ${err.message}`);
                return null;
            }
        };
        const normalizeCookieHeader = ({ cookiesJson }) => {
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
        cookieHeader = normalizeCookieHeader({ cookiesJson });
    }

    // Create proxy configuration
    const proxyConf = proxyConfiguration ? await Actor.createProxyConfiguration(proxyConfiguration) : await Actor.createProxyConfiguration({
        groups: ['RESIDENTIAL'],
        checkAccess: true
    });

    // Set maxRequestsPerCrawl based on results_wanted
    const maxRequestsPerCrawl = results_wanted * 4; // Allow for retries and pagination
    const maxConcurrency = 3; // Keep low for stealth

    // Launch Camoufox Firefox with stealth options
    const crawler = new PlaywrightCrawler({
        // Limit concurrency for stealth
        maxRequestsPerCrawl,
        maxConcurrency,
        proxyConfiguration: proxyConf,
        requestHandler: router,

        // Camoufox + Firefox launcher with stealth fingerprint
        launchContext: {
            launcher: firefox,
            launchOptions: await camoufoxLaunchOptions({
                headless: false,  // Full browser mode to avoid headless detection
                config: {
                    platform: 'Win32',  // Correct property for OS
                    locale: 'en-US',
                    screen: { width: 1920, height: 1080 },
                    viewport: { width: 1366, height: 768 },
                    webgl: { vendor: 'Mozilla', renderer: 'Mozilla -- Intel(R) UHD Graphics' },
                    fonts: ['Arial', 'Helvetica', 'Times New Roman'],
                    geoip: true  // Let Camoufox handle geolocation based on proxy
                },
                args: [
                    '--disable-blink-features=AutomationControlled',
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-web-security',  // For CORS if needed
                ],
                firefoxUserPrefs: {
                    'layout.css.prefers-reduced-motion': 1,  // Disable animations
                    'dom.webdriver.enabled': false,  // Hide automation flag
                    'media.navigator.enabled': false,  // Block WebRTC leaks
                },
                // Residential proxy rotation per session
                proxy: await proxyConfiguration.newUrl(),
                geoip: true,  // Match proxy geo-location
            }),
        },

        // Pre-navigation stealth hooks
        preNavigationHooks: [
            async (crawlingContext, gotoOptions) => {
                const { page, session } = crawlingContext;
                
                // Block ads/trackers to reduce detection
                await page.route('**/*', (route) => {
                    const url = route.request().url();
                    if (url.includes('ads') || url.includes('tracker') || url.includes('analytics')) {
                        route.abort();
                    } else {
                        route.continue();
                    }
                });
                
                // Spoof headers for realism
                await page.setExtraHTTPHeaders({
                    'Accept-Language': 'en-US,en;q=0.9',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                });
                
                // Set cookies if provided
                if (cookieHeader && !session.userData?.cookiesSet) {
                    try {
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
                        await page.context().addCookies(cookieHeaderToPlaywrightCookies(cookieHeader));
                        session.userData = { ...(session.userData || {}), cookiesSet: true };
                    } catch (e) {
                        log.debug(`Failed to set cookies: ${e.message}`);
                    }
                }
                
                // Randomize viewport slightly
                gotoOptions.viewport = {
                    width: 1366 + Math.floor(Math.random() * 100),
                    height: 768 + Math.floor(Math.random() * 50),
                };
                
                // Human-like delay before navigation
                await new Promise(resolve => setTimeout(resolve, Math.random() * 2000 + 1000));
                
                // Slow human-like navigation speed
                gotoOptions.waitUntil = 'networkidle';
            }
        ],

        // Request handler timeout (CareerBuilder can be slow)
        requestHandlerTimeoutSecs: 60,

        // Retry failed requests with new proxy/fingerprint
        maxRequestRetries: 3,
        retryOnBlockedRequest: true
    });

    // Add search URLs dynamically
    const finalUrlsToCrawl = initialUrlsToCrawl.map(url => ({
        url,
        userData: { label: 'LIST' }
    }));

    console.log(`🚀 Starting CareerBuilder crawl with ${finalUrlsToCrawl.length} URLs`);
    await crawler.run(finalUrlsToCrawl);

    // Graceful exit
    await Actor.exit();
})();
