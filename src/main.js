// CareerBuilder Jobs Scraper - Complete Single-File Implementation with Stealth
// Production-ready with 3-tier extraction: API → HTML → Browser (Camoufox)

import { Actor } from 'apify';
import { PlaywrightCrawler } from 'crawlee';
import { firefox } from 'playwright';
import { launchOptions as camoufoxLaunchOptions } from 'camoufox-js';
import { gotScraping } from 'got-scraping';
import * as cheerio from 'cheerio';

// ============================================================================
// HELPER UTILITIES
// ============================================================================

const cleanText = (value) => {
    if (!value) return '';
    return String(value).replace(/\s+/g, ' ').trim();
};

const parseJSON = (maybeJson) => {
    if (!maybeJson) return null;
    try {
        return typeof maybeJson === 'string' ? JSON.parse(maybeJson) : maybeJson;
    } catch {
        return null;
    }
};

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const randomBetween = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;

// ============================================================================
// DEDUPLICATION
// ============================================================================

class Deduplicator {
    constructor() {
        this.seenUrls = new Set();
        this.seenIds = new Set();
    }

    isSeen(job) {
        const url = job.url?.trim().toLowerCase();
        const id = job.id || job.job_id;

        if (url && this.seenUrls.has(url)) return true;
        if (id && this.seenIds.has(id)) return true;
        return false;
    }

    markSeen(job) {
        const url = job.url?.trim().toLowerCase();
        const id = job.id || job.job_id;

        if (url) this.seenUrls.add(url);
        if (id) this.seenIds.add(id);
    }

    getCount() {
        return this.seenUrls.size;
    }
}

// ============================================================================
// JOB NORMALIZATION
// ============================================================================

const normalizeJob = (job, { source, url }) => {
    if (!job) return null;

    const title = job.title || job.job_title || job.name || job.headline;
    const company = job.company ||
        job.company_name ||
        job.companyName ||
        job.hiringOrganization?.name ||
        job.hiring_organization ||
        job.employer;
    const jobUrl = job.url || job.jobUrl || job.job_url || job.link || url;

    if (!title || !jobUrl) return null;

    // Extract location
    let location = 'Not specified';
    if (job.location) {
        if (typeof job.location === 'string') {
            location = job.location;
        } else if (job.location.address) {
            const addr = job.location.address;
            const parts = [addr.addressLocality, addr.addressRegion, addr.addressCountry].filter(Boolean);
            location = parts.join(', ');
        } else if (job.location.name) {
            location = job.location.name;
        }
    } else if (job.job_location) {
        location = job.job_location;
    }

    // Extract salary
    let salary = 'Not specified';
    if (job.salary) {
        salary = job.salary;
    } else if (job.baseSalary) {
        if (typeof job.baseSalary === 'string') {
            salary = job.baseSalary;
        } else if (job.baseSalary.value) {
            const val = job.baseSalary.value;
            if (val.minValue || val.maxValue) {
                const curr = val.currency || 'USD';
                const min = val.minValue ? `${curr}${val.minValue}` : '';
                const max = val.maxValue ? `${curr}${val.maxValue}` : '';
                salary = [min, max].filter(Boolean).join(' - ');
            }
        }
    } else if (job.estimatedSalary) {
        salary = job.estimatedSalary;
    }

    // Extract job type
    let jobType = 'Not specified';
    if (job.employmentType) {
        jobType = Array.isArray(job.employmentType) ? job.employmentType[0] : job.employmentType;
    } else if (job.job_type) {
        jobType = job.job_type;
    }

    const datePosted = job.datePosted || job.posted_date || job.postedDate || job.date_posted || 'Not specified';
    const descriptionRaw = job.description || job.job_description || job.jobDescription || '';

    return {
        title: cleanText(title),
        company: cleanText(company) || 'Not specified',
        location: cleanText(location),
        date_posted: cleanText(datePosted),
        salary: cleanText(salary),
        job_type: cleanText(jobType),
        description_html: descriptionRaw,
        description_text: cleanText(descriptionRaw),
        url: jobUrl.trim(),
        scraped_at: new Date().toISOString(),
        source,
        raw: source.includes('api') ? job : undefined,
    };
};

const extractJsonLdFromHtml = (html) => {
    const jobs = [];
    const scriptRegex = /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
    let match;

    while ((match = scriptRegex.exec(html)) !== null) {
        try {
            const json = JSON.parse(match[1]);
            const inspect = (node) => {
                if (!node) return;
                if (Array.isArray(node)) return node.forEach(inspect);
                if (node['@type'] === 'JobPosting') jobs.push(node);
                if (node['@graph']) inspect(node['@graph']);
            };
            inspect(json);
        } catch {
            // Ignore parse errors
        }
    }
    return jobs;
};

// ============================================================================
// URL BUILDING
// ============================================================================

const buildSearchUrl = ({ keyword, location, posted_date, radius, page_number = 1 }) => {
    const baseUrl = 'https://www.careerbuilder.com/jobs';
    const url = new URL(baseUrl);

    if (keyword?.trim()) url.searchParams.set('keywords', keyword.trim());
    if (location?.trim()) url.searchParams.set('location', location.trim());

    if (posted_date && posted_date !== 'anytime') {
        const POSTED_DATE_MAP = { '24h': '1', '7d': '7', '30d': '30' };
        if (POSTED_DATE_MAP[posted_date]) {
            url.searchParams.set('posted', POSTED_DATE_MAP[posted_date]);
        }
    }

    if (radius) url.searchParams.set('radius', String(radius));
    if (page_number > 1) url.searchParams.set('page_number', String(page_number));
    url.searchParams.set('cb_apply', 'false');

    return url.toString();
};

const buildNextPageUrl = (currentUrl) => {
    try {
        const url = new URL(currentUrl);
        const currentPage = Number(url.searchParams.get('page_number') || url.searchParams.get('page') || 1);
        url.searchParams.set('page_number', String(currentPage + 1));
        return url.toString();
    } catch {
        return null;
    }
};

const extractJobLinksFromHtml = ($) => {
    const links = [];
    $('a[href*="/job/"]').each((_, el) => {
        const href = $(el).attr('href');
        if (href && href.includes('/job/')) {
            const fullUrl = href.startsWith('http') ? href : `https://www.careerbuilder.com${href}`;
            links.push(fullUrl);
        }
    });
    return [...new Set(links)];
};

// ============================================================================
// TIER 1: API EXTRACTION (GraphQL/REST)
// ============================================================================

const tryGraphQLAPI = async ({ keyword, location, posted_date, radius, page = 1, proxyUrl, log }) => {
    try {
        log.info('[API] Attempting GraphQL extraction...');

        const graphqlQuery = {
            operationName: 'JobSearch',
            variables: {
                keywords: keyword || '',
                location: location || '',
                radius: radius || 50,
                posted: posted_date === '24h' ? 1 : posted_date === '7d' ? 7 : posted_date === '30d' ? 30 : null,
                pageNumber: page,
                pageSize: 25
            },
            query: `query JobSearch($keywords: String, $location: String, $radius: Int, $posted: Int, $pageNumber: Int, $pageSize: Int) {
                jobSearch(keywords: $keywords, location: $location, radius: $radius, posted: $posted, pageNumber: $pageNumber, pageSize: $pageSize) {
                    jobs { id title company location description datePosted employmentType salary url }
                    totalResults pageNumber hasNextPage
                }
            }`
        };

        const response = await gotScraping({
            url: 'https://www.careerbuilder.com/graphql',
            method: 'POST',
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Accept-Language': 'en-US,en;q=0.9',
                'Origin': 'https://www.careerbuilder.com',
                'Referer': 'https://www.careerbuilder.com/jobs',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
            },
            json: graphqlQuery,
            responseType: 'json',
            proxyUrl,
            timeout: 30000,
            retry: { limit: 2 }
        });

        if (response.body?.data?.jobSearch?.jobs) {
            const rawJobs = response.body.data.jobSearch.jobs;
            const jobs = rawJobs.map(job => normalizeJob(job, { source: 'api-graphql', url: job.url })).filter(Boolean);
            const hasNextPage = response.body.data.jobSearch.hasNextPage;

            log.info(`[API] GraphQL success: ${jobs.length} jobs`);
            return { success: true, jobs, nextPage: hasNextPage ? page + 1 : null };
        }

        log.warning('[API] GraphQL returned no jobs');
        return { success: false, jobs: [], nextPage: null };

    } catch (error) {
        log.warning(`[API] GraphQL failed: ${error.message}`);
        return { success: false, jobs: [], nextPage: null };
    }
};

const extractViaAPI = async (params) => {
    const { log } = params;
    await sleep(randomBetween(1000, 3000));

    const graphQLResult = await tryGraphQLAPI(params);
    if (graphQLResult.success && graphQLResult.jobs.length > 0) {
        return graphQLResult;
    }

    log.info('[API] All API methods failed');
    return { success: false, jobs: [], nextPage: null };
};

// ============================================================================
// TIER 2: HTML EXTRACTION (got-scraping + Cheerio)
// ============================================================================

const extractListingPage = async ({ url, proxyUrl, log }) => {
    try {
        log.info(`[HTML] Fetching listing page: ${url}`);

        const response = await gotScraping({
            url,
            headers: {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'DNT': '1',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0'
            },
            proxyUrl,
            timeout: 30000,
            retry: { limit: 2 }
        });

        const html = response.body;

        if (response.statusCode === 403 || html.includes('regional_sites') || html.includes('MCB Bermuda')) {
            log.warning('[HTML] Geo-blocked or 403 detected');
            return { success: false, jobs: [], jobLinks: [], nextPageUrl: null, blocked: true };
        }

        const jsonLdJobs = extractJsonLdFromHtml(html);
        const jobs = jsonLdJobs.map(job => normalizeJob(job, { source: 'html-json-ld-list', url })).filter(Boolean);

        const $ = cheerio.load(html);
        const jobLinks = extractJobLinksFromHtml($);

        let nextPageUrl = null;
        const nextLink = $('a[aria-label*="Next"], a.next, a[rel="next"]').first();
        if (nextLink.length) {
            const href = nextLink.attr('href');
            nextPageUrl = href?.startsWith('http') ? href : `https://www.careerbuilder.com${href}`;
        } else {
            nextPageUrl = buildNextPageUrl(url);
        }

        log.info(`[HTML] Extracted: ${jobs.length} jobs, ${jobLinks.length} links`);

        return { success: true, jobs, jobLinks, nextPageUrl };

    } catch (error) {
        log.warning(`[HTML] Failed: ${error.message}`);
        return { success: false, jobs: [], jobLinks: [], nextPageUrl: null };
    }
};

const extractViaHTML = async ({ url, proxyUrl, log }) => {
    await sleep(randomBetween(1500, 3500));

    const listingResult = await extractListingPage({ url, proxyUrl, log });

    if (listingResult.blocked) {
        return { success: false, jobs: [], nextPageUrl: null, blocked: true };
    }

    return {
        success: listingResult.success,
        jobs: listingResult.jobs,
        nextPageUrl: listingResult.nextPageUrl
    };
};

// ============================================================================
// TIER 3: BROWSER EXTRACTION (Playwright + Camoufox Stealth)
// ============================================================================

const extractJsonLdJobs = async (page) => {
    const payloads = await page.$$eval('script[type="application/ld+json"]', (nodes) =>
        nodes.map((n) => n.textContent).filter(Boolean)
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
            // Ignore
        }
    }
    return jobs;
};

const performHumanInteractions = async (page) => {
    try {
        const viewport = await page.viewportSize();
        const mouseX = randomBetween(100, viewport.width - 100);
        const mouseY = randomBetween(100, viewport.height - 100);

        await page.mouse.move(mouseX, mouseY, { steps: randomBetween(15, 25) });
        await page.evaluate(() => {
            const distance = Math.floor(document.body.scrollHeight * (0.2 + Math.random() * 0.3));
            window.scrollBy({ top: distance, behavior: 'smooth' });
        });
        await sleep(randomBetween(1500, 3000));
    } catch {
        // Ignore
    }
};

const extractViaBrowser = async ({ url, proxyConf, cookieHeader, log }) => {
    let crawler;

    try {
        log.info('[Browser] Starting Camoufox stealth extraction...');

        crawler = new PlaywrightCrawler({
            maxConcurrency: 1,
            maxRequestsPerCrawl: 10,
            proxyConfiguration: proxyConf, // FIXED: Proxy here, not in launchOptions
            requestHandlerTimeoutSecs: 90,
            maxRequestRetries: 3,

            launchContext: {
                launcher: firefox,
                launchOptions: await camoufoxLaunchOptions({
                    headless: false, // Full browser for maximum stealth
                    config: {
                        platform: 'Win32',
                        locale: 'en-US',
                        screen: { width: 1920, height: 1080 },
                        viewport: { width: 1366, height: 768 },
                        webgl: {
                            vendor: 'Mozilla',
                            renderer: 'Mozilla -- Intel(R) UHD Graphics'
                        },
                        fonts: ['Arial', 'Helvetica', 'Times New Roman', 'Courier New'],
                        geoip: true
                    },
                    args: [
                        '--disable-blink-features=AutomationControlled',
                        '--no-sandbox',
                        '--disable-dev-shm-usage'
                    ],
                    firefoxUserPrefs: {
                        'layout.css.prefers-reduced-motion': 1,
                        'dom.webdriver.enabled': false,
                        'media.navigator.enabled': false,
                        'privacy.trackingprotection.enabled': true
                    }
                    // NO proxy here - moved to proxyConfiguration
                }),
            },

            preNavigationHooks: [
                async (crawlingContext, gotoOptions) => {
                    const { page, session } = crawlingContext;

                    // Block ads/trackers
                    await page.route('**/*', (route) => {
                        const url = route.request().url();
                        if (url.includes('ads') || url.includes('tracker') ||
                            url.includes('analytics') || url.includes('doubleclick')) {
                            route.abort();
                        } else {
                            route.continue();
                        }
                    });

                    // Stealth headers
                    await page.setExtraHTTPHeaders({
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'DNT': '1',
                        'Upgrade-Insecure-Requests': '1'
                    });

                    // Set cookies
                    if (cookieHeader && !session.userData?.cookiesSet) {
                        try {
                            const cookies = cookieHeader.split(';').map((p) => p.trim()).filter(Boolean).map((p) => {
                                const idx = p.indexOf('=');
                                if (idx <= 0) return null;
                                return {
                                    name: p.slice(0, idx).trim(),
                                    value: p.slice(idx + 1).trim(),
                                    url: 'https://www.careerbuilder.com/'
                                };
                            }).filter(Boolean);

                            await page.context().addCookies(cookies);
                            session.userData = { ...(session.userData || {}), cookiesSet: true };
                            log.debug('[Browser] Cookies set');
                        } catch (e) {
                            log.warning(`[Browser] Cookie error: ${e.message}`);
                        }
                    }

                    // Randomize viewport
                    gotoOptions.viewport = {
                        width: 1366 + randomBetween(0, 100),
                        height: 768 + randomBetween(0, 50)
                    };

                    await sleep(randomBetween(2000, 4000));
                    gotoOptions.waitUntil = 'domcontentloaded';
                }
            ]
        });

        const result = { success: false, jobs: [], nextPageUrl: null };

        await crawler.run([{ url, userData: { type: 'listing' } }], {
            requestHandler: async ({ page, request }) => {
                await page.waitForLoadState('domcontentloaded', { timeout: 60000 });

                const content = await page.content();
                if (content.includes('regional_sites') || content.includes('MCB Bermuda')) {
                    log.warning('[Browser] Geo-blocked even with proxy');
                    result.blocked = true;
                    return;
                }

                await performHumanInteractions(page);

                const jsonLdJobs = await extractJsonLdJobs(page);
                result.jobs = jsonLdJobs.map(job => normalizeJob(job, { source: 'browser-json-ld', url: request.url })).filter(Boolean);

                const nextHref = await page.$$eval(
                    'a[aria-label*="Next"], a.next, a[rel="next"]',
                    (as) => {
                        for (const a of as) {
                            const text = (a.textContent || '').trim().toLowerCase();
                            if (text === 'next' || text === '»' || text === '›') return a.href;
                        }
                        return null;
                    }
                );

                result.nextPageUrl = nextHref || buildNextPageUrl(request.url);
                result.success = true;

                log.info(`[Browser] Extracted ${result.jobs.length} jobs`);
            }
        });

        return result;

    } catch (error) {
        log.error(`[Browser] Failed: ${error.message}`);
        return { success: false, jobs: [], nextPageUrl: null };
    } finally {
        if (crawler) {
            await crawler.teardown();
        }
    }
};

// ============================================================================
// MAIN SCRAPER ORCHESTRATION
// ============================================================================

const parseCookies = (cookiesJson) => {
    const parsed = parseJSON(cookiesJson);
    if (!parsed) return '';

    const parts = [];
    if (Array.isArray(parsed)) {
        for (const item of parsed) {
            if (typeof item === 'string') {
                parts.push(item.trim());
            } else if (item?.name) {
                parts.push(`${item.name}=${item.value ?? ''}`);
            }
        }
    } else if (typeof parsed === 'object') {
        for (const [k, v] of Object.entries(parsed)) {
            parts.push(`${k}=${v ?? ''}`);
        }
    }

    return parts.join('; ');
};

(async () => {
    await Actor.init();

    try {
        const input = await Actor.getInput();
        const log = Actor.log;

        // Log raw input for debugging
        log.info('📥 Raw input received:', JSON.stringify(input));

        const {
            startUrls = 'https://www.careerbuilder.com/jobs',
            keyword = '',
            location = '',
            posted_date = 'anytime',
            radius = 50,
            results_wanted = 20,
            max_pages = 10,
            cookiesJson,
            proxyConfiguration,
            extractionMethod = 'auto'
        } = input || {};

        log.info('🚀 CareerBuilder Scraper - 3-Tier Extraction with Camoufox Stealth');
        log.info(`📊 Target: ${results_wanted} jobs, Max pages: ${max_pages}`);
        log.info(`🔧 Extraction method: ${extractionMethod}`);

        // Build URLs
        let urlsToCrawl = [];
        if (keyword.trim() || location.trim()) {
            const searchUrl = buildSearchUrl({ keyword, location, posted_date, radius });
            urlsToCrawl = [searchUrl];
            log.info(`🔍 Search: ${searchUrl}`);
        } else {
            // Handle both string and array inputs
            if (typeof startUrls === 'string') {
                urlsToCrawl = startUrls.split('\n').map(u => u.trim()).filter(Boolean);
            } else if (Array.isArray(startUrls)) {
                urlsToCrawl = startUrls.map(item => {
                    return typeof item === 'string' ? item : (item.url || '');
                }).filter(Boolean);
            } else {
                urlsToCrawl = ['https://www.careerbuilder.com/jobs'];
            }
            log.info(`📋 URLs: ${urlsToCrawl.length} - ${urlsToCrawl[0]}`);
        }

        if (urlsToCrawl.length === 0) {
            throw new Error('No URLs to crawl! Please provide startUrls or keyword+location.');
        }

        // Create USA residential proxy (REQUIRED for geo-blocking)
        const proxyConf = proxyConfiguration
            ? await Actor.createProxyConfiguration(proxyConfiguration)
            : await Actor.createProxyConfiguration({
                groups: ['RESIDENTIAL'],
                countryCode: 'US'
            });

        log.info('✅ Proxy: RESIDENTIAL (USA)');

        const cookieHeader = parseCookies(cookiesJson);
        if (cookieHeader) log.info('🍪 Cookies configured');

        const dedup = new Deduplicator();
        let totalSaved = 0;
        let currentPage = 1;

        for (const initialUrl of urlsToCrawl) {
            let currentUrl = initialUrl;

            while (currentUrl && currentPage <= max_pages && totalSaved < results_wanted) {
                log.info(`\n📄 Page ${currentPage}: ${currentUrl}`);

                const proxyUrl = await proxyConf.newUrl();
                let extractionResult = { success: false, jobs: [], nextPageUrl: null };

                // TIER 1: API
                if (extractionMethod === 'auto' || extractionMethod === 'api') {
                    log.info('🔹 Tier 1: API');
                    extractionResult = await extractViaAPI({
                        keyword, location, posted_date, radius,
                        page: currentPage,
                        proxyUrl,
                        log
                    });

                    if (extractionResult.success && extractionResult.jobs.length > 0) {
                        log.info(`✅ API: ${extractionResult.jobs.length} jobs`);
                    } else if (extractionMethod === 'api') {
                        log.warning('❌ API failed (API-only mode)');
                        break;
                    }
                }

                // TIER 2: HTML
                if (!extractionResult.success && (extractionMethod === 'auto' || extractionMethod === 'html')) {
                    log.info('🔹 Tier 2: HTML');
                    extractionResult = await extractViaHTML({
                        url: currentUrl,
                        proxyUrl,
                        log
                    });

                    if (extractionResult.success && extractionResult.jobs.length > 0) {
                        log.info(`✅ HTML: ${extractionResult.jobs.length} jobs`);
                    } else if (extractionResult.blocked) {
                        log.warning('🚫 HTML blocked - escalating to browser');
                    } else if (extractionMethod === 'html') {
                        log.warning('❌ HTML failed (HTML-only mode)');
                        break;
                    }
                }

                // TIER 3: BROWSER (Camoufox Stealth)
                if (!extractionResult.success && (extractionMethod === 'auto' || extractionMethod === 'browser')) {
                    log.info('🔹 Tier 3: Browser (Camoufox)');
                    extractionResult = await extractViaBrowser({
                        url: currentUrl,
                        proxyConf,
                        cookieHeader,
                        log
                    });

                    if (extractionResult.success && extractionResult.jobs.length > 0) {
                        log.info(`✅ Browser: ${extractionResult.jobs.length} jobs`);
                    } else if (extractionResult.blocked) {
                        log.error('🚫 Blocked even with browser - check USA proxy');
                        break;
                    } else {
                        log.warning('❌ Browser failed');
                        break;
                    }
                }

                if (extractionResult.jobs.length === 0) {
                    log.warning('⚠️ No jobs extracted');
                    break;
                }

                // Save jobs
                for (const job of extractionResult.jobs) {
                    if (totalSaved >= results_wanted) {
                        log.info(`🎯 Target reached: ${results_wanted} jobs`);
                        break;
                    }

                    if (dedup.isSeen(job)) {
                        log.debug(`⏭️ Duplicate: ${job.url}`);
                        continue;
                    }

                    await Actor.pushData(job);
                    dedup.markSeen(job);
                    totalSaved++;

                    log.info(`💾 [${totalSaved}/${results_wanted}]: ${job.title} @ ${job.company}`);
                }

                if (totalSaved >= results_wanted) {
                    log.info(`✅ Complete: ${totalSaved} jobs`);
                    break;
                }

                if (extractionResult.nextPageUrl) {
                    currentUrl = extractionResult.nextPageUrl;
                    currentPage++;
                } else {
                    log.info('📍 No more pages');
                    break;
                }
            }

            if (totalSaved >= results_wanted) break;
        }

        log.info('\n' + '='.repeat(60));
        log.info(`✅ Scraping complete!`);
        log.info(`📊 Total: ${totalSaved} jobs`);
        log.info(`📄 Pages: ${currentPage}`);
        log.info('='.repeat(60));

        if (totalSaved === 0) {
            log.warning('⚠️ No jobs extracted. Check:');
            log.warning('   1. USA RESIDENTIAL proxy enabled');
            log.warning('   2. Keywords/location valid');
            log.warning('   3. Site structure may have changed');
        }

    } catch (error) {
        Actor.log.error(`❌ Error: ${error.message}`);
        Actor.log.error(error.stack);
        throw error;
    } finally {
        await Actor.exit();
    }
})();
