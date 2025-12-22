// CareerBuilder Jobs Scraper - Production Grade (AGENTS.md Compliant)
// Follows all Apify best practices for stealth and reliability

import { Actor, log } from 'apify';  // Use apify/log for secure logging
import { PlaywrightCrawler, Dataset, createPlaywrightRouter } from 'crawlee';
import { firefox } from 'playwright';
import * as cheerio from 'cheerio';

// ============================================================================
// ROUTER PATTERN (AGENTS.md: use router pattern for complex crawls)
// ============================================================================

const router = createPlaywrightRouter();

// Default handler for search/listing pages
router.addDefaultHandler(async ({ page, request, enqueueLinks, crawler }) => {
    const { maxJobs, currentJobCount, seenUrls, maxPages, currentPage } = crawler.userData;

    log.info(`📄 Processing listing page: ${request.url}`);

    // Check limits
    if (currentJobCount.value >= maxJobs) {
        log.info(`🎯 Target reached: ${currentJobCount.value} jobs`);
        return;
    }

    if (currentPage.value > maxPages) {
        log.info(`📄 Max pages reached: ${currentPage.value - 1}`);
        return;
    }

    try {
        // Wait for job results to load
        await page.waitForSelector('.data-results-content-parent, .job-listing, [data-job-did]', {
            timeout: 30000
        });
    } catch (e) {
        log.warning('⏳ Timeout waiting for results, checking page content...');
    }

    const content = await page.content();

    // Check for blocking
    if (content.includes('regional_sites') || content.includes('MCB Bermuda') || content.includes('Access Denied')) {
        log.error('🚫 BLOCKED! Need USA RESIDENTIAL proxy.');
        return;
    }

    currentPage.value++;
    log.info(`✅ Page loaded (Page ${currentPage.value})`);

    // Parse with Cheerio (AGENTS.md: use cheerio for static content)
    const $ = cheerio.load(content);

    // Extract JSON-LD (Primary strategy)
    const jobs = [];
    $('script[type="application/ld+json"]').each((_, el) => {
        try {
            const json = JSON.parse($(el).html());
            const processJob = (item) => {
                if (item['@type'] === 'JobPosting') {
                    jobs.push(item);
                }
                if (item['@graph']) {
                    item['@graph'].forEach(processJob);
                }
            };
            if (Array.isArray(json)) {
                json.forEach(processJob);
            } else {
                processJob(json);
            }
        } catch (e) {
            // Ignore parse errors
        }
    });

    log.info(`📊 Found ${jobs.length} jobs via JSON-LD`);

    // Save jobs (AGENTS.md: clean and validate data before pushing)
    for (const job of jobs) {
        if (currentJobCount.value >= maxJobs) break;

        const jobUrl = job.url || request.url;
        if (seenUrls.has(jobUrl)) continue;
        seenUrls.add(jobUrl);

        // Clean and validate data
        const jobData = cleanJobData({
            id: job.identifier?.value || job['@id'] || `cb-${Date.now()}-${currentJobCount.value}`,
            title: job.title,
            company: job.hiringOrganization?.name,
            location: formatLocation(job.jobLocation),
            type: Array.isArray(job.employmentType) ? job.employmentType[0] : job.employmentType,
            postedAt: job.datePosted,
            salary: formatSalary(job.baseSalary),
            description: job.description,
            url: jobUrl,
            scrapedAt: new Date().toISOString(),
        });

        await Dataset.pushData(jobData);
        currentJobCount.value++;
        log.info(`💾 [${currentJobCount.value}/${maxJobs}] ${jobData.title} @ ${jobData.company}`);
    }

    // CSS Fallback Strategy (AGENTS.md: use semantic CSS selectors and fallback strategies)
    if (jobs.length === 0) {
        log.info('🔄 No JSON-LD, trying CSS selectors...');

        $('.data-results-content-parent .block, [data-job-did], .job-listing-item').each((_, el) => {
            if (currentJobCount.value >= maxJobs) return false;

            const $el = $(el);
            const title = $el.find('.data-results-title, .job-title, a[data-gtm="job-title"]').text().trim();
            const company = $el.find('.data-details span:first-child, .company-name').text().trim();
            const location = $el.find('.data-details span:nth-child(2), .job-location').text().trim();
            const link = $el.find('a.data-results-content, a[data-gtm="job-title"]').attr('href');

            if (title && link) {
                const fullUrl = link.startsWith('http') ? link : `https://www.careerbuilder.com${link}`;
                if (seenUrls.has(fullUrl)) return;
                seenUrls.add(fullUrl);

                const jobData = cleanJobData({
                    id: $el.attr('data-job-did') || `cb-css-${Date.now()}-${currentJobCount.value}`,
                    title,
                    company,
                    location,
                    type: null,
                    postedAt: null,
                    salary: null,
                    description: '',
                    url: fullUrl,
                    scrapedAt: new Date().toISOString(),
                });

                Dataset.pushData(jobData);
                currentJobCount.value++;
                log.info(`💾 [${currentJobCount.value}/${maxJobs}] ${jobData.title} @ ${jobData.company}`);
            }
        });
    }

    // Pagination: Add next page if we need more
    if (currentJobCount.value < maxJobs && currentPage.value <= maxPages) {
        const currentUrl = new URL(request.url);
        const pageNum = parseInt(currentUrl.searchParams.get('page_number') || '1', 10);
        currentUrl.searchParams.set('page_number', String(pageNum + 1));
        const nextUrl = currentUrl.toString();

        log.info(`📄 Enqueueing next page: ${nextUrl}`);
        await crawler.addRequests([{ url: nextUrl }]);
    }
});

// ============================================================================
// HELPER FUNCTIONS (AGENTS.md: clean and validate data)
// ============================================================================

function cleanJobData(job) {
    return {
        id: job.id || 'unknown',
        title: cleanString(job.title) || 'Unknown',
        company: cleanString(job.company) || 'Unknown',
        location: cleanString(job.location) || 'Not specified',
        type: cleanString(job.type) || 'Not specified',
        postedAt: job.postedAt || null,
        salary: job.salary || 'Not specified',
        description: cleanString(job.description) || '',
        url: job.url,
        scrapedAt: job.scrapedAt,
    };
}

function cleanString(str) {
    if (!str) return null;
    return String(str).replace(/\s+/g, ' ').trim();
}

function formatLocation(jobLocation) {
    if (!jobLocation) return null;
    if (typeof jobLocation === 'string') return jobLocation;

    const addr = jobLocation.address;
    if (addr) {
        const parts = [addr.addressLocality, addr.addressRegion, addr.addressCountry].filter(Boolean);
        return parts.join(', ') || null;
    }

    if (jobLocation.name) return jobLocation.name;
    return null;
}

function formatSalary(baseSalary) {
    if (!baseSalary) return null;
    if (typeof baseSalary === 'string') return baseSalary;

    const value = baseSalary.value;
    if (!value) return null;

    const currency = value.currency || 'USD';
    const min = value.minValue;
    const max = value.maxValue;

    if (min && max) {
        return `${currency} ${min.toLocaleString()} - ${max.toLocaleString()}`;
    } else if (min || max) {
        return `${currency} ${(min || max).toLocaleString()}`;
    }

    return null;
}

function parseCookiesJson(cookiesJson) {
    if (!cookiesJson) return [];

    try {
        const parsed = typeof cookiesJson === 'string' ? JSON.parse(cookiesJson) : cookiesJson;

        if (Array.isArray(parsed)) {
            return parsed.map(cookie => ({
                name: cookie.name,
                value: cookie.value,
                domain: cookie.domain || '.careerbuilder.com',
                path: cookie.path || '/',
            })).filter(c => c.name && c.value);
        }

        // Object format: { name: value, ... }
        return Object.entries(parsed).map(([name, value]) => ({
            name,
            value: String(value),
            domain: '.careerbuilder.com',
            path: '/',
        }));
    } catch (e) {
        log.warning(`Failed to parse cookies: ${e.message}`);
        return [];
    }
}

// ============================================================================
// MAIN ACTOR (AGENTS.md: validate input early, fail gracefully)
// ============================================================================

await Actor.init();

log.info('🚀 CareerBuilder Scraper Starting (AGENTS.md Compliant)');

try {
    // Get and validate input (AGENTS.md: validate input early with proper error handling)
    const input = await Actor.getInput() ?? {};
    log.info('📥 Input received', { ...input, cookiesJson: input.cookiesJson ? '[REDACTED]' : undefined });

    const {
        startUrls = '',
        keyword = '',
        location = '',
        results_wanted = 20,
        max_pages = 10,
        cookiesJson = null,
        proxyConfiguration = { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'], countryCode: 'US' }
    } = input;

    // Input validation (AGENTS.md: validate input early)
    if (!startUrls && !keyword) {
        log.warning('⚠️ No startUrls or keyword provided, using default test search');
    }

    // Build start URL
    let startUrl = 'https://www.careerbuilder.com/jobs';

    if (startUrls && startUrls !== 'https://www.careerbuilder.com/jobs') {
        startUrl = typeof startUrls === 'string' ? startUrls.trim() : startUrls[0]?.url || startUrls[0] || startUrl;
        log.info(`📋 Using provided URL: ${startUrl}`);
    } else if (keyword || location) {
        const url = new URL('https://www.careerbuilder.com/jobs');
        if (keyword) url.searchParams.set('keywords', keyword);
        if (location) url.searchParams.set('location', location);
        url.searchParams.set('cb_apply', 'false');
        startUrl = url.toString();
        log.info(`🔍 Built search URL: ${startUrl}`);
    } else {
        const url = new URL('https://www.careerbuilder.com/jobs');
        url.searchParams.set('keywords', 'admin');
        url.searchParams.set('cb_apply', 'false');
        startUrl = url.toString();
        log.info(`⚠️ Using default test: ${startUrl}`);
    }

    // Parse cookies
    const cookies = parseCookiesJson(cookiesJson);
    if (cookies.length > 0) {
        log.info(`🍪 Loaded ${cookies.length} cookies from input`);
    }

    // Create proxy configuration
    log.info('🔧 Configuring proxy...');
    let proxyConfig;
    try {
        proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);
        log.info('✅ Proxy configured: RESIDENTIAL USA');
    } catch (e) {
        log.warning(`⚠️ Proxy config failed: ${e.message}`);
        try {
            proxyConfig = await Actor.createProxyConfiguration();
            log.info('✅ Default proxy configured');
        } catch (e2) {
            log.warning('❌ No proxy available, running without proxy');
            proxyConfig = undefined;
        }
    }

    // Shared state for tracking
    const userData = {
        maxJobs: results_wanted,
        maxPages: max_pages,
        currentJobCount: { value: 0 },
        currentPage: { value: 0 },
        seenUrls: new Set(),
    };

    // Create PlaywrightCrawler (AGENTS.md: proper concurrency settings Browser: 1-5)
    log.info('🚀 Initializing PlaywrightCrawler with Firefox stealth...');

    const crawler = new PlaywrightCrawler({
        proxyConfiguration: proxyConfig,
        requestHandler: router,
        maxConcurrency: 2,  // AGENTS.md: Browser: 1-5
        maxRequestRetries: 3,  // AGENTS.md: implement retry strategies
        requestHandlerTimeoutSecs: 120,
        navigationTimeoutSecs: 60,

        // Firefox for stealth (fingerprinting)
        browserPoolOptions: {
            useFingerprints: true,
            fingerprintOptions: {
                fingerprintGeneratorOptions: {
                    browsers: [{ name: 'firefox', minVersion: 115 }],
                    devices: ['desktop'],
                    operatingSystems: ['windows'],
                },
            },
        },

        launchContext: {
            launcher: firefox,
            launchOptions: {
                headless: true,
                args: ['--disable-blink-features=AutomationControlled'],
                firefoxUserPrefs: {
                    'privacy.resistFingerprinting': false,
                    'media.navigator.enabled': false,
                    'dom.webdriver.enabled': false,
                },
            },
        },

        // AGENTS.md: use preNavigationHooks instead of additionalHttpHeaders
        preNavigationHooks: [
            async ({ page, request }, gotoOptions) => {
                // Set cookies from input
                if (cookies.length > 0) {
                    try {
                        await page.context().addCookies(cookies);
                        log.debug('🍪 Cookies injected');
                    } catch (e) {
                        log.warning(`Cookie injection failed: ${e.message}`);
                    }
                }

                // Set stealth headers via page
                await page.setExtraHTTPHeaders({
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'DNT': '1',
                    'Upgrade-Insecure-Requests': '1',
                });

                // Random delay for human-like behavior
                const delay = Math.floor(Math.random() * 2000) + 1000;
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        ],

        failedRequestHandler: async ({ request }, error) => {
            log.error(`❌ Request failed: ${request.url}`, { error: error.message });
        },
    });

    // Attach shared state
    crawler.userData = userData;

    // Run the crawler
    log.info(`🚀 Starting crawl from: ${startUrl}`);
    await crawler.run([{ url: startUrl }]);

    // Final summary
    log.info('='.repeat(50));
    log.info(`✅ Scraping complete!`);
    log.info(`📊 Jobs collected: ${userData.currentJobCount.value}`);
    log.info(`📄 Pages processed: ${userData.currentPage.value}`);
    log.info('='.repeat(50));

    if (userData.currentJobCount.value === 0) {
        log.warning('⚠️ No jobs found. Possible causes:');
        log.warning('   1. Geo-blocked (need USA RESIDENTIAL proxy)');
        log.warning('   2. Invalid search parameters');
        log.warning('   3. Site structure changed');
        log.warning('   4. Try providing cookies in cookiesJson input');
    }

} catch (error) {
    log.error(`❌ Fatal error: ${error.message}`, { stack: error.stack });
}

await Actor.exit();
