// CareerBuilder Jobs Scraper - Production Grade with Cloudflare Handling
// AGENTS.md Compliant + Enhanced Block Detection

import { Actor, log } from 'apify';
import { PlaywrightCrawler, Dataset, createPlaywrightRouter } from 'crawlee';
import { firefox } from 'playwright';
import * as cheerio from 'cheerio';

// ============================================================================
// ROUTER PATTERN
// ============================================================================

const router = createPlaywrightRouter();

router.addDefaultHandler(async ({ page, request, crawler }) => {
    const { maxJobs, currentJobCount, seenUrls, maxPages, currentPage } = crawler.userData;

    log.info(`📄 Processing: ${request.url}`);

    if (currentJobCount.value >= maxJobs) {
        log.info(`🎯 Target reached: ${currentJobCount.value} jobs`);
        return;
    }

    if (currentPage.value >= maxPages) {
        log.info(`📄 Max pages reached: ${currentPage.value}`);
        return;
    }

    // Wait for page to fully load (Cloudflare passes after JS execution)
    log.info('⏳ Waiting for page to load...');

    try {
        // First wait for document to be ready
        await page.waitForLoadState('networkidle', { timeout: 45000 });
    } catch (e) {
        log.warning('Network not idle after 45s, continuing anyway...');
    }

    // Check for Cloudflare challenge and wait for it to pass
    let attempts = 0;
    const maxAttempts = 3;

    while (attempts < maxAttempts) {
        const content = await page.content();
        const lowerContent = content.toLowerCase();

        // Check for Cloudflare challenge indicators
        const isCloudflareChallenge =
            lowerContent.includes('checking your browser') ||
            lowerContent.includes('please wait') ||
            lowerContent.includes('just a moment') ||
            lowerContent.includes('cf-browser-verification') ||
            lowerContent.includes('challenge-platform');

        if (isCloudflareChallenge) {
            attempts++;
            log.info(`☁️ Cloudflare challenge detected (attempt ${attempts}/${maxAttempts}), waiting...`);
            await new Promise(r => setTimeout(r, 10000)); // Wait 10s for challenge to pass
            continue;
        }

        // Check for geo-blocking
        const isGeoBlocked =
            content.includes('regional_sites') ||
            content.includes('MCB Bermuda') ||
            content.includes('not available in your region');

        if (isGeoBlocked) {
            log.error('🚫 GEO-BLOCKED: CareerBuilder not available in proxy region');

            // Save screenshot for debugging
            try {
                const screenshot = await page.screenshot({ fullPage: true });
                await Actor.setValue('BLOCKED_PAGE', screenshot, { contentType: 'image/png' });
                log.info('📸 Screenshot saved as BLOCKED_PAGE in key-value store');
            } catch (e) {
                log.warning('Could not save screenshot');
            }

            // Log first 500 chars for debugging
            log.info('Page content preview:', content.substring(0, 500));
            return;
        }

        // Check for 403/Access Denied
        const isAccessDenied =
            lowerContent.includes('access denied') ||
            lowerContent.includes('403 forbidden');

        if (isAccessDenied) {
            log.error('🚫 ACCESS DENIED (403)');
            return;
        }

        // If we get here, page should be good
        break;
    }

    // Check if we have job results
    const content = await page.content();
    const $ = cheerio.load(content);

    // Look for job containers
    const hasJobResults =
        $('.data-results-content-parent').length > 0 ||
        $('[data-job-did]').length > 0 ||
        $('script[type="application/ld+json"]').length > 0;

    if (!hasJobResults) {
        log.warning('⚠️ No job results found on page');
        log.info('Page title:', await page.title());

        // Save screenshot for debugging
        try {
            const screenshot = await page.screenshot({ fullPage: true });
            await Actor.setValue('NO_RESULTS_PAGE', screenshot, { contentType: 'image/png' });
            log.info('📸 Screenshot saved as NO_RESULTS_PAGE');
        } catch (e) {
            log.warning('Could not save screenshot');
        }

        // Log some content for debugging
        log.info('Body classes:', $('body').attr('class') || 'none');
        log.info('H1 text:', $('h1').first().text().trim() || 'none');
        return;
    }

    currentPage.value++;
    log.info(`✅ Page loaded successfully (Page ${currentPage.value})`);

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

    // Save jobs
    for (const job of jobs) {
        if (currentJobCount.value >= maxJobs) break;

        const jobUrl = job.url || request.url;
        if (seenUrls.has(jobUrl)) continue;
        seenUrls.add(jobUrl);

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

    // CSS Fallback if no JSON-LD
    if (jobs.length === 0) {
        log.info('🔄 Trying CSS selectors...');

        $('[data-job-did], .data-results-content-parent .block, .job-listing-item').each((_, el) => {
            if (currentJobCount.value >= maxJobs) return false;

            const $el = $(el);
            const title = $el.find('.data-results-title, .job-title, a[data-gtm="job-title"]').text().trim() ||
                $el.find('a').first().text().trim();
            const company = $el.find('.data-details span:first-child, .company-name').text().trim();
            const location = $el.find('.data-details span:nth-child(2), .job-location').text().trim();
            const link = $el.find('a.data-results-content, a[data-gtm="job-title"], a').first().attr('href');

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

    // Pagination
    if (currentJobCount.value < maxJobs && currentPage.value < maxPages) {
        const currentUrl = new URL(request.url);
        const pageNum = parseInt(currentUrl.searchParams.get('page_number') || '1', 10);
        currentUrl.searchParams.set('page_number', String(pageNum + 1));
        const nextUrl = currentUrl.toString();

        log.info(`📄 Enqueueing next page: ${nextUrl}`);
        await crawler.addRequests([{ url: nextUrl }]);
    }
});

// ============================================================================
// HELPER FUNCTIONS
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
// MAIN ACTOR
// ============================================================================

await Actor.init();

log.info('🚀 CareerBuilder Scraper v3.1 (Enhanced Cloudflare Handling)');

try {
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

    const cookies = parseCookiesJson(cookiesJson);
    if (cookies.length > 0) {
        log.info(`🍪 Loaded ${cookies.length} cookies from input`);
    }

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
            log.warning('❌ No proxy available');
            proxyConfig = undefined;
        }
    }

    const userData = {
        maxJobs: results_wanted,
        maxPages: max_pages,
        currentJobCount: { value: 0 },
        currentPage: { value: 0 },
        seenUrls: new Set(),
    };

    log.info('🚀 Initializing PlaywrightCrawler...');

    const crawler = new PlaywrightCrawler({
        proxyConfiguration: proxyConfig,
        requestHandler: router,
        maxConcurrency: 1,  // Single browser instance for stealth
        maxRequestRetries: 3,
        requestHandlerTimeoutSecs: 180,  // 3 minutes for slow pages
        navigationTimeoutSecs: 90,  // 1.5 minutes for navigation

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

        preNavigationHooks: [
            async ({ page }, gotoOptions) => {
                // Inject cookies
                if (cookies.length > 0) {
                    try {
                        await page.context().addCookies(cookies);
                        log.debug('🍪 Cookies injected');
                    } catch (e) {
                        log.warning(`Cookie injection failed: ${e.message}`);
                    }
                }

                // Set headers
                await page.setExtraHTTPHeaders({
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache',
                });

                // Human-like delay
                const delay = Math.floor(Math.random() * 3000) + 2000;
                await new Promise(resolve => setTimeout(resolve, delay));

                // Set longer timeout for navigation
                gotoOptions.timeout = 90000;
                gotoOptions.waitUntil = 'domcontentloaded';
            }
        ],

        failedRequestHandler: async ({ request }, error) => {
            log.error(`❌ Request failed: ${request.url}`, { error: error.message });
        },
    });

    crawler.userData = userData;

    log.info(`🚀 Starting crawl from: ${startUrl}`);
    await crawler.run([{ url: startUrl }]);

    log.info('='.repeat(50));
    log.info(`✅ Scraping complete!`);
    log.info(`📊 Jobs collected: ${userData.currentJobCount.value}`);
    log.info(`📄 Pages processed: ${userData.currentPage.value}`);
    log.info('='.repeat(50));

    if (userData.currentJobCount.value === 0) {
        log.warning('⚠️ No jobs found. Check BLOCKED_PAGE or NO_RESULTS_PAGE screenshots in Key-Value store.');
    }

} catch (error) {
    log.error(`❌ Fatal error: ${error.message}`, { stack: error.stack });
}

await Actor.exit();
