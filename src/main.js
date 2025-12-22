// CareerBuilder Scraper - Main (Apify Camoufox Template)
// Uses official Apify recommended Camoufox configuration

import { PlaywrightCrawler } from 'crawlee';
import { Actor, log } from 'apify';
import { launchOptions as camoufoxLaunchOptions } from 'camoufox-js';
import { firefox } from 'playwright';
import { router, setLimits, getStats } from './routes.js';

// Initialize Apify
await Actor.init();

log.info('🚀 CareerBuilder Scraper v4.0 (Official Camoufox Template)');

try {
    const input = await Actor.getInput() ?? {};
    log.info('📥 Input:', { ...input, cookiesJson: input.cookiesJson ? '[HIDDEN]' : undefined });

    const {
        startUrls = '',
        keyword = '',
        location = '',
        results_wanted = 20,
        max_pages = 10,
        cookiesJson = null,
        proxyConfiguration = { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'], countryCode: 'US' }
    } = input;

    // Set limits in router
    setLimits(results_wanted, max_pages);

    // Build start URL
    let startUrl = 'https://www.careerbuilder.com/jobs';
    if (startUrls && startUrls !== 'https://www.careerbuilder.com/jobs') {
        startUrl = typeof startUrls === 'string' ? startUrls.trim() : startUrls[0]?.url || startUrls[0] || startUrl;
        log.info(`📋 URL: ${startUrl}`);
    } else if (keyword || location) {
        const url = new URL(startUrl);
        if (keyword) url.searchParams.set('keywords', keyword);
        if (location) url.searchParams.set('location', location);
        startUrl = url.toString();
        log.info(`🔍 Search: ${startUrl}`);
    } else {
        startUrl = 'https://www.careerbuilder.com/jobs?keywords=admin';
        log.info(`⚠️ Default: ${startUrl}`);
    }

    // Parse cookies
    let cookies = [];
    if (cookiesJson) {
        try {
            const parsed = typeof cookiesJson === 'string' ? JSON.parse(cookiesJson) : cookiesJson;
            cookies = (Array.isArray(parsed) ? parsed : Object.entries(parsed).map(([n, v]) => ({ name: n, value: String(v) })))
                .map(c => ({ name: c.name, value: c.value, domain: '.careerbuilder.com', path: '/' }))
                .filter(c => c.name && c.value);
            log.info(`🍪 ${cookies.length} cookies loaded`);
        } catch (e) {
            log.warning(`Cookie parse error: ${e.message}`);
        }
    }

    // Create proxy configuration
    log.info('🔧 Configuring proxy...');
    const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);
    log.info('✅ Proxy: RESIDENTIAL USA');

    // Get proxy URL for Camoufox (CRITICAL: proxy goes in launchOptions!)
    const proxyUrl = await proxyConfig.newUrl();
    log.info('🔗 Proxy URL obtained');

    // Create crawler with official Camoufox configuration
    const crawler = new PlaywrightCrawler({
        proxyConfiguration: proxyConfig,
        requestHandler: router,
        maxConcurrency: 1,
        maxRequestRetries: 3,
        requestHandlerTimeoutSecs: 180,
        navigationTimeoutSecs: 90,
        useSessionPool: true,
        persistCookiesPerSession: true,

        // Official Camoufox launch configuration
        launchContext: {
            launcher: firefox,
            launchOptions: await camoufoxLaunchOptions({
                headless: true,
                proxy: proxyUrl,  // Camoufox handles proxy internally
                geoip: true,      // Auto-configure locale based on proxy IP
                // fonts: ['Arial', 'Helvetica', 'Times New Roman'],
            }),
        },

        // Pre-navigation hooks for cookies and headers
        preNavigationHooks: [
            async ({ page }) => {
                // Inject cookies
                if (cookies.length > 0) {
                    try {
                        await page.context().addCookies(cookies);
                        log.debug('🍪 Cookies injected');
                    } catch (e) {
                        log.debug(`Cookie error: ${e.message}`);
                    }
                }

                // Set headers
                await page.setExtraHTTPHeaders({
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                });

                // Human-like delay
                await page.waitForTimeout(Math.random() * 2000 + 1500);
            }
        ],

        failedRequestHandler: async ({ request }, error) => {
            log.error(`❌ Failed: ${request.url} - ${error.message}`);
        },
    });

    // Run
    log.info(`🚀 Starting crawl: ${startUrl}`);
    await crawler.run([{ url: startUrl }]);

    // Summary
    const stats = getStats();
    log.info('='.repeat(50));
    log.info(`✅ Complete! Jobs: ${stats.jobsCollected}, Pages: ${stats.pagesProcessed}`);
    log.info('='.repeat(50));

    if (stats.jobsCollected === 0) {
        log.warning('⚠️ No jobs found. Check screenshots in Key-Value store.');
    }

} catch (error) {
    log.error(`❌ Fatal: ${error.message}`, { stack: error.stack });
}

await Actor.exit();
