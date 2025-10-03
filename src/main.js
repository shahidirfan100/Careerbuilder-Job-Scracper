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
    if (kw) {
        url.searchParams.set('keywords', kw);
    }
    if (loc) {
        url.searchParams.set('location', loc);
    }
    if (date && date !== 'anytime') {
        const dateMap = {
            '24h': '1', // CareerBuilder uses days, so 24h = 1 day
            '7d': '7', // 7 days
            '30d': '30', // 30 days
        };
        if (dateMap[date]) {
            url.searchParams.set('posted', dateMap[date]);
        }
    }
    return url.href;
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

// ------------------------- PROXY & START URLS -------------------------
const proxyConf = proxyConfiguration
    ? await Actor.createProxyConfiguration(proxyConfiguration)
    : undefined;

let jobsScraped = 0;

const crawler = new CheerioCrawler({
    proxyConfiguration: proxyConf, // Use the configured proxy
    maxRequestsPerMinute: 600, // Higher throughput
    requestHandlerTimeoutSecs: 60,
    navigationTimeoutSecs: 60,
    maxConcurrency: 10, // A safe concurrency level
    useSessionPool: true,
    persistCookiesPerSession: true,
    sessionPoolOptions: {
        maxPoolSize: 50,
        sessionOptions: {
            maxUsageCount: 30,
            maxErrorScore: 3,
        },
    },
    preNavigationHooks: [
        ({ request }) => {
            // Set anti-blocking headers
            request.headers = {
                ...request.headers, // Preserve existing headers
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Sec-Ch-Ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
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
            crawlerLog.info(`Target number of jobs (${RESULTS_WANTED}) reached, skipping further requests.`);
            return;
        }

        if (label === 'LIST' || !label) {
            crawlerLog.info(`Scraping LIST page ${page}: ${request.url}`);
            const jobCards = $('div.data-results-content-parent > div.card-mobile-wrapper').toArray();
            crawlerLog.info(`Found ${jobCards.length} job cards on page ${page}.`);

            if (jobCards.length === 0) {
                crawlerLog.warning(`No job cards found on page ${page}. This might indicate a block or an empty search.`);
                session.retire();
                return;
            }

            const linksToEnqueue = [];
            for (const card of jobCards) {
                if (jobsScraped + linksToEnqueue.length >= RESULTS_WANTED) {
                    break;
                }
                const jobLink = $(card).find('a.data-results-title').attr('href');
                if (jobLink) {
                    const absoluteUrl = new URL(jobLink, 'https://www.careerbuilder.com').href;
                    linksToEnqueue.push(absoluteUrl);
                }
            }

            if (linksToEnqueue.length > 0) {
                crawlerLog.info(`Enqueuing ${linksToEnqueue.length} detail pages.`);
                await enqueueLinks({
                    urls: linksToEnqueue,
                    userData: { label: 'DETAIL' },
                });
            }

            // Pagination logic
            if (jobsScraped + linksToEnqueue.length < RESULTS_WANTED && page < MAX_PAGES) {
                const nextPageUrl = $('a.next-page').attr('href');
                if (nextPageUrl) {
                    const absoluteNextUrl = new URL(nextPageUrl, 'https://www.careerbuilder.com').href;
                    crawlerLog.info(`Enqueuing next list page: ${page + 1}`);
                    await enqueueLinks({
                        urls: [absoluteNextUrl],
                        userData: { label: 'LIST', page: page + 1 },
                    });
                } else {
                    crawlerLog.info('No "next page" link found. Reached the end of pagination.');
                }
            } else if (page >= MAX_PAGES) {
                crawlerLog.info(`Max pages limit (${MAX_PAGES}) reached. Stopping pagination.`);
            }
        }

        if (label === 'DETAIL') {
            if (jobsScraped >= RESULTS_WANTED) {
                crawlerLog.info(`Skipping detail page as limit is reached: ${request.url}`);
                return;
            }

            const title = $('h1.h2').text().trim();

            // If title is missing, it's likely a bad page or a different layout
            if (!title) {
                crawlerLog.warning(`Could not extract title from ${request.url}. Retiring session.`);
                session.retire();
                // Re-enqueue with a new session
                await crawler.addRequests([{ url: request.url, userData: request.userData, uniqueKey: `${request.url}-retry` }]);
                return;
            }

            const company = $('div.data-details > span:nth-child(1)').text().trim();
            const jobLocation = $('div.data-details > span:nth-child(2)').text().trim();
            const date_posted = $('div.data-details > span:nth-child(3)').text().trim();

            const description_html = $('#jdp_description > .jdp-description-details').html() || '';
            const description_text = $('#jdp_description > .jdp-description-details').text().trim();

            await Dataset.pushData({
                title,
                company,
                location: jobLocation,
                date_posted,
                description_html,
                description_text,
                url: request.url,
            });

            jobsScraped++;
            crawlerLog.info(`✅ Scraped job ${jobsScraped}/${RESULTS_WANTED}: ${title}`);
        }
    },

    failedRequestHandler: async ({ request }, error) => {
        log.error(`Request ${request.url} failed: ${error.message}`);
    },
});

const initialUrl = startUrl || buildStartUrl(keyword, location, posted_date);

if (!keyword && !location && !startUrl) {
    log.warning('No keyword, location, or startUrl provided. The scraper will run on the default CareerBuilder jobs page, which may yield broad results.');
}

log.info('🚀 Starting scraper...');
log.info(`- Target jobs: ${RESULTS_WANTED}`);
log.info(`- Max pages: ${MAX_PAGES}`);
log.info(`- Start URL: ${initialUrl}`);
await crawler.run([{ url: initialUrl, userData: { label: 'LIST', page: 1 } }]);
log.info(`🏁 Scraping finished. Total jobs scraped: ${jobsScraped}.`);
await Actor.exit();
