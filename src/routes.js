// CareerBuilder Scraper - Routes (Apify Camoufox Template)

import { createPlaywrightRouter, Dataset } from 'crawlee';
import { Actor, log } from 'apify';
import * as cheerio from 'cheerio';

export const router = createPlaywrightRouter();

// Shared state
let jobsCollected = 0;
let pagesProcessed = 0;
const seenUrls = new Set();
let maxJobs = 20;
let maxPages = 10;

export function setLimits(jobs, pages) {
    maxJobs = jobs;
    maxPages = pages;
}

// Default handler for search pages
router.addDefaultHandler(async ({ page, request, crawler }) => {
    log.info(`📄 Processing: ${request.url}`);

    if (jobsCollected >= maxJobs) {
        log.info(`🎯 Target reached: ${jobsCollected} jobs`);
        return;
    }

    if (pagesProcessed >= maxPages) {
        log.info(`📄 Max pages reached: ${pagesProcessed}`);
        return;
    }

    // Wait for page
    try {
        await page.waitForLoadState('networkidle', { timeout: 30000 });
    } catch (e) {
        log.debug('Network not idle, continuing...');
    }

    // Accept cookies
    try {
        const btn = page.locator('button:has-text("Accept All Cookie Settings"), button:has-text("Accept")');
        if (await btn.isVisible({ timeout: 3000 })) {
            log.info('🍪 Clicking cookie consent...');
            await btn.click();
            await page.waitForTimeout(2000);
        }
    } catch (e) { }

    // Wait for Cloudflare
    for (let i = 0; i < 3; i++) {
        const html = await page.content();
        if (html.toLowerCase().includes('checking your browser') ||
            html.toLowerCase().includes('just a moment')) {
            log.info(`☁️ Cloudflare (${i + 1}/3), waiting...`);
            await page.waitForTimeout(10000);
        } else break;
    }

    // Check blocking
    const content = await page.content();
    if (content.includes('regional_sites') || content.includes('not available')) {
        log.error('🚫 GEO-BLOCKED');
        await Actor.setValue('BLOCKED', await page.screenshot({ fullPage: true }), { contentType: 'image/png' });
        return;
    }

    pagesProcessed++;
    log.info(`✅ Page ${pagesProcessed}`);

    // Save debug screenshot
    await Actor.setValue(`PAGE_${pagesProcessed}`, await page.screenshot(), { contentType: 'image/png' });

    // Extract jobs using page.evaluate
    const jobs = await page.evaluate(() => {
        const results = [];

        // Find all job links
        document.querySelectorAll('a[href*="/job/"]').forEach(link => {
            const href = link.getAttribute('href');
            const title = link.textContent?.trim();
            const container = link.closest('div, li, article');

            if (href && title && title.length > 5) {
                // Try to get company/location from container
                const spans = container?.querySelectorAll('span');
                let company = '', location = '';
                spans?.forEach((s, i) => {
                    const t = s.textContent?.trim();
                    if (i === 0 && t && t.length < 50) company = t;
                    if (t?.includes('|') || t?.includes(',') || t?.includes('(')) location = t;
                });

                results.push({
                    title: title.substring(0, 150),
                    company,
                    location,
                    url: href.startsWith('http') ? href : `https://www.careerbuilder.com${href}`
                });
            }
        });

        // Also check data attributes
        document.querySelectorAll('[data-job-did]').forEach(el => {
            const id = el.getAttribute('data-job-did');
            const link = el.querySelector('a');
            const title = link?.textContent?.trim() || el.querySelector('[class*="title"]')?.textContent?.trim();
            const href = link?.getAttribute('href');

            if (title && href) {
                results.push({
                    id,
                    title,
                    company: el.querySelector('[class*="company"]')?.textContent?.trim() || '',
                    location: el.querySelector('[class*="location"]')?.textContent?.trim() || '',
                    url: href.startsWith('http') ? href : `https://www.careerbuilder.com${href}`
                });
            }
        });

        return results;
    });

    log.info(`📊 Found ${jobs.length} jobs`);

    // Dedupe and save
    for (const job of jobs) {
        if (jobsCollected >= maxJobs) break;
        if (seenUrls.has(job.url)) continue;
        seenUrls.add(job.url);

        await Dataset.pushData({
            id: job.id || `cb-${Date.now()}`,
            title: job.title || 'Unknown',
            company: job.company || 'Unknown',
            location: job.location || 'Not specified',
            url: job.url,
            scrapedAt: new Date().toISOString(),
        });

        jobsCollected++;
        log.info(`💾 [${jobsCollected}/${maxJobs}] ${job.title}`);
    }

    // Pagination
    if (jobsCollected < maxJobs && pagesProcessed < maxPages && jobs.length > 0) {
        const url = new URL(request.url);
        const pageNum = parseInt(url.searchParams.get('page_number') || '1', 10);
        url.searchParams.set('page_number', String(pageNum + 1));
        log.info(`📄 Next: ${url.toString()}`);
        await crawler.addRequests([{ url: url.toString() }]);
    }
});

export function getStats() {
    return { jobsCollected, pagesProcessed };
}
