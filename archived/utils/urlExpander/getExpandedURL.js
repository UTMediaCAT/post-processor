const puppeteer = require('puppeteer');

url = process.argv[2];
let url = process.argv[2];

(async function main() {
    try {
        const browser = await puppeteer.launch({
            headless: true,
            args: ['--no-sandbox']
        });
        const [page] = await browser.pages();

        await page.goto(url);

        console.log(page.url());

        await browser.close();
    } catch (err) {
        console.log('error_page')
    }
})();