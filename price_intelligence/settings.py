BOT_NAME = "price_intelligence"
SPIDER_MODULES = ["price_intelligence.spiders"]
NEWSPIDER_MODULE = "price_intelligence.spiders"

ROBOTSTXT_OBEY = False
DOWNLOAD_DELAY = 3
RANDOMIZE_DOWNLOAD_DELAY = True
CONCURRENT_REQUESTS = 2
CONCURRENT_REQUESTS_PER_DOMAIN = 1

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-MA,fr;q=0.9,en;q=0.8",
}

ITEM_PIPELINES = {
    "price_intelligence.pipelines.CsvPipeline":  100,
    "price_intelligence.pipelines.JsonPipeline": 200,
}


LOG_LEVEL = "INFO"
FEED_EXPORT_ENCODING = "utf-8"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"