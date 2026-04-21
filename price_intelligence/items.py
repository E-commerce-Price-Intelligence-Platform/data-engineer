import scrapy

class SmartphoneItem(scrapy.Item):
    name        = scrapy.Field()
    brand       = scrapy.Field()
    model       = scrapy.Field()
    price       = scrapy.Field()
    old_price   = scrapy.Field()
    currency    = scrapy.Field()
    discount    = scrapy.Field()
    rating      = scrapy.Field()
    reviews     = scrapy.Field()
    url         = scrapy.Field()
    source_site = scrapy.Field()
    scraped_at  = scrapy.Field()