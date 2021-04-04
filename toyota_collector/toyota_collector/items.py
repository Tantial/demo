# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ToyotaCollectorItem(scrapy.Item):
    model = scrapy.Field()
    year = scrapy.Field()
    new_or_used = scrapy.Field()
    vin = scrapy.Field()
    stock_num = scrapy.Field()
    odometer = scrapy.Field()
    fuel_economy = scrapy.Field()
    exterior_color = scrapy.Field()
    interior_color = scrapy.Field()
    body = scrapy.Field()
    transmission = scrapy.Field()
    drivetrain = scrapy.Field()
    engine = scrapy.Field()
    dealer_notes = scrapy.Field()
