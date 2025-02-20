# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
import pymongo

class ChothuenhaCrawlerItem(scrapy.Item):
    # define the fields for your item here like:
    Posting_id = scrapy.Field()
    Title = scrapy.Field()
    Address = scrapy.Field()
    City = scrapy.Field()
    District = scrapy.Field()
    Ward = scrapy.Field()
    Street = scrapy.Field()
    Rental_price = scrapy.Field()
    Room_area = scrapy.Field()
    Bedrooms = scrapy.Field()
    Toilets = scrapy.Field()
    poster = scrapy.Field()
    Posting_date = scrapy.Field()
    Type_of_listing = scrapy.Field()
    View = scrapy.Field()
    Describe = scrapy.Field()

    pass
