# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
    
from itemadapter import ItemAdapter
import pymongo

class MongoDBPipeline:
    def __init__(self):
        self.mongo_uri = 'mongodb://mongodb:27017' # localhost , mongodb
        self.mongo_db = 'chothuenha_db'
        self.mongo_collection = 'chothuenha'

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        self.db[self.mongo_collection].update_one(
            {'Posting_id': adapter['Posting_id']},  # Lọc theo Posting_id
            {'$set': dict(item)},  # Cập nhật các trường từ item
            upsert=True  # Chèn mới nếu không tồn tại
        )
        return item
