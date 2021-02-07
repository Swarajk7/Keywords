# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from bs4 import BeautifulSoup
import json
from urllib.parse import urlparse
from blogs_scrapper.utils import SpiderDataConfig
import pymongo


class AnnontatorPipeline:
    def open_spider(self, spider):
        data = SpiderDataConfig()
        self.all_spider_data = data.sitemap_spider_data
        self.all_spider_data.update(data.crawl_spider_data)
        del data

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        url = adapter.get('url')
        adapter['company'] = self.all_spider_data.get(
            urlparse(url).netloc, {}).get('company_name', '')
        return item


class JsonWriterPipeline:
    def open_spider(self, spider):
        self.file = open('items.json', 'w')

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        line = json.dumps(ItemAdapter(item).asdict()) + "\n"
        self.file.write(line)
        return item


class MongoPipeline:

    collection_name = 'blogs'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(mongo_uri=crawler.settings.get('MONGO_URI'),
                   mongo_db=crawler.settings.get('MONGO_DATABASE'))

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        self.db[self.collection_name].update_one({'url': adapter.get('url')},
                                                 {'$set': adapter.asdict()},
                                                 upsert=True)
        return item