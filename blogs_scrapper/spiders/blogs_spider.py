from scrapy.spiders import SitemapSpider, CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.loader import ItemLoader
from scrapy.http import Request, XmlResponse
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from blogs_scrapper.utils import SpiderDataConfig, XPathExtractor
from blogs_scrapper.items import BlogItem
import re

SITEMAP_KEY = "sitemap"
URL_REGEX_KEY = "url_valid_regex"


def valid_url(data, url):
    key = urlparse(url).netloc
    regex = data.get(key, {}).get(URL_REGEX_KEY, None)
    if not regex:
        return True
    return bool(re.search(regex, url))


class BlogsSitemapSpider(SitemapSpider):
    name = 'blogs_sitemap'
    sitemap_follow = ['\.xml$']

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.spider_data = SpiderDataConfig().sitemap_spider_data
        self.xpath_extractor = XPathExtractor()

    def start_requests(self):
        for _, item in self.spider_data.items():
            yield Request(item[SITEMAP_KEY], self._parse_sitemap)

    def parse(self, response):
        self.logger.info(response.url)
        loader = ItemLoader(item=BlogItem(), selector=response)
        title, body = self.xpath_extractor.get_clean_body_title(response)
        loader.add_value('url', response.url)
        loader.add_value('body', body)
        loader.add_value('title', title)
        yield loader.load_item()

    def sitemap_filter(self, entries):
        for entry in entries:
            if self._valid_url(entry['loc']):
                yield entry

    def _valid_url(self, url):
        return valid_url(self.spider_data, url)


class BlogsCrawlSpider(CrawlSpider):
    name = "blogs_crawl"
    start_urls = []
    rules = []

    def __init__(self, *a, **kw):
        self.spider_data = SpiderDataConfig().crawl_spider_data
        for _, item in self.spider_data.items():
            self.start_urls.append(item['start_url'])
            allow = item.get('link_extractor_rule', {}).get('allow', [])
            deny = item.get('link_extractor_rule', {}).get('deny', [])
            self.rules.append(
                Rule(LinkExtractor(allow=allow, deny=deny),
                     callback=self.parse_blog,
                     follow=True))
        self.xpath_extractor = XPathExtractor()
        super().__init__(*a, **kw)

    def parse_blog(self, response):
        self.logger.info(response.url)
        if self._valid_url(response.url):
            loader = ItemLoader(item=BlogItem(), selector=response)
            title, body = self.xpath_extractor.get_clean_body_title(response)
            loader.add_value('url', response.url)
            loader.add_value('body', body)
            loader.add_value('title', title)
            yield loader.load_item()

    def _valid_url(self, url):
        return valid_url(self.spider_data, url)