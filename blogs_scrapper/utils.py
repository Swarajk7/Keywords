import json
from urllib.parse import urlparse
from bs4 import BeautifulSoup

SPACE = " "


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton,
                                        cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class SpiderDataConfig:
    __metaclass__ = Singleton
    DATA_PATH = "blogs_scrapper/configs/setup.json"

    def __init__(self):
        with open(SpiderDataConfig.DATA_PATH, "r") as f:
            json_data = json.load(f)
            sitemap_spider_data = json_data['sitemap_spider_data']
            crawl_spider_data = json_data['crawl_spider_data']
            del json_data

        self._sitemap_spider_data = dict()
        for item in sitemap_spider_data:
            netloc = urlparse(item['sitemap']).netloc
            self._sitemap_spider_data[netloc] = {}
            for key in [
                    'company_name', 'url_valid_regex', 'sitemap',
                    'xpath_config'
            ]:
                if key in item.keys():
                    self._sitemap_spider_data[netloc][key] = item[key]
        print(self._sitemap_spider_data)

        self._crawl_spider_data = dict()
        for item in crawl_spider_data:
            netloc = urlparse(item['start_url']).netloc
            self._crawl_spider_data[netloc] = {}
            for key in [
                    'company_name', 'url_valid_regex', 'link_extractor_rule',
                    'start_url', 'xpath_config'
            ]:
                if key in item.keys():
                    self._crawl_spider_data[netloc][key] = item[key]
        print(self._crawl_spider_data)

        self._crate_xpath_config()

    def _crate_xpath_config(self):
        self._xpath_data = {}
        for key, item in list(self.sitemap_spider_data.items()) + list(
                self.crawl_spider_data.items()):
            xpath_info = item.get("xpath_config", None)
            if xpath_info:
                self._xpath_data[key] = xpath_info
        print(self._xpath_data)

    @property
    def sitemap_spider_data(self):
        return self._sitemap_spider_data

    @property
    def crawl_spider_data(self):
        return self._crawl_spider_data

    @property
    def xpath_data(self):
        return self._xpath_data


class XPathExtractor(object):
    def __init__(self):
        self.xpath_data = SpiderDataConfig().xpath_data

    def get_xpath(self, url, field_name):
        if url:
            key = urlparse(url).netloc
            xpath = self.xpath_data.get(key, {}).get(field_name, "")
            if xpath:
                return xpath
        return None

    def get_clean_body_title(self, response):
        body_xpath = self.get_xpath(response.url, 'body')
        title_xpath = self.get_xpath(response.url, 'title')
        title, body = "", ""
        default_soup = BeautifulSoup(response.body)

        if body_xpath:
            body = BeautifulSoup(response.xpath(body_xpath).get()).get_text(
                separator=SPACE, strip=True)
        else:
            body = default_soup.body.get_text(separator=SPACE, strip=True)

        if title_xpath:
            title = BeautifulSoup(response.xpath(title_xpath).get()).get_text(
                separator=SPACE, strip=True)
        else:
            title = default_soup.title.get_text(separator=SPACE, strip=True)
        return title, body