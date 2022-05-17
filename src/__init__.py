from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from osha.osha.spiders import oshaSpider, StandardRegsSpider
from .clean import Clean
from .engine import Engine, Exporter
from .utils import logger_util

#generic func to run spider
def run_spider(spider_name : str , settings = get_project_settings):
    
    """Runs a spider with the given name - only two options are currently supported:

    args:
        spider_name: str - name of spider to run
        settings: settings object - settings to use for spider
        
    example:
        run_spider('oshaSpider')

    Raises:
        ValueError: if the spider_name is not one of the supported options
    """

    spider_names = {'oshaSpider' : oshaSpider, 'StandardRegsSpider' : StandardRegsSpider}

    if spider_name not in spider_names:
        raise ValueError('spider_name not found in spider_names')
    
    spider_name = spider_names.get(spider_name)

    process = CrawlerProcess(settings())
    process.crawl(spider_name)
    process.start()


__all__ = [logger_util, run_spider, Clean, Engine, Exporter]