import scrapy
from bs4 import BeautifulSoup

class InsideSpider(scrapy.Spider):
    name = 'inside'
#     allowed_domains = ['udn.com']
    start_urls = ['https://udn.com/news/breaknews/2']

    def parse(self, response):
        soup = BeautifulSoup( response.text , 'html.parser' )
        allTitle = soup.find_all('h2', {'class': 'story-list__text'})
        for each_title in allTitle:
            print('標題：' + each_title.text)  
    
    
