
import requests
from bs4 import BeautifulSoup
import re
from time import sleep
from pymongo import MongoClient

news_base_url = 'http://www.news.bayer.com/baynews/baynews.nsf/id/'
counter = 1

# Connect to MongoDB
client = MongoClient(port=27017)
db = client.business

# Creating business entity
class News():
    pass

def get_soup_object(url, parser_type='html.parser'):
    resp = requests.get(url)
    if  resp.status_code == 200:
        soup = BeautifulSoup(resp.text, parser_type)
    else:
        soup = None
    return soup

def extract_data(url, total_count):
    try:
        soup = get_soup_object(url)
        main_tag = soup.find('div',{'class':'newslist'})
        articles = main_tag.find_all('article',{'class':'news'})
        global counter

        for article in articles:
            news = News()
            if article.find('div', {'class' : 'news__bdy'}) != None:
                anchor_tag = article.find('div', {'class' : 'news__bdy'}).find('a')
            else: 
                anchor_tag = article.find('a')

            news.topline = anchor_tag.find('div',{'class':'newstopline'}).text.strip() if anchor_tag.find('div',{'class':'newstopline'}) != None else None
            news.header_one = anchor_tag.find('h3').text.strip() if anchor_tag.find('h3') != None else None
            news.header_two = anchor_tag.find('h5').text.strip() if anchor_tag.find('h5') != None else None
            news.date = anchor_tag.find('div',{'class':'news__date'}).text.strip() if anchor_tag.find('div',{'class':'news__date'}) != None else None
            relative_url = anchor_tag.get('href')
            news.url = news_base_url + relative_url

            business = {
                'topline' : news.topline,
                'headers' : [news.header_one, news.header_two],
                'date' : news.date,
                'url' : news.url 
            }
            
            # Insert business object directly into MongoDB via isnert_one
            result=db.news.insert_one(business)
            # Print to the console the ObjectID of the new document
            print('Created {0} of {1} as {2}'.format(counter, total_count, result.inserted_id))
            counter += 1

    except Exception as e:
        # write error log to DB or file or eventviewer
        print('In exception: {0}', str(e))    


if __name__ == '__main__':
    
    start_range = 1
    end_range = 250
    for i in range(start_range,end_range,10):
        #pass
        sleep(3)
        url = "http://www.news.bayer.com/baynews/baynews.nsf/id/news-overview-category-search-en?Open&sNEWS={0}#/search".format(str(i))
        print(url)
        extract_data(url, end_range-start_range+1)
        print('*' * 100)
    
