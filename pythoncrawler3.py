import requests
from bs4 import BeautifulSoup
import re
from time import sleep
from pymongo import MongoClient, DESCENDING, ASCENDING
from Utility import client, get_soup_object, News
import traceback
import urllib.parse as urlparse 

news_base_url = 'https://www.pfizer.com/news/'
db = client.business
list_news = []
latest_url = ''
stop_process = False    #this is used for subsequent extraction of news until the latest entry is found


def extract_data(url, index):
    global stop_process
    try:
        if not stop_process:
            soup = get_soup_object(url)
            news_main_tag = soup.find('div',{'class':'panel-pane pane-views pane-full-list-of-press-release'})
            rows = news_main_tag.find_all('div', {'class':'field-content'})
            counter = 0

            for row in rows:
                news = News()
                #print(row)
                anchor_tag = row.find('div', {'class':'story-link'}).find('span', {'class':'rssTitle'}).find('a')

                row_content = row.text.strip()
                #print(row_content)

                if anchor_tag != None:
                    news.topline = anchor_tag.text.strip()
                else:
                    continue  # skipping blank rows and cols

                relative_url = anchor_tag.get('href')
                news.url = news_base_url + relative_url

                #while looping if the current news is same as the latest entried in db then stop process
                if news.url == latest_url:
                    stop_process = True
                    break
                
                business = {
                    'topline' : news.topline,
                    'short_desc' : row_content,
                    'url' : news.url
                }
                
                list_news.append(business) 
                counter += 1           
        
        print('Created {0} for page {1}'.format(counter, str(index)))

    except Exception as e:
        # write error log to DB or file or eventviewer
        print('Page: {0} \t Exception: {1}', index, str(e))
        traceback.print_exc()    


if __name__ == '__main__':
    
    default_url = news_base_url + 'press-release/press-releases-archive'
    #print(default_url)
    
    #checking if previously data has been stored; if yes then get the latest data and url
    if db.pnews.find().count() > 0:
        data = db.pnews.find(no_cursor_timeout=True).sort( [ ( '_id', DESCENDING)]).limit(1)
        for csr in data:
            latest_url = csr.get('url')
        print(latest_url) 
    

    soup = get_soup_object(default_url)
    last_page_url = soup.find('li',{'class':'pager-last'}).find('a').get('href')
    #last_page_href = 'https://www.pfizer.com/news/press-release/press-releases-archive?page=69'

    print(last_page_url)
    parsed = urlparse.urlparse(last_page_url)
    page_no = urlparse.parse_qs(parsed.query)['page'][0]

    #print(page_no)
    start_range = 0
    end_range = int(page_no) + 1

    #loop through each page and extract data into a list
    for i in range(start_range,end_range,1):
        if stop_process:
            break
        sleep(3)
        if i == 0:
            url = default_url
        else:
            url = default_url+ '?page={0}'.format(i)
        
        print(url)
        extract_data(url, i)
        print('*' * 100)
    
    #reverse the list and store the collection in db so that the latest news data is stored at the last in the collection
    list_news.reverse()
    db.pnews.insert_many(list_news)
    print(len(list_news))