import requests
from bs4 import BeautifulSoup
import re
from time import sleep
from datetime import datetime, time, date
from pymongo import MongoClient, DESCENDING, ASCENDING
from Utility import client, get_soup_object, News
import traceback

news_base_url = 'http://www.lupin.com/'

# Connect to MongoDB
#client = MongoClient(port=27017)
db = client.business


def extract_data(url, year):
    try:
        soup = get_soup_object(url)
        rows = soup.find_all('td',{'class':'tahoma11'})
        #global counter
        list_news = []

        for row in rows:
            news = News()
            #print(row)
            anchor_tag = row.find('a')

            row_content = row.text.strip()

            if anchor_tag != None:
                news.topline = anchor_tag.text.strip()
            else:
                continue  # skipping blank rows and cols
            
            try:
                news.date =  ((row_content.split('Date:'))[1]).strip() + ', ' + str(year)
            except:
                news.date = row.parent.parent.find('td', {'class':'rates_tbl_header rates_linewhite'}).text.strip()

            relative_url = anchor_tag.get('href')
            news.url = news_base_url + relative_url

            business = {
                'topline' : news.topline,
                'date' : news.date,
                'url' : news.url 
            }
            
            list_news.append(business)            
        db.lnews.insert_many(list_news)
        print('Created {0} for year {1}'.format(len(list_news), str(year)))

    except Exception as e:
        # write error log to DB or file or eventviewer
        print('Year: {0} \t Exception: {1}', year, str(e))
        traceback.print_exc()    


if __name__ == '__main__':
    
    current_year = int(date.today().strftime("%Y"))
    start_range = 2002
    end_range = current_year + 1
    for i in range(start_range,end_range,1):
        #pass
        sleep(3)
        if i < current_year and i != 2013:
            url = "http://www.lupin.com/archives-press-release-{0}.php".format(str(i))
        elif i == 2013:
            url = "http://www.lupin.com/archives-press-release.php"
        else:
            url = "http://www.lupin.com/press-release.php"    
        print(url)
        extract_data(url, i)
        print('*' * 100)
    