import requests
from bs4 import BeautifulSoup
import re
from time import sleep
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient(port=27017)

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