import requests
from bs4 import BeautifulSoup, Tag
import pandas as pd
import timeit
from cache import memoize
import os 
from tqdm import tqdm 
import time
from os.path import dirname, abspath

# Datapath for caching scraped data -news weblinks
parent_dir = dirname(dirname(abspath(__file__)))
datapath = parent_dir + '/data/mc_news_links.pkl'

# Some utility functions
@memoize(datapath = parent_dir + '/data/mc_news_articles_text.pkl')
def get_mc_article(url_article):
    
    """
    To get text corresponding to a specific article from its
    web page.
    """
     
    req = requests.get(url_article)
    src = req.text

    soup = BeautifulSoup(src, 'lxml')
    article_body = soup.select('div.arti-flow')
    try:
        article_text = '.'.join(map(str,[each.text for each in article_body[0].children if (type(each) is Tag) and (each.name == u'p')]))
    except:
        if not article_body:
            print('No article at link - {}'.format(url_article))
            return None
        else:
            print('Unknown issue at link - {}'.format(url_article))
            return None
    
    print('Done with link - {}'.format(url_article))
    return article_text

def get_elem(tag):

    """
    To get elements of a given tag
    """
    
    def search(elem):
        return tag.find(elem)	    
    return search
 
def parse_mc_news(news_item):

    """
    To parse and get metadata of all the articles in a given page.
    Relevant information is article web-links, summary, timestamp and title
    """ 
     
    global datapath
    links, title, timestamp, summary  = ('a', 'href'), ('a', 'title'), ('span'), ('p')
    news_title  = get_elem(news_item)(title[0])[title[1]]
 
    @memoize(datapath)
    def parse(news_title):
        def get_data(news_item):
            news_link = get_elem(news_item)(links[0])[links[1]]
            news_timestamp = get_elem(news_item)(timestamp).text
            news_summary = get_elem(news_item)(summary).text
            return {'link': news_link, 'title': news_title, 'timestamp': news_timestamp, 'summary': news_summary}    
        return get_data(news_item)
       
    return parse(news_title)

# Main webpage - Business news section
url_start = 'https://www.moneycontrol.com/news/business/'
req = requests.get(url_start)
src = req.text

# Working with beginning page of the articles
soup = BeautifulSoup(src, 'lxml')
list_of_articles = soup.select('li.clearfix')
news_data = [parse_mc_news(li) for li in list_of_articles]

# Working with follosing pages of articles in  business news
pages_info = soup.select('div.pagenation')
a_last = [each.find_all('a', 'last') for each in pages_info][0] 
num_pages = max([int(each_a['data-page']) for each_a in a_last])

url_all = 'https://www.moneycontrol.com/news/business/page-{}'
num_pages = 10 #Fixing it for testing purposes

for page in tqdm(range(2, num_pages)):
    src = requests.get(url_all.format(page)).text
    soup = BeautifulSoup(src, 'lxml')
    articles = soup.select('li.clearfix')
    page_articles_list = [parse_mc_news(li) for li in articles]
    news_data.extend(page_articles_list)
    print('Done with page - {0} out of {1}'.format(page, num_pages))
    time.sleep(2)

# DataFrame with columns - Link, title, summary and timestamp for all the articles 
news_metadata_df = pd.DataFrame(news_data)
print(news_metadata_df)
# To get full article for each(relevant) article using links in news_metadata_df
news_article_df = news_metadata_df
news_article_df['full_text'] = news_metadata_df.apply(lambda x: get_mc_article(x['link']), axis = 1)
print(news_article_df)

