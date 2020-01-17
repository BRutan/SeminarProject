#################################################
# HelperFunctions.py
#################################################
# Description:
# * Functions pull data from Edgar website, local files
# and others.

from bs4 import BeautifulSoup
from collections import Counter

import csv
import DataBase
import os
#import memcache
import nltk
from sec_edgar_downloader import Downloader
from nltk.stem import WordNetLemmatizer, SnowballStemmer
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, RegexpTokenizer
from PullTwitterData import TwitterPuller
import requests
import sys

# Memcached documentation here:
# https://commaster.net/posts/installing-memcached-windows/

# Initiate cache:
#cache = memcache.Client(['127.0.0.1:11211'], debug=0)
__cacheKeySig = "brands%s"

nltk.download('punkt')
nltk.download('wordnet')

def PrintTweetsToCSV(tweets, path):
    """
    * Print all tweets to csv specified at path.
    """
    key = list(tweets.keys())[0]
    tweet = tweets[key][0]
    columns = [attr for attr in dir(tweet) if '__' not in attr]
    columns.insert(0, "SearchTerm")
    with open(path, "w", newline='') as f:
        writer = csv.writer(f)
        # Write all columns to csv:
        writer.writerow(columns)
        for term in tweets.keys():
            for tweet in tweets[term]:
                currRow = []
                for column in columns:
                    if column == "SearchTerm":
                        currRow.append(term)
                    elif column == "date":
                        currRow.append(getattr(tweet, column).strftime("%m/%d/%Y"))
                    else:
                        strVersion = str(getattr(tweet, column))
                        currRow.append(TwitterPuller.AsciiStr(strVersion))
                writer.writerow(currRow)


def PullTickers(symbolCSVPath):
    """
    * Pull in all consumer discretionary tickers from local file.
    Store Ticker -> ( Name, Sector )
    """
    tickers = {}
    with open(symbolCSVPath, 'r') as f:
        reader = csv.reader(f)
        atHeader = True
        # Columns:
        # Name	Ticker	Identifier	SEDOL	Weight	Sector	Shares Held	Local Currency
        for row in reader:
            if not atHeader:
                ticker = row[1].strip()
                tickers[ticker] = (row[0], row[5])
            atHeader = False
    return tickers

def Pull10Ks(outputFolderPath, tickers):
    """
    * Pull all 10Ks for consumer discretionary companies from local file.
    Documentation: https://pypi.org/project/sec-edgar-downloader/
    """
    if not os.path.exists(outputFolderPath):
        raise Exception("Output folder does not exist.")
    # Exit if already executed:
    if os.path.exists("sec_edgar_filings"):
        return outputFolderPath + 'sec_edgar_filings'

    dl = Downloader(outputFolderPath)

    # Output all 10ks to local folder:
    for ticker in tickers.keys():
        dl.get_10k_filings(ticker.upper(), 1)

    return outputFolderPath + 'sec_edgar_filings'

def __Move10Ks(topFolder, path):
    """
    * Move all 10ks in folder to top folder, rename as date for 10k.
    """
    # Move all pulled 10K text files into top folder, and rename to ticker:
    fileNames = [fileName for fileName in os.listdir(path) if os.path.isfile(os.path.join(path, fileName))]
    fullPaths = [os.path.join(path, fileName) for fileName in os.listdir(path) if os.path.isfile(os.path.join(path, fileName))]
    
    for fileName in fileNames:
        pass


######################################
# Private Helpers:
######################################
def __ExtractBrands(ticker, corpName, date):
    """
    * Extract all brands from 10K (in business section).
    """
    
    soup = PullFilesCleanSoup(ticker, corpName, date)
    engStopWords = stopwords.words('english')

    # Remove all stopwords from text portion:
    sentenceNoStopWords = ''.join([word for word in sentence if not word in engStopWords])
    # Extract all brands:
        

def __GetCachedBrands(tickers):
    """
    * Get map of all cached brands (to prevent running multiple times)
    """
    cachedBrands = {}
    for ticker in tickers.keys():
        keySig = __cacheKeySig % ticker
        brands = cache.get(keySig)
        if not brands is None:
            cachedBrands[keySig] = brands
    
    return cachedBrands
        
def __SetCachedBrands(tickerToBrands):
    """
    * Store all pulled brands in cache.
    """
    for ticker in tickerToBrands.keys():
        keySig = __cacheKeySig % ticker
        brands = cache.get(keySig)
        if brands is None:
            # Store brands for 20 days in memcached:
            cache.set(keySig, tickerToBrands[ticker], 86400 * 20)
    
def temp():
    """
    * 
    """
    mc.set("some_key", "Some value")
    value = mc.get("some_key")
    mc.set("another_key", 3)
    mc.delete("another_key")
    mc.set("key", "1")   # note that the key used for incr/decr must be a string.
    mc.incr("key")
    mc.decr("key")
