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
import memcache
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
cache = memcache.Client(['127.0.0.1:11211'], debug=0)
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
                        currRow.append(TwitterPuller.UnicodeStr(strVersion))
                writer.writerow(currRow)

def InsertTweetsIntoTable(ticker, tweets, database):
    """
    * Insert tweets for particular ticker into table.
    """
    # Get matching twitter name:
    tableName = [table for table in db.Tables.keys() if ticker.lower() in table][0]
    columns = db.Tables[tableName]
    columnData = {}
    # Get corporate id for AAP:
    corpID = db.ExecuteQuery("SELECT CorpID FROM Corporations WHERE Ticker = 'AAP'")

    for column in columns.keys():
        columnData[column] = []

    for term in tweets.keys():
        for column in columns.keys():
            if column == 'CorpID':
                columnData[column].append(corpID)
            elif column == 'User':
                columnData[column].append(TwitterPuller.UnicodeStr(tweets[term].username))
            elif column == 'SearchTerm':
                columnData[column].append(term)
            elif column == 'Date':
                columnData[column].append(tweets[term].date)
            elif column == 'Tweet':
                columnData[column].append(TwitterPuller.UnicodeStr(tweets[term].text))

    # Push all data into the database:
    db.InsertValues(tableName, "Research_Seminar_Project", columnData)

def CreateTables(tickerToCorps):
    """
    * Create all tables to store data.
    Parameters:
    * tickerToCorps: Dictionary mapping Ticker -> ( CorpName, Industry).
    """
    # Connect to database, pull in current company table names:
    db = DataBase.MYSQLDatabase("root", "Correlation$", "127.0.0.1", "Research_Seminar_Project")
    # Skip creating corporations table if already created:
    if not db.TableExists("Corporations"):
        tables = db.Tables
        # Create Corporations table that maps corporation name to ticker, insert all corporations into database:
        corpTableColumns = {"CorpID" : ["int", True, ""], "Name" : ["text", False, ""], "Ticker" : ["varchar(5)", False, ""], "Industry" : ["text", False, ""]}
        db.CreateTable("Corporations", "Research_Seminar_Project", corpTableColumns)
        corpData = {}
        for key in corpTableColumns.keys():
            if key == "CorpID":
                corpData[key] = list(range(1, len(tickerToCorps.keys())))
            elif key == "Ticker":
                corpData[key] = tickerToCorps.keys()
            elif key == "Name":
                corpData[key] = []
                for value in tickerToCorps.values():
                    corpData[key].append(value[0])
            elif key == "Industry":
                corpData[key] = []
                for value in tickerToCorps.values():
                    corpData[key].append(value[1])

        db.InsertValues("Corporations", "Research_Seminar_Project", corpData)
    # Insert all data into the Corporations table:
    dataColumns = { "CorpID" : ["int", True, "Corporations(CorpID)"], "SearchTerm" : ["text", False, ""], 
                   "User" : ["text", False, ""], "Date" : ["date", False, ""], "Tweet" : ["text", False, ""] }
    tableSig = "Tweets_%s"
    # Create Tweets_{Ticker} table for each corporation:
    # One table for each ticker using listed columns:
    for ticker in tickerToCorps.keys():
        tableName = tableSig % ticker.strip()
        if not db.TableExists(tableName):
            db.CreateTable(tableName, "Research_Seminar_Project", dataColumns)

    return db

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


def PullBrandsFrom10Ks(tickers, priorDate):
    """
    * Pull all brands from Business sections in 10ks.
    Returns dictionary mapping ticker to full 10K text.
    """
    # Skip if all brands were pulled in already:
    cachedBrands = __GetCachedBrands(tickers)

    # Extract brands from 10K text file, map to ticker:
    tickerToBrands = {}
    for ticker in tickers.keys():
        if ticker not in cachedBrands.keys():
            # Extract all brands from 10K:
            tickerToBrands[ticker] = __ExtractBrands(ticker, ticker, priorDate)

    # Store all pulled brands in the cache for 20 days:
    __SetCachedBrands(tickerToBrands)
    
    return tickerToBrands

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
