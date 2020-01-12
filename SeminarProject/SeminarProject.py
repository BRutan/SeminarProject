#################################################
# SeminarProject.py
#################################################
# 

from BrandQuery import BrandQuery
from CorporateFiling import CorporateFiling, TableItem, DocumentType, PullingSteps, SoupTesting
import csv
import DataBase
from datetime import datetime, timedelta
import gc
import memcache
from pandas.tseries import offsets
from numpy.random import choice as choose
import re
from PullTwitterData import TwitterPuller

# Initiate cache to store brands that were searched:
cache = memcache.Client(['127.0.0.1:11211'], debug=0)
# Use key signature to keep track of brands that have been pulled for company, in case
# script fails:
__cacheKeySig = "{Corp:%s}{Brand:%s}"

class SeminarProject(object):
    """
    * Key objects required for performing seminar project.
    """
    __utfSupport = 'CHARACTER SET utf8 COLLATE utf8_unicode_ci'
    __cacheKeySig = "{Corp:%s}{Brand:%s}"
    def __init__(self, tickerPath, database):
        """
        * Initialize new object.
        """
        self.DB = database
        self.Tickers = {}
        # Get tickers from CSV file at tickerPath:
        self.__CorpNumToTicker = {}
        self.__TickerToCorpNum = {}
        self.__PullTickers(tickerPath)
        self.CorpTableColumns = {"CorpID" : ["int", True, ""], "Name" : ["text", False, ""], 
                                 "Ticker" : ["varchar(5)", False, ""], "Industry" : ["text", False, ""], "Weight" : ["float", False, ""] }
        self.CorpBrandTableColumns = {"CorpID" : ["int", False, "Corporations(CorpID)"], "Brands" : ["text", False, ""]}
        self.SubsidariesTableColumns = {"CorpID" : ["int", False, "Corporations(CorpID)"], "Subsidiaries" : ["text", False, ""]}
        self.DataColumns = { "CorpID" : ["int", False, "Corporations(CorpID)"], "SearchTerm" : ["text", False, ""], 
                       "User" : ["text " + SeminarProject.__utfSupport, False, ''], "Date" : ["date", False, ""], 
                       "Tweet" : ["text " + SeminarProject.__utfSupport, False, ''] }
        self.CorpToBrands = {}
        self.__PulledBrands = {}
        
    #######################
    # Interface Methods:
    #######################
    def ExecuteAll(self):
        """
        * Execute all steps in sequential order.
        """
        self.CreateTables()
        self.GetSubsidiaries()
        self.LoadAllBrands()
        self.SampleAndInsertTweets()

    def CreateTables(self):
        """
        * Create all tables to store data.
        """
        # Connect to database, pull in current company table names:
        db = self.DB
        tickerToCorps = self.Tickers
        self.TickerToTable = {}
        # Skip creating corporations table if already created:
        if not db.TableExists("Corporations"):
            corpTableColumns = self.CorpTableColumns
            # Create Corporations table that maps corporation name to ticker, insert all corporations into database:
            db.CreateTable("Corporations", corpTableColumns, schema = "Research_Seminar_Project")
            corpData = {}
            for key in corpTableColumns.keys():
                corpData[key] = []
                for ticker in tickerToCorps.keys():
                    if key == "CorpID":
                        corpData[key].append(self.__TickerToCorpNum[ticker]) 
                    elif key == "Ticker":
                        corpData[key].append(ticker)
                    elif key == "Name":
                        corpData[key].append(tickerToCorps[ticker][0])
                    elif key == "Industry":
                        corpData[key].append(tickerToCorps[ticker][1])
                    elif key == "Weight":
                        corpData[key].append(tickerToCorps[ticker][2])
            # Insert pulled corporation data from local XLY file into Corporations database:
            db.InsertValues("Corporations", corpData)
        if not db.TableExists("Subsidiaries"):
            db.CreateTable("Subsidiaries", self.SubsidariesTableColumns, schema = "Research_Seminar_Project")
        if not db.TableExists("CorporateBrands"):
            db.CreateTable("CorporateBrands", self.CorpBrandTableColumns, schema = "Research_Seminar_Project")

        # Create all Corporations tables:
        dataColumns = self.DataColumns
        tableSig = "Tweets_%s"
        # Create Tweets_{Ticker} table for each corporation:
        # One table for each ticker using listed columns:
        for ticker in tickerToCorps.keys():
            tableName = tableSig % ticker.strip()
            self.TickerToTable[ticker] = tableName
            if not db.TableExists(tableName):
                db.CreateTable(tableName, dataColumns)

    def GetSubsidiaries(self):
        """
        * Pull subsidiaries from each corporation's 10K, and load into 
        database. If already loaded subsidiaries into database then pull 
        using query.
        """
        db = self.DB
        queryString = ['SELECT A.Ticker, B.Subsidiaries FROM Corporations AS A']
        queryString.append('INNER JOIN Subsidiaries As B ON A.CorpID = B.CorpID WHERE B.Subsidiaries IS NOT NULL;')
        queryString = ' '.join(queryString)
        results = db.ExecuteQuery(queryString, getResults = True)
        # Testing:
        #results = None
        self.TickerToSubs = {}
        # Determine if pulled some/all subsidiaries already:
        if results and len(results[list(results.keys())[0]]) > 0:
            tickers = results['corporations.ticker']
            subs = results['subsidiaries.subsidiaries']
            row = 0
            for ticker in tickers:
                if ticker not in self.TickerToSubs.keys():
                    self.TickerToSubs[ticker] = []
                self.TickerToSubs[ticker].append(subs[row])
                row += 1
        if len(self.TickerToSubs.keys()) < len(self.Tickers.keys()):
            # Pull some subsidiaries from 10-Ks, if haven't been pulled in yet:
            yearEnd = datetime.today() + offsets.YearEnd()
            subs = re.compile('subsidiaries', re.IGNORECASE)
            name = re.compile('name', re.IGNORECASE)
            steps = PullingSteps(False, True, False)
            # Testing:
            #tableDocPath = 'D:\\Git Repos\\SeminarProject\\SeminarProject\\SeminarProject\\Notes\\TableNames\\'
            for ticker in self.Tickers.keys():
                if ticker not in self.TickerToSubs.keys():
                    doc = CorporateFiling(ticker, DocumentType.TENK, steps, date = yearEnd)
                    insertData = {'CorpID' : [], 'Subsidiaries' : []}
                    # Testing:
                    #doc.PrintTables(tableDocPath)
                    #SoupTesting.PrintTableAttributes(doc, tableDocPath)
                    self.TickerToSubs[ticker] = []
                    tableDoc, table = doc.FindTable(subs, False)
                    nameColumn = None
                    if table:
                        nameColumn = table.FindColumn(name, False)
                    if not nameColumn is None:
                        self.TickerToSubs[ticker] = list(nameColumn)
                    # Add the corporation's name itself:
                    self.TickerToSubs[ticker].append(self.Tickers[ticker][0])
                    # Insert data into Subsidiaries table:
                    insertData['CorpID'] = [self.__TickerToCorpNum[ticker]] * len(self.TickerToSubs[ticker])
                    insertData['Subsidiaries'] = self.TickerToSubs[ticker]
                    db.InsertValues("Subsidiaries", insertData)
                    gc.collect()
                    
    def LoadAllBrands(self):
        """
        * Pull all brands from WIPO website, push into database.
        """
        # Determine if brands were already loaded for each corporation:
        db = self.DB
        results = db.ExecuteQuery('SELECT Ticker, Brands FROM Brands WHERE Brands IS NOT NULL;', getResults = True)
        if results and len(results.keys()) == len(self.Tickers.keys()):
            for row in results.keys():
                ticker = results[row]['ticker']
                brand = results[row]['brands']
                self.CorpToBrands[ticker].append(brand)

        
        # Pull all brands from WIPO database website:
        for ticker in self.Tickers.keys():
            subsidiaries = self.Subsdiaries[ticker]
            for sub in subsidiaries:
                pass

            # Push brands into the mysql database:
            for brand in brands:
                insertValues['ticker'].append(ticker)
                insertValues['brand'].append(brand)
            db.InsertValues(tableName, insertValues)

    def SampleAndInsertTweets(self):
        """
        * Randomly sample all tweets and insert into associated table in schema.
        """
        puller = TwitterPuller()
        for ticker in self.Tickers.keys():
            for brand in self.CorpToBrands[ticker]:
                self.__PullFromCache(ticker,brand)

            for brand in self.CorpToBrands[ticker]:
                if brand not in self.__PulledBrands[ticker]:
                    # Sample tweets that mention brand.

                    samples = choose(100, 45, replace=False)
                    tweets = puller.PullTweets('', 100, brand)
                    tweets = [tweets[i] for i in samples]
                    self.__InsertTweetsIntoTable(ticker, tweets)
                    # Indicate that have already pulled brand tweets for company:
                    self.__InsertIntoCache(ticker, brand)
                    
                
        
    ########################
    # Private Helpers:
    ########################
    def __InsertTweetsIntoTable(self, keyword, ticker, tweets):
        """
        * Insert tweets for particular ticker into table.
        """
        db = self.DB
        # Get matching twitter name:
        tableName = [table for table in db.Tables.keys() if ticker.lower() in table][0]
        columns = db.Tables[tableName]
        columnData = {}
        # Get corporate id for ticker:
        corpID = db.ExecuteQuery("SELECT CorpID FROM Corporations WHERE Ticker = %s" % ticker)

        for column in columns.keys():
            columnData[column] = []

        for tweet in tweets:
            for column in columns.keys():
                if column == 'CorpID':
                    columnData[column].append(corpID)
                elif column == 'User':
                    columnData[column].append(tweet.username)
                elif column == 'SearchTerm':
                    columnData[column].append(keyword)
                elif column == 'Date':
                    columnData[column].append(tweet.date)
                elif column == 'Tweet':
                    columnData[column].append(tweet.text)

        # Push all data into the database:
        db.InsertValues(tableName, "Research_Seminar_Project", columnData)

    def __PullTickers(self, tickerPath):
        """
        * Pull in all consumer discretionary tickers and other attributes from local file.
        Store Ticker -> ( Name, Sector, Weight) )
        """
        tickers = {}
        nameToTicker = {}
        classMatch = re.compile('Class [A-Z]')
        unassignedMatch = re.compile('unassigned', re.IGNORECASE)
        with open(tickerPath, 'r') as f:
            reader = csv.reader(f)
            atHeader = True
            # Map { Ticker -> (Name, Sector, Weight) }:
            # CSV Columns:
            # Name	Ticker	Identifier	SEDOL	Weight	Sector	Shares Held	Local Currency
            corpNum = 1
            for row in reader:
                if not atHeader:
                    name = row[0].strip()
                    name = classMatch.sub('', name).strip()
                    ticker = row[1].strip()
                    weight = row[4].strip()
                    sector = row[5].strip()
                    # Skip companies with unassigned sectors:
                    if unassignedMatch.search(sector):
                        continue
                    # If hit another share class for same company, then accumulate the weight in the index:
                    if name in nameToTicker.keys():
                        origTicker = nameToTicker[name]
                        tickers[origTicker] = (name, sector, str(float(weight) + float(tickers[origTicker][2])))
                    else:
                        tickers[ticker] = (name, sector, weight)
                        nameToTicker[name] = ticker
                    self.__CorpNumToTicker[corpNum] = ticker
                    self.__TickerToCorpNum[ticker] = corpNum
                    corpNum += 1
                atHeader = False

        self.Tickers = tickers

    def __InsertIntoCache(self, ticker, brand):
        """
        * Store information regarding pulled brands for corps in local cache
        to handle script stoppage issues.
        """
        val = SeminarProject.__cacheKeySig % (ticker, brand)
        cache.set(val, 30000)
    def __PullFromCache(self, ticker, brand):
        """
        * Pull all information regarding pulled brands for corp from cache.
        """
        val = SeminarProject.__cacheKeySig % (ticker, brand)
        result = cache.get(val)
        if result:
            self.__PulledBrands[ticker].append(brand)
            