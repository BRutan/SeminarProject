#################################################
# SeminarProject.py
#################################################
# * Object performs key steps in seminar project.

from TargetedWebScraping import BrandQuery, SubsidiaryQuery
from CorporateFiling import CorporateFiling, TableItem, DocumentType, PullingSteps, SoupTesting
from CorpDataPuller import CorpDataPuller
import csv
from DataBase import MYSQLDatabase
from datetime import datetime, date, timedelta
import gc
#import memcache
import nltk
from nltk.corpus import stopwords
from numpy.random import choice as choose
from pandas import DataFrame
from pandas.tseries import offsets
import re
from SentimentAnalyzer import SentimentAnalyzer
from PullTwitterData import TwitterPuller

class SeminarProject(object):
    """
    * Key objects/methods required for performing seminar project.
    """
    __utfSupport = 'CHARACTER SET utf8 COLLATE utf8_unicode_ci'
    __CorpTableColumns = {"CorpID" : ["int", True, ""], 
                                 "LongName" : ["text", False, ""], 
                                 "Ticker" : ["varchar(5)", False, ""], 
                                 "Sector" : ["text", False, ""], 
                                 "Region" : ["text", False, ""],
                                 "Currency" : ["text", False, ""],
                                 "Exchange" : ["text", False, ""],
                                 "ExchangeTimeZoneName" : ["text", False, ""],
                                 "SharesOutstanding" : ["bigint", False, ""],
                                 "BookValue" : ["float", False, ""],
                                 "MarketCap" : ["bigint", False, ""]
                                 }
    __CorpBrandTableColumns = {"CorpID" : ["int", False, "Corporations(CorpID)"], "Brands" : ["text " + __utfSupport, False, ""], 
                                    "AppDate" : ["Date", False, ""], "SubNum" : ["int", False, "Subsidiaries(Number)"]}
    __SubsidariesTableColumns = { "Number" : ["int", True, ""], "CorpID" : ["int", False, "Corporations(CorpID)"], "Subsidiaries" : ["text", False, ""]}
    __DataColumns = { "CorpID" : ["int", False, "Corporations(CorpID)"], "SearchTerm" : ["text", False, ""], 
                    "User" : ["text " + __utfSupport, False, ''], 
                    "Date" : ["date", False, ""], 
                    "Tweet" : ["text " + __utfSupport, False, ''], 
                    "Retweets" : ["int", False, ""], 
                    "SubNum" : ["int", False, "Subsidiaries(Number)"], "Coordinate" : ["Point", False, ""] }
    __HistoricalPriceCols = { 'CorpID' : ['int', False, 'Corporations(CorpID)'], 'Close' : ['float', False, ''], 
                                'Date' : ['Date', True, ''], 'Volume' : ['bigint', False, ''] }    
    __TweetTableSig = "tweets_%s"
    __PriceTableSig = "prices_%s"
    def __init__(self, tickerInputData, database, schema = None):
        """
        * Initialize new object.
        """
        self.TickersSearchAttrs = tickerInputData
        self.DB = database
        if schema:
            self.__schema = schema
        else:
            self.__schema = self.DB.ActiveSchema
        self.TickerToCorpID = {}
        self.TickerToTweetTable = {}
        self.TickerToReturnTable = {}
        
    #######################
    # Interface Methods:
    #######################
    def ExecuteAll(self):
        """
        * Execute all steps in sequential order.
        """
        self.CreateTables()
        self.InsertCorpAttributes()
        self.GetHistoricalData()
        self.GetSubsidiaries()
        self.GetBrands()
        self.GetTweets()
    
    def CreateTables(self):
        """
        * Create all tables to store relevant data for project. If ticker is specified, and does not exist
        as a table, then create returns and tweet table for ticker if not created already, and 
        add ticker to Corporations table.
        """
        db = self.DB
        # Skip creating tables if already created:
        if not db.TableExists("Corporations"):
            db.CreateTable("Corporations", SeminarProject.__CorpTableColumns, schema = self.__schema)
        if not db.TableExists("Subsidiaries"):
            db.CreateTable("Subsidiaries", SeminarProject.__SubsidariesTableColumns, schema = self.__schema)
        if not db.TableExists("CorporateBrands"):
            db.CreateTable("CorporateBrands", SeminarProject.__CorpBrandTableColumns, schema = self.__schema)
        
        # Create all Corporations tables:
        tweetColumns = SeminarProject.__DataColumns
        returnColumns = SeminarProject.__HistoricalPriceCols
        tweetTableSig = SeminarProject.__TweetTableSig
        priceTableSig = SeminarProject.__PriceTableSig
        # Create Tweets_{Ticker}, Returns_{Ticker} table for tweet and returns data for 
        # each corporation:
        for rowNum in range(0, len(self.TickersSearchAttrs)):
            ticker = self.TickersSearchAttrs['ticker'][rowNum].lower()
            # Create tweet data table:
            tableName = tweetTableSig % ticker.strip()
            self.TickerToTweetTable[ticker] = tableName
            if not db.TableExists(tableName):
                db.CreateTable(tableName, tweetColumns)
            # Create return data table:
            tableName = priceTableSig % ticker.strip()
            self.TickerToReturnTable[ticker] = tableName
            if not db.TableExists(tableName):
                db.CreateTable(tableName, returnColumns)

    def InsertCorpAttributes(self):
        """
        * Pull all corporate attributes for stored tickers or passed
        ticker.
        """
        results = self.DB.ExecuteQuery("SELECT CorpID, Ticker From Corporations", getResults=True, useDataFrame=True)
        tickers = set(self.TickersSearchAttrs['ticker'])
        maxCorpID = 0
        if (isinstance(results, DataFrame) and not results.empty) or (isinstance(results, dict) and results):
            # Determine which tickers already have information, skip pulling attributes for those tickers:
            maxCorpID = max(results['corpid'])
            for num, ticker in enumerate(results['ticker']):
                self.TickerToCorpID[ticker] = results['corpid'][num]
                if ticker in tickers:
                    tickers.remove(ticker)
        if tickers:
            corpID = maxCorpID + 1
            targetAttrs = [attr for attr in list(SeminarProject.__CorpTableColumns.keys()) if attr.lower() not in ['corpid', 'ticker']]
            puller = CorpDataPuller(targetAttrs)
            for ticker in tickers:
                columnData = { key.lower() : [] for key in SeminarProject.__CorpTableColumns }
                columnData['ticker'].append(ticker)
                columnData['corpid'].append(corpID)
                attrs = puller.GetAttributes(ticker)
                for attr in attrs:
                    columnData[attr].append(attrs[attr])
                self.DB.InsertValues('Corporations', columnData)
                self.TickerToCorpID[ticker] = corpID
                corpID = corpID + 1

    def GetSubsidiaries(self):
        """
        * Pull subsidiaries from each corporation's 10K, and load into 
        database. If already loaded subsidiaries into database then pull 
        using query.
        """
        db = self.DB
        tickerAttrs = self.TickersSearchAttrs
        yearEnd = datetime.today() + offsets.YearEnd()
        subs = re.compile('subsidiaries', re.IGNORECASE)
        nameRE = re.compile('name', re.IGNORECASE)
        steps = PullingSteps(False, True, False)
        query = SubsidiaryQuery()
        queryString = ['SELECT A.Ticker, B.Subsidiaries, B.Number FROM Corporations AS A']
        queryString.append('INNER JOIN Subsidiaries As B ON A.CorpID = B.CorpID WHERE B.Number IS NOT NULL;')
        queryString = ' '.join(queryString)
        results = db.ExecuteQuery(queryString, getResults = True, useDataFrame = True)
        maxSubNum = 1
        # Determine if pulled some/all subsidiaries already:
        if results:
            tickers = results['corporations.ticker']
            subs = results['subsidiaries.subsidiaries']
            subNums = results['subsidiaries.number']
            row = 0
            for ticker in tickers:
                lowered = ticker.lower()
                if lowered not in self.TickerToSubs:
                    self.TickerToSubs[lowered] = {}
                self.TickerToSubs[lowered][subs[row]] = subNums[row]
                row += 1
            maxSubNum = max(subNums) + 1
        
        if len(self.TickerToSubs.keys()) < len(self.Tickers.keys()):
            # Pull some subsidiaries from 10-Ks, if haven't been pulled in yet:    
            for ticker in self.Tickers.keys() if not ticker else [_ticker] if isinstance(_ticker, str) else _ticker:
                if ticker not in self.TickerToSubs:
                    doc = CorporateFiling(ticker, DocumentType.TENK, steps, date = yearEnd)
                    insertData = { 'CorpID' : [], 'Subsidiaries' : [], 'Number' : [] }
                    self.TickerToSubs[ticker] = {}
                    tableDoc, table = doc.FindTable(subs, False)
                    nameColumn = None
                    if table:
                        nameColumn = table.FindColumn(nameRE, False)
                    else:
                        # Search google for subsidiaries:
                        query.GetResults(self.Tickers[ticker][0])
                        for result in query.Results:
                            self.TickerToSubs[ticker][result] = maxSubNum
                            maxSubNum += 1
                    if not nameColumn is None:
                        for name in list(nameColumn):
                            self.TickerToSubs[ticker][name] = maxSubNum
                            maxSubNum += 1
                    # Add the corporation's name itself:
                    self.TickerToSubs[ticker][self.Tickers[ticker][0]] = maxSubNum
                    maxSubNum += 1
                    # Insert data into Subsidiaries table:
                    insertData['CorpID'] = [self.__TickerToCorpNum[ticker]] * len(self.TickerToSubs[ticker].keys())
                    insertData['Subsidiaries'] = MYSQLDatabase.RemoveInvalidChars([val for val in self.TickerToSubs[ticker].keys()])
                    insertData['Number'] = [self.TickerToSubs[ticker][name] for name in self.TickerToSubs[ticker].keys()]
                    db.InsertValues("Subsidiaries", insertData)
                    
    def GetBrands(self):
        """
        * Pull all brands from WIPO website, push into database.
        """
        # Determine if brands were already loaded for each corporation:
        db = self.DB
        query = ['SELECT A.ticker, B.brands, B.appdate, B.subnum FROM corporations as A INNER JOIN corporatebrands as B']
        query.append(' on A.corpid = B.corpid WHERE B.brands IS NOT NULL;')
        query = ''.join(query)
        results = db.ExecuteQuery(query, getResults = True)
        if results:
            tickers = results['corporations.ticker']
            brands = results['corporatebrands.brands']
            appdates = results['corporatebrands.appdate']
            subnums = results['corporatebrands.subnum']
            row = 0
            _brands = []
            _appdates = []
            _subnums = []
            while row < len(tickers):
                ticker = tickers[row]
                lowered = ticker.lower()
                while row < len(tickers) and ticker in tickers[row]:
                    _brands.append(brands[row])
                    _appdates.append(appdates[row])
                    _subnums.append(subnums[row])
                    row += 1
                # Map Ticker -> ([Brands], [AppDates], [SubNums]):
                self.TickerToBrands[lowered] = (_brands, _appdates, _subnums)
                _brands = []
                _appdates = []
                _subnums = []
        # Pull all brands from WIPO database website:
        if len(self.TickerToBrands.keys()) < len(self.Tickers.keys()):
            for ticker in self.Tickers.keys() if not _ticker else [_ticker] if isinstance(_ticker, str) else _ticker:
                if ticker not in self.TickerToBrands:
                    query = BrandQuery()
                    insertValues = {}
                    subsidiaries = self.TickerToSubs[ticker]
                    brands = query.PullBrands(subsidiaries)
                    # Add the company name itself as a brand:
                    corpName = self.Tickers[ticker][0]
                    if corpName not in brands:
                        brands[corpName] = (datetime(year=1900, month=1, day=1).strftime('%Y-%m-%d'), corpName)
                    insertValues['corpid'] = [self.__TickerToCorpNum[ticker]] * len(brands.keys())
                    insertValues['brands'] = MYSQLDatabase.RemoveInvalidChars(list(brands.keys()))
                    insertValues['appdate'] = MYSQLDatabase.RemoveInvalidChars([re.sub('[^0-9\-]','', brands[key][0]) for key in brands.keys()])
                    insertValues['subnum'] = [self.TickerToSubs[ticker][brands[key][1]] for key in brands.keys()]
                    # Push brands into the mysql database:
                    db.InsertInChunks("corporatebrands", insertValues, 10, skipExceptions = True)
                    if ticker not in self.TickerToBrands:
                        self.TickerToBrands[ticker] = (insertValues['brands'], insertValues['appdate'], insertValues['subnum'])
                    else:
                        self.TickerToBrands[ticker][0].extend(insertValues['brands'])
                        self.TickerToBrands[ticker][1].extend(insertValues['appdate'])
                        self.TickerToBrands[ticker][2].extend(insertValues['subnum'])

    def GetTweets(self, ticker = None, toptweets = False):
        """
        * Randomly sample all tweets and insert into associated table in schema.
        """
        args = {}
        args['since'] = self.StartDate
        args['until'] = self.EndDate
        args['interDaySampleSize'] = 50
        args['termSampleSize'] = self.__termSampleSize if self.__termSampleSize else 100
        args['dateStep'] = self.__dateStep if self.__dateStep else 1
        tickersToSearchTerms = {}
        insertValues = {}
        puller = TwitterPuller()
        # Determine which companies have already been sampled, if getting tweets for all companies:
        query = ['SELECT A.Name, B.SearchTerm FROM Corporations AS A INNER JOIN ', '', ' AS B ON A.CorpID = B.CorpID WHERE B.SearchTerm IS NOT NULL;']
        db = self.DB
        for ticker in self.TickerToTweetTable.keys():
            table = self.TickerToTweetTable[ticker].lower()
            query[1] = table
            results = db.ExecuteQuery(''.join(query), getResults = True)
            if results and len(results.keys()) > 0 and len(results[table + '.searchterm']) > 0:
                tickersToSearchTerms[ticker] = {term.lower() : True for term in results[table + '.searchterm']}
        # Pull tweets for all corporations that haven't been sampled already:
        for ticker in self.TickerToBrands.keys() if not _ticker else [_ticker] if isinstance(_ticker, str) else _ticker:
            if ticker not in tickersToSearchTerms:
                tickersToSearchTerms[ticker] = {}
            table = self.TickerToTweetTable[ticker]
            corpID = self.__TickerToCorpNum[ticker]
            # Skip all brands that were trademarked after the analysis start date, 
            # or are short or commond words:
            vals = self.__FilterAndSampleSearchTerms(tickersToSearchTerms[ticker], self.TickerToBrands[ticker], args['termSampleSize'])
            args['searchTerms'] = [val[0] for val in vals]
            # Randomly sample tweets based upon args:
            for num, term in enumerate(args['searchTerms']):
                puller.PullTweetsAndInsert(args, corpID, sub, table, term, db, self.__pullTopTweets, numTweets = args['interDaySampleSize'])
                tickersToSearchTerms[ticker][term] = True

    def GetHistoricalData(self):
        """
        * Get historical data for all tickers for date range specified in file.
        """
        priceTable = SeminarProject.__PriceTableSig
        skipCols = ['corpid', 'date']
        priceTypes = [key.lower() for key in SeminarProject.__HistoricalPriceCols if key.lower() not in skipCols]
        puller = CorpDataPuller(priceTypes = priceTypes)
        tickerToPeriod = {}
        for row in range(0, len(self.TickersSearchAttrs)):
            row = self.TickersSearchAttrs.iloc[row]
            table = priceTable % row['ticker']
            if not self.DB.TableExists(table):
                tickerToPeriod[row['ticker']] = (row['startdate'], row['enddate'])
            else:
                results = self.DB.ExecuteQuery(''.join(["SELECT * FROM ", table]), getResults = True, useDataFrame = True)
                start = row['startdate']
                end = row['enddate']    
                if (isinstance(results, DataFrame) and not results.empty) or (not isinstance(results, DataFrame) and results):
                    earliest = min(results['date'])
                    latest = max(results['date'])
                    # Determine which period to use:
                    if not (start >= earliest and end <= latest):
                        if end >= earliest:
                            days = (earliest - end).days - 1
                            end += timedelta(days=days)
                        if start <= latest:
                            days = (latest - start).days + 1
                            start += timedelta(days=days)
                tickerToPeriod[row['ticker']] = (start, end)
                    
        # Pull return data and insert into database:
        for ticker in tickerToPeriod:
            table = priceTable % ticker
            start = tickerToPeriod[ticker][0]
            end = tickerToPeriod[ticker][1]
            prices = puller.GetAssetPrices(ticker, start, end)
            colData = { key.lower() : [] for key in SeminarProject.__HistoricalPriceCols }
            colData['corpid'] = [self.TickerToCorpID[ticker]] * len(prices)
            for key in prices:
                colData[key.lower()] = list(prices[key])
            colData['date'] = list(prices.index)
            self.DB.InsertValues(table, colData)
            

        
    ########################
    # Private Helpers:
    ########################
    def __FilterAndSampleSearchTerms(self, existingSearchTerms, newSearchTerms, sampleSize):
        """
        * Filter out new searchterms to query with based upon existing search
        terms.
        """
        output = []
        # Normalize the existing search terms:
        existingSearchTerms = { re.sub('[^\w\d]', '', term.lower()) : True for term in existingSearchTerms.keys() }
        searchTerms = newSearchTerms[0]
        appdates = newSearchTerms[1]
        subnums = newSearchTerms[2]
        # Filter out already sampled search terms, simple words etc:
        for row in range(0, len(searchTerms)):
            term = searchTerms[row]
            lowered = re.sub('[^\w\d]', '', term.lower())
            appdate = appdates[row]
            subnum = subnums[row]
            if len(term) > 2 and lowered not in existingSearchTerms and lowered not in SeminarProject.__stopWords\
                and appdate <= self.StartDate.date():
                output.append((term, appdate, subnum))
                existingSearchTerms[term] = True
        # Randomly sample:
        sampleSize = min(len(output), sampleSize)
        indices = choose(range(0, len(output)), sampleSize, replace = False)
        return [output[i] for i in indices]
    
    def __DownloadStopWords(self):
        """
        * Download stopwords if necessary.
        """
        nltk.download('stopwords')
        #if not os.path.exists('C:\\Users\\rutan\\AppData\\Roaming\\nltk_data\\corpora\\stopwords\\'):
        #    nltk.download('stopwords')
    #######################
    # Deprecated:
    #######################
    def __InsertIntoCache(self, ticker, brand):
        """
        * Store information regarding pulled brands for corps in local cache
        to handle script stoppage issues.
        """
        val = SeminarProject.__cacheKeySig % (ticker, brand)
        #cache.set(val, 30000)
    def __PullFromCache(self, ticker, brand):
        """
        * Pull all information regarding pulled brands for corp from cache.
        """
        val = SeminarProject.__cacheKeySig % (ticker, brand)
        self.__PulledBrands = {}
        #result = cache.get(val)
        if result:
            self.__PulledBrands[ticker].append(brand)
            