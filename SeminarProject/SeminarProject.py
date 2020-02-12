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
import nltk
from nltk.corpus import stopwords
import numpy as np
from numpy.random import choice as choose
from pandas import DataFrame, concat
import pickle
import re
from SentimentAnalyzer import SentimentAnalyzer
from PullTwitterData import TwitterPuller
import warnings

warnings.filterwarnings("ignore")

def NumpyDTtoDT(dt64):
    dt64 = (dt64 - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's')
    return datetime.utcfromtimestamp(dt64)

class SeminarProject(object):
    """
    * Key objects/methods required for performing seminar project.
    """
    __utfSupport = 'CHARACTER SET utf8 COLLATE utf8_unicode_ci'
    __CorpTableColumns = {"CorpID" : ["int", True, ""], 
                                 "LongName" : ["text", False, ""], 
                                 "Ticker" : ["varchar(5)", False, ""], 
                                 "Sector" : ["text", False, ""], 
                                 "Industry" : ['text', False, ""],
                                 "Region" : ["text", False, ""],
                                 "Currency" : ["text", False, ""],
                                 "Exchange" : ["text", False, ""],
                                 "ExchangeTimeZoneName" : ["text", False, ""],
                                 "SharesOutstanding" : ["bigint", False, ""],
                                 "BookValue" : ["float", False, ""],
                                 "MarketCap" : ["bigint", False, ""]
                                 }
    __CorpBrandTableColumns = {"CorpID" : ["int", False, "Corporations(CorpID)"], "Brands" : ["text " + __utfSupport, False, ""], 
                                    "AppDate" : ["Date", False, ""]}
    __SubsidariesTableColumns = { "Number" : ["int", True, ""], "CorpID" : ["int", False, "Corporations(CorpID)"], "Subsidiaries" : ["text", False, ""]}
    __TweetDataColumns = { "CorpID" : ["int", False, "Corporations(CorpID)"], "SearchTerm" : ["text", False, ""], 
                    "User" : ["text " + __utfSupport, False, ''], 
                    "Date" : ["date", False, ""], 
                    "Tweet" : ["text " + __utfSupport, False, ''],
                    'TweetID' : ['bigint', False, ''],
                    "Retweets" : ["int", False, ""], "GeoCode" : ["Point", False, ""] }
    __HistoricalPriceCols = { 'CorpID' : ['int', False, 'Corporations(CorpID)'], 'Close' : ['float', False, ''], 
                                'Date' : ['Date', False, ''], 'Volume' : ['bigint', False, ''] }    
    __RetweetDistTableCols = { 'CorpID' : ['int', False, 'Corporations(CorpID)'], 'TweetID' : ['bigint', False, ''], 'Date' : ['Date', False, ''], 'Min' : ['int', False, ''],
                              'Max' : ['int', False, ''], 'Mean' : ['double', False, ''], 'Var' : ['double', False, ''], 'Median' : [ 'double', False, ''],
                              'Skew' : ['double', False, ''], 'Kurt' : ['double', False, ''] }
    __AllRetweetDataColumns = {'TweetID' : ['bigint', False, ''], 'Retweets' : ['int', False, ''] }
    __PickleFolder = "//pickle//"
    __tableToColumns = {'corporations' : __CorpTableColumns, 'corporatebrands' : __CorpBrandTableColumns, 'tweetdata' : __TweetDataColumns, 
                        'subsidiaries' : __SubsidariesTableColumns, 'prices' : __HistoricalPriceCols, 'retweetdata' : __AllRetweetDataColumns }
    def __init__(self, tickerInputData, database, schema = None):
        """
        * Initialize new object.
        """
        self.TickersSearchAttrs = tickerInputData.set_index('ticker')
        self.DB = database
        if schema:
            self.__schema = schema
        else:
            self.__schema = self.DB.ActiveSchema
        attrColumns = [col.lower() for col in SeminarProject.__CorpTableColumns.keys()]
        attrColumns.append('tweettable')
        attrColumns.append('pricetable')
        self.TickerToCorpAttribute = None
        self.SubsidiariesAttributes = None
        self.BrandAttributes = None
        
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
        # Create Tweets_{Ticker}, Returns_{Ticker} table for tweet and returns data for 
        # each corporation:

        # Testing:
        for table in SeminarProject.__tableToColumns:
            if not db.TableExists(table):
                db.CreateTable(table, SeminarProject.__tableToColumns[table])
        # Skip creating tables if already created:
        #if not db.TableExists("Corporations"):
        #    db.CreateTable("Corporations", SeminarProject.__CorpTableColumns, schema = self.__schema)
        #if not db.TableExists("Subsidiaries"):
        #    db.CreateTable("Subsidiaries", SeminarProject.__SubsidariesTableColumns, schema = self.__schema)
        #if not db.TableExists("CorporateBrands"):
        #    db.CreateTable("CorporateBrands", SeminarProject.__CorpBrandTableColumns, schema = self.__schema)
        
        # Create all Corporations tables:

        #tweetColumns = SeminarProject.__TweetDataColumns
        #returnColumns = SeminarProject.__HistoricalPriceCols
        #tweetTableSig = SeminarProject.__TweetTableSig
        #priceTableSig = SeminarProject.__PriceTableSig
        #for rowNum in range(0, len(self.TickersSearchAttrs)):
        #    ticker = self.TickersSearchAttrs.index[rowNum].lower()
            # Create tweet data table:
        #    tableName = tweetTableSig % ticker.strip()
        #    if not db.TableExists(tableName):
        #        db.CreateTable(tableName, tweetColumns)
        #    # Create return data table:
        #    tableName = priceTableSig % ticker.strip()
        #    if not db.TableExists(tableName):
        #        db.CreateTable(tableName, returnColumns)
        # Create AllRetweetData table storing all retweet frequencies for selected tweets:
        #if not db.TableExists("RetweetData"):
        #    db.CreateTable("RetweetData", SeminarProject.__AllRetweetDataColumns)

    def InsertCorpAttributes(self):
        """
        * Pull all corporate attributes for stored tickers or passed
        ticker.
        """
        results = self.DB.ExecuteQuery("SELECT * From Corporations", getResults=True, useDataFrame=True)
        tickers = set(self.TickersSearchAttrs.index)
        maxCorpID = 0
        # Determine which tickers already have information, skip pulling attributes for those tickers:
        if not results is None and not results.empty:
            maxCorpID = max(results['corpid'])
            tickers -= set(results['ticker']) 
        self.TickerToCorpAttribute = results if not results is None else DataFrame(columns = [col.lower() for col in SeminarProject.__CorpTableColumns])
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
                self.TickerToCorpAttribute = concat([self.TickerToCorpAttribute, DataFrame(columnData, columns=columnData.keys())], axis=0)
                corpID += 1
        self.TickerToCorpAttribute = self.TickerToCorpAttribute.set_index('ticker')

    def GetSubsidiaries(self):
        """
        * Pull subsidiaries from each corporation's 10K, and load into 
        database. If already loaded subsidiaries into database then pull 
        using query.
        """
        db = self.DB
        toPull = set(self.TickersSearchAttrs.index)
        # Skip pulling subsidiaries if skipping brand pulling for companies:
        toPull -= set(self.TickersSearchAttrs.loc[self.TickersSearchAttrs['overridebrands'] == True].index)
        if not toPull:
            return
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
        if not results is None and not results.empty:
            self.SubsidiariesAttributes = results
            self.SubsidiariesAttributes = self.SubsidiariesAttributes.rename(columns= {col : col[col.index('.') + 1:].lower() for col in self.SubsidiariesAttributes.columns})
            tickers = set(results['corporations.ticker'])
            toPull -= tickers
            maxSubNum = max(results['subsidiaries.number']) + 1
        else:
            cols = [col.lower() for col in SeminarProject.__SubsidariesTableColumns]
            cols.append('ticker')
            self.SubsidiariesAttributes = DataFrame(columns=cols)
        # Pull subsidiaries from 10-Ks, if haven't been pulled in yet:    
        for ticker in toPull:
            corpName = self.TickerToCorpAttribute.loc[self.TickerToCorpAttribute.index == ticker]['longname'].values[0]
            corpId = self.TickerToCorpAttribute.loc[self.TickerToCorpAttribute.index == ticker]['corpid'].values[0]
            targetDate = self.TickersSearchAttrs.loc[self.TickersSearchAttrs.index == ticker]['startdate'].values[0]
            insertData = {col.lower() : [] for col in SeminarProject.__SubsidariesTableColumns.keys()}
            results = []
            nameColumn = None
            tableDoc, table = (None, None)
            try:
                doc = CorporateFiling(ticker, DocumentType.TENK, steps, date = targetDate)
                tableDoc, table = doc.FindTable(subs, False)
            except:
                pass
            if table:
                nameColumn = table.FindColumn(nameRE, False)
            if nameColumn is None:
                # Search google for subsidiaries:
                query.GetResults(corpName)
                results = query.Results
            else:
                results = list(nameColumn)
            # Add company itself as a subsidiary:
            results.append(corpName)
            for result in results:
                insertData['corpid'].append(corpId)
                insertData['subsidiaries'].append(result)
                insertData['number'].append(maxSubNum)
                maxSubNum += 1
            # Insert data into Subsidiaries table:
            db.InsertValues("subsidiaries", insertData)
            insertData['ticker'] = [ticker] * len(insertData['corpid'])
            self.SubsidiariesAttributes = concat([self.SubsidiariesAttributes, DataFrame(insertData, columns=insertData.keys())], axis=0)
        self.SubsidiariesAttributes = self.SubsidiariesAttributes.set_index('ticker')
            
    def GetBrands(self):
        """
        * Pull all brands from WIPO website, push into database.
        """
        # Determine if brands were already loaded for each corporation:
        db = self.DB
        toPull = set(self.TickersSearchAttrs.index)
        toPull -= set(self.TickersSearchAttrs.loc[self.TickersSearchAttrs['overridebrands'] == True].index)
        if not toPull:
            return
        query = ['SELECT A.ticker, B.appdate, B.brands FROM corporations as A INNER JOIN corporatebrands as B']
        query.append(' on A.corpid = B.corpid WHERE B.brands IS NOT NULL;')
        query = ''.join(query)
        results = db.ExecuteQuery(query, getResults = True, useDataFrame = True)
        if not results is None and not results.empty:
            self.BrandAttributes = results
            self.BrandAttributes = self.BrandAttributes.rename(columns = {col : col[col.index('.') + 1:] for col in self.BrandAttributes.columns})
            pulledTickers = set([ticker.lower() for num, ticker in enumerate(results['corporations.ticker'])])
            toPull -= pulledTickers
        else:
            cols = [col.lower() for col in SeminarProject.__CorpBrandTableColumns]
            cols.append('ticker')
            self.BrandAttributes = DataFrame(columns=cols)
        # Skip pulling brands for all companies that were specified as t in overridebrands column:
        overrideTickers = self.TickersSearchAttrs.loc[self.TickersSearchAttrs['overridebrands'] == True].index
        toPull -= set(overrideTickers)
        # Pull all brands from WIPO database website:
        for ticker in toPull:
            query = BrandQuery()
            corpId = self.TickerToCorpAttribute.loc[self.TickerToCorpAttribute.index == ticker]['corpid'].values[0]
            insertValues = { col.lower() : [] for col in SeminarProject.__CorpBrandTableColumns }
            subInfo = self.SubsidiariesAttributes.loc[self.SubsidiariesAttributes.index == ticker]
            subInfo = subInfo['subsidiaries'].values
            brands = query.PullBrands(subInfo)
            insertValues['corpid'] = [corpId] * len(brands.keys())
            insertValues['brands'] = MYSQLDatabase.RemoveInvalidChars(list(brands.keys()))
            insertValues['appdate'] = MYSQLDatabase.RemoveInvalidChars([re.sub('[^0-9\-]','', brands[key][0]) for key in brands.keys()])
            # Push brands into the mysql database:
            db.InsertInChunks("corporatebrands", insertValues, 5, skipExceptions = True)
            insertValues['ticker'] = [ticker] * len(insertValues['corpid'])
            self.BrandAttributes = concat([self.BrandAttributes, DataFrame(insertValues, columns = insertValues.keys())], axis=0)
        self.BrandAttributes = self.BrandAttributes.set_index('ticker')

    def GetTweets(self):
        """
        * Pull tweets from Twitter using GoT library.
        """
        insertValues = {}
        puller = TwitterPuller()
        # Determine which companies have already been sampled:
        query = ['SELECT B.SearchTerm FROM Corporations AS A INNER JOIN tweetdata AS B ON A.CorpID = B.CorpID WHERE B.SearchTerm IS NOT NULL AND A.Ticker = "', '', '"']
        db = self.DB
        toPull = set(self.TickerToCorpAttribute.index)
        existingSearch = DataFrame(columns=['ticker', 'searchterm'])
        table = "tweetdata"
        for ticker in toPull:
            query[1] = ticker
            results = db.ExecuteQuery(''.join(query), getResults = True)
            if results:
                terms = set(results[table + '.searchterm'])
                terms = { 'searchterm' : list(terms), 'ticker' : [ticker] * len(terms) }
                existingSearch = concat([existingSearch, DataFrame.from_dict(terms)], axis = 0)
        existingSearch = existingSearch.set_index('ticker')
        # Pull tweets for all corporations that haven't been sampled already:
        for ticker in toPull:
            args = {}
            skipBrands = self.TickersSearchAttrs.loc[self.TickersSearchAttrs.index == ticker]['overridebrands'].values[0]
            row = self.TickersSearchAttrs.loc[self.TickersSearchAttrs.index == ticker]
            corpId = self.TickerToCorpAttribute.loc[self.TickerToCorpAttribute.index == ticker]['corpid'].values[0]
            addlSearch = row['addlsearchterms'].values[0]
            args['since'] = NumpyDTtoDT(row['startdate'].values[0])
            args['until'] = NumpyDTtoDT(row['enddate'].values[0])
            args['periodSampleSize'] = row['periodsamplesize'].values[0]
            args['dateStep'] = row['daystep'].values[0]
            args['topTweets'] = row['toptweets'].values[0]
            args['corpid'] = corpId
            args['ticker'] = ticker
            brands = self.BrandAttributes.loc[self.BrandAttributes.index == ticker][['brands','appdate']] if not skipBrands else []
            # Skip pulling if pulled using a sufficient number of search terms:
            if not skipBrands and row['numbrands'].values[0] == len(existingSearch.loc[existingSearch.index == ticker]['searchterm']):
                continue
            # Skip all brands that were trademarked after the analysis start date, 
            # or are short or commond words:
            pulledTerms = existingSearch.loc[existingSearch.index == ticker]['searchterm'].values 
            sampleSize = row['numbrands'].values[0] - len(addlSearch)
            sampled = self.__FilterAndSampleSearchTerms(pulledTerms,brands,sampleSize, args['since']) if not skipBrands else []
            args['searchTerms'] = sampled if not skipBrands else []
            # Append custom search terms if included in file:
            args['searchTerms'].extend(addlSearch)
            # Pull all tweets:
            puller.PullTweetsAndInsert(args, "tweetdata", db)

    def GetHistoricalData(self):
        """
        * Get historical data for all tickers for date range specified in file.
        """
        priceTable = "prices"
        if not self.DB.TableExists(priceTable):
            self.DB.CreateTable(priceTable, SeminarProject.__HistoricalPriceCols)
        skipCols = ['corpid', 'date']
        priceTypes = [key.lower() for key in SeminarProject.__HistoricalPriceCols if key.lower() not in skipCols]
        puller = CorpDataPuller(priceTypes = priceTypes)
        tickerToPeriod = {}
        for row in range(0, len(self.TickersSearchAttrs)):
            row = self.TickersSearchAttrs.iloc[row]
            start = datetime.combine(row['startdate'], datetime.min.time())
            end = datetime.combine(row['enddate'], datetime.min.time())
            ticker = row.name
            results = self.DB.ExecuteQuery(''.join(["SELECT MAX(Date) as max, MIN(Date) as min FROM ", priceTable]), getResults = True)
            if results and results['max'][0] and results['min'][0]:
                earliest = datetime.combine(results['min'][0], datetime.min.time())
                latest = datetime.combine(results['max'][0], datetime.min.time())
                # Determine which period to use:
                if not (start >= earliest and end <= latest):
                    if start < earliest and end >= earliest:
                        days = (earliest - end).days - 1
                        end += timedelta(days=days)
                    elif end > latest and start < latest:
                        days = (latest - end).days - 1
                        start += timedelta(days=days)
                    tickerToPeriod[ticker] = (start, end)
            else:
                tickerToPeriod[ticker] = (start, end)
        # Pull return data and insert into database:
        for ticker in tickerToPeriod:
            start = tickerToPeriod[ticker][0]
            end = tickerToPeriod[ticker][1]
            if start == end:
                continue
            corpId = self.TickerToCorpAttribute.loc[self.TickerToCorpAttribute.index == ticker]['corpid'].values[0]
            prices = puller.GetAssetPrices(ticker, start, end)
            if not isinstance(prices, DataFrame):
                continue
            colData = { key.lower() : [] for key in SeminarProject.__HistoricalPriceCols }
            colData['corpid'] = [corpId] * len(prices)
            for key in prices:
                colData[key.lower()] = list(prices[key])
            colData['date'] = list(prices.index)
            self.DB.InsertInChunks(priceTable, colData, 1, skipExceptions=True)
        
    ########################
    # Private Helpers:
    ########################
    @staticmethod
    def __ReadPickle(dfName):
        """
        * Read serialized object from local pickle folder folder.
        """
        path = ''.join([SeminarProject.__PickleFolder, dfName, '.pickle'])
        with open(path, 'rb') as f:
            return pickle.load(f)
    @staticmethod
    def __DumpPickle(df, dfName):
        """
        * Dump dataframe to pickle file for easier pulling.
        """
        path = ''.join([SeminarProject.__PickleFolder, dfName, '.pickle'])
        with open(path, 'wb') as f:
            pickle.dump(df, f)

    def __FilterAndSampleSearchTerms(self, existingSearchTerms, newSearchAttributes, sampleSize, pullDate):
        """
        * Filter out new searchterms to query with based upon existing search
        terms.
        """
        # Normalize the existing search terms:
        existingSearchTerms = { re.sub('[^\w\d]', '', term.lower()) : True for term in existingSearchTerms }
        # Filter out already sampled search terms, simple words etc:
        filtered = [re.sub('[^\w\d]', '', row[0].lower()) for row in newSearchAttributes.values if row[0] not in existingSearchTerms and row[1] <= pullDate.date()]
        # Randomly sample:
        sampleSize = min(len(filtered), sampleSize)
        indices = choose(range(0, len(filtered)), sampleSize, replace = False)
        return [filtered[i] for i in indices]
    
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
            