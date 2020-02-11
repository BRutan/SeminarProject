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
from numpy.random import choice as choose
from pandas import DataFrame, concat
import pickle
import re
from SentimentAnalyzer import SentimentAnalyzer
from PullTwitterData import TwitterPuller
import warnings

warnings.filterwarnings("ignore")

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
    __DataColumns = { "CorpID" : ["int", False, "Corporations(CorpID)"], "SearchTerm" : ["text", False, ""], 
                    "User" : ["text " + __utfSupport, False, ''], 
                    "Date" : ["date", False, ""], 
                    "Tweet" : ["text " + __utfSupport, False, ''], 
                    "Retweets" : ["int", False, ""], "Coordinate" : ["Point", False, ""] }
    __HistoricalPriceCols = { 'CorpID' : ['int', False, 'Corporations(CorpID)'], 'Close' : ['float', False, ''], 
                                'Date' : ['Date', True, ''], 'Volume' : ['bigint', False, ''] }    
    __TweetTableSig = "tweets_%s"
    __PriceTableSig = "prices_%s"
    __PickleFolder = "//pickle//"
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
            ticker = self.TickersSearchAttrs.index[rowNum].lower()
            # Create tweet data table:
            tableName = tweetTableSig % ticker.strip()
            if not db.TableExists(tableName):
                db.CreateTable(tableName, tweetColumns)
            # Create return data table:
            tableName = priceTableSig % ticker.strip()
            if not db.TableExists(tableName):
                db.CreateTable(tableName, returnColumns)

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
        subs = re.compile('subsidiaries', re.IGNORECASE)
        nameRE = re.compile('name', re.IGNORECASE)
        steps = PullingSteps(False, True, False)
        query = SubsidiaryQuery()
        queryString = ['SELECT A.Ticker, B.Subsidiaries, B.Number FROM Corporations AS A']
        queryString.append('INNER JOIN Subsidiaries As B ON A.CorpID = B.CorpID WHERE B.Number IS NOT NULL;')
        queryString = ' '.join(queryString)
        results = db.ExecuteQuery(queryString, getResults = True, useDataFrame = True)
        maxSubNum = 1
        toPull = set(self.TickersSearchAttrs.index)
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
        query = ['SELECT A.ticker, B.appdate, B.brands FROM corporations as A INNER JOIN corporatebrands as B']
        query.append(' on A.corpid = B.corpid WHERE B.brands IS NOT NULL GROUP BY A.ticker;')
        query = ''.join(query)
        toPull = set(self.TickersSearchAttrs.index)
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

    def GetTweets(self, toptweets = False):
        """
        * Pull tweets from Twitter using GoT library.
        """
        insertValues = {}
        puller = TwitterPuller()
        # Determine which companies have already been sampled:
        query = ['SELECT B.SearchTerm FROM Corporations AS A INNER JOIN ', '', ' AS B ON A.CorpID = B.CorpID WHERE B.SearchTerm IS NOT NULL;']
        db = self.DB
        toPull = set(self.TickerToCorpAttribute.index)
        for ticker in toPull:
            table = SeminarProject.__TweetTableSig % ticker.lower()
            query[1] = table
            results = db.ExecuteQuery(''.join(query), getResults = True)
            if results:
                toPull.remove(ticker)
        # Pull tweets for all corporations that haven't been sampled already:
        for ticker in toPull:
            args = {}
            row = self.TickersSearchAttrs.loc[self.TickersSearchAttrs.index == ticker]
            corpId = self.TickerToCorpAttribute.loc[self.TickerToCorpAttribute.index == ticker]['corpid'].values[0]
            addlSearch = row['addlsearchterms'].values[0].split(',')
            args['since'] = row['startdate'].values[0]
            args['until'] = row['enddate'].values[0]
            args['interDaySampleSize'] = 50
            args['termSampleSize'] = row['numbrands'].values[0]
            args['dateStep'] = row['daystep'].values[0]
            table = SeminarProject.__TweetTableSig % ticker.lower()
            # Skip all brands that were trademarked after the analysis start date, 
            # or are short or commond words:
            vals = self.__FilterAndSampleSearchTerms(, , )
            args['searchTerms'] = [val[0] for val in vals]
            # Append custom search terms if included in file:
            args['searchTerms'].extend(addlSearch)
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
            ticker = row.name
            table = priceTable % ticker
            if not self.DB.TableExists(table):
                self.DB.CreateTable(table, SeminarProject.__HistoricalPriceCols)
                tickerToPeriod[ticker] = (row['startdate'], row['enddate'])
            else:
                results = self.DB.ExecuteQuery(''.join(["SELECT * FROM ", table]), getResults = True, useDataFrame = True)
                start = datetime.combine(row['startdate'], datetime.min.time())
                end = datetime.combine(row['enddate'], datetime.min.time())
                if not results is None and not results.empty:
                    earliest = datetime.combine(min(results['date']), datetime.min.time())
                    latest = datetime.combine(max(results['date']), datetime.min.time())
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
            table = priceTable % ticker
            corpId = self.TickerToCorpAttribute.loc[self.TickerToCorpAttribute.index == ticker]['corpid'].values[0]
            start = tickerToPeriod[ticker][0]
            end = tickerToPeriod[ticker][1]
            prices = puller.GetAssetPrices(ticker, start, end)
            if not isinstance(prices, DataFrame):
                continue
            colData = { key.lower() : [] for key in SeminarProject.__HistoricalPriceCols }
            colData['corpid'] = [corpId] * len(prices)
            for key in prices:
                colData[key.lower()] = list(prices[key])
            colData['date'] = list(prices.index)
            self.DB.InsertInChunks(table, colData, 1, skipExceptions=True)
        
    ########################
    # Private Helpers:
    ########################
    def __ReadPickle(self, dfName):
        """
        * Read serialized object from local pickle folder folder.
        """
        path = ''.join([SeminarProject.__PickleFolder, dfName, '.pickle'])
        with open(path, 'rb') as f:
            return pickle.load(f)

    def __DumpPickle(self, df, dfName):
        """
        * Dump dataframe to pickle file for easier pulling.
        """
        path = ''.join([SeminarProject.__PickleFolder, dfName, '.pickle'])
        with open(path, 'wb') as f:
            pickle.dump(df, f)

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
            