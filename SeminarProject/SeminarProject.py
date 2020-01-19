#################################################
# SeminarProject.py
#################################################
# * Object performs key steps in seminar project.

from TargetedWebScraping import BrandQuery, SubsidiaryQuery
from CorporateFiling import CorporateFiling, TableItem, DocumentType, PullingSteps, SoupTesting
from GetTweets import TweetPuller, Tweet
import csv
from DataBase import MYSQLDatabase
from datetime import datetime, timedelta
import gc
#import memcache
import nltk
from nltk.corpus import stopwords
from numpy.random import choice as choose
from pandas.tseries import offsets
import re
from PullTwitterData import TwitterPuller

class SeminarProject(object):
    """
    * Key objects required for performing seminar project.
    """
    __utfSupport = 'CHARACTER SET utf8 COLLATE utf8_unicode_ci'
    __cacheKeySig = "{Corp:%s}{Brand:%s}"
    __stopWords = { re.sub('[^a-z]', '', word.lower()) : True for word in set(stopwords.words('english')) }
    def __init__(self, startDate, endDate, tickerPath, database):
        """
        * Initialize new object.
        """
        self.StartDate = startDate
        self.EndDate = endDate
        self.DB = database
        self.Tickers = {}
        # Get tickers from CSV file at tickerPath:
        self.__CorpNumToTicker = {}
        self.__TickerToCorpNum = {}
        self.__PullTickers(tickerPath)
        self.CorpTableColumns = {"CorpID" : ["int", True, ""], "Name" : ["text", False, ""], 
                                 "Ticker" : ["varchar(5)", False, ""], "Industry" : ["text", False, ""], "Weight" : ["float", False, ""] }
        self.CorpBrandTableColumns = {"CorpID" : ["int", False, "Corporations(CorpID)"], "Brands" : ["text " + SeminarProject.__utfSupport, False, ""], 
                                      "AppDate" : ["Date", False, ""], "SubNum" : ["int", False, "Subsidiaries(Number)"]}
        self.SubsidariesTableColumns = {"Number" : ["int", True, ""], "CorpID" : ["int", False, "Corporations(CorpID)"], "Subsidiaries" : ["text", False, ""]}
        self.DataColumns = { "CorpID" : ["int", False, "Corporations(CorpID)"], "SearchTerm" : ["text", False, ""], 
                       "User" : ["text " + SeminarProject.__utfSupport, False, ''], 
                       "Date" : ["date", False, ""], 
                       "Tweet" : ["text " + SeminarProject.__utfSupport, False, ''], 
                       "Retweets" : ["int", False, ""], 
                       "SubNum" : ["int", False, "Subsidiaries(Number)"] }
        self.HistoricalPriceCols = { 'CorpID' : ['int', False, 'Corporations(CorpID)'], 'Adj_Close' : ['float', False, ''], 'Date' : ['Date', False, ''] }    

        self.TickerToBrands = {}
        # Map { Corporation -> { Subsidiary -> Number } }:
        self.TickerToSubs = {}
        self.TickerToReturnTable = {}
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
        self.GetBrands()
        self.GetTweets()

    def CreateTables(self):
        """
        * Create all tables to store data.
        """
        # Connect to database, pull in current company table names:
        db = self.DB
        tickerToCorps = self.Tickers
        self.TickerToTweetTable = {}
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
        tweetColumns = self.DataColumns
        returnColumns = self.HistoricalPriceCols
        tweetTableSig = "Tweets_%s"
        returnTableSig = "Returns_%s"
        # Create Tweets_{Ticker}, Returns_{Ticker} table for tweet and returns data for 
        # each corporation:
        for ticker in tickerToCorps.keys():
            # Create tweet data table:
            tableName = tweetTableSig % ticker.strip()
            self.TickerToTweetTable[ticker] = tableName
            if not db.TableExists(tableName):
                db.CreateTable(tableName, tweetColumns)
            # Create return data table:
            tableName = returnTableSig % ticker.strip()
            self.TickerToReturnTable[ticker] = tableName
            if not db.TableExists(tableName):
                db.CreateTable(tableName, returnColumns)

    def GetSubsidiaries(self):
        """
        * Pull subsidiaries from each corporation's 10K, and load into 
        database. If already loaded subsidiaries into database then pull 
        using query.
        """
        db = self.DB
        queryString = ['SELECT A.Ticker, B.Subsidiaries, B.Number FROM Corporations AS A']
        queryString.append('INNER JOIN Subsidiaries As B ON A.CorpID = B.CorpID WHERE B.Number IS NOT NULL;')
        queryString = ' '.join(queryString)
        results = db.ExecuteQuery(queryString, getResults = True)
        maxSubNum = 1
        # Determine if pulled some/all subsidiaries already:
        if len(results['corporations.ticker']) > 0:
            tickers = results['corporations.ticker']
            subs = results['subsidiaries.subsidiaries']
            subNums = results['subsidiaries.number']
            row = 0
            for ticker in tickers:
                if ticker not in self.TickerToSubs:
                    self.TickerToSubs[ticker] = {}
                self.TickerToSubs[ticker][subs[row]] = subNums[row]
                row += 1
            maxSubNum = max(subNums) + 1
        if len(self.TickerToSubs.keys()) < len(self.Tickers.keys()):
            # Pull some subsidiaries from 10-Ks, if haven't been pulled in yet:
            yearEnd = datetime.today() + offsets.YearEnd()
            subs = re.compile('subsidiaries', re.IGNORECASE)
            nameRE = re.compile('name', re.IGNORECASE)
            steps = PullingSteps(False, True, False)
            query = SubsidiaryQuery()
            for ticker in self.Tickers.keys():
                if ticker not in self.TickerToSubs:
                    doc = CorporateFiling(ticker, DocumentType.TENK, steps, date = yearEnd)
                    insertData = {'CorpID' : [], 'Subsidiaries' : [], 'Number' : [] }
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
                    gc.collect()
                    
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
        if results and len(results['corporations.ticker']) > 0:
            tickers = results['corporations.ticker']
            brands = results['corporatebrands.brands']
            appdates = results['corporatebrands.appdate']
            subnums = results['corporatebrands.subnum']
            row = 0
            for ticker in tickers: 
                if ticker not in self.TickerToBrands:
                    self.TickerToBrands[ticker] = []
                # Map to (Brand, Date, SubNum):
                self.TickerToBrands[ticker].append((brands[row], appdates[row], subnums[row]))
                row += 1
              
        # Pull all brands from WIPO database website:
        if len(self.TickerToBrands.keys()) < len(self.Tickers.keys()):
            # Testing:
            return
            query = BrandQuery()
            for ticker in self.Tickers.keys():
                if ticker not in self.TickerToBrands:
                    insertValues = {}
                    subsidiaries = self.TickerToSubs[ticker]
                    brands = query.PullBrands(subsidiaries)
                    if brands and len(brands[list(brands.keys())[0]]) > 0:
                        insertValues['corpid'] = [self.__TickerToCorpNum[ticker]] * len(brands.keys())
                        insertValues['brands'] = MYSQLDatabase.RemoveInvalidChars(list(brands.keys()))
                        insertValues['appdate'] = MYSQLDatabase.RemoveInvalidChars([re.sub('[^0-9\-]','', brands[key][0]) for key in list(brands.keys())])
                        insertValues['subnum'] = [self.TickerToSubs[ticker][brands[key][1]] for key in brands.keys()]
                        # Push brands into the mysql database:
                        db.InsertInChunks("corporatebrands", insertValues, 50)
                    gc.collect()

    def GetTweets(self):
        """
        * Randomly sample all tweets and insert into associated table in schema.
        """
        # Determine which companies have already been sampled:
        query = ['SELECT A.Name, B.SearchTerm FROM Corporations AS A INNER JOIN ', '', ' AS B ON A.CorpID = B.CorpID WHERE B.SearchTerm IS NOT NULL;']
        db = self.DB
        tickersToSearchTerms = {}
        for ticker in self.TickerToTweetTable.keys():
            table = self.TickerToTweetTable[ticker].lower()
            query[1] = table
            results = db.ExecuteQuery(''.join(query), getResults = True)
            if results and len(results.keys()) > 0 and len(results[table + '.searchterm']) > 0:
                tickersToSearchTerms[ticker] = {term.lower() : True for term in results[table + '.searchterm']}

        args = {}
        args['since'] = self.StartDate
        args['until'] = self.EndDate
        args['interDaySampleSize'] = 50
        args['termSampleSize'] = 100
        args['dateStep'] = 5
        insertValues = {}
        puller = TwitterPuller()
        #puller = TweetPuller()
        # Pull tweets for all corporations that haven't been sampled already:
        for ticker in self.TickerToBrands.keys():
            if ticker not in tickersToSearchTerms:
                tickersToSearchTerms[ticker] = {}
            table = self.TickerToTweetTable[ticker]
            corpID = self.__TickerToCorpNum[ticker]
            # Select brands that were filed:
            # Map to (Brand, Date, SubNum):
            vals = self.TickerToBrands[ticker]
            # Skip all brands that were trademarked after the analysis start date, 
            # or are short words/letters:
            vals = self.__FilterAndSampleSearchTerms(tickersToSearchTerms[ticker], vals, args['termSampleSize'])
            args['searchTerms'] = [val[0] for val in vals]
            args['subs'] = [val[2] for val in vals]
            # Randomly sample tweets based upon args:
            #results = puller.PullTweets(args)
            for num, term in enumerate(args['searchTerms']):
                sub = args['subs'][num]
                puller.PullTweetsAndInsert(args, corpID, sub, table, term, db, numTweets = args['interDaySampleSize'])
                tickersToSearchTerms[ticker][term] = True
           
    def OutputSentimentScoreReport(self, ticker):
        """
        * Calculate sentiment scores using stored tweets, output to local csv file.
        """
        table = self.TickerToTweetTable[ticker]
        fileName = ''.join([ticker, '_scores.csv'])
        query = ['SELECT A.searchTerm, A.user, A.date, A.tweet, A.retweets, B.SubNum FROM ']
        query.append(table)
        query.append(' AS A INNER JOIN subsidiaries AS B ON A.SubNum = B.Number;')
        query = ''.join(query)
        results = self.DB.ExecuteQuery(query, getResults = True)
        if len(results[list(results.keys())[0]]) > 0:
            with open(fileName, 'w') as f:
                writer = csv.writer(f)
                columns = results.keys()
                headers = [header[header.index('.') : len(header)] for header in columns]
                writer.writerow(headers)
                for result in results:
                    row = []
                    for column in columns:
                        row.append(result[column])
                    writer.writerow(row)


    def GetHistoricalData(self):
        """
        * Get historical data for all tickers over past year.
        """

        pass
    def __GetTweetsPrev(self):
        for searchTerm in results:
            if len(results[searchTerm]) > 0:
                for dateKey in results[searchTerm]:
                    tweets = results[searchTerm][dateKey][0]
                    sub = results[searchTerm][dateKey][1]
                    for tweet in tweets:
                        insertValues['CorpID'].append(corpID)
                        insertValues['SearchTerm'].append(searchTerm)
                        insertValues['User'].append(tweet.username)
                        insertValues['Date'].append(tweet.date.strftime('%Y-%m-%d'))
                        insertValues['Tweet'].append(MYSQLDatabase.RemoveInvalidChars(tweet.text))
                        insertValues['SubNum'].append(sub)
                db.InsertInChunks(table, insertValues, 50)
                gc.collect()
        
    ########################
    # Private Helpers:
    ########################
    def __FilterAndSampleSearchTerms(self, existingSearchTerms, newSearchTerms, sampleSize):
        """
        * Filter out new searchterms to query with based upon existing search
        terms.
        """
        output = []
        # Filter out already sampled search terms, simple words and 
        for term in newSearchTerms:
            val = term[0].lower()
            if len(val) > 2 and val not in existingSearchTerms and val not in SeminarProject.__stopWords\
                and term[1] <= self.StartDate.date():
                output.append(term)
        # Randomly sample:
        sampleSize = min(len(output), sampleSize - len(existingSearchTerms.keys()))
        indices = choose(range(0, len(output)), sampleSize)
        return [output[i] for i in indices]
    
    def __DownloadStopWords(self):
        """
        * Download stopwords if necessary.
        """
        if not os.path.exists('C:\\Users\\rutan\\AppData\\Roaming\\nltk_data\\corpora\\stopwords\\'):
            nltk.download('stopwords')

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
                    if name in nameToTicker:
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
        #result = cache.get(val)
        if result:
            self.__PulledBrands[ticker].append(brand)
            