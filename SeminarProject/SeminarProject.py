#################################################
# SeminarProject.py
#################################################
# * Object performs key steps in seminar project.

from TargetedWebScraping import BrandQuery, SubsidiaryQuery
from CorporateFiling import CorporateFiling, TableItem, DocumentType, PullingSteps, SoupTesting
from CorpDataPuller import DataPuller
import csv
from DataBase import MYSQLDatabase
from datetime import datetime, date, timedelta
import gc
#import memcache
import nltk
from nltk.corpus import stopwords
from numpy.random import choice as choose
from pandas.tseries import offsets
import re
from SentimentAnalyzer import SentimentAnalyzer
from PullTwitterData import TwitterPuller

class SeminarProject(object):
    """
    * Key objects/methods required for performing seminar project.
    """
    __utfSupport = 'CHARACTER SET utf8 COLLATE utf8_unicode_ci'
    __cacheKeySig = "{Corp:%s}{Brand:%s}"
    __stopWords = { re.sub('[^a-z]', '', word.lower()) : True for word in set(stopwords.words('english')) }
    CorpTableColumns = {"CorpID" : ["int", True, ""], "Name" : ["text", False, ""], 
                                 "Ticker" : ["varchar(5)", False, ""], "Industry" : ["text", False, ""], "Weight" : ["float", False, ""] }
    CorpBrandTableColumns = {"CorpID" : ["int", False, "Corporations(CorpID)"], "Brands" : ["text " + __utfSupport, False, ""], 
                                    "AppDate" : ["Date", False, ""], "SubNum" : ["int", False, "Subsidiaries(Number)"]}
    SubsidariesTableColumns = {"Number" : ["int", True, ""], "CorpID" : ["int", False, "Corporations(CorpID)"], "Subsidiaries" : ["text", False, ""]}
    DataColumns = { "CorpID" : ["int", False, "Corporations(CorpID)"], "SearchTerm" : ["text", False, ""], 
                    "User" : ["text " + __utfSupport, False, ''], 
                    "Date" : ["date", False, ""], 
                    "Tweet" : ["text " + __utfSupport, False, ''], 
                    "Retweets" : ["int", False, ""], 
                    "SubNum" : ["int", False, "Subsidiaries(Number)"] }
    HistoricalPriceCols = { 'CorpID' : ['int', False, 'Corporations(CorpID)'], 'Adj_Close' : ['float', False, ''], 'Date' : ['Date', False, ''] }
    def __init__(self, startDate, endDate, database, tickers, schema = None, toptweets = False, termSample = None, dateStep = None):
        """
        * Initialize new object.
        """
        self.__dateStep = dateStep
        self.__termSampleSize = termSample
        self.__pullTopTweets = toptweets
        self.Tickers = tickers
        self.StartDate = startDate
        self.EndDate = endDate
        self.DB = database
        if schema:
            self.__schema = schema
        else:
            self.__schema = self.DB.ActiveSchema
        
        # Get tickers from CSV file at tickerPath:
        self.__CorpNumToTicker = {}
        self.__TickerToCorpNum = {}
        self.CorpTableColumns = {"CorpID" : ["int", True, ""], 
                                 "Name" : ["text", False, ""], 
                                 "Ticker" : ["varchar(5)", False, ""], 
                                 "Sector" : ["text", False, ""], 
                                 "Region" : ["text", False, ""],
                                 "Currency" : ["float", False, ""],
                                 "Exchange" : ["float", False, ""],
                                 "ExchangeTimeZone" : ["text", False, ""],
                                 "SharesOutstanding" : ["int", False, ""],
                                 "BookValue" : ["float", False, ""],
                                 "MarketCap" : ["int", False, ""]
                                 }
        self.CorpBrandTableColumns = {"CorpID" : ["int", False, "Corporations(CorpID)"], "Brands" : ["text " + SeminarProject.__utfSupport, False, ""], 
                                      "AppDate" : ["Date", False, ""], "SubNum" : ["int", False, "Subsidiaries(Number)"]}
        self.SubsidariesTableColumns = { "Number" : ["int", True, ""], "CorpID" : ["int", False, "Corporations(CorpID)"], "Subsidiaries" : ["text", False, ""]}
        self.DataColumns = { "CorpID" : ["int", False, "Corporations(CorpID)"], "SearchTerm" : ["text", False, ""], 
                       "User" : ["text " + SeminarProject.__utfSupport, False, ''], 
                       "Date" : ["date", False, ""], 
                       "Tweet" : ["text " + SeminarProject.__utfSupport, False, ''], 
                       "Retweets" : ["int", False, ""], 
                       "SubNum" : ["int", False, "Subsidiaries(Number)"], "Coordinate" : ["Point", False, ""] }
        self.HistoricalPriceCols = { 'CorpID' : ['int', False, 'Corporations(CorpID)'], 'Close' : ['float', False, ''], 
                                    'Date' : ['Date', False, ''], 'Volume' : ['int', False, ''] }    

        self.TickerToBrands = {}
        # Map { Corporation -> { Subsidiary -> Number } }:
        self.TickerToSubs = {}
        self.TickerToReturnTable = {}
        
    #######################
    # Interface Methods:
    #######################
    def ExecuteAll(self):
        """
        * Execute all steps in sequential order.
        """
        self.GetCorpAttributes()
        self.CreateTables()
        self.GetSubsidiaries()
        self.GetBrands()
        self.GetTweets()
        
    def GetCorpAttributes(self):
        """
        * Pull all corporate attributes for stored tickers or passed
        ticker.
        """
        # Determine which tickers already have information:
        for ticker in self.Tickers:
            pass
    def CreateTables(self):
        """
        * Create all tables to store relevant data for project. If ticker is specified, and does not exist
        as a table, then create returns and tweet table for ticker if not created already, and 
        add ticker to Corporations table.
        """
        db = self.DB
        self.TickerToTweetTable = {}
        # Skip creating corporations table if already created:
        if not db.TableExists("Corporations"):
            corpTableColumns = self.CorpTableColumns
            # Create Corporations table that maps corporation name to ticker, insert all corporations into database:
            db.CreateTable("Corporations", corpTableColumns, schema = self.__schema)
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
        else:
            results = db.ExecuteQuery("SELECT * FROM Corporations", getResults = True)
            if results:
                for row, ticker in enumerate(results['ticker']):
                    lowered = ticker.lower()
                    self.Tickers[lowered] = (results['name'][row], results['industry'][row])
                    self.__TickerToCorpNum[lowered] = results['corpid'][row]
                    self.__CorpNumToTicker[results['corpid'][row]] = lowered
            if _ticker and _ticker not in self.Tickers:
                # Add to Corporations table:
                corpID = max(results['corpID']) + 1
                corpData = {'ticker' : _ticker, 'corpid' : corpID}
                db.InsertValues("Corporations", corpData)
                self.Tickers.append(_ticker)
        if not db.TableExists("Subsidiaries"):
            db.CreateTable("Subsidiaries", self.SubsidariesTableColumns, schema = self.__schema)
        if not db.TableExists("CorporateBrands"):
            db.CreateTable("CorporateBrands", self.CorpBrandTableColumns, schema = self.__schema)
        
        # Create all Corporations tables:
        tweetColumns = self.DataColumns
        returnColumns = self.HistoricalPriceCols
        tweetTableSig = "tweets_%s"
        returnTableSig = "returns_%s"
        # Create Tweets_{Ticker}, Returns_{Ticker} table for tweet and returns data for 
        # each corporation:
        for ticker in self.Tickers.keys():
            # Create tweet data table:
            tableName = tweetTableSig % ticker.strip()
            self.TickerToTweetTable[ticker.lower()] = tableName
            if not db.TableExists(tableName):
                db.CreateTable(tableName, tweetColumns)
            # Create return data table:
            tableName = returnTableSig % ticker.strip()
            self.TickerToReturnTable[ticker.lower()] = tableName
            if not db.TableExists(tableName):
                db.CreateTable(tableName, returnColumns)

    def GetSubsidiaries(self, ticker = None):
        """
        * Pull subsidiaries from each corporation's 10K, and load into 
        database. If already loaded subsidiaries into database then pull 
        using query.
        """
        if ticker and isinstance(ticker, str):
            _ticker = ticker.lower()
        elif ticker and isinstance(ticker, list):
            _ticker = [tick.lower() for tick in ticker]
        else:
            _ticker = None
        db = self.DB
        yearEnd = datetime.today() + offsets.YearEnd()
        subs = re.compile('subsidiaries', re.IGNORECASE)
        nameRE = re.compile('name', re.IGNORECASE)
        steps = PullingSteps(False, True, False)
        query = SubsidiaryQuery()
        queryString = ['SELECT A.Ticker, B.Subsidiaries, B.Number FROM Corporations AS A']
        queryString.append('INNER JOIN Subsidiaries As B ON A.CorpID = B.CorpID WHERE B.Number IS NOT NULL;')
        queryString = ' '.join(queryString)
        results = db.ExecuteQuery(queryString, getResults = True)
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
                    
    def GetBrands(self, ticker = None):
        """
        * Pull all brands from WIPO website, push into database.
        """
        # Determine if brands were already loaded for each corporation:
        if ticker and isinstance(ticker, str):
            _ticker = ticker.lower()
        elif ticker and isinstance(ticker, list):
            _ticker = [tick.lower() for tick in ticker]
        else:
            _ticker = None
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
        if ticker and isinstance(ticker, str):
            _ticker = ticker.lower()
        elif ticker and isinstance(ticker, list):
            _ticker = [tick.lower() for tick in ticker]
        else:
            _ticker = None
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
            args['subs'] = [val[2] for val in vals]
            # Randomly sample tweets based upon args:
            for num, term in enumerate(args['searchTerms']):
                sub = args['subs'][num]
                puller.PullTweetsAndInsert(args, corpID, sub, table, term, db, self.__pullTopTweets, numTweets = args['interDaySampleSize'])
                tickersToSearchTerms[ticker][term] = True


    def GetHistoricalData(self, tickerInfo):
        """
        * Get historical data for all tickers over past year.
        """
        puller = DataPuller()
        priceTypes = ['Close', 'Volume']
        returns = puller.GetAssetPrices(ticker, args.startdate, args.enddate)

        
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
                    ticker = row[1].strip().lower()
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
                        tickers[ticker] = (name, sector, marketCap, location)
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
        self.__PulledBrands = {}
        #result = cache.get(val)
        if result:
            self.__PulledBrands[ticker].append(brand)
            