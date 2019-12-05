#################################################
# SeminarProject.py
#################################################
# 

import csv
import DataBase
from Corporate10KDocument import Corporate10KDocument
from datetime import datetime, timedelta
from pandas.tseries import offsets
import re

class SeminarProject(object):
    """
    * Key objects required for performing seminar project.
    """
    __tradeMark = '\w+®'
    __tmRE = re.compile(__tradeMark)
    __utfSupport = 'CHARACTER SET utf8 COLLATE utf8_unicode_ci'
    def __init__(self, tickerPath, database):
        """
        * Initialize new object.
        """
        self.DB = database
        self.__PullTickers(tickerPath)
        self.CorpTableColumns = {"CorpID" : ["int", True, ""], "Name" : ["text", False, ""], 
                                 "Ticker" : ["varchar(5)", False, ""], "Industry" : ["text", False, ""], "Brands" : ["text", False, ""]}

        self.DataColumns = { "CorpID" : ["int", True, "Corporations(CorpID)"], "SearchTerm" : ["text", False, ""], 
                       "User" : ["text", False, SeminarProject.__utfSupport], "Date" : ["date", False, ""], 
                       "Tweet" : ["text", False, SeminarProject.__utfSupport] }
        self.CorpToBrands = {}

    #######################
    # Interface Methods:
    #######################
    def ExecuteAll(self):
        """
        * Execute all steps in sequential order.
        """
        self.CreateTables()
        self.LoadAllBrands()
        self.SampleAndInsertTweets()

    def LoadAllBrands(self):
        """
        * Pull all brands from corporation's 10K, push into database.
        """
        # Determine if brands were already loaded for each corporation:
        db = self.DB
        results = db.ExecuteQuery('SELECT DISTINCT Ticker FROM Corporations WHERE Brands IS NOT NULL;', getResults = True)
        if len(results['ticker']) == len(self.Tickers.keys()):
            for result in results.values():
                pass
            return

        # Determine the year end date for this year:
        yearEnd = datetime.today() + offsets.YearEnd()

        # Load 10K object, pull all brands from 10K:
        for ticker in self.Tickers.keys():
            doc = Corporate10KDocument(ticker, yearEnd)
            sectionText = self.Sections['Business']['']
            # Search section text for all trademarks:
            tradeMarks = SeminarProject.__tmRE.findall(self.__tradeMark)
            for brand in tradeMarks:
                pass


    def CreateTables(self):
        """
        * Create all tables to store data.
        Parameters:
        * tickerToCorps: Dictionary mapping Ticker -> ( CorpName, Industry).
        """
        # Connect to database, pull in current company table names:
        db = self.DB
        tickerToCorps = self.Tickers
        # Skip creating corporations table if already created:
        if not db.TableExists("Corporations"):
            tables = db.Tables
            corpTableColumns = self.CorpTableColumns
            # Create Corporations table that maps corporation name to ticker, insert all corporations into database:
            db.CreateTable("Corporations", corpTableColumns, schema = "Research_Seminar_Project")
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

            db.InsertValues("Corporations", corpData)
        # Insert all data into the Corporations table:
        dataColumns = self.DataColumns
        tableSig = "Tweets_%s"
        # Create Tweets_{Ticker} table for each corporation:
        # One table for each ticker using listed columns:
        for ticker in tickerToCorps.keys():
            tableName = tableSig % ticker.strip()
            if not db.TableExists(tableName):
                db.CreateTable(tableName, dataColumns)

        return db    

    def SampleAndInsertTweets(self):
        """
        * Randomly sample all tweets and insert into associated table in schema.
        """
        for ticker in self.Tickers.keys():
            tweets = []
            self.__InsertTweetsIntoTable(ticker, tweets)
        
    ########################
    # Private Helpers:
    ########################
    def __InsertTweetsIntoTable(self, ticker, tweets):
        """
        * Insert tweets for particular ticker into table.
        """
        db = self.DB
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


    def __PullTickers(self, tickerPath):
        """
        * Pull in all consumer discretionary tickers from local file.
        Store Ticker -> ( Name, Sector )
        """
        tickers = {}
        with open(tickerPath, 'r') as f:
            reader = csv.reader(f)
            atHeader = True
            # Columns:
            # Name	Ticker	Identifier	SEDOL	Weight	Sector	Shares Held	Local Currency
            for row in reader:
                if not atHeader:
                    ticker = row[1].strip()
                    tickers[ticker] = (row[0], row[5])
                atHeader = False

        self.Tickers = tickers

