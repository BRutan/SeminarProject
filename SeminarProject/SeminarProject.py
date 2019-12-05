#################################################
# SeminarProject.py
#################################################
# 

import csv
import DataBase
import Corporate10KDocument

class SeminarProject(object):
    """
    * Key objects required for performing seminar project.
    """
    __tradeMark = '®'
    def __init__(self, tickerPath, database):
        """
        * 
        """
        self.DB = database
        self.Tickers = self.__PullTickers(tickerPath)

    def LoadAllBrands(self):
        """
        * Pull all brands from corporation's 10K.
        """
        for ticker in self.Tickers.keys():
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

    def SampleTweets(self, keyword):

        pass

    def InsertTweetsIntoTable(self, ticker, tweets):
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


    def __PullTickers(tickerPath):
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
        return tickers

