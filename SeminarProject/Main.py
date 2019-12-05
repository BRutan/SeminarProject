#################################
# Main.py
#################################
# Description:
# * 

import csv
import DataBase
import os
from HelperFunctions import CreateTables, PullBrandsFrom10Ks, Pull10Ks, PullTickers, PrintTweetsToCSV
from PullTwitterData import TwitterPuller
from Pull10Ks import get_files
from Corporate10KDocument import Corporate10KDocument

#### TODO:
## 2. Figure out how to extract all brands from consumer product corps' 10ks, using regex 
## or 
## 3. Perform tweet queries on brands for each corp, maintaining mapping of corp to  

def mapStatus(statuses):
    # Return list of tuples for given status, so that results can be stored in
    # cache:
    objs = []
    for status in statuses:
        userName = unicodeStr(status.user.name)
        created_at = unicodeStr(status.created_at)
        text = unicodeStr(status.text)
        tup = (userName, created_at, text)
        objs.append(tup)

    return objs

def test():
    get_files('AAPL', 'Apple')

def test2():
    doc = Corporate10KDocument('aapl', '20181231')

def main():
    outputPath = "C:\\Users\\rutan\\OneDrive\\Desktop\\Fordham MSQF Courses\\Research Seminar\\Project\\Project\\"
    tickerPath = "C:\\Users\\rutan\\OneDrive\\Desktop\\Fordham MSQF Courses\\Research Seminar\\Project\\Project\\XLY_All_Holdings.csv"

    corpInfo = PullTickers(tickerPath)
    
    # Create all required tables:
    db = CreateTables(corpInfo)
    # Pull all 10ks for tickers:
    # outputPath = Pull10Ks(outputPath, corpInfo)
    # Pull all brands from 10Ks:
    #corpsToBrands = PullBrandsFrom10Ks(corpInfo, '20191116')
    # Pull tweets from twitter for each corporation
    path = 'C:\\Users\\rutan\\OneDrive\\Desktop\\Fordham MSQF Courses\\Research Seminar\\Project\\Project\\Brands\\AdvancedAutoPartsBrands.csv'
    keywords = []
    with open(path, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            keywords.append(row[0])

    # Pull in tweets:
    puller = TwitterPuller(keywords)
    tableName = [table for table in db.Tables if 'aap' in table][0]
    columns = db.Tables[tableName]
    try:
        # Get corporate id for AAP:
        corpID = db.ExecuteQuery("Research_Seminar_Project", "SELECT CorpID FROM Corporations WHERE Ticker = 'AAP'", True)
        corpID = corpID['corpid'][0]
        tweets = puller.PullTweets('2017-01-01', 100)
        columnData = {}
        
        for column in columns.keys():
            columnData[column] = []

        for term in tweets.keys():
            for tweet in tweets[term]:
                for column in columns.keys():
                    if column == 'corpid':
                        columnData[column].append(corpID)
                    elif column == 'user':
                        columnData[column].append(TwitterPuller.UnicodeStr(tweet.username))
                    elif column == 'searchterm':
                        columnData[column].append(term)
                    elif column == 'date':
                        columnData[column].append(tweet.date.strftime("%m/%d/%Y"))
                    elif column == 'tweet':
                        columnData[column].append(TwitterPuller.UnicodeStr(tweet.text))
        # Push all data into the database:
        db.InsertValues(tableName, "Research_Seminar_Project", columnData)
    except Exception as ex:
        if not tweets is None and len(tweets.keys()) > 0:
            PrintTweetsToCSV(tweets, "Tweets_AAP.csv")
    
if __name__ == '__main__':
    test2()
    #main()