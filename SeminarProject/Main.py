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

def GetBrandsFrom10K():
    tickers, db = PullTickersCreateTables()
    doc = Corporate10KDocument('aapl', '20181231')

    db = DataBase.MYSQLDatabase("root", "Correlation$", "127.0.0.1", "Research_Seminar_Project")

    for ticker in tickers:
        pass
    

def PullTickersCreateTables():
    outputPath = "C:\\Users\\rutan\\OneDrive\\Desktop\\Fordham MSQF Courses\\Research Seminar\\Project\\Project\\"
    tickerPath = "C:\\Users\\rutan\\OneDrive\\Desktop\\Fordham MSQF Courses\\Research Seminar\\Project\\Project\\XLY_All_Holdings.csv"

    corpInfo = PullTickers(tickerPath)
    
    
    
    return corpInfo, db
    
if __name__ == '__main__':
    test2()
    #main()