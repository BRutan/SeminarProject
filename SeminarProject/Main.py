#################################
# Main.py
#################################
# Description:
# * 

import csv
import DataBase
import os
from HelperFunctions import PullTickers, PrintTweetsToCSV
from PullTwitterData import TwitterPuller
from Corporate10KDocument import Corporate10KDocument
from SeminarProject import SeminarProject

def GetBrandsFrom10K():
    tickers, db = PullTickersCreateTables()
    doc = Corporate10KDocument('aapl', '20181231')

    for ticker in tickers:
        pass
    


def PullTickersCreateTables():
    outputPath = "C:\\Users\\rutan\\OneDrive\\Desktop\\Fordham MSQF Courses\\Research Seminar\\Project\\Project\\"
    tickerPath = "C:\\Users\\rutan\\OneDrive\\Desktop\\Fordham MSQF Courses\\Research Seminar\\Project\\Project\\XLY_All_Holdings.csv"

    corpInfo = PullTickers(tickerPath)
        
    return corpInfo, db
    
if __name__ == '__main__':
    tickerPath = "C:\\Users\\rutan\\OneDrive\\Desktop\\Fordham MSQF Courses\\Research Seminar\\Project\\Project\\XLY_All_Holdings.csv"
    db = DataBase.MYSQLDatabase("root", "Correlation$", "127.0.0.1", "Research_Seminar_Project")
    seminar = SeminarProject(tickerPath, db)
    seminar.ExecuteAll()

