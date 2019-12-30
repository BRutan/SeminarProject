#################################
# Main.py
#################################
# Description:
# * 

import csv
import DataBase
import os
from PullTwitterData import TwitterPuller
from Corporate10KDocument import Corporate10KDocument
from SeminarProject import SeminarProject
    
if __name__ == '__main__':
    
    path = 'D:\\Git Repos\\SeminarProject\\SeminarProject\\SeminarProject\\Notes\\amzn_10K_20191231_unclean.html'
    doc = Corporate10KDocument('amzn', htmlPath = path)

    #tickerPath = "C:\\Users\\rutan\\OneDrive\\Desktop\\Fordham MSQF Courses\\Fall 2019\\Research Seminar\\Project\\Project\\XLY_All_Holdings.csv"
    #db = DataBase.MYSQLDatabase("root", "Correlation$", "127.0.0.1", "Research_Seminar_Project")
    #seminar = SeminarProject(tickerPath, db)
    #seminar.ExecuteAll()

