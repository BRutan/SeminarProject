#################################
# Main.py
#################################
# Description:
# * Perform all key steps in the 

import csv
import DataBase
import os
import re
from BrandQuery import BrandQuery
from PullTwitterData import TwitterPuller
from CorporateFiling import CorporateFiling, DocumentType, PullingSteps
from SeminarProject import SeminarProject
    
if __name__ == '__main__':
    tickerPath = "D:\\Git Repos\\SeminarProject\\SeminarProject\\SeminarProject\\XLY_All_Holdings.csv"
    db = DataBase.MYSQLDatabase("root", "Correlation$", "127.0.0.1", "Research_Seminar_Project")
    seminar = SeminarProject(tickerPath, db)
    seminar.ExecuteAll()
