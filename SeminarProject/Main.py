#################################
# Main.py
#################################
# Description:
# * Perform all key steps in the project, depending upon
# command line arguments.

import argparse
import DataBase
from datetime import datetime
from SeminarProject import SeminarProject
    
if __name__ == '__main__':
    # Get command line arguments:
    parser = argparse.ArgumentParser()
    parser.add_argument('pw', type = str, help="Password to MYSQL instance.")
    parser.add_argument('schema', type = str, help="Name of schema containing all tables.")
    parser.add_argument('startdate', type = str, help="Begin date for pulling all data.")
    parser.add_argument('enddate', type = str, help="End date for pulling all data.")
    # Optional arguments:
    parser.add_argument('--host', type = str, help="IP Address of MYSQL instance.")
    parser.add_argument('--ticker', type = str, help="Provide if only want to pull data for single ticker.")
    parser.parse_args()
    try:
        startDate = datetime.strptime(parser.startdate, '%Y-%m-%d')
        endDate = datetime.strptime(parser.enddate, '%Y-%m-%d')
    except:
        raise Exception("Could not convert startDate or endDate to datetime.")

    if not parser.host:
        host = "127.0.0.1"
    else:
        host = parser.host

    #db = DataBase.MYSQLDatabase("root", "Correlation$", "127.0.0.1", "Research_Seminar_Project")
    db = DataBase.MYSQLDatabase("root", parser.pw, host, parser.schema)
    seminar = SeminarProject(startDate, endDate, "XLY_All_Holdings.csv", db)

    # Perform key steps:
    if parser.ticker:
        seminar.GetSubsidiaries(parser.ticker)
        seminar.GetBrands(parser.ticker)
        seminar.GetTweets(parser.ticker)
    else:
        seminar.ExecuteAll()
        
