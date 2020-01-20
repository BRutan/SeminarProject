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
    parser = argparse.ArgumentParser(prog='SeminarProject')
    parser.add_argument('username', type = str, help="Username for MYSQL instance.")
    parser.add_argument('pw', type = str, help="Password to MYSQL instance.")
    parser.add_argument('schema', type = str, help="Name of schema containing all tables.")
    parser.add_argument('startdate', type = str, help="Begin date for pulling all data.")
    parser.add_argument('enddate', type = str, help="End date for pulling all data.")
    parser.add_argument('tickerpath', type = str, help="XLY_All_Holdings.csv file path.")
    # Optional arguments:
    parser.add_argument('--host', type = str, help="IP Address of MYSQL instance.")
    parser.add_argument('--ticker', type = str, help="Provide if only want to pull data for single ticker.")
    args = parser.parse_args()
    try:
        startDate = datetime.strptime(args.startdate, '%Y-%m-%d')
        endDate = datetime.strptime(args.enddate, '%Y-%m-%d')
    except:
        raise Exception("Could not convert startDate or endDate to datetime.")

    if not args.host:
        host = "127.0.0.1"
    else:
        host = args.host

    #db = DataBase.MYSQLDatabase("root", "Correlation$", "127.0.0.1", "Research_Seminar_Project")
    db = DataBase.MYSQLDatabase(args.username, args.pw, host, args.schema)
    seminar = SeminarProject(startDate, endDate, args.tickerpath, db)

    # Perform key steps for single ticker if was specified, else perform for all tickers listed in XLY_ALL_Holdings.csv:
    if args.ticker:
        #tickers = ['F','HOG','GPC','LKQ','HRB','MCD','SBUX','MAR','YUM','HLT','RCL','LVS','CMG','CCL','MGM','DRI','WYNN','NCLH','DHI','LEN','GRMN','NVR','WHR','PHM','MHK','NWL','LEG','AMZN','BKNG','EBAY','EXPE','HAS','TGT','DG','DLTR','KSS','M','JWN','HD','LOW','TJX','ROST','ORLY','AZO','BBY','KMX','ULTA','TIF','AAP','TSCO','LB','GPS','NKE','VFC','TPR','PVH','RL','HBI','CPRI','UAA','UA']
        seminar.CreateTables(args.ticker)
        seminar.GetSubsidiaries(args.ticker)
        seminar.GetBrands(args.ticker)
        seminar.GetTweets(args.ticker)
    else:
        seminar.ExecuteAll()
        
