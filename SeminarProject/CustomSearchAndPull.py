#################################
# CustomSearchAndPull.py
#################################
# Description:
# * Pull tweets/financials for single company, push 
# into database.

import argparse
import DataBase
from datetime import datetime
import os
import re
from PullTwitterData import TwitterPuller
from CorpDataPuller import DataPuller
from SeminarProject import SeminarProject

def IsDate(string):
    if re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2}').match(string):
        return string
    else:
        raise argparse.ArgumentTypeError

if __name__ == '__main__':
    # Get command line arguments:
    parser = argparse.ArgumentParser(prog='SeminarProject')
    parser.add_argument('username', type = str, help="Username for MYSQL instance.")
    parser.add_argument('pw', type = str, help="Password to MYSQL instance.")
    parser.add_argument('schema', type = str, help="Name of schema containing all tables.")
    parser.add_argument('ticker', type = str, help="Company ticker to pull data for.")
    parser.add_argument('startdate', type = IsDate, help="Start date for tweets and returns.")
    parser.add_argument('enddate', type = IsDate, help="End date for tweets and returns.")
    # Optional arguments:
    parser.add_argument('--host', type = str, help="IP Address of MYSQL instance.")
    parser.add_argument('--searchterms', type = str, nargs = '+', help = 'One or more search terms for tweets. Will not use brands if provided.')
    parser.add_argument('--toptweets', action = "store_true", help = "Include if want to pull in top tweets only.")
    args = parser.parse_known_args()
    valid_args = args[0]
    errs = []    

    # Check invalid arguments:
    if args[1]:
        errs.append('')

    try:
        db = DataBase.MYSQLDatabase(args.username, args.pw, host, args.schema)
    except:
        errs.append('Could not open MYSQL database instance.')

    if errs:
        raise Exception('\n'.join(errs))

    # Create tweet, return tables for ticker:
    ticker = args.ticker.strip().lower()
    returnTable = 'returns_%s' % ticker
    tweetTable = 'tweets_%s' % ticker
    if not db.TableExists(tweetTable):
        db.CreateTable(tweetTable, SeminarProject.DataColumns)
    if not db.TableExists(returnTable):
        db.CreateTable(returnTable, SeminarProject.HistoricalPriceCols)
    # Insert ticker and company name into Corporations table:
    if not db.TableExists('corporations'):
        db.CreateSchema('corporations', SeminarProject.CorpTableColumns)
    results = db.ExecuteQuery('SELECT * FROM corporations;')
    corpPuller = DataPuller()
    if results and ticker not in [_ticker.lower() for _ticker in results['ticker']]:
        corpID = max(results['corpid']) + 1
        name = corpPuller.GetName(ticker)
        insertVals = {'corpid' : [corpId], 'name' : [name], 'ticker' : [ticker.upper()]}
        db.InsertValues('corporations', insertVals)

    # Perform custom tweet pulling.
    puller = TwitterPuller()

    if args.searchterms:
        pass
    else:
        pass



    