#################################
# PullAllData.py
#################################
# Description:
# * Pull tweets/financials for single company, push into database.

import argparse
from CorpDataPuller import DataPuller
import csv
import DataBase
from datetime import datetime
import os
import re
from PullTwitterData import TwitterPuller
from SeminarProject import SeminarProject
import sys

def GetAllData(ticker, terms, db):
    # Create tweet, return tables for ticker:
    ticker = args.ticker.strip().lower()
    returnTable = 'returns_%s' % ticker
    tweetTable = 'tweets_%s' % ticker
    if not db.TableExists(tweetTable):
        db.CreateTable(tweetTable, SeminarProject.DataColumns)
    if not db.TableExists(returnTable):
        db.CreateTable(returnTable, SeminarProject.HistoricalPriceCols)
    # Insert ticker and company attributes into Corporations table:
    if not db.TableExists('corporations'):
        db.CreateTable('corporations', SeminarProject.CorpTableColumns)
    results = db.ExecuteQuery('SELECT * FROM corporations', getResults = True)
    if results and ticker not in [_ticker.lower() for _ticker in results['ticker']]:
        corpPuller = DataPuller()
        corpID = max(results['corpid']) + 1
        insertVals = {'corpid' : [corpID], 'name' : ['NULL'], 'ticker' : [ticker.upper()], 'industry' : ['NULL'] }
        db.InsertValues('corporations', insertVals)
    elif results:
        index = results['ticker'].index(ticker.upper())
        corpID = results['corpid'][index]
    
    # Perform custom tweet pulling.
    puller = TwitterPuller()
    terms = args.searchterms
    insertValues = { key : [] for key in SeminarProject.DataColumns.keys() }
    pullArgs = {}
    pullArgs['dateStep'] = args.daystep
    pullArgs['since'] = args.startdate
    pullArgs['until'] = args.enddate

    for term in terms:
        puller.PullTweetsAndInsert(pullArgs, corpID, 'NULL', tweetTable, term, db, args.toptweets, args.interdaysamplesize)

def GetSearchTerms(path):
    """
    * Pull search terms from local file. Local file must have 
    Ticker, Term as columns.
    """
    reqCols = {'ticker' : False, 'term' : False}
    data = pandas.read_csv(path)
    invalidCols = []
    for col in data.columns:
        if col not in reqCols:
            invalidCols.append(col)
    missingCols = [col for col in reqCols if not reqCols[col]]
    errs = []
    if invalidCols:
        errs.append('The following columns are invalid:')
        errs.append(','.join(invalidCols))
    if missingCols:
        errs.append('The following columns are missing:')
        errs.append(','.join(missingcols))
    if errs:
        raise Exception('\n'.join(errs))
    
    return data

def PullAllData():
    # Get command line arguments:
    parser = argparse.ArgumentParser(prog='PullAllData')
    parser.add_argument('username', type = str, help="Username for MYSQL instance.")
    parser.add_argument('pw', type = str, help="Password to MYSQL instance.")
    parser.add_argument('schema', type = str, help="Name of schema containing all tables.")
    parser.add_argument('startdate', type = IsDate, help="Start date for tweets and returns.")
    parser.add_argument('enddate', type = IsDate, help="End date for tweets and returns.")
    parser.add_argument('interdaysamplesize', type = int, help = 'Number of tweets to pull per day (positive).')
    parser.add_argument('daystep', type = int, help = 'Day step between startdate and enddate (positive).')
    # Optional arguments (one of ticker + searchTerms, termfile must be supplied):
    parser.add_argument('--host', type = str, help="IP Address of MYSQL instance.")
    parser.add_argument('--ticker', type = str, help="Company ticker to pull data for.")
    parser.add_argument('--toptweets', action = "store_true", help = "Include if want to pull in top tweets only.")
    parser.add_argument('--termFile', type = str, help="Filepath with searchterms as list.")
    parser.add_argument('--searchterms', type = str, nargs = '+', help = 'One or more search terms for tweets.')
    args = parser.parse_known_args()
    errs = []
    # Check if any unknown arguments were passed:
    if args[1]:
        errs.append(''.join(['The following arguments are unknown: ', '\n'.join(args[1])]))
    args = args[0]

    val = hasattr(args, 'termFile')
    val != hasattr(args, 'ticker')

    if not val:
        errs.append('Ticker or termFile (exclusive) must be specified.')
    elif hasattr(args, 'ticker') and not hasattr(args, 'searchTerms'):
        errs.append('If using ticker, must specify searchTerms.')
    elif hasattr(args, 'termFile') and not os.path.exists(args.termFile):
        errs.append('termFile path does not exist.')

    if errs:
        raise Exception(''.join(errs))

    if not args.host:
        host = "127.0.0.1"
    else:
        host = args.host.strip()

    try:
        db = DataBase.MYSQLDatabase(args.username, args.pw, host, args.schema)
    except:
        errs.append('Could not open MYSQL database instance.')

    if errs:
        raise Exception('\n'.join(errs))

    if hasattr(args, "termFile"):
        data = GetSearchTerms(args.termFile)
    else:
        data = { args.ticker : args.searchterms }
        data = pandas.core.frame.DataFrame(data)

    for ticker in data['ticker']:
        terms = data.loc[data[ticker] ]
        GetAllData(ticker, terms, db)


def IsDate(string):
    try:
        return datetime.strptime(string, '%Y-%m-%d')
    except:
        raise argparse.ArgumentTypeError

def IsPositive(val):
    if val > 0:
        return val
    else:
        raise argparse.ArgumentTypeError

if __name__ == '__main__':
    CustomSearchAndPull()
    