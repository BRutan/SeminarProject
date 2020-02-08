#################################
# PullAllData.py
#################################
# Description:
# * Pull tweets/financials for single company, push into database.

import argparse
import csv
import DataBase
from datetime import datetime
import os
import re
from PullTwitterData import TwitterPuller
from SeminarProject import SeminarProject
import sys

def GetAllData(data, db, args):
    """
    * Pull all data for single ticker:
    """
    # Create tweet, return tables for ticker:
    ticker = args.ticker.strip().lower()
    tickerData = data.loc[data['Ticker'] == ticker]
    terms = list(tickerData['Terms'])
    subs = list(tickerData['Subs'])
    seminar = SeminarProject(args.startdate, args.enddate, db,toptweets = args.toptweets, termSample = "", dateStep = args.daystep)
    seminar.CreateTables(tickerInfo)
    # Pull returns and insert data for ticker:
    seminar.GetHistoricalData(tickerInfo)
    # Get twitter data:
    seminar.GetTweets(tickerInfo, args.toptweets)

def GetSearchTerms(path):
    """
    * Pull search terms from local file. Local file must have 
    Ticker, Term, Subs as columns.
    """
    reqCols = { 'ticker' : False, 'term' : False, 'subs' : False }
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
        data = { 'Ticker' : [args.ticker] * len(args.searchterms), 'Terms' : args.searchterms, 'Subs' : ['NULL'] * len(args.searchterms) }
        data = pandas.core.frame.DataFrame(data)

    # Get all data for ticker/tickers:
    uniqueTickers = set(data['ticker'])
    for ticker in uniqueTickers:
        GetAllData(ticker, data)


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
    