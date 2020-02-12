#################################
# PullTimeSeries_RunScript.py
#################################
# Description:
# * Calculate sentiment scores of all tweets
# for passed ticker stored in database, push
# into file at path.

import argparse
import csv
import DataBase
from datetime import datetime, date, timedelta
from dateutil.rrule import rrule, YEARLY, MONTHLY, WEEKLY, DAILY
from dateutil.relativedelta import relativedelta
import os
from pandas import DataFrame, concat
import re
from SentimentAnalyzer import SentimentAnalyzer

def IsAscii(string):
    try:
        string.decode('ascii')
    except:
        return False
    return True

def CalculateSentimentScores():
# Get command line arguments:
    parser = argparse.ArgumentParser(prog='SeminarProject')
    parser.add_argument('username', type = str, help="Username for MYSQL instance.")
    parser.add_argument('pw', type = str, help="Password to MYSQL instance.")
    parser.add_argument('schema', type = str, help="Name of schema containing all tables.")
    parser.add_argument('ticker', type = str, help="Ticker to generate sentiment scores.")
    parser.add_argument('path', type = str, help='Full filepath (including filename) to output scores.')
    # Optional arguments:
    parser.add_argument('--filterzeros', action='store_true', help='Filter out all zero sentiment scores.')
    parser.add_argument('--host', type = str, help="IP Address of MYSQL instance.")
    parser.add_argument('--dateperiod', type = str, nargs = 2, help="Include start and end date (YYYY-MM-DD) for pulling sentiment scores.")
    parser.add_argument('--datestep', type = str, help="Pass if want to output data at intervals: {'d': daily, 'w' : weekly, 'm' : monthly, 'y' : yearly}, or number for number of days.")
    parser.add_argument('--periodsubsample', type = int, help="Random selection sample size of tweets within the period.")
    #parser.add_argument('--lag', type=int, help="# day (or period if --datepstep given) to all other attributes to sentiment scores in tweet table.")
    args = parser.parse_args()
    datePattern = re.compile('[1|2][0-9]{3}-[01][0-9]-[0123][0-9]')
    dateSteps = { 'm' : MONTHLY, 'd' : DAILY, 'w' : WEEKLY, 'y' : YEARLY }
    datestep = None
    #lag = None
    dates = []
    errs = []

    if os.path.exists(args.path):
        errs.append('File at path already exists.')
    elif '.csv' not in args.path:
        errs.append('File must be csv.')
    if not args.host:
        host = "127.0.0.1"
    else:
        host = args.host.strip()
    try:
        db = DataBase.MYSQLDatabase(args.username, args.pw, host, args.schema)
    except:
        errs.append('Could not open MYSQL database instance.')
    if args.dateperiod:
        dateErrs = []
        for dt in args.dateperiod:
            if not datePattern.match(dt):
                dateErrs.append(dt)
        if dateErrs:
            errs.append(''.join(['The following --dateperiods could not be converted:', ','.join(dateErrs)]))
    if args.datestep:
        if args.datestep.isdigit():
            datestep = int(args.datestep)
        elif args.datestep not in dateSteps:
            errs.append("--datestep must be 'd'/'w'/'m'/'y' or number for number of days.")
        else:
            datestep = args.datestep
    if args.periodsubsample and not args.periodsubsample > 0:
        errs.append('--periodsubsample must be positive integer.')
    # Get corpid of ticker:
    query = 'SELECT Corpid FROM corporations WHERE Ticker = "%s"' % args.ticker
    corpid = db.ExecuteQuery(query, getResults=True)
    if not corpid is None and not corpid['corpid'][0]:
        errs.append('Ticker %s is not present in corporations table.' % args.ticker)
    if not db.TableExists('tweetdata'):
        errs.append('tweetdata table does not exist.')
    
    if errs:
        raise BaseException('\n'.join(errs))
    corpid = corpid['corpid'][0]

    # Use stepping if necessary:
    if datestep:
        columns = {'Date' : ['date', True, '']}
        if db.TableExists('targetdates'):
            db.ExecuteQuery('DROP TABLE targetdates')
        db.CreateTable('targetdates', columns)
        startDate = None
        endDate = None
        if args.dateperiod:
            startDate = datetime.strptime(args.dateperiod[0], '%Y-%m-%d')
            endDate = datetime.strptime(args.dateperiod[1], '%Y-%m-%d')
            if startDate > endDate:
                temp = endDate
                endDate = startDate
                startDate = temp
        else:
            minMax = db.ExecuteQuery('SELECT MIN(Date) as min, MAX(Date) as max FROM tweetdata WHERE corpid = %d' % corpid, getResults=True)
            if minMax:
                startDate = minMax['min'][0]
                endDate = minMax['max'][0]

        if startDate and endDate:
            if isinstance(datestep, int):
                dateTimes = rrule(DAILY, dtstart = startDate, until = endDate, interval = datestep)
            else:
                dateTimes = rrule(dateSteps[datestep], dtstart = startDate, until = endDate)
            dates = { 'Date' : list(dateTimes)}
            db.InsertValues('targetdates', dates)
    # Get all the tweet data:
    query = ['SELECT * FROM']
    query.append('tweetdata')
    query.append('AS A')
    if datestep:
        query.append('INNER JOIN targetdates AS B ON A.date = B.date')
    query.append('WHERE A.CorpID = %d' % corpid)
    if args.dateperiod:
        query.append('AND A.date >= ')
        query.append(args.dateperiod[0])
        query.append('AND A.date <= ')
        query.append(args.dateperiod[1])
    query = ' '.join(query)
    results = db.ExecuteQuery(query, getResults = True)
    # Remove temporary table for date stepping:
    if db.TableExists('targetdates'):
        db.ExecuteQuery('DROP TABLE targetdates')
    # Exit if no matching tweets in table:
    if not results:
        print(''.join(["No tweets available for ", args.ticker.upper(), '.']))
        return
    else:
        # Calculate sentiment scores:
        results = {col[col.find('.') + 1:len(col)] : results[col] for col in results}
        scores = SentimentAnalyzer.CalculateSentiments(results, 'tweet', 'tweetid')
        results['SentimentScores'] = list(scores.values()) 
        results = DataFrame.from_dict(results)
        if args.filterzeros:
            results = results.loc[results['SentimentScores'] != 0]
        results = results.set_index('tweetid')
        results = results.drop('corpid', axis=1)
        results = results.rename(columns = {col : col.capitalize() for col in results.columns})
        print("Generating sentiment score report at:")
        results.to_csv(args.path.strip())
        print("Finished generating report.")

if __name__ == '__main__':
    CalculateSentimentScores()