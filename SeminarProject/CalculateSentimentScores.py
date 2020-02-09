#################################
# CalculateSentimentScores.py
#################################
# Description:
# * Calculate sentiment scores of all tweets
# for passed ticker stored in database.

import argparse
import csv
import DataBase
from datetime import datetime, date, timedelta
from dateutil.rrule import rrule, YEARLY, MONTHLY, WEEKLY, DAILY
from dateutil.relativedelta import relativedelta
import os
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
    parser.add_argument('--host', type = str, help="IP Address of MYSQL instance.")
    parser.add_argument('--nosubs', action = "store_true", help="Put if want to skip pulling subsidiary information.")
    parser.add_argument('--dateperiod', type = str, nargs = 2, help="Include start and end date (YYYY-MM-DD) for pulling sentiment scores.")
    parser.add_argument('--datestep', type = str, help="Include if using --dateperiod, where 'd': daily, 'w' : weekly, 'm' : monthly, 'y' : yearly, or number for number of days.")
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
    
    if errs:
        raise BaseException('\n'.join(errs))

    # Calculate sentiment scores using stored tweets, output to local csv file.
    table = 'tweets_%s' % args.ticker.lower()
    # Exit immediately if tweet table for ticker does not exist yet:
    if not db.TableExists(table):
        print(''.join(['Tweet table (', table, ') for ', ticker, ' does not exist.']))
        return
    joinSubs = False
    # Determine if need to include subsidiary information:
    results = {}
    if not args.nosubs:
        query = ['SELECT B.Ticker FROM subsidiaries AS A ']
        query.append('INNER JOIN corporations as B ')
        query.append('ON A.CorpID = B.CorpID ')
        query.append('WHERE subsidiaries IS NOT NULL AND ticker = "')
        query.append(args.ticker.upper())
        query.append('"')
        query = ''.join(query)
        results = db.ExecuteQuery(query, getResults = True)
    # Use stepping if necessary:
    if datestep:
        columns = {'Date' : ['date', True, '']}
        if db.TableExists('targetdates'):
            db.ExecuteQuery('DROP TABLE targetdates')
        db.CreateTable('targetdates', columns)

        if not args.dateperiod:
            query = ['SELECT Min(Date) as Min, Max(Date) as Max FROM ']
            query.append(table)
            results = db.ExecuteQuery(''.join(query), getResults = True)
            startDate = results['min'][0]
            endDate = results['max'][0]
        else:
            startDate = datetime.strptime(args.dateperiod[0], '%Y-%m-%d')
            endDate = datetime.strptime(args.dateperiod[1], '%Y-%m-%d')
        
        if startDate > endDate:
            temp = endDate
            endDate = startDate
            startDate = temp

        if isinstance(datestep, int):
            dateTimes = rrule(DAILY, dtstart = startDate, until = endDate, interval = datestep)
        else:
            dateTimes = rrule(dateSteps[datestep], dtstart = startDate, until = endDate)
        dates = { 'Date' : list(dateTimes)}
        db.InsertValues('targetdates', dates)
    
    # Get all the tweet data:
    query = ['SELECT A.searchterm, A.user, A.date, A.tweet, A.retweets', '', ' FROM ']
    query.append(table)
    query.append(' AS A ')
    if not args.nosubs and results:
        query[1] = ", B.subsidiaries "
        query.append('INNER JOIN subsidiaries AS B ON A.SubNum = B.Number ')
    if datestep:
        query.append('INNER JOIN targetdates AS C ON A.date = C.date')
    elif args.dateperiod:
        query.append(' WHERE A.date >= ')
        query.append(args.dateperiod[0])
        query.append(' AND A.date <= ')
        query.append(args.dateperiod[1])
    query = ''.join(query)
    results = db.ExecuteQuery(query, getResults = True)
    # Exit immediately if no tweets in table:
    if not results:
        print(''.join(["No tweets available for ", args.ticker.upper(), '.']))
        return
    else:
        print("Generating sentiment score report at:")
        print(args.path.strip())
        rowCount = len(results[list(results.keys())[0]])
        with open(args.path.strip(), 'w', newline='') as f:
            writer = csv.writer(f)
            columns = [header for header in results.keys() if '.tweet' not in header]
            formattedColumns = [header[header.index('.') + 1: len(header)] for header in columns]
            formattedColumns.append('Polarity Score')
            columns.append('PS')
            text = results[table.lower() + '.tweet']
            writer.writerow(formattedColumns)
            for row in range(0, rowCount):
                rowText = []
                for colNum in range(0, len(columns)):
                    if colNum < len(columns) - 1:
                        column = columns[colNum]
                        if isinstance(results[column][row], (datetime, date)):
                            val = results[column][row].strftime('%Y-%m-%d')
                        else:
                            # Encode cell in unicode if necessary:
                            val = str(results[column][row])
                            if not IsAscii(val):
                                val = val.encode('utf-8')
                        rowText.append(val)
                    else:
                        rowText.append(SentimentAnalyzer.CalculateSentiment(text[row]))
                writer.writerow(rowText)
            print("Finished generating report.")

if __name__ == '__main__':
    CalculateSentimentScores()