#################################
# CalculateSentimentScores.py
#################################
# Description:
# * Calculate sentiment scores of all tweets
# for passed ticker stored in database.

import argparse
import csv
import DataBase
from datetime import datetime, date
import os
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
    args = parser.parse_args()
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
    if errs:
        raise Exception('\n'.join(errs))

    # Calculate sentiment scores using stored tweets, output to local csv file.
    table = 'tweets_%s' % args.ticker.lower()
    # Exit immediately if tweet table for ticker does not exist yet:
    if not db.TableExists(table):
        print(''.join(['Tweet table (', table, ') for ', ticker, ' does not exist.']))
        return
    query = ['SELECT A.searchterm, A.user, A.date, A.tweet, A.retweets, B.subsidiaries FROM ']
    query.append(table)
    query.append(' AS A INNER JOIN subsidiaries AS B ON A.SubNum = B.Number;')
    query = ''.join(query)
    results = db.ExecuteQuery(query, getResults = True)
    # Exit immediately if no tweets in table:
    if not results:
        print(''.join(["No tweets available for ", ticker, '.']))
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