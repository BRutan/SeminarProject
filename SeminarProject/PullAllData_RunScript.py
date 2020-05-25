#################################
# PullAllData_RunScript.py
#################################
# Description:
# * Pull tweets/financials for single company, push into database.

import argparse
import csv
import DataBase
from datetime import datetime
import os
import pandas as pd
from PullTwitterData import TwitterPuller
import re
from SeminarProject import SeminarProject
import sys

def PullAllData():
    # Get command line arguments:
    parser = argparse.ArgumentParser(prog='PullAllData_RunScript')
    parser.add_argument('username', type = str, help="Username for MYSQL instance.")
    parser.add_argument('pw', type = str, help="Password to MYSQL instance.")
    parser.add_argument('schema', type = str, help="Name of schema containing all tables.")
    parser.add_argument('inputpath', type = str, help='Path to file containing all tickers with pulling attributes.')
    # Optional arguments:
    parser.add_argument('--host', type = str, help='IP Address of MYSQL instance.')
    args = parser.parse_known_args()
    errs = []
    # Check if any unknown arguments were passed:
    if args[1]:
        errs.append(''.join(['The following arguments are unknown: ', '\n'.join(args[1])]))
    args = args[0]

    if not args.host:
        host = "127.0.0.1"
    else:
        host = args.host.strip()

    try:
        db = DataBase.MYSQLDatabase(args.username, args.pw, host, args.schema)
    except:
        errs.append('Could not open MYSQL database instance %s.' % args.schema)
    try:
        inputs = GetPullInputs(args.inputpath)
    except BaseException as ex:
        errs.append(str(ex))

    if errs:
        raise Exception('\n'.join(errs))

    ##########################
    # Perform key steps:
    ##########################
    seminar = SeminarProject(inputs, db)
    print("Creating tables.")
    seminar.CreateTables()
    print("Inserting corporation attributes.")
    seminar.InsertCorpAttributes()
    print("Inserting historical prices.")
    seminar.GetHistoricalData()
    print("Inserting subsidiaries.")
    seminar.GetSubsidiaries()
    print("Inserting brands.")
    seminar.GetBrands()
    print("Gathering and inserting tweet data.")
    seminar.GetTweets()
    print("Finished with all key steps.")

def GetPullInputs(path):
    """
    * Pull ticker, startdate, enddate, daystep, numbrands, addlsearchtermsm, toptweets and overridebrands from local file.
    File is required to have these columns (case insensitive).
    """
    errs = []
    validDate = re.compile('[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}')
    if '.' not in path:
        errs.append('path must point to file.')
    if not os.path.exists(path):
        errs.append('File at path does not exist.')
    if  errs:
        raise Exception(''.join(errs))
    reqCols = { 'ticker' : False, 'startdate' : False, 'enddate' : False, 'daystep' : False, 'numbrands' : False, 'addlsearchterms' : False, 
               'periodsamplesize' : False, 'toptweets' : False, 'overridebrands' : False }
    data = pd.read_csv(path)
    data = data.rename(columns = { col : col.strip().lower() for col in data.columns })
    invalidCols = []
    for col in data.columns:
        if col not in reqCols:
            invalidCols.append(col)
        else:
            reqCols[col] = True
    missingCols = [col for col in reqCols if reqCols[col] == False]
    if invalidCols:
        errs.append(''.join(['The following columns are invalid:',','.join(invalidCols)]))
    if missingCols:
        errs.append(''.join(['The following columns are missing:',','.join(missingCols)]))
    if [dt for dt in data['startdate'] if not validDate.match(dt)] or [dt for dt in data['enddate'] if not validDate.match(dt)]:
        errs.append(''.join(['startdate and enddate columns must use MM/DD/YYYY format.']))
    if errs:
        raise BaseException('\n'.join(errs))
    
    # Convert data to appropriate format downstream:
    data['startdate'] = [datetime.strptime(dt, '%m/%d/%Y') for dt in data['startdate']]
    data['enddate'] = [datetime.strptime(dt, '%m/%d/%Y') for dt in data['enddate']]
    data['addlsearchterms'] = [val.split(';') for val in data['addlsearchterms']]
    data['toptweets'] = [True if val.lower() == 't' else False for val in data['toptweets']]
    data['overridebrands'] = [True if val.lower() == 't' else False for val in data['overridebrands']]

    return data

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
    PullAllData()
    