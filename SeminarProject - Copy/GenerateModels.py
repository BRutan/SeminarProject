#################################################
# GenerateModels.py
#################################################
# Description:
# * Generate %stock price vs %chg polarity score model 
# for company.

from argparse import ArgumentParser
import csv
from datetime import datetime
from math import log, exp, sqrt
import pandas as pd
from scipy.stats import t
from sklearn import metrics
from sklearn.linear_model import LinearRegression
import statsmodels.api as sm
import os
import re

def GenerateModels():
    parser = ArgumentParser()
    # Add required positional arguments:
    parser.add_argument('inpath', type = str, help = "Path to raw sentiment data file.")
    parser.add_argument('outpath', type = str, help="Path to output file.")
    # Add optional arguments:
    parser.add_argument('--lags', type = int, nargs = '+', help="Series of period lags on data to generate models with.")
    parser.add_argument('--models', type = str, nargs='+', help="All modeling types. Can be one of ['lin','log','semilog','loglog']")
    parser.add_argument('--staling', action = "store_true", help="Put if want to use previous day's sentiment score if currently 0.")
    parser.add_argument('--searchterms', action = "store_true", help="Put if want to use each search term as parameters.")
    
    # Will raise exception if any args failed:
    inpath, outpath, models, lags, staling, searchterms = CheckArgs(parser)
    # Get data:
    data, searchterms = PullFile(inpath, searchterms)
    data = ParseData(data, staling, searchterms)
    # Generate all models and write results:
    data = GenerateModels(data,lags,models,searchterms)
    WriteResults(outpath, data)

def CheckArgs(parser):
    """
    * Check argument validity.
    """
    errs = []
    validModels = { 'lin' : False, 'log' : False, 'semilog' : False, 'loglog' : False }
    try:
        args = parser.parse_known_args()
    except BaseException as ex:
        pass

    if not os.path.exists(args.inpath):
        errs.append('File at inpath does not exist.')
    if os.path.exists(args.outpath):
        errs.append('Output file at outpath already exists.')
    if args.models:
        invalid = []
        for model in args.models:
            if model not in validModels:
                invalid.append(model)
            else:
                validModels[model] = True
        if invalid:
            errs.append(''.join(['The following models are invalid:', ','.join(invalid)]))
    else:
        # Generate all models:
        validModels = { key : True for key in validModels.keys() }
    validModels = [key for key in validModels.keys() if validModels[key]]
    lags = [] if not args.lags else list(args.lags).sort()
    
    if errs:
        raise Exception(''.join(errs))
    return (args.inpath, args.outpath, validModels, lags, args.staling, args.searchterms)

def PullFile(path, searchterms):
    """
    * Pull data from file.
    """
    reqCols = { 'return' : -1, 'date' : -1, 'retweets' : -1, 'polarityscore' : -1 }
    searchTermParams = []
    # Include search term column in data pull:
    if searchterms:
        reqCols['searchterm'] = -1
    colnames = [pair[0] for pair in reqCols]
    data = { key : [] for key in colnames }
    with open(path, 'r') as f:
        reader = csv.reader(f)
        atHeader = True
        missingHeaders = []
        for row in reader:
            if not atHeader:
                for num, col in enumerate(colnames):
                    # Strip unicode characters if necessary:
                    data[col].append(re.sub("(b\'|\')", '', row[reqCols[col]]('')))
            else:
                # Determine header column numbers, and if all required headers present:
                for cell, colNum in enumerate(row):
                    cleaned = ''.join([ch for ch in cell.lower() if ch != ' '])
                    if cleaned in expCols:
                        expCols[cleaned] = colNum
                for pair in expCols:
                    if pair[1] == -1:
                        missingHeaders.append(pair[0])
                if missingHeaders:
                    raise Exception(''.join(['The following headers are missing from input file:', ','.join(missingHeaders)]))
                atHeader = False
        if searchterms:
            searchTermParams = set(data['searchterm'])
        return data, searchTermParams

def ParseData(data, staling = False, searchterms = None):
    """
    * Calculate average sentiment score for each date.
    """
    numRows = len(data['polarityscore'])
    uniqueDates = {}
    dateIndex = 1
    for date in data['date']:
        if date not in uniqueDates:
            uniqueDates[date] = dateIndex
            dateIndex += 1
    # Output maps date index to (date, return, period_sentiment_score).
    # Expects that 1-to-1 mapping of return -> date.
    output = {}
    row = 0
    while row < numRows:
        currDate = data['date'][row]
        sentiment = data['polarityscore'][row] * (data['retweets'] + 1)
        _return = data['return'][row]
        count = 1
        row += 1
        while currDate == data['date'][row] and row < numRows:
            count += 1
            sentiment += data['polarityscore'][row] * (data['retweets'] + 1)
            row += 1
        # Perform staling if requested and necessary:
        if not staling:
            output[uniqueDates[currDate]] = (currDate, _return, sentiment / count)
        elif sentiment == 0 and output:
            output[uniqueDates[currDate]] = (currDate, _return, output[uniqueDates[currDate] - 1][2])

    return output
    
def GenerateModels(data, lags, models):
    """
    * Return object detailing Lag, Beta, ModelType, P-Val.
    """
    numRows = len(data.keys())
    # { 'ModelType' -> {'Lag' -> { 'Beta', 'PVal' } } }
    output = { }
    if not lags:
        lags = [0]

    for model in models:
        output[model] = {}
        # Perform appropriate lagging:
        for lag in lags:
            # Transform dataset based upon model type:
            modelData = GenModelData(lag, data, model)
            # Generate model:
            output[model][lag] = Model(modelData)

    return output

def GenModelData(lag, data, model):
    """
    * Generate data for modeling.
    """
    modelData = {'y' : [], 'x' : []}
    for rowIndex in range(lag, numRows):
        rowTup = data[rowIndex - lag]
        x = rowTup[2]
        y = rowTup[1]
        if model == 'lin':
            modelData['y'].append(x)
            modelData['x'].append(y)
        elif model == 'log':
            modelData['y'].append(log(y))
            modelData['x'].append(x)
        elif model == 'semilog':
            modelData['y'].append(y)
            modelData['x'].append(log(x))
        elif model == 'loglog':
            modelData['y'].append(log(y))
            modelData['x'].append(log(x))
     
    return modelData

def Model(data):
    """
    * Calculate all relevant results for given lag and model specification.
    """
    reg = LinearRegression()
    reg.fit(data['x'], data['y'])
    params = reg.get_params()
    y_pred = regressor.predict(data['x'])
    r_sq = metrics.r2_score(y_test, y_pred)
    reg = sm.OLS(data['y'], data['x'])
    reg = reg.fit()
    r_2 = reg.rsquared
    beta_0 = reg.params[0]
    beta_1 = reg.params[1]
    pval_0 = reg.pvalues[0]
    pval_1 = reg.pvalues[1]
    return {'R2' : r_2, 'Beta_0' : beta_0, 'Beta_1' : beta_1, 'PVal_0' : pval_0, 'PVal_1' : pval_1}

def WriteResults(path, results):
    """
    * Write results to file.
    """
    dataCols = ['R2','Beta_0','PVal_0', 'Beta_1','PVal_1']
    with open(path, 'w', newline='') as f:
        writer = csv.writer(f)
        # Write headers:
        writer.writerow(['Model:', 'Lag:', 'R2', 'B_0:', 'P_0:', 'B_1:', 'P_1:'])
        for model in results.keys():
            for lag in results[model].keys():
                row = [model]
                row.append(lag)
                row.extend([results[model][lag][col] for col in dataCols])
                writer.writerow(row)
        