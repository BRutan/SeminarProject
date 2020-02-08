#################################################
# CorpDataPuller.py
#################################################
# Description:
# * Pull historical returns for given ticker and
# time period.

from datetime import datetime, date, timedelta
from math import log
import requests_cache
import numpy as np
from pandas import DataFrame
import yfinance as yf
import requests
import os

class DataPuller(object):
    """
    * Pull historical returns, company data for ticker.
    """
    __haveAPIKeys = False
    __session = requests_cache.CachedSession(cache_name = 'cache', backend = 'sqlite', expire_after = timedelta(days=3))
    __validPriceTypes = { pType : True for pType in ['Open', 'High', 'Low', 'Close', 'Volume']}
    __allAttributes = ['language', 'region', 'quoteType', 'triggerable', 'quoteSourceName', 'currency', 'tradeable', 'exchange', 'shortName', 'longName', 'messageBoardId', 
                         'exchangeTimezoneName', 'exchangeTimezoneShortName', 'gmtOffSetMilliseconds', 'market', 
                         'esgPopulated', 'firstTradeDateMilliseconds', 'priceHint', 'postMarketChangePercent', 'postMarketTime', 'postMarketPrice', 'postMarketChange', 
                         'regularMarketChange', 'regularMarketChangePercent', 'regularMarketTime', 'regularMarketPrice', 'regularMarketDayHigh', 'regularMarketDayRange', 
                         'regularMarketDayLow', 'regularMarketVolume', 'regularMarketPreviousClose', 'bid', 'ask', 'bidSize', 'askSize', 'fullExchangeName', 
                         'financialCurrency', 'regularMarketOpen', 'averageDailyVolume3Month', 'averageDailyVolume10Day', 'fiftyTwoWeekLowChange', 
                         'fiftyTwoWeekLowChangePercent', 'fiftyTwoWeekRange', 'fiftyTwoWeekHighChange', 'fiftyTwoWeekHighChangePercent', 'fiftyTwoWeekLow', 
                         'fiftyTwoWeekHigh', 'dividendDate', 'earningsTimestamp', 'earningsTimestampStart', 'earningsTimestampEnd', 'trailingAnnualDividendRate', 
                         'trailingPE', 'trailingAnnualDividendYield', 'marketState', 'epsTrailingTwelveMonths', 'epsForward', 'sharesOutstanding', 
                         'bookValue', 'fiftyDayAverage', 'fiftyDayAverageChange', 'fiftyDayAverageChangePercent', 'twoHundredDayAverage', 'twoHundredDayAverageChange', 
                         'twoHundredDayAverageChangePercent', 'marketCap', 'forwardPE', 'priceToBook', 'sourceInterval', 'exchangeDataDelayedBy', 'symbol']
    __validAttributes = { key.lower() : key for key in __allAttributes}
    def __init__(self):
        """
        * Instantiate new object.
        """
        pass
    def GetAttributes(self, ticker, attributes):
        """
        * Get attributes of company with ticker.
        Inputs:
        * ticker: String with company ticker.
        * attributes: Put 'all' if want all possible attributes. Otherwise must be string in ValidAttributes().
        Outputs:
        * Returns map containing { Attr -> Value }.
        """
        errs = []
        invalid = []
        if not isinstance(ticker, str):
            errs.append('ticker must be string.')
        elif isinstance(attributes, str) and not attributes.lower() != 'all':
            errs.append('attributes must be "all" if a string.')
        elif isinstance(attributes, list) and attributes:
            attributes = [attr.lower() for attr in attributes]
            invalid = []
            for attr in attributes:
                if attr not in DataPuller.__validAttributes:
                    invalid.append(lowered)
            if invalid:
                errs.append(''.join(['The following attributes are invalid: ', ','.join(invalid)]))
        elif isinstance(attributes, list) and len(attributes) == 0:
            errs.append('attributes must include at least one attribute.')
        else:
            errs.append('attributes must be "all" or list.')
        if errs:
            raise Exception('\n'.join(errs))
        
        # Get requested attributes for company:
        output = {}
        data = yf.Ticker(ticker.upper())
        output = {}
        if isinstance(attributes, list):
            for attr in attributes:
                output[attr] = data.info[DataPuller.__validAttributes[attr]].trim()
        else:
            output = data.info

        return output

    def GetAssetPrices(self, ticker, startDate, endDate, priceType = 'all', contReturn = False, methodLambda = False):
        """
        * Get prices of security with ticker between dates.
        Inputs:
        * startDate: Expecting datetime/string with format YYYY-MM-DD.
        * endDate: Expecting datetime/string with format YYYY-MM-DD. 
        * ticker: Security ticker string.
        * priceType: List of price types, or single string denoting price type, or 'all' if want all. Must be in ValidPriceTypes().
        * contReturn: Put True if want to calculate continuously compounded returns.
        * methodLambda: (Optional) Expecting a lambda to calculate returns. Must use functions 
        that support Pandas.DataFrame.
        """
        errs = []
        if not isinstance(ticker, str):
            errs.append('ticker must be a string.')
        elif isinstance(priceType, str) and priceType != 'all' and priceType not in __validPriceTypes:
            errs.append('priceType is invalid.')
        elif isinstance(priceType, list) and not priceType:
            errs.append('Need at least one priceType.')
        elif isinstance(priceType, list):
            invalid = [pType for pType in priceType if pType not in __validPriceTypes]
            if invalid:
                errs.append(''.join(['The following priceTypes are invalid: ', ','.join(priceType)]))
        else:
            errs.append('priceType must be a string or a list.')
        if not isinstance(contReturn, bool):
            errs.append('contReturn must be a boolean.')

        startDate, endDate, dateErrs = DataPuller.__ConvertDates(startDate, endDate)
        errs.extend(dateErrs)

        if errs:
            raise Exception('\n'.join(errs))
        
        ticker = ticker.lower()
        # Swap date order if necessary:
        if startDate > endDate:
            copy = endDate
            endDate = startDate
            startDate = copy

        startDate = startDate.strftime('%Y-%m-%d')
        endDate = endDate.strftime('%Y-%m-%d')

        try:
            data = yf.Ticker(ticker.upper())
            prices = data.history(start = startDate, end = endDate)
        except:
            raise Exception('Could not get data for ticker.');
        if isinstance(priceType, str) and priceType != 'all':
            priceType = [priceType]
        if isinstance(priceType, list):
            # Drop all unused columns:
            cols = set(prices.columns)
            targetCols = set(priceType)
            dropCols = cols - targetCols
            if dropCols:
                prices = prices.drop(dropCols, axis=1)
        # Calculate returns:
        if contReturn:
            method = lambda x_2, x_1 : np.log(x_2 / x_1)
        else:
            method = lambda x_2, x_1 : x_2 / x_1 - 1
        if methodLambda:
            method = methodLambda

        return DataPuller.__CalcReturns(prices, method)

    ###################
    # Private Helpers:
    ###################
    @staticmethod
    def ValidAttributes():
        return list(DataPuller.__validAttributes.keys())
    @staticmethod
    def ValidPriceTypes():
        return list(__validPriceTypes.keys())
    @staticmethod
    def __ConvertDates(startDate, endDate):
        errs = []
        if not isinstance(startDate, (date, datetime, str)):
            errs.append('startDate must be date/datetime/"YYYY-MM-DD" string.')
        elif isinstance(startDate, str):
            try:
                startDate = datetime.strptime(startDate, '%Y-%m-%d')
            except:
                errs.append('startDate must be "YYYY-MM-DD".')
        if not isinstance(endDate, (date, datetime, str)):
            errs.append('endDate must be date/datetime/"YYYY-MM-DD" string.')
        elif isinstance(endDate, str):
            try:
                endDate = datetime.strptime(startDate, '%Y-%m-%d')
            except:
                errs.append('endDate must be "YYYY-MM-DD".')
            
        return (startDate, endDate, errs)
    @staticmethod
    def __CalcReturns(prices, method):
        earliestDate = min(prices.index)
        returns = method(prices, prices.shift(1))
        # Drop first row:
        return returns.drop(index = min(returns.index))

        



