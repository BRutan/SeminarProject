#################################################
# CorpDataPuller.py
#################################################
# Description:
# * Pull historical returns for given ticker and
# time period.

from datetime import datetime, date, timedelta
import lxml
from lxml import html
from math import log
import requests
#import requests_cache
import numpy as np
from pandas import DataFrame
import yfinance as yf
import requests
import os

class CorpDataPuller(object):
    """
    * Pull historical returns, company data for ticker.
    """
    __haveAPIKeys = False
    #__session = requests_cache.CachedSession(cache_name = 'cache', backend = 'sqlite', expire_after = timedelta(days=3))
    __validPriceTypes = set(['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])
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
    __requestAttributes = { 'sector' : 'sector', 'industry' : 'industry', 'full time employees' : 'full time employees' }
    __validYFinanceAttributes = { key.lower() : key for key in __allAttributes }
    def __init__(self, attributes = None, priceTypes = None):
        """
        * Instantiate new object.
        """
        self.__PriceTypes = []
        self.__RequestAttrs = []
        self.__YFinAttrs = []
        errs = []
        if attributes:
            errs.extend(self.__CheckAttrs(attributes))
        if priceTypes:
            errs.extend(self.__CheckPriceTypes(priceTypes))
        if errs:
            raise Exception('\n'.join(errs))

    def GetAttributes(self, ticker, attributes = None, priceTypes = None):
        """
        * Get attributes of company with ticker.
        Inputs:
        * ticker: String with company ticker.
        Optional Inputs:
        * attributes: Put 'all' if want all possible attributes. Otherwise must be string in ValidAttributes().
        Outputs:
        * Returns map containing { Attr -> Value }.
        """
        errs = []
        if not isinstance(ticker, str):
            errs.append('ticker must be string.')
        if attributes:
            result = self.__CheckAttrs(attributes)
            if result:
                errs.extend(result)
        if errs:
            raise Exception('\n'.join(errs))
        # Get requested attributes for company:
        output = {}
        ticker = ticker.upper()
        if self.__YFinAttrs:
            data = yf.Ticker(ticker)
            for attr in self.__YFinAttrs:
                if attr not in data.info:
                    val = None
                else:
                    val = data.info[attr]
                val = val if not isinstance(val, str) else val.strip()
                output[attr] = val
        
        if self.__RequestAttrs:
            url = 'https://finance.yahoo.com/quote/%s/profile?p=%s' % (ticker, ticker)
            result = requests.get(url)
            tree = html.fromstring(result.content)
            for label in self.__RequestAttrs:
                cap = label.capitalize()
                xp = f"//span[text()='{cap}']/following-sibling::span[1]"
                s = None
                try:
                    s = tree.xpath(xp)[0]
                    val = s.text_content()
                except:
                    val = None
                output[label] = val

        output = { key.lower() : output[key] for key in output }
        
        return output

    def GetAssetPrices(self, ticker, startDate, endDate, priceTypes = None):
        """
        * Get prices of security with ticker between dates.
        Inputs:
        * startDate: Expecting datetime/string with format YYYY-MM-DD.
        * endDate: Expecting datetime/string with format YYYY-MM-DD. 
        * ticker: string security ticker or list.
        Optional:
        * priceType: List of price types, or single string denoting price type, or 'all' if want all. 
        Must be in ValidPriceTypes(). Is 'Adj Close' if omitted.
        Output:
        * Returns dataframe filled with asset prices with Date and PriceTypes as columns for ticker.
        """
        errs = []
        if not isinstance(ticker, (str, list)):
            errs.append('ticker must be a string or list of strings.')
        elif isinstance(ticker, str):
            ticker = [ticker]
        if priceTypes:
            errs.extend(self.__CheckPriceTypes(priceTypes))
        else:
            priceTypes = ['Adj Close']
        startDate, endDate, dateErrs = CorpDataPuller.__ConvertDates(startDate, endDate)
        errs.extend(dateErrs)
        if errs:
            raise BaseException('\n'.join(errs))
        # Swap date order if necessary:
        if startDate > endDate:
            copy = endDate
            endDate = startDate
            startDate = copy
        
        coldiffs = set(CorpDataPuller.__validPriceTypes) - set(priceTypes) 
        prices = yf.download(tickers = ticker, start = startDate.strftime('%Y-%m-%d'), end = endDate.strftime('%Y-%m-%d'))
        # Return prices with only requested columns:
        return prices.drop([col for col in prices.columns if col[0] in coldiffs], axis = 1)

    ###################
    # Private Helpers:
    ###################
    def __CheckAttrs(self, attributes):
        """
        * Ensure attributes are valid.
        """
        errs = []
        if isinstance(attributes, str) and not attributes.lower() != 'all':
            errs.append('attributes must be "all" if a string.')
        elif isinstance(attributes, str):
            self.__YFinAttrs = CorpDataPuller.__validYFinanceAttributes.keys()
            self.__RequestAttrs = CorpDataPuller.__requestAttributes.keys()
        elif isinstance(attributes, list) and attributes:
            attributes = [attr.lower() for attr in attributes]
            invalid = [attr for attr in attributes if attr not in CorpDataPuller.__validYFinanceAttributes and attr not in CorpDataPuller.__requestAttributes]
            if invalid:
                errs.append(''.join(['The following attributes are invalid: ', ','.join(invalid)]))
            else:
                self.__YFinAttrs = []
                self.__RequestAttrs = []
                for attr in attributes:
                    if attr in CorpDataPuller.__validYFinanceAttributes:
                        self.__YFinAttrs.append(CorpDataPuller.__validYFinanceAttributes[attr])
                    elif attr in CorpDataPuller.__requestAttributes:
                        self.__RequestAttrs.append(CorpDataPuller.__requestAttributes[attr])
        elif isinstance(attributes, list) and len(attributes) == 0:
            errs.append('attributes must include at least one attribute.')
        else:
            errs.append('attributes must be "all" or list.')
        
        return errs
        
    def __CheckPriceTypes(self, priceTypes):
        errs = []
        if isinstance(priceTypes, str) and priceTypes != 'all' and priceTypes not in CorpDataPuller.__validPriceTypes:
            errs.append('priceType is invalid.')
        elif isinstance(priceTypes, str) and priceTypes == 'all':
            self.__PriceTypes = CorpDataPuller.__validPriceTypes.keys()
        elif not priceTypes:
            errs.append('Need at least one priceType.')
        elif isinstance(priceTypes, list):
            invalid = [pType for pType in priceTypes if pType.capitalize() not in CorpDataPuller.__validPriceTypes]
            if invalid:
                errs.append(''.join(['The following priceTypes are invalid: ', ','.join(invalid)]))
        else:
            errs.append('priceType must be a string or a list.')
        return errs

    ###################
    # Static Methods:
    ###################
    @staticmethod
    def ValidAttributes():
        return list(CorpDataPuller.__validAttributes.keys())
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

        



