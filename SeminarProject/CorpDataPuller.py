#################################################
# CorpDataPuller.py
#################################################
# Description:
# * Pull historical returns for given ticker and
# time period.

from datetime import datetime, date
from pandas_datareader.data import DataReader
import pandas_finance 
from pandas_finance import Equity
import requests

class DataPuller(object):
    """
    * Pull historical returns, company data for ticker.
    """
    __validPriceTypes = { 'adjclose' : 'adj_close', 'close' : 'close' }

    __dataReaderAttributes = { 'exchange' : 'exch', 'type' : 'typeDisp'  }
    __pdFinanceAttributes = { 'employees' : 'employees', 'industry' : 'industry', 'currency' : 'currency', 'marketcap' : 'market_cap', 
                      'sector' : 'sector', 'shares' : 'shares_os', 'profile' : 'profile', 'name' : 'name', 'div' : 'dividends' }
    __nameUrl = "http://d.yimg.com/autoc.finance.yahoo.com/autoc?query={}&region=1&lang=en"
    def GetAttributes(self, ticker, attributes, startDate = datetime.today()):
        """
        * Get attributes of company with ticker.
        Inputs:
        * ticker: String with company ticker.
        * attributes: Put 'all' if want all possible attributes. 
            Otherwise, list that can contain following strings:
            - employees: # of employees in company.
            - industry: Company industry.
            - currency: Currency security is traded in.
            - marketcap: Total equity market capitalization.
            - shares: # of shares outstanding.
            - profile: Brief description of company.
            - div: Annual dividends.
            - exchange: Exchange security is traded on.
            - sector: Company sector.
            - type: Security type.
        Outputs:
        * Returns map containing requested attributes.
        """
        errs = []
        invalid = []
        if not isinstance(ticker, str):
            errs.append('ticker must be string.')
        if isinstance(attributes, str):
             if attributes.lower() == 'all':
                 errs.append('attributes must be "all" if a string.')
             else:
                 pdFinanceAttrs = { attr : '' for attr in DataPuller.__pdFinanceAttributes }
                 drAttributes = { attr : '' for attr in DataPuller.__dataReaderAttributes }
        if not isinstance(attributes, list):
            errs.append('attributes must be a list.')
        else:
            invalid = [val for val in attributes if not (isinstance(val, str) and val in __validAttributes)]
        if invalid:
            errs.append(''.join(['The following attributes are invalid:', ','.join([str(val) for str in invalid])]))
        startDate, endDate, dateErrs = __ConvertDates(startDate, datetime.today())
        errs.extend(dateErrs)
        if errs:
            raise Exception('\n'.join(errs))
        
        # Get requested attributes for company:
        ticker = ticker.lower()
        output = {}
        # Do partial search if 'all' was not specified for attributes:
        if not pdFinanceAttrs and not drAttributes:
            pdFinanceAttrs = { attr : '' for attr in attributes if attr in DataPuller.__pdFinanceAttributes }
            drAttributes = { attr : '' for attr in attributes if attr in DataPuller.__dataReaderAttributes }

        # Use requests library at website if requested security type/ exchange information:
        if drAttributes:
            symbol_list = requests.get(DataPuller.__nameUrl.format(ticker)).json()
            result = requests.get(url).json()
            for x in result['ResultSet']['Result']:
                if x['symbol'].lower() == ticker:
                    for attr in drAttributes.keys():
                        if DataPuller.__dataReaderAttributes[attr] in x and x[DataPuller.__dataReaderAttributes[attr]].strip():
                            output[attr] = x[DataPuller.__dataReaderAttributes[attr]].strip()
                        else:
                            # Put NA if could not find requested attribute:
                            output[attr] = 'NA'
                        break

        # Use pandas_finance library to pull other information:
        if pdFinanceAttrs:
            # Set start date for analysis:
            pandas_finance.START_DATE = startDate
            eq = Equity(ticker)
            for attr in pdFinanceAttrs.keys():
                val = getattr(eq, DataPuller.__pdFinanceAttributes[attr]).strip()
                if val:
                    output[attr] = val
                else:
                    output[attr] = 'NA'

        return output
    
    def GetPrices(self, startDate, endDate, ticker, priceType = 'adjclose'):
        """
        * Get prices of security with ticker between dates.
        Inputs:
        * startDate: Expecting datetime/string with format YYYY-MM-DD.
        * endDate: Expecting datetime/string with format YYYY-MM-DD. 
        * ticker: Security ticker string.
        * priceType: String denoting price type (adjusted close, close, etc).
        """
        errs = []
        if not isinstance(ticker, str):
            errs.append('ticker must be a string.')
        if not isinstance(priceType, str):
            errs.append('priceType must be a string.')
        elif not priceType.lower() in DataPuller.__validPriceTypes:
            errs.append(''.join(['priceType must be one of {', ','.join(DataPuller.__validPriceTypes.keys())], '}'))
        startDate, endDate, dateErrs = __ConvertDates(startDate, endDate)
        errs.extend(dateErrs)
		if errs:
            raise Exception('\n'.join(errs))

        ticker = ticker.lower()
        priceType = DataPuller.__validPriceTypes[priceType.lower()]
        # Swap date order if necessary:
        if startDate > endDate:
            copy = endDate
            endDate = startDate
            startDate = endDate
        
        pandas_finance.START_DATE = startDate

        try:
            eq = Equity(ticker)
            col = DataPuller.__validPriceTypes[priceType]
            data = getattr(eq, col)
        except:
            raise Exception(''.join(['Failed to get data for ', ticker]))
        
        return data

    ###################
    # Private Helpers:
    ###################
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