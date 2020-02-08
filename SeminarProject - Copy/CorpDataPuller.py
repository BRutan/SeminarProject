#################################################
# CorpDataPuller.py
#################################################
# Description:
# * Pull historical returns for given ticker and
# time period.

from datetime import datetime, date, timedelta
from pandas_datareader.data import DataReader
import requests_cache
from pandas_finance import Equity
import requests
import os

class DataPuller(object):
    """
    * Pull historical returns, company data for ticker.
    """
    __alphaVantageAPIKey = ''
    __intrinioAPIKey = ''
    __intrinioURL = 'https://api-v2.intrinio.com/companies/%s?api_key=%s'
    __haveAPIKeys = False
    __session = requests_cache.CachedSession(cache_name = 'cache', backend = 'sqlite', expire_after = timedelta(days=3))

    #__validPriceTypes = { 'adjclose' : 'adj_close', 'close' : 'close' }
    #__validPriceTypes = { 'high' : 'High', 'low' : 'Low', 'open' : 'Open', 'close' : 'Close', 'volume' : 'Volume', 'adjclose' : 'Adj Close' }
    __validPriceTypes = { 'daily' : 'av-daily', 'intraday' : 'av-intraday', 'daily_adj' : 'av-daily-adjusted', 'weekly' : 'av-weekly',
                         'weekly_adj' : 'av-weekly-adjusted', 'monthly' : 'av-monthly', 'monthly_adj' : 'av-monthly-adjusted' }

    __validAttributes = { "ticker": True,"name": True,"legal_name": True,"stock_exchange": True,"short_description": True,"long_description": True,"ceo": True,
      "company_url": True,"business_address": True,"mailing_address": True,"business_phone_no": True,"hq_address1": True,"hq_address2": True,"hq_address_city": True,
      "hq_address_postal_code": True,"entity_legal_form": True,"cik": True,"latest_filing_date": True,"hq_state": True,"hq_country": True,"inc_state": True,"inc_country": True,
      "employees": True,"entity_status": True,"sector": True,"industry_category": True,"industry_group": True,"template": True,"standardized_active": True,"first_fundamental_date": True,
      "last_fundamental_date": True,"first_stock_price_date": True,"last_stock_price_date": True }
    
    def __init__(self):
        """
        * Instantiate new object.
        """
        pass
    def GetAttributes(self, ticker, attributes, startDate = datetime.today()):
        """
        * Get attributes of company with ticker.
        Inputs:
        * ticker: String with company ticker.
        * attributes: Put 'all' if want all possible attributes. Otherwise must be string in ValidAttributes().
        Outputs:
        * Returns map containing { Attr -> Value }.
        """
        DataPuller.__GetAPIKey()
        errs = []
        invalid = []
        if not isinstance(ticker, str):
            errs.append('ticker must be string.')
        if isinstance(attributes, str):
             if attributes.lower() != 'all':
                 errs.append('attributes must be "all" if a string.')
             else:
                 attributes = list(DataPuller.__validAttributes.keys())
        elif isinstance(attributes, list) and len(attributes) > 0:
            invalid = [val for val in attributes if not (isinstance(val.lower(), str) and val.lower() in DataPuller.__validAttributes)]
        elif len(attributes) == 0:
            errs.append('attributes must include at least one attribute.')
        else:
            errs.append('attributes must be a list or "all".')
        if not DataPuller.__intrinioAPIKey:
            errs.append('Missing Intrio API key at "__intrinioapikey__.dat" in local folder. See documentation.')
        if invalid:
            errs.append(''.join(['The following attributes are invalid: ', ','.join([str(val) for val in invalid])]))
        
        startDate, endDate, dateErrs = DataPuller.__ConvertDates(startDate, datetime.today())
        errs.extend(dateErrs)
        if errs:
            raise Exception('\n'.join(errs))
        
        # Get requested attributes for company:
        ticker = ticker.upper()
        output = {}
        
        # Use requests library at website if requested security type/ exchange information:
        url = DataPuller.__intrinioURL % (ticker, DataPuller.__intrinioAPIKey)
        try:
            result = requests.get(url).json()
        except:
            raise Exception(''.join(['Failed to pull attribute data for ', ticker, '.']))

        if result and hasattr(result, human):
            raise Exception(result['message'])
        
        for attr in attributes:
            output[attr] = result[attr].trim()

        return output

    def GetAssetPrices(self, startDate, endDate, ticker, priceType = 'daily'):
        """
        * Get prices of security with ticker between dates.
        Inputs:
        * startDate: Expecting datetime/string with format YYYY-MM-DD.
        * endDate: Expecting datetime/string with format YYYY-MM-DD. 
        * ticker: Security ticker string.
        * priceType: String denoting price type. Must be in ValidPriceTypes().
        """
        DataPuller.__GetAPIKey()
        errs = []
        if not isinstance(ticker, str):
            errs.append('ticker must be a string.')
        if not isinstance(priceType, str):
            errs.append('priceType must be a string.')
        elif not priceType.lower() in DataPuller.__validPriceTypes:
            errs.append(''.join(['priceType must be one of {', ','.join(DataPuller.__validPriceTypes.keys()), '}']))
        if not DataPuller.__alphaVantageAPIKey:
            errs.append('Missing Alpha Vantage API key at local "__alphavantage__.dat" in local folder. See documentation.')

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
        try:
            data = DataReader(ticker.upper(), priceType, startDate, endDate, 
                              session = DataPuller.__session, access_key = DataPuller.__alphaVantageAPIKey)
        except:
            raise Exception(''.join(['Failed to get data for ', ticker, '.']))
        
        return data

    ###################
    # Private Helpers:
    ###################
    @staticmethod
    def ValidPriceTypes():
        return list(DataPuller.__validPriceTypes.copy())
    @staticmethod
    def ValidAttributes():
        return list(DataPuller.__validAttributes)
    @staticmethod
    def __GetAPIKey():
        """
        * Get API Keys for Alpha Vantage, Intrinio.
        """
        errs = []
        if not DataPuller.__alphaVantageAPIKey:
            if os.path.exists('__alphavantageapikey__.dat'): 
                with open('__alphavantageapikey__.dat', 'r') as f:
                    DataPuller.__alphaVantageAPIKey = f.read().strip()
        if not DataPuller.__intrinioAPIKey:
            if os.path.exists('__intrinioapikey__.dat'):
                with open('__intrinioapikey__.dat', 'r') as f:
                    DataPuller.__intrinioAPIKey = f.read().strip()

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



