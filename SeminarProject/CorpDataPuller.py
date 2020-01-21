#################################################
# CorpDataPuller.py
#################################################
# Description:
# * Pull historical returns for given ticker and
# time period.

from pandas_datareader.data import DataReader
import requests

class DataPuller(object):
    """
    * Pull historical returns, company data for ticker.
    """
    __nameUrl = "http://d.yimg.com/autoc.finance.yahoo.com/autoc?query={}&region=1&lang=en"
    def GetName(self, ticker):
        """
        * Get name of company.
        """
        symbol_list = requests.get(DataPuller.__nameUrl.format(ticker)).json()
        result = requests.get(url).json()
        for x in result['ResultSet']['Result']:
            if x['symbol'] == ticker:
                return x['name']
    
    def GetPrices(self, startDate, endDate, ticker, column = 'Adj Close'):
        """
        * Get prices of security with ticker between dates.
        """
	    data = DataReader(ticker, 'yahoo', startDate, endDate)[column]
	    for row, date in enumerate(list(data.keys())):
		    if row == 0:
			    S_T_0 = data[date]
		    else:
			    currReturn = float(data[date]) / float(S_T_0) - 1
                # Determine if need to stale data:
			    corpReturns["CorpID"].append(id)
			    corpReturns["Date"].append(date)
			    corpReturns["Corp_Ret"].append(currReturn)
			    S_T_0 = results[date]
			    dayOffset += 1
		    currRow += 1
		
