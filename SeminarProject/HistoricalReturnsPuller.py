#################################################
# HistoricalReturnsPuller.py
#################################################
# Description:
# * Pull historical returns for given ticker and
# time period.

from pandas_datareader.data import DataReader


class ReturnsPuller(object):
    """
    * Pull historical returns for ticker between dates.
    """
    
    def GetPrices(self, ticker):
	    data = DataReader(ticker, 'yahoo', self.__startDate, self.__endDate)['Adj Close']
	    for row, date in enumerate(list(data.keys())):
		    if row == 0:
			    S_T_0 = data[date]
		    else:
			    # 
			    currReturn = float(data[date]) / float(S_T_0) - 1
			    corpReturns["CorpID"].append(id)
			    corpReturns["Date"].append(date)
			    corpReturns["Corp_Ret"].append(currReturn)
			    S_T_0 = results[date]
			    dayOffset += 1
		    currRow += 1
		
