#################################
# PullTwitterData.py
#################################
# Description:
# * Pull data from twitter.

from DataBase import MYSQLDatabase
import gc
from datetime import datetime, date, timedelta
import got3 as got

__all__ = ['TwitterPuller']

class TwitterPuller(object):
    """
    * Pulls in tweets using keywords.
    """
    __columnNames = ['CorpID', 'SearchTerm', 'Retweets', 'User', 'Date', 'Tweet', 'SubNum']
    ###############################
    # Constructors:
    ###############################
    def __init__(self):
        """
        * Create new twitter puller object.
        """
        pass
    ###############################
    # Public Functions:
    ###############################
    # tweetCriteria = got.manager.TweetCriteria().setQuerySearch('test').setMaxTweets(5)
    # ['author_id', 'date', 'favorites', 'formatted_date', 'geo', 'hashtags', 'id', 'mentions', 'permalink', 'retweets', 'text', 'urls', 'username']
    def StepAndSampleTweets(self, args, dayStepSize = 1, numTweets = 3000):
        """
        * Step through dates and sample using provided arguments.
        """
        pass
    
    def PullTweetsAndInsert(self, args, corpID, subNum, tableName, searchTerm, db, numTweets = 500):
        """
        * Pull all tweets using single search term.
        """
        stepSize = args['dateStep']
        numDays = (args['until'] - args['since']).days
        tweetCriteria = got.manager.TweetCriteria()
        tweetCriteria = tweetCriteria.setMaxTweets(numTweets)
        currDate = args['since']
        tweetCriteria = tweetCriteria.setQuerySearch(searchTerm)
        dateStep = stepSize
        while dateStep <= numDays:
            nextDate = currDate + timedelta(days=stepSize)
            nextDateStr = nextDate.strftime('%Y-%m-%d')
            tweetCriteria = tweetCriteria.setSince(currDate.strftime('%Y-%m-%d'))
            tweetCriteria = tweetCriteria.setUntil(nextDateStr)
            try:
                tweets = got.manager.TweetManager.getTweets(tweetCriteria)
                if tweets:
                    insertValues = {}
                    TwitterPuller.__ResetColumns(insertValues)
                    for tweet in tweets:
                        insertValues['CorpID'].append(corpID)
                        insertValues['SearchTerm'].append(searchTerm)
                        insertValues['User'].append(tweet.username)
                        insertValues['Date'].append(tweet.date.strftime('%Y-%m-%d'))
                        insertValues['Retweets'].append(str(tweet.retweets))
                        insertValues['Tweet'].append(MYSQLDatabase.RemoveInvalidChars(tweet.text))
                        insertValues['SubNum'].append(subNum)
                    db.InsertInChunks(tableName, insertValues, 10, skipExceptions = True)
            except Exception as ex:
                1 == 1
            currDate = nextDate
            dateStep += stepSize

    def PullTweets(self, args, numTweets = 500):
        """
        * Pull all tweets using list of search terms.
        """
        results = {}
        subs = args['subs']
        keywords = args['searchTerms']
        stepSize = args['dateStep']
        numDays = (args['until'] - args['since']).days
        tweetCriteria = got.manager.TweetCriteria()
        tweetCriteria = tweetCriteria.setMaxTweets(numTweets)
        for num, keyword in enumerate(keywords):
            sub = subs[num]
            currDate = args['since']
            results[keyword] = {}
            tweetCriteria = tweetCriteria.setQuerySearch(keyword)
            for dateStep in range(stepSize, numDays, stepSize):
                nextDate = currDate + timedelta(days=stepSize)
                nextDateStr = nextDate.strftime('%Y-%m-%d')
                tweetCriteria = tweetCriteria.setSince(currDate.strftime('%Y-%m-%d'))
                tweetCriteria = tweetCriteria.setUntil(nextDateStr)
                try:
                    tweets = got.manager.TweetManager.getTweets(tweetCriteria)
                    if tweets:
                        results[keyword][nextDateStr] = (tweets, sub)
                        return results
                except Exception as ex:
                    pass
                gc.collect()
                currDate = nextDate
        # Return dictionary containing keywords mapped to list of status objects or list:
        return results

    def AsciiStr(string):
        """
        * Convert unicode string.
        """
        # Return string containing only unicode characters:
        return ''.join([i if ord(i) < 128 else '' for i in string])

    @staticmethod
    def __ResetColumns(insertValues):
        for col in TwitterPuller.__columnNames:
            insertValues[col] = []

    ###################
    # Deprecated:
    ###################
    def __temp(self):
        """
        * Store info regarding memcached.
        """
        pulled_corp_searches = mc.get("corp_searches")
        if not pulled_corp_searches:
            pulled_corp_searches = {}



class APICallExceeded(Exception):
    """
    * Exception raised if # API calls exceeded.
    """
    def __init__(self, results):
        self.__results = results
    

