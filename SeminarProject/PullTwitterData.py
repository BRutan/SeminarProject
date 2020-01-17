#################################
# PullTwitterData.py
#################################
# Description:
# * Pull data from twitter.

import got3 as got
from datetime import datetime, date

class TwitterPuller(object):
    """
    * Pulls in tweets using keywords.
    """
    ###############################
    # Constructors:
    ###############################
    def __init__(self, keywords = None):
        """
        * Create new twitter puller object using default tokens listed in file.
        Parameters:
        * keywords: Expecting list of all keywords.
        """
        if keywords and not isinstance(keywords, list):
            raise Exception('keywords must be None or a list.')
        # Remove all non-unicode characters and blanks in each keyword:
        if keywords:
            self.__keywords = [keyword.strip() for keyword in keywords if keyword.strip()]
        else:
            self.__keywords = []
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
    
    def PullTweets(self, startDate, endDate, subs, numTweets = 3000):
        """
        * Pull all tweets using stored keywords object.
        """
        startDate = startDate.strftime('%Y-%m-%d')
        endDate = endDate.strftime('%Y-%m-%d')
        results = {}
        tweetCriteria = got.manager.TweetCriteria()
        tweetCriteria = tweetCriteria.setSince(startDate)
        tweetCriteria = tweetCriteria.setUntil(endDate)
        tweetCriteria = tweetCriteria.setMaxTweets(numTweets)
        for num, keyword in enumerate(self.__keywords):
            try:
                tweetCriteria = tweetCriteria.setQuerySearch(keyword)
                tweets = got.manager.TweetManager.getTweets(tweetCriteria)
                results[keyword] = tweets
                #results[keyword] = [(, subs[num])
            except Exception as ex:
                continue
        # Return dictionary containing keywords mapped to list of status objects or list:
        return results

    def AsciiStr(string):
        """
        * Convert unicode string.
        """
        # Return string containing only unicode characters:
        return ''.join([i if ord(i) < 128 else '' for i in string])

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



    
class DecomposedStatuses(object):
    """
    * Object contains list of mapped object statuses.
    """
    __validOptions = { 'user': 0, 'text' : 0 }
    def __init__(self, *options):
        """
        * Initialize new decomposed status object.
        """
        self.Results = []
        # Ensure that all options were valid:
        # Determine all parameters to pull:
        self.__options = []
        for option in options:
            if option in __validOptions.keys():
                self.__options.append(option)

    def ConvertAll(self, statuses):
        """
        * Convert list of statuses into list of list of requested
        status members.
        """
        output = []
        for status in status:
            convStatus = []
            for option in self.__options:
                convStatus.append(self.__statusMap(status, option))
            output.append(convStatus)
        
        return output

    ###############################
    # Private Helpers:
    ###############################
    @staticmethod
    def __statusMap(status, member):
        """
        * Request member from object.
        """
        if member == 'user':
            return status.user
        if member == 'nothing':
            return None
        if member == 'text':
            return status.text


class APICallExceeded(Exception):
    """
    * Exception raised if # API calls exceeded.
    """
    def __init__(self, results):
        self.__results = results
    

