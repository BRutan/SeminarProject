#################################
# PullTwitterData.py
#################################
# Description:
# * Pull data from twitter.

from DataBase import MYSQLDatabase
from datetime import datetime, date, timedelta
from dateutil.rrule import rrule, DAILY
import gc
import got3 as got
import os
import re
import pickle
import string

__all__ = ['TwitterPuller']

class TwitterPuller(object):
    """
    * Pulls in tweets using keywords.
    """
    __PickleFolder = "tweet_pickles\\"
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
    def PullTweetsAndInsert(self, args, tableName, db):
        """
        * Pull all tweets using single search term.
        """
        tweetCriteria = got.manager.TweetCriteria()
        # Testing:
        tweetCriteria = tweetCriteria.setMaxTweets(args['periodSampleSize'])
        #tweetCriteria = tweetCriteria.setMaxTweets(5000)
        tweetCriteria = tweetCriteria.setLang('en')
        corpid = args['corpid']
        ticker = args['ticker']
        if args['topTweets']:
            tweetCriteria = tweetCriteria.setTopTweets(True)
        # Skip all dates that have been sampled enough (at periodSampleSize):
        queryStr = ['SELECT Date FROM %s WHERE CorpID = %d' % (tableName, corpid)]
        queryStr.append(' GROUP BY Date HAVING Count(date) >= %d;' % args['periodSampleSize']) 
        excludeDates = db.ExecuteQuery(''.join(queryStr), getResults = True)
        excludeDates = {dt : True for dt in excludeDates['date']} if excludeDates else {}
        dates = list(rrule(DAILY, dtstart = args['since'], until = args['until'], interval = args['dateStep']))
        dateStrs = [dates[num].strftime('%Y-%m-%d') for num in range(0, len(dates))]
        tweetCols = ['corpid', 'tweetid', 'searchterm', 'user', 'date', 'retweets', 'tweet']
        #tweetCols = ['corpid', 'tweetid', 'searchterm', 'user', 'date', 'retweets', 'tweet', 'geocode']
        retweetCols = ['tweetid', 'retweets']
        # Get the max tweet id from table:
        results = db.ExecuteQuery('SELECT MAX(tweetid) as max FROM ' + tableName, getResults = True)
        id = 0
        if results and results['max'][0]:
            id = results['max'][0]
        for term in args['searchTerms']:
            tweetCriteria = tweetCriteria.setQuerySearch(term)
            for num in range(0, len(dateStrs) - 1):
                if dates[num].date in excludeDates:
                    continue
                tweetCriteria = tweetCriteria.setSince(dateStrs[num])
                tweetCriteria = tweetCriteria.setUntil(dateStrs[num + 1])
                pickleName = '_'.join(['tweets',ticker,term.replace(string.punctuation,''),dates[num].strftime('%m_%d_%Y')])
                try:
                    tweets = TwitterPuller.__ReadPickle(pickleName)
                    if not tweets:
                        tweets = got.manager.TweetManager.getTweets(tweetCriteria)
                    if tweets:
                        # Testing: randomly sample large number of tweets:
                        #tweets = [tweets[i] for i in choose(range(0, len(filtered)), sampleSize, replace = False)]
                        tweetTableValues = {col : [] for col in tweetCols }
                        retweetTableValues = {col : [] for col in retweetCols }
                        for tweet in tweets:
                            tweetTableValues['corpid'].append(corpid)
                            tweetTableValues['tweetid'].append(id)
                            tweetTableValues['searchterm'].append(term)
                            tweetTableValues['user'].append(tweet.username)
                            tweetTableValues['date'].append(tweet.date)
                            tweetTableValues['retweets'].append(tweet.retweets)
                            tweetTableValues['tweet'].append(MYSQLDatabase.RemoveInvalidChars(tweet.text))
                            # Testing:
                            #tweetTableValues['geocode'].append(tweet.geo)
                            #retweetTableValues['corpid'].append(corpID)
                            #retweetTableValues['tweetid'].append(id)
                            #retweetTableValues['term'].append(term)
                            #retweetTableValues['retweets'].append(tweet.retweets)
                            id += 1
                        db.InsertInChunks(tableName, tweetTableValues, 10, skipExceptions = True)
                        db.InsertInChunks("RetweetData", retweetTableValues, 10, skipExceptions = True)
                        #TwitterPuller.__DeletePickle(pickleName)
                except BaseException as ex:
                    TwitterPuller.__DumpPickle(tweets, pickleName)

    def PullTweets(self, tickerArgs):
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

    ########################
    # Private Helpers:
    ########################
    @staticmethod
    def __ReadPickle(objName):
        """
        * Read serialized object/s from local pickle folder.
        """
        data = []
        if not os.path.exists(TwitterPuller.__PickleFolder):
            os.mkdir(TwitterPuller.__PickleFolder)
            return []
        pickleRE = re.compile(objName + '(_[0-9]+){0,1}\.pickle')
        matchFiles = [os.join(TwitterPuller.__PickleFolder, file) for file in os.listdir(TwitterPuller.__PickleFolder) if pickleRE.match(file)]
        for path in matchFiles:
            with open(path, 'rb') as f:
                data.extend(pickle.load(f))
        return data

    @staticmethod
    def __DeletePickle(objName):
        pickleRE = re.compile(objName + '(_[0-9]+){0,1}\.pickle')
        matchFiles = [os.join(TwitterPuller.__PickleFolder, file) for file in os.listdir(TwitterPuller.__PickleFolder) if pickleRE.match(file)]
        for path in matchFiles:
            os.remove(path)

    @staticmethod
    def __DumpPickle(tweets, objName):
        """
        * Dump dataframe to pickle file for easier pulling, over multiple files
        if large.
        """
        if not os.path.exists(TwitterPuller.__PickleFolder):
            os.mkdir(TwitterPuller.__PickleFolder)
        path = [TwitterPuller.__PickleFolder, objName, '', '.pickle']
        if tweets and len(tweets) > 100:
            count = 1
            for start in range(0, len(tweets)):
                path[2] = str(count)
                with open(''.join(path), 'wb') as f:
                    pickle.dump(tweets[start, min(start + 100, len(tweets))], f)
                    count += 1
        elif tweets:
            pickle.dump(tweets, open(''.join(path), 'wb'))

    

