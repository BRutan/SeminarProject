#################################
# GetTweets.py
#################################
# Description:
# * Replicate got library, simplify usage to match our projects' needs.

import re
import urllib.request,urllib.parse,urllib.error,urllib.request,urllib.error,urllib.parse,json,re,datetime,sys,http.cookiejar
from numpy.random import choice as choose
from pyquery import PyQuery

class TweetPuller(object):
    __url = "https://twitter.com/i/search/timeline?f=tweets&q=%s&src=typd&%smax_position=%s"
    __headers = [('Host', 'twitter.com'), ('User-Agent','Mozilla/5.0 (Windows NT 6.1; Win64; x64)')
                 ,('Accept','application/json, test/javascript, */*; q=0.01')
                 ,('Accept-Language','de,en-US;q=0.7,en;q=0.3')
                 ,('X-Requested-With','XMLHttpRequest')
                 ,('Put (Referer, url) here at index 5', ''),('Connection','keep-alive')]
    def __init__(self):
        """
        * Populate headers for http request:
        """
        self.__headers = TweetPuller.__headers
        
    def PullTweets(self, args):
        """
        * Pull all tweets.
        Required Inputs: expecting following mapping in args (dictionary)
        * since: Start date for tweets (datetime, earlier than 'until').
        * until: End date for tweets (datetime, later than 'since').
        * searchTerms: List of terms to use for search.
        * interDaySampleSize: # tweets to sample for each day in period.
        Optional Input:
        * termSampleSize: # terms to randomly select to perform queries.
        """
        # Map search term to [Tweets]:
        results = {}
        resultsAux = []
        cookieJar = http.cookiejar.CookieJar()
        _args = {}
        _args['refreshCursor'] = ''
        _args['jar'] = cookieJar
        _args['interDaySampleSize'] = args['interDaySampleSize']

        numDays = (args['until'] - args['since']).days
        startDate = args['since']
        endDate = args['until']
        
        if 'termSampleSize' in args.keys():
            terms = choose(args['searchTerms'], args['termSampleSize'])
        else:
            terms = args['searchTerms']

        subs = args['subs']
        
        for termNum, term in enumerate(terms):
            results[term] = []
            sub = subs[termNum]
            # Testing:
            _args['search'] = term
            _args['start'] = startDate.strftime('%Y-%m-%d')
            _args['until'] = endDate.strftime('%Y-%m-%d')
            query = self.__JsonQuery(_args)
            # Check if no results occurred:
            if len(query['items_html'].strip()) == 0:
                continue

            _args['refreshCursor'] = query['min_position']
            scrapedTweets = PyQuery(query['items_html'])
            # Remove incomplete tweets withheld by Twitter Guidelines:
            scrapedTweets.remove('div.withheld-tweet')
            tweets = scrapedTweets('div.js-stream-tweet')
            if len(tweets) == 0:
                continue

            for tweetHTML in tweets:
                tweetPQ = PyQuery(tweetHTML)
                tweet = Tweet(tweetPQ, sub)
                results[term].append(tweet)

            if 1 > 2:
                for day in range(1, numDays + 1):
                    _args['start'] = (startDate + datetime.timedelta(days = day)).strftime('%Y-%m-%d')
                    _args['until'] = (startDate + datetime.timedelta(days = day - 1)).strftime('%Y-%m-%d')
                    query = self.__JsonQuery(_args)
                    # Check if no results occurred:
                    if len(query['items_html'].strip()) == 0:
                        continue

                    _args['refreshCursor'] = query['min_position']
                    scrapedTweets = PyQuery(query['items_html'])
                    # Remove incomplete tweets withheld by Twitter Guidelines:
                    scrapedTweets.remove('div.withheld-tweet')
                    tweets = scrapedTweets('div.js-stream-tweet')
                    if len(tweets) == 0:
                        continue

                    for tweetHTML in tweets:
                        tweetPQ = PyQuery(tweetHTML)
                        tweet = Tweet(tweetPQ, sub)
                        results[term].append(tweet)

        return results

    def __JsonQuery(self, args):
        """
        * Perform JSON query on twitter website.
        """
        urlGetData = ''.join([' since:', args['until'], ' until:', args['start'], ' ', args['search']])
        url = TweetPuller.__url % (urllib.parse.quote(urlGetData), '', args['refreshCursor'])
        self.__headers[5] = ('Referer', url)

        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(args['jar']))
        opener.addheaders = TweetPuller.__headers
        response = opener.open(url)
        response = response.read()

        return json.loads(response.decode())

class Tweet(object):
    """
    * Store required information from JSON query.
    """
    def __init__(self, pq, sub):
        """
        * Convert all data into usable form.
        """
        self.Text = pq
        self.Date = pq
        self.Retweets = pq
        self.Username = pq
        self.__sub = sub
    #####################
    # Properties:
    #####################
    @property
    def Text(self):
        return self.__text
    @property
    def Date(self):
        return self.__date
    @property
    def Retweets(self):
        return self.__retweets
    @property
    def Subsidiary(self):
        return self.__sub
    @property
    def Username(self):
        return self.__username
    @Text.setter
    def Text(self, pq):
        text = pq('p.js-tweet-text').text().replace('# ', '#')
        text = text.replace('@ ', '@')
        self.__text = re.sub(r"\s+", ' ', text)
    @Date.setter
    def Date(self, pq):
        self.__date = pq('small.time span.js-short-timestamp').attr('data-time')
    @Retweets.setter
    def Retweets(self, pq):
        retweet = pq('span.ProfileTweet-action--retweet span.ProfileTweet-actionCount')
        self.__retweets = int(retweet.attr('data-tweet-stat-count').replace(',', ''))
    @Username.setter
    def Username(self, pq):
        username = pq('span.username.js-action-profile-name b').text().strip()
        self.__username = username