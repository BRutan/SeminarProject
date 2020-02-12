##################################################
# SentimentAnalyzer.py
##################################################
# Description:
# * 

from pandas import DataFrame
from textblob import TextBlob
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import TweetTokenizer

__all__ = [ 'SentimentAnalyzer' ]

class SentimentAnalyzer(object):
    __tokenizer = TweetTokenizer()
    """
    * Class performs sentiment analysis on tweets.
    """
    @staticmethod
    def CalculateSentiment(text):
        """
        * Calculate sentiment score for passed text.
        """
        blob = TextBlob(text)
        score = 0
        for sentence in blob.sentences:
            score += sentence.sentiment.polarity
        
        return score

    @staticmethod
    def CalculateSentiments(data, textColumn = None, pkey = None):
        """
        * Calculate sentiment score for each text element
        in data.
        Inputs:
        * data: Dictionary containing text to analyze.
        a primary key column to map the sentiment to (ex: tweet ID).
        * pKey: Key to use as primary key for returned sentiments mapping.
        * textColumn: String denoting column name in data dictionary
        associated with text to analyze.
        """
        # Output as list if passed list or dataframe:
        if isinstance(data, list):
            sentiments = []
            for text in data:
                sentiments.append(SentimentAnalyzer.CalculateSentiment(text))
        elif isinstance(data, DataFrame):
            if textColumn is None:
                raise Exception('Need to provide text column if passing dataframe.')
            sentiments = []
            for text in data[textColumn].values:
                sentiments.append(SentimentAnalyzer.CalculateSentiment(text))
        else:
            # Output as dictionary if passed a dictionary:
            sentiments = {}
            for row, text in enumerate(data[textColumn]):
                sentiments[data[pkey][row]] = SentimentAnalyzer.CalculateSentiment(text)

        return sentiments


    # Functions:
    def GenerateSentiment(self, tweetText):
        """
        * Generate sentiment score using tweet text.
        Sentiment score is ratio of negative words to positive words.
        See https://www.twinword.com/blog/interpreting-the-score-and-ratio-of-sentiment/
        """
        # Tokenize the tweet text:
        
        # Pull out adjectives from tweet:
        adjectives = []
        pass
    
    



