##################################################
# SentimentAnalyzer.py
##################################################
# Description:
# * 

from textblob import TextBlob
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import TweetTokenizer

__all__ = [ 'SentimentAnalyzer' ]

class SentimentAnalyzer(object):
    __tokenizer = TweetTokenizer()
    """
    * Class performs sentiment analysis on tweets.
    """
    def CalculateSentiments(self, data, pKey, textColumn):
        """
        * Calculate sentiment score for each text element
        in data.
        Inputs:
        * data: Dictionary containing text to analyze. Requires
        a primary key column to map the sentiment to (ex: tweet ID).
        * pKey: Key to use as primary key for returned sentiments mapping.
        * textColumn: String denoting column name in data dictionary
        associated with text to analyze.
        """
        sentiments = {}
        for row, text in enumerate(data[textColumn]):
            blob = TextBlob(text)
            key = data[pKey][row]
            score = 0
            for sentence in blob.sentences:
                score += sentence.sentiment.polarity
            sentiments[key] = score

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
    
    



