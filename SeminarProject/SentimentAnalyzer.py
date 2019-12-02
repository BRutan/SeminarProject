##################################################
# SentimentAnalyzer.py
##################################################
# Description:
# * 


from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import TweetTokenizer

__all__ = [ 'SentimentAnalyzer' ]

class SentimentAnalyzer(object):
    __tokenizer = TweetTokenizer()
    """
    * Class performs sentiment analysis on tweets.
    """
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
    
    



