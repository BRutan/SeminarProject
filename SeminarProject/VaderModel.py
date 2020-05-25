#################################################
# VaderModel.py
#################################################
# * Application of Vader model to tweets.

from pandas import DataFrame
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

nltk.download('vader_lexicon')

class VaderSentimentModel:
    def __init__(self):
        """
        * Initialize empty object.
        """
        pass

    def GenerateSentimentScores(self, data, textCol, scoreCol):
        """
        * Generate sentiment scores using passed dataset.
        Inputs:
        * data: DataFrame that has an index.
        * textCol: String pointing to column in data that has
        text to analyze.
        * scoreCol: Column to hold sentiment scores.
        Output:
        * DataFrame with 'Score' as column with VADER sentiment
        score.
        """
        errs = []
        if not isinstance(data, DataFrame):
            errs.append('data must be a DataFrame.')
        if not isinstance(textCol, str):
            errs.append('textCol must be a string.')
        elif isinstance(data, DataFrame) and not textCol in data.columns:
            errs.append('textCol is not a column in data.')
        if not isinstance(scoreCol, str):
            errs.append('scoreCol must be a string.')
        if errs:
            raise Exception('\n'.join(errs))
        analyzer = SentimentIntensityAnalyzer()
        scores = [analyzer.polarity_scores(text)['compound'] for text in data[textCol]]
        data = { col : data[col] for col in data.columns }
        data[scoreCol] = scores
        
        return DataFrame(data)