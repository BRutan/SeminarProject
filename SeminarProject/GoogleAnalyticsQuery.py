#################################
# GoogleAnalyticsQuery.py
#################################
# Description:
# * Query google analytics to get search term popularity data.

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

# https://console.developers.google.com/apis/credentials?showWizardSurvey=true&project=strange-mind-265822


class GoogleAnalyticsQuery(object):
    SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
    #KEY_FILE_LOCATION = 'My Project-6d3277c980e2.json'
    KEY_FILE_LOCATION = "D:\Git Repos\SeminarProject\SeminarProject\My Project-6d3277c980e2.json"
    VIEW_ID = '<REPLACE_WITH_VIEW_ID>'
    
    def __init__(self):
        """
        * Instantiate new object.
        """
        pass
    

def Test():
    obj = GoogleAnalyticsQuery();

if __name__ == '__main__':
    Test()