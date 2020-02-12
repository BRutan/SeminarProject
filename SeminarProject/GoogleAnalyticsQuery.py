#################################
# GoogleAnalyticsQuery.py
#################################
# Description:
# * Query google analytics to get search term popularity data.

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

# https://console.developers.google.com/apis/credentials?showWizardSurvey=true&project=strange-mind-265822


class GoogleAnalyticsQuery(object):
    _SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
    #KEY_FILE_LOCATION = 'My Project-6d3277c980e2.json'
    _KEY_FILE_LOCATION = "D:\Git Repos\SeminarProject\SeminarProject\My Project-6d3277c980e2.json"
    _VIEW_ID = '<REPLACE_WITH_VIEW_ID>'
    
    def __init__(self):
        """
        * Instantiate new object.
        """
        self.__credentials = ServiceAccountCredentials.from_json_keyfile_name(GoogleAnalyticsQuery._KEY_FILE_LOCATION, _SCOPES)
        self.__analytics = build('analyticsreporting', 'v4', credentials=self.__credentials)

    def GenerateReport(self, startdate, enddate, type):
        pass

def Test():
    obj = GoogleAnalyticsQuery();

if __name__ == '__main__':
    Test()