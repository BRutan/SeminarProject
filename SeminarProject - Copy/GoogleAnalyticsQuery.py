#################################
# GoogleAnalyticsQuery.py
#################################
# Description:
# * Query google analytics to get search term popularity data.

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

# https://console.developers.google.com/apis/credentials?showWizardSurvey=true&project=strange-mind-265822

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'My Project-6d3277c980e2.json'
VIEW_ID = '<REPLACE_WITH_VIEW_ID>'


if __name__ == '__main__':
    test = GoogleAnalyticsQuery()


class GoogleAnalyticsQuery(object):
    pass
    