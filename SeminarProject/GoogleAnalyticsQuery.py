#################################
# GoogleAnalyticsQuery.py
#################################
# Description:
# * Query google analytics to get popularity of search terms.

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = '<REPLACE_WITH_JSON_FILE>'
VIEW_ID = '<REPLACE_WITH_VIEW_ID>'


class GoogleAnalyticsQuery(object):
    pass
    