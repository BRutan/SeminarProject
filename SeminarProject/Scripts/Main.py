#################################
# Main.py
#################################
# Description:
# * 


# import beautifulsoup4
import csv
import datetime
import numpy
import re
import twitter

def main():
    #################### INSTRUCTIONS:
    ### 0. Pip install python-twitter (with Python 3).
    ### 1. Create developer account at https://developer.twitter.com/.
    ### 2. Create 'app' at https://developer.twitter.com/en/apps.
    ### 3. Enter tokens listed on "Keys and Tokens" page (in order they appear) after you have created the app.
    ### 4. Run script.

    ############# ENTER YOUR TOKENS HERE: 
    cons_key = 'IWxeUOPt4aN2wA9wOSnuQveOs'
    cons_key_secret = 'hMXLSKQdsUotO8xZs03S3e8AnL0bVgbmcfgIXKPZJcIX0npxD3'
    access_key = '1192869663778263040-WAE84Xw1sPv4joAn1rKAqclzxjG9hI'
    access_key_secret = 'VlFiaaaRLFvILMrang4GhwQTeUv6G3taUJupjdwFSevMa'

    # Establish connection with twitter api:
    api = twitter.Api(consumer_key = cons_key, 
                    consumer_secret = cons_key_secret, 
                    access_token_key = access_key, 
                    access_token_secret = access_key_secret)

    # Import S&P 500 components:
    components = {}
    atHeader = True
    with open('Components.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if not atHeader:
                symbol = row[2].strip()
                company = row[1].strip()
                components[symbol] = company
            atHeader = False
    # Select top 50 corporations from the components list:
    firms = {}
    corpCount = 0
    for comp in components.keys():
        firms[comp] = components[comp]
        corpCount += 1
        if corpCount == 50:
            break

    # Pull in tweets: 
    startDate = '2018-07-19'
    startQuery = "q=" 
    midQuery = "%20&result_type=recent&since="
    endQuery = "&count=100"
    queryStrings = [startQuery, '', midQuery, startDate, endQuery]
    results = {}
    # Search for symbol and corporation name:
    exceptCount = 0
    for symbol in firms.keys():
        try:
            queryStrings[1] = symbol
            filledQuery = ''.join(queryStrings)
            results[symbol] = api.GetSearch(raw_query = filledQuery)
            corpName = firms[symbol]
            queryStrings[1] = corpName
            filledQuery = ''.join(queryStrings)
            results[corpName] = api.GetSearch(raw_query = filledQuery)
        except Exception as ex:
            print(ex.message)
            exceptCount += 1

    # Print SearchTerm, ScreenName, CreatedDate, Text to csv file:
    outFileHeaders = ['SearchTerm', 'ScreenName', 'Date', 'Tweet_Text']
    currRow = [None] * 4
    with open('Data.csv', 'w') as outFile:
        writer = csv.writer(outFile)
        writer.writerow(outFileHeaders)
        for key in results.keys():
            currRow[0] = key
            for status in results[keys]:
                currRow[1] = status.user.name
                currRow[2] = status.created_at
                currRow[3] = status.text
                writer.writerow(currRow)

if __name__ == '__main__':
    main()



