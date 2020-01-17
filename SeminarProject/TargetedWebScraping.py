#################################################
# TargetedWebScraping.py
#################################################
# * Pull registered trademarks for given companies 
# from wipo website (using BrandQuery object) or 
# subsidiaries from google search.

from bs4 import BeautifulSoup as Soup
import csv
import pyautogui
import random
import re
import requests
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import string
import time
from unidecode import unidecode

class BrandQuery(object):
    """
    * Pull brands from brand database.
    """
    __htmlFilePath = 'WebElements.html'
    __site = 'https://www3.wipo.int/branddb/en/'
    __chromePath = 'C:\\Program Files (x86)\\Google\\Chrome\\ChromeDriver\\chromedriver.exe'
    __firefoxPath = 'C:\\Program Files\\Mozilla Firefox\\firefox.exe'
    __inputBoxPath = '/html/body/div[4]/div[2]/form/div[1]/div/div[1]/div[1]/div[1]/div[3]/div[2]/div[1]/div[3]/input'
    __namePanePath = '/html/body/div[4]/div[2]/form/div[1]/div/div[1]/div[1]/div[1]/div[1]/div[2]/div[3]/ul/li[2]/a'
    __tablePath = '/html/body/div[4]/div[2]/form/div[3]/div[2]/div/div[3]/div[3]/div/table'
    __restartEndCount = 10
    def __init__(self):
        """
        * Create new BrandQuery object with browser window at WIPO website.
        """
        self.__driver = None
        self.__nextPageButton = None 
        self.__namePane = None
        self.__inputBox = None 
        self.__pageNumBox = None 
        self.__resultsTable = None
        self.__StartBrowser()

    def __del__(self):
        """
        * Close existing browser.
        """
        self.__EndSession()
        
    def PullBrands(self, subsidiaries):
        """
        * Pull all registered trademarks for passed list of companies 
        from WIPO website.
        Inputs:
        * subsidiaries: Expecting list of subsidiaries to query.
        """
        if not isinstance(subsidiaries, dict):
            raise Exception('subsidiaries needs to be a dictionary.')

        if not self.__driver:
            self.__StartBrowser()

        brands = {}
        searchSubs = {}
        cleanedSubs = {}
        cleanedToSub = {}
        for sub in subsidiaries.keys():
            # Standardize by removing punctuation from subsidiary name, lower-case:
            cleaned = re.sub('[^a-z]', '', sub.lower())
            cleanedSubs[cleaned] = True
            cleanedToSub[cleaned] = sub
        # Enter subs to search for into search box:
        subs = ', '.join([sub for sub in list(subsidiaries.keys())])
        self.__inputBox.send_keys(subs)
        self.__inputBox.send_keys(Keys.RETURN)
        self.__resultsTable = self.__driver.find_element_by_xpath('//*[@id="results"]')
        #soup = Soup(self.__driver.page_source, 'lxml')
        # Extract all rows from the grid:
        time.sleep(3)
        numPages = self.__driver.find_element_by_xpath('//div[@class="skipWindow"]').text
        numPages = int(re.search('[0-9]+(,[0-9]+){0,1}', unidecode(numPages))[0].replace(',',''))
        pageNum = 1
        restartCount = 0
        while pageNum < numPages + 1:
            # Testing:
            #if pageNum % 20 == 0:
                #BrandQuery.__MoveMouse()
            try:
                soup = Soup(self.__driver.page_source, 'lxml')    
                table = soup.find('div', {'id' : 'results'})
                rows = table.find_all('tr', {'role' : 'row'})
                for row in rows:
                    isActive = row.find('td', { 'aria-describedby' : 'gridForsearch_pane_STATUS' })
                    if isActive and unidecode(isActive.text) == 'Active':
                        holder = row.find('td', { 'aria-describedby' : 'gridForsearch_pane_HOL' })
                        if holder:
                            holderName = re.sub('[^a-z]', '', unidecode(holder.text).strip().lower())
                            if holderName in cleanedSubs.keys():
                                brand = row.find('td', { 'aria-describedby' : 'gridForsearch_pane_BRAND'}).text.strip()
                                if brand not in brands.keys():
                                    # Store date filed, holder:
                                    brands[brand] = (unidecode(row.find('td', {'aria-describedby' : 'gridForsearch_pane_AD'}).text).strip(), cleanedToSub[holderName])
                # Move to the next page:
                self.__nextPageButton = WebDriverWait(self.__resultsTable, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@aria-label="next page"]')))
                self.__nextPageButton.click()
                pageNum += 1
                # Exit if pulled over 1000 brands or passed through large number of pages:
                if len(brands.keys()) > 300 or pageNum > 500:
                    return brands
            except:
                # Testing:
                #self.__PrintAttributes(self.__WebElements)
                self.__EndSession()
                return brands
                #restartCount += 1
                #if len(brands.keys()) > 1000 or restartCount >= BrandQuery.__restartEndCount:
                #    self.__EndSession()
                #    return brands
                #self.__StartBrowser(restartCount, pageNum, subs)
        return brands

    ####################
    # Helpers:
    ####################
    @property
    def __WebElements(self):
        """
        * Return all web elements used in query.
        """
        return 	{"NextPage" : self.__nextPageButton, "Name Pane" : self.__namePane, "Input Box" : self.__inputBox, "PageNum Box" : self.__pageNumBox, "Result Table" : self.__resultsTable }

    def __PrintAttributes(self, elems):
        """
        * Print HTML attributes for WebElements that cause automation to fail.
        """
        with open(BrandQuery.__htmlFilePath, 'w+') as f:
            for key in elems:
                if elems[key] is None:
                    continue
                html = Soup(elems[key].get_attribute('outerHTML'), 'lxml').prettify() 
                for i in range(0, len(html)):
                    f.write(html[i])

    @staticmethod
    def __MoveMouse():
        """
        * Move mouse to fool the automation detection software on wipo website.
        """
        pyautogui.moveTo(random.uniform(0, pyautogui.size()[0]), random.uniform(0, pyautogui.size()[1]), duration = 1)

    def __EndSession(self):
        """
        * Close window and release memory.
        """
        if not self.__driver is None:
            self.__driver.quit()
            self.__driver = None

    def __StartBrowser(self, restartCount = None, pageNum = None, searchTerm = None):
        """
        * Open browser window until object is terminated.
        """
        if self.__driver:
            self.__EndSession()
        #if not restartCount or (restartCount and restartCount % 2 == 1):f
        #    self.__driver = webdriver.Chrome(executable_path=BrandQuery.__chromePath) 
        #elif restartCount % 2 == 0:
        #    self.__driver = webdriver.FireFox(executable_path=BrandQuery.__firefoxPath)
        self.__driver = webdriver.Chrome(executable_path=BrandQuery.__chromePath)
        # Open random number of tabs:
        #numPages = int(random.uniform(1, 30))
        #for val in range(0, numPages):
        #    self.__driver.execute_script("window.open('https://www.google.com');")
        #self.__driver.execute_script(''.join(["window.open('", BrandQuery.__site, "');"]))
        self.__driver.get(BrandQuery.__site)
        try:
            time.sleep(3)
            #time.sleep(random.uniform(0, 4))
            self.__namePane = self.__driver.find_element_by_xpath(BrandQuery.__namePanePath)
            self.__namePane.click()
            self.__inputBox = self.__driver.find_element_by_xpath(BrandQuery.__inputBoxPath)
            if pageNum and searchTerm:
                self.__pageNumBox = WebDriverWait(self.__resultsTable, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="skipValue1"]')))
                self.__pageNumBox.clear()
                self.__pageNumBox.send_keys(str(pageNum))
                self.__pageNumBox.send_keys(Keys.RETURN)
                self.__inputBox.send_keys(searchTerm)
                self.__inputBox.send_keys(Keys.RETURN)
                self.__resultsTable = self.__driver.find_element_by_xpath('//*[@id="results"]')
        except:
            # Testing:
            #self.__PrintAttributes(self.__WebElements)
            self.__EndSession()
            raise Exception('WIPO website has kicked you off for detecting automation. Try again later.')

        pyautogui.FAILSAFE = False

class SubsidiaryQuery(object):
    """
    * Grab subsidiaries from google.com.
    """
    __chromePath = 'C:\\Program Files (x86)\\Google\\Chrome\\ChromeDriver\\chromedriver.exe'
    __search = "%s subsidiaries"
    def __init__(self):
        self.__results = []

    def GetResults(self, company):
        """
        * Get subsidiary names from google.
        """
        self.__results = []
        search = SubsidiaryQuery.__search % (company)
        with webdriver.Chrome(executable_path=SubsidiaryQuery.__chromePath) as driver:
            driver.get("http:\\www.google.com")
            searchBox = driver.find_element_by_xpath('//*[@title="Search"]')
            searchBox.send_keys(search)
            searchBox.send_keys(Keys.RETURN)
            time.sleep(2)
            # Extract all subsidiaries from generated grid:
            soup = Soup(driver.page_source, 'lxml')
            grid = soup.find('g-scrolling-carousel')
            subs = {}
            if grid:
                data = grid.find_all('a')
                for point in data:
                    text = unidecode(point.text).strip()
                    if text not in subs.keys():
                        subs[text] = True
                self.__results = [val for val in list(subs.keys()) if val.strip()]
                # Clean invalid characters:
                for row in range(0, len(self.__results)):
                    self.__results[row] = re.sub('(\#|\(|\)|\*|\"|\')', '', self.__results[row])

    @property
    def Results(self):
        """
        * Results from query.
        """
        return self.__results

