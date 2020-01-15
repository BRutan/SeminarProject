#################################################
# TargetedWebScraping.py
#################################################
# * Pull registered trademarks for given companies 
# from wipo website (using BrandQuery object) or 
# subsidiaries from google search.

from bs4 import BeautifulSoup as Soup
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
    __site = 'https://www3.wipo.int/branddb/en/'
    __chromePath = 'C:\\Program Files (x86)\\Google\\Chrome\\ChromeDriver\\chromedriver.exe'
    __inputBoxPath = '/html/body/div[4]/div[2]/form/div[1]/div/div[1]/div[1]/div[1]/div[3]/div[2]/div[1]/div[3]/input'
    __executeButtonPath = '/html/body/div[4]/div[2]/form/div[1]/div/div[1]/div[1]/div[1]/div[3]/div[3]/a/span[1]'
    __namePanePath = '/html/body/div[4]/div[2]/form/div[1]/div/div[1]/div[1]/div[1]/div[1]/div[2]/div[3]/ul/li[2]/a'
    __nextPageButton = '/html/body/div[4]/div[2]/form/div[3]/div[1]/div[2]/div[3]/a[1]/span[1]'
    __pageNumBoxPath = '/html/body/div[4]/div[2]/form/div[3]/div[1]/div[2]/div[3]/div/input'
    __tablePath = '/html/body/div[4]/div[2]/form/div[3]/div[2]/div/div[3]/div[3]/div/table'
    __restartEndCount = 10
    def __init__(self):
        """
        * Create new BrandQuery object, begin importing immediately.
        Inputs:
        * companies: Expecting dictionary mapping { Company -> [Subsidiaries] }.
        """
        # Open browser window until object is terminated:
        self.__driver = webdriver.Chrome(executable_path=BrandQuery.__chromePath) 
        self.__driver.get(BrandQuery.__site)
        time.sleep(2)
        namePane = self.__driver.find_element_by_xpath(BrandQuery.__namePanePath)
        namePane.click()
        self.__inputBox = self.__driver.find_element_by_xpath(BrandQuery.__inputBoxPath)
        pyautogui.FAILSAFE = False
        
    def __del__(self):
        """
        * Close existing browser.
        """
        if not self.__driver is None:
            self.__driver.close()
        
    def PullBrands(self, subsidiaries):
        """
        * Pull all registered trademarks for passed list of companies 
        from WIPO website.
        Inputs:
        * subsidiaries: Expecting list of subsidiaries to query.
        """
        if not isinstance(subsidiaries, list):
            raise Exception('subsidiaries needs to be a list.')

        brands = {}
        subDict = {}
        for sub in subsidiaries:
            # Standardize by removing punctuation from subsidiary name, lower-case:
            subDict[re.sub('[^\w\s]', '', sub.lower())] = True

        subs = ','.join(list(subDict.keys()))
        self.__inputBox.send_keys(subs)
        self.__inputBox.send_keys(Keys.RETURN)
        self.__resultsTable = self.__driver.find_element_by_xpath('//*[@id="results"]')
        soup = Soup(self.__driver.page_source, 'lxml')
        # Extract all rows from the grid:
        numPages = unidecode(soup.find('div', {'class' : 'skipWindow'}).text)
        numPages = int(re.search('[0-9]+(,[0-9]+){0,1}', numPages)[0].replace(',',''))
        pageNum = 1
        restartCount = 0
        while pageNum < numPages + 1:
            if pageNum % 20 == 0:
                BrandQuery.__MoveMouse()
            try:
                soup = Soup(self.__driver.page_source, 'lxml')    
                table = soup.find('div', {'id' : 'results'})
                rows = table.find_all('tr', {'role' : 'row'})
                for row in rows:
                    isActive = row.find('td', { 'aria-describedby' : 'gridForsearch_pane_STATUS' })
                    if isActive and unidecode(isActive.text) == 'Active':
                        holder = row.find('td', { 'aria-describedby' : 'gridForsearch_pane_HOL' })
                        if holder:
                            holderName = re.sub('[^\w\s]', '', unidecode(holder.text).strip().lower())
                            if holderName in subDict.keys():
                                brand = unidecode(row.find('td', { 'aria-describedby' : 'gridForsearch_pane_BRAND'}).text).strip()
                                if brand not in brands.keys():
                                    brands[brand] = unidecode(row.find('td', {'aria-describedby' : 'gridForsearch_pane_AD'}).text).strip()
                # Move to the next page:
                nextPageButton = WebDriverWait(self.__resultsTable, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@aria-label="next page"]')))
                nextPageButton.click()
                pageNum += 1
            except:
                restartCount += 1
                if restartCount >= BrandQuery.__restartEndCount:
                    return brands
                self.__RestartQuery(subs, pageNum)
        return brands

    ####################
    # Helpers:
    ####################
    @staticmethod
    def __MoveMouse():
        """
        * Move mouse to fool the automation detection software on wipo website.
        """
        pyautogui.moveTo(random.uniform(0, pyautogui.size()[0]), random.uniform(0, pyautogui.size()[1]), duration = 1)

    def __RestartQuery(self, searchTerm, pageNum):
        """
        * Open new instance at page if kicked off.
        """
        self.__driver.close()
        self.__driver = webdriver.Chrome(executable_path=BrandQuery.__chromePath) 
        self.__driver.get(BrandQuery.__site)
        time.sleep(3)
        namePane = self.__driver.find_element_by_xpath(BrandQuery.__namePanePath)
        namePane.click()
        self.__inputBox = self.__driver.find_element_by_xpath(BrandQuery.__inputBoxPath)
        pageNumBox = WebDriverWait(self.__resultsTable, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="skipValue1"]')))
        pageNumBox.clear()
        pageNumBox.send_keys(str(pageNum))
        pageNumBox.send_keys(Keys.RETURN)


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

