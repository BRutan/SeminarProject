#################################################
# TargetedWebScraping.py
#################################################
# * Pull registered trademarks for given companies 
# from wipo website (using BrandQuery object) or 
# subsidiaries from google search.

from bs4 import BeautifulSoup as Soup
import re
import requests
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from sortedcontainers import SortedSet
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
    def __init__(self):
        """
        * Create new BrandQuery object, begin importing immediately.
        Inputs:
        * companies: Expecting dictionary mapping { Company -> [Subsidiaries] }.
        """
        if not isinstance(companies, dict):
            raise Exception('companies needs to be a dictionary mapping { Company -> [Subsidiaries] }.')
        # Open browser window until object is terminated:
        self.driver = webdriver.Chrome(executable_path=BrandQuery.__chromePath) 
        self.driver.get(BrandQuery.__site)
        time.sleep(5)
        namePane = driver.find_element_by_xpath(BrandQuery.__namePanePath)
        namePane.click()
        self.__inputBox = driver.find_element_by_xpath(BrandQuery.__inputBoxPath)
        self.__nextPageButton = driver.find_element_by_xpath(BrandQuery.__nextPageButton)
        

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

        brands = SortedSet()
        for sub in subsidiaries:
            # Remove punctuation from subsidiary name:
            cleaned = sub.replace(string.punctuation, sub)
            self.__inputBox.send_keys(cleaned)
            self.__inputBox.send_keys(Keys.RETURN)
            resultsTable = driver.find_element_by_xpath(BrandQuery.__tablePath)
            pageNumBox = driver.find_element_by_xpath(BrandQuery.__pageNumBoxPath)
            soup = Soup(driver.page_source, 'lxml')
            # Extract all rows from the grid:
            numPages = unidecode(soup.find('div', {'class' : 'skipWindow'}).text)
            numPages = int(re.search('[0-9]+(,[0-9]+){0,1}', numPages)[0])
            for pageNum in range(0, numPages):
                soup = Soup(driver.page_source, 'lxml')    
                table = soup.find('div', {'id' : 'results'})
                rows = table.find_all('tr', {'role' : 'row'})
                for row in rows:
                    holder = row.find('td', { 'aria-describedby' : 'gridForsearch_pane_HOL'})
                    if unidecode(holder.text).strip().lower() == sub:
                        brands.add(unidecode(row.find('td', { 'aria-describedby' : 'gridForsearch_pane_BRAND'}).text))
                self.__nextPageButton.click()
        
        return brands

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

