#################################################
# BrandQuery.py
#################################################
#

from bs4 import BeautifulSoup as Soup
import requests
from selenium import webdriver
from sortedcontainers import SortedSet

class BrandQuery(object):
    """
    * Pull brands from brand database.
    """
    __site = 'https://www3.wipo.int/branddb/en/'
    def __init__(self, companies):
        self.PullBrands(companies)

    def PullBrands(self, companies):
        self.Brands = {}
        driver = webdriver.Chrome(executable_path='C:\\Program Files (x86)\\Google\\Chrome\\ChromeDriver\\chromedriver.exe')
        driver.get(BrandQuery.__site)
        inputBox = driver.find_element_by_id("HOL_input")
        executeButton = driver.find_element_by_xpath('//*[@id="name_search"]/div[3]')
        resultsTable = driver.find_element_by_xpath('//*[@id="gridForsearch_pane"]')
        for company in companies:
            inputBox.send_keys(company)
            executeButton.click()
            # Extract all rows from the grid:
            soup = Soup(driver.page_source, 'lxml')
            brands = SortedSet()
            rows = soup.find_all('tr', {'role' : 'row'})
            for row in rows:
                holder = row.find('td', { 'aria-describedby' : 'gridForsearch_pane_HOL'})
                brand = row.find('td', { 'aria-describedby' : 'gridForsearch_pane_BRAND'})







