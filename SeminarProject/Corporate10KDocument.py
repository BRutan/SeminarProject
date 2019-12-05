##########################################
# Corporate10KDocument.py
##########################################
# Description:
# * Class pulls 10K for ticker from SEC website, 
# cleans into usable form, then divides text up into
# appropriate sections.

from bs4 import BeautifulSoup as Soup
import csv
from datetime import date, datetime
import requests
import re
import os
import unicodedata
import Pull10Ks

class Corporate10KDocument(object):
    """
    * Class pulls 10K for ticker from SEC website, cleans into usable form, 
    then divides text up into appropriate sections.
    """
    __tm = '®'
    __blacklistTags = ["script", "style"]
    __attrlist = ["class", "id", "name", "style", 'cellpadding', 'cellspacing']
    __skiptags = ['font', 'a', 'b', 'i', 'u']
    __SubSectionRE = re.compile('\d*[A-Z]')
    __SectionRE = re.compile('[I|i]tem\s\d*.[\w|\W]*\d*')
    __ItemRE = re.compile('Item\s\d*')
    def __init__(self, ticker, date, localPath = None):
        """
        * Create new object. Pull from local file if localPath specified,
        (using predetermined format) or pull from 
        """
        self.Sections = {}
        self.Ticker = ticker
        self.Date = date
        self.__itemMap = {}
        self.__sectionToItemMap = {}
        # Pull text from SEC Edgar website, load into object:
        self.__Pull10KText(localPath)

    @property
    def Name(self):
        """
        * Return name of object (for identifying in local files).
        """
        return ''.join([self.Ticker, '_10K_',self.DateStr])
    @property
    def FilePath(self):
        """
        * Return predetermined file path name for this object, to read from and output to.
        """
        return ''.join([self.Name + '.txt'])
    @property
    def Sections(self):
        """
        * Maps SectionName -> { SubSectionName -> SubSectionName
        """
        return self.__section
    @property
    def __ItemToSection(self):
        """
        * Maps Item # + Subchar -> Section.
        """
        return self.__itemMap
    @property
    def __SectionToItem(self):
        """
        * Map SectionName -> Item #.
        """
        return self.__sectionToItemMap
    @property
    def Ticker(self):
        return self.__ticker
    @property
    def Date(self):
        return self.__date
    @property
    def DateStr(self):
        return self.__date.strftime('%Y%m%d')
    @Sections.setter
    def Sections(self, _dict):
        if not isinstance(_dict, dict):
            raise Exception('Sections must be a dictionary')
        self.__section = _dict
    @Ticker.setter
    def Ticker(self, ticker):
        if not isinstance(ticker, str):
            raise Exception('ticker must be a string.')
        self.__ticker = ticker.lower()
    @Date.setter
    def Date(self, dt):
        if isinstance(dt, datetime):
            self.__date = dt.date()
        elif isinstance(dt, str):
            self.__date = datetime.strptime(dt, '%Y%m%d').date()
        elif not isinstance(dt, date):
            raise Exception('dt must be a date/datetime object.')
        else:
            self.__date = dt
    ###################
    # Interface Methods:
    ###################
    def WriteHTMLFile(self, )
    def WriteCleanedFile(self, items, path):
        """
        * 
        """
        rowStrs = []
        for item in items:
            rowStr = str(item.get_text())
            if rowStr != '':
                rowStr = ''.join([ch if ord(ch) < 255 else ' ' for ch in rowStr])
                rowStrs.append(rowStr)

        with open(path, 'w', newline='') as f:
            writer = csv.writer(f)
            for row in rowStrs:
                writer.writerow([row])


    ###################
    # Private Helpers:
    ###################
    def __Pull10KText(self, path = None):
        """
        * Pull 10K text from local file or from SEC Edgar website.
        """
        # Pull from local file if path was specified:
        if path != None:
            # Pull from local file:
            self.__LoadDocFromFile(path)
            return

        # Pull from website:
        links = self.__GetLinks()
        # Assuming that links have been output in descending order, and that 
        # the first link is the one we want.
        link = links[0]
    
        # Pull file using BeautifulSoup library from link, extract all sections and
        # place into map:
        soup = self.__Clean(link)
        self.__ExtractSections(soup)
        
        # Write cleaned text to local file:
        fileName = ''.join([self.Ticker, '_10K_', self.DateStr, '_Sections.txt'])
    
        #self.__WriteSectionsToFile(path)
    
    def __GetLinks(self):
        """
        * Pull all potential matching links from SEC website.
        """
        link = "http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK="+ \
            str(self.Ticker)+"&type=10-K&dateb="+str(self.DateStr)+"&owner=exclude&output=xml&count=1"
    
        # Extract potential links to filing:
        data = requests.get(link).text
        soup = Soup(data, "lxml")

        # If the link is .htm convert it to .html
        return Corporate10KDocument.__ConvertHTMLinksToHTML(soup)
    
    @staticmethod
    def __Clean(link):
        """
        * Clean all tags from document text.
        """
        data = requests.get(link).text
        soup = Soup(data, "lxml")
    
        for tag in soup.findAll():
            if tag.name.lower() in Corporate10KDocument.__blacklistTags:
                # blacklisted tags are removed in their entirety
                tag.extract()
            if tag.name.lower() in Corporate10KDocument.__skiptags:
                tag.replaceWithChildren()            
            for attribute in Corporate10KDocument.__attrlist:
                del tag[attribute]
        return soup

    def __ExtractSections(self, soup):
        """
        * Map all { SectionName -> { SubSectionName -> Text } }using beautiful soup object.
        """
        tables = soup.find_all(("table", "tr", "td"))
        for table in tables:
            table.find_all("div")
            for i, item in enumerate(table)
        # loop over all tables
        items = soup.find_all(("table", "div"))
        itemNum = ''
        subChar = ''
        for i, item in enumerate(items):
            if 'Item' in item:
                text = item.get_text()
                sectionStr, itemNum, subChar = Corporate10KDocument.__PullSectionAttrs(text)
                # If hit a subsection, then determine the super section name, and add
                # subsection title to map.
                if itemNum in self.__ItemToSection.Keys():
                    topSection = self.__ItemToSection[itemNum]
                    self.Sections[topSection][sectionStr] = ''
                    self.__SectionToItem[sectionStr] = itemNum + subChar
                    self.__ItemToSection[itemNum + subChar] = sectionStr
                else:
                    self.__ItemToSection[itemNum] = sectionStr
                if sectionStr not in self.Sections.keys():
                    self.Sections[sectionStr] = {}
                    self.Sections[sectionStr][sectionStr] = ''
            elif itemNum:
                # Put the text into the map:
                mapKey = itemNum + subChar
                topSection = self.__ItemToSection[itemNum]
                if subChar:
                    subSec = self.__ItemToSection[itemNum + subChar]
                    self.Sections[topSection][subSec] = text
                else:
                    self.Sections[topSection][topSection] = text
                itemNum = ''
                subChar = ''

    def __WriteSectionsToFile(self, path):
        """
        * Write cleaned text to local file, using custom
        tags to indicate sections and section names.
        """
        path = path.strip()
        # Ensure that folder exists:
        if not os.path.exists(__GetFolder(path)):
            raise Exception('file folder does not exist.')
        elif os.path.exists(path):
            raise Exception('file already exists.')

        with open(path, 'w') as f:
            writer = csv.writer(f)
            for topSection in self.Sections.keys():
                itemNum = self.__ItemNumToSectionStr
                writer.writerow('----')

    def WriteSoupToFile(self, soup, folderPath):
        """
        * Write soup object to local html file.
        """
        if not os.path.exists(folderPath):
            raise Exception('folderPath at path does not exist.')
        path= ''.join([path, self.Name, '.html'])
        html = soup.prettify()  
        with open(path,"w") as f:
            for i in range(0, len(html)):
                try:
                    f.write(html[i])
                except Exception:
                    pass

    def LoadSoupFromFile(self, folderPath):
        """
        * Load BeautifulSoup object from local file at path.
        """
        if not os.path.exists(folderPath):
            raise Exception('folderPath does not exist.')
        path= ''.join([path, self.Name, '.html'])

        return Soup(path, "html.parser")


    def LoadDocFromFile(self, folderPath):
        """
        * Pull in all sections from local file, load into object.
        """
        if not os.path.exists(folderPath):
            raise Exception('folderPath does not exist.')
        path = ''.join([folderPath, self.Name, '.txt'])

        with open(path, 'r') as f:
            reader = csv.reader(f)

    def __PullSectionAttrs(sectionStr):
        """
        * Extract the name of the section from the string.
        """
        sectionName = Corporate10KDocument.__SectionRE.findall(sectionStr)
        item = ''
        subSec = ''
        if sectionName:
            sectionName = sectionName[0]
            item = Corporate10KDocument.__ItemRE.findall(sectionName)
        if item:
            item = item[0]
            sectionStr = sectionName[sectionName.find(item), len(sectionName)]
            subSec = Corporate10KDocument.__SubSectionRE.findall(item)
        if subSec:
            subSec = subSec[0]
        
        return (sectionStr.strip(), item.strip(), subSec.strip())

    #################
    # Static Helpers:
    #################
    @staticmethod
    def __ConvertHTMLinksToHTML(soupObj):
        """
        * Convert link to HTML if htm.
        """
        links = []

        for link in soupObj.find_all('filinghref'):
            # convert http://*-index.htm to http://*.txt
            url = link.string
            if link.string.split(".")[len(link.string.split("."))-1] == "htm":
                url += "l"
            required_url = url.replace('-index.html', '')
            txtdoc = required_url + ".txt"
            links.append(txtdoc)

        return links

    @staticmethod
    def __NormalizeTXT(txt):
        """
        * Normalize string to use unicode.
        """
        return unicodedata.normalize("NFKD",txt)

    @staticmethod
    def __GetFolder(path):
        """
        * Return enclosing folder for passed file.
        """
        return path[0:path.rfind('\\')]