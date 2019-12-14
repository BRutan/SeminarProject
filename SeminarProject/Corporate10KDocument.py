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
import pandas as p 
import re
import requests
import os
import unicodedata

class Corporate10KDocument(object):
    """
    * Class pulls 10K for ticker from SEC website, divides text up into appropriate sections,
    and stores financials data in easily accessible tables.
    """
    ###############
    #
    ###############
    #__importantDocs = {'subsidaries' : re.compile('subsidiaries', 'i')}
    ###############
    # For reading raw xml data:
    ###############
    __blacklistTags = {"script" : 0, "style" : 0}
    __attrlist = {"class" : 0, "id" : 0, "name" : 0, "style": 0, 'cellpadding': 0, 'cellspacing': 0}
    __skiptags = {'font' : 0, 'a': 0, 'b': 0, 'i': 0, 'u': 0}
    __itemREStr = '<tr><td><div>Item \d+[A-Z]?\.?<\/div><\/td>'
    __titleREStr = '<td><div>[\w|\s|\d]+<\/div><\/td>'
    __headerRE = re.compile(__itemREStr + __titleREStr)
    __itemRE = re.compile(__itemREStr)
    __itemTitleRE = re.compile(__titleREStr)
    __tagTextRE = re.compile('.*>.*<.*')
    # <ticker:.*>
    ###############
    # For outputting using custom tags:
    ###############
    __tags = {'financial' : '<FinancialsTable:%s>', 'section' : '<TextSection:%s>'}
    __FinancialsTag = re.compile("<FinancialsTable:.*>")
    def __init__(self, ticker, yearEndDate, localDocPath = None, localSoupPath = None):
        """
        * Create new object. Pull from local file if localPath specified,
        (using predetermined format) or pull from 
        """
        self.Sections = {}
        self.Ticker = ticker
        self.Date = yearEndDate
        self.__itemMap = {}
        self.__sectionToItemMap = {}
        # Pull text from SEC Edgar website, load into object:
        self.__Pull10KText(localDocPath, localSoupPath)
    ########## 
    # Properties:
    ##########
    @property
    def Date(self):
        return self.__date
    @property
    def DateStr(self):
        return self.__date.strftime('%Y%m%d')
    @property
    def FilePath(self):
        """
        * Return predetermined file path name for this object, to read from and output to.
        """
        return ''.join([self.Name + '.txt'])
    @property
    def Financials(self):
        """
        * Map TableName -> numpy.array()
        """
        return self.__Financials
    @property
    def Name(self):
        """
        * Return name of object (for identifying in local files).
        """
        return ''.join([self.Ticker, '_10K_',self.DateStr])
    @property
    def Sections(self):
        """
        * Maps SectionName -> { SubSectionName -> SubSectionName
        """
        return self.__section
    @property
    def Subsidiaries(self):
        """
        * Return all company subsidiaries.
        """
        return self.__subidiaries
    @property
    def Ticker(self):
        return self.__ticker
    @Date.setter
    def Date(self, dt):
        if isinstance(dt, datetime):
            self.__date = dt.date()
        elif isinstance(dt, str):
            self.__date = datetime.strptime(dt, '%Y%m%d').date()
        elif not isinstance(dt, date):
            raise Exception('Date must be a date/datetime object.')
        else:
            self.__date = dt
    @Financials.setter
    def Financials(self, fin):
        if not isinstance(fin, dict):
            raise Exception('Financials must be a dictionary.')
        self.__Financials = fin
    @Sections.setter
    def Sections(self, _dict):
        if not isinstance(_dict, dict):
            raise Exception('Sections must be a dictionary')
        self.__section = _dict
    @Subsidiaries.setter
    def Subsidiaries(self, subs):
        if not isinstance(subs, p.DataFrame):
            raise Exception('Subsidiaries must be a Pandas.Dataframe.')
        self.__subidiaries = subs
    @Ticker.setter
    def Ticker(self, ticker):
        if not isinstance(ticker, str):
            raise Exception('Ticker must be a string.')
        self.__ticker = ticker.lower()
    ########## 
    # Private properties:
    ##########
    @property
    def __FinancialsTag(self):
        """
        * XML tag containing financials data (<ticker:.*>).
        """
        return '<%s:.*>' % self.Ticker
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
    ###################
    # Interface Methods:
    ###################
    def LoadSoupFromFile(self, folderPath):
        """
        * Load BeautifulSoup object from local file at path.
        """
        if not os.path.exists(folderPath):
            raise Exception('folderPath does not exist.')
        path = ''.join([path, self.Name, '.html'])

        return Soup(path, "lxml")

    def LoadDocFromFile(self, folderPath):
        """
        * Pull in all sections from local file, load into object.
        """
        if not os.path.exists(folderPath):
            raise Exception('folderPath does not exist.')
        path = ''.join([folderPath, self.Name, '.txt'])

        with open(path, 'r') as f:
            reader = csv.reader(f)

    def WriteSoupToFile(self, soup, folderPath, fileType = '.html'):
        """
        * Write soup object to local file.
        """
        if not os.path.exists(folderPath):
            raise Exception('folderPath at path does not exist.')
        path= ''.join([folderPath, self.Name, fileType])
        html = soup.prettify()  
        with open(path,"w") as f:
            for i in range(0, len(html)):
                try:
                    f.write(html[i])
                except Exception:
                    pass        

    def WriteToFile(self, folderPath = '\\10Ks\\'):
        """
        * Write cleaned text to local file, using custom
        tags to indicate sections and section names, that can be pulled in more easily.
        """
        folderPath = folderPath.strip()
        # Ensure that folder exists:
        if not os.path.exists(folderPath):
            raise Exception('file folder does not exist.')
        path = ''.join([folderPath, self.Name, '.txt'])
        chunkSize = 116
        with open(path, 'w') as f:
            writer = csv.writer(f)
            for topSection in self.Sections.keys():
                writer.writerow('SECTION ')
                for subSection in self.Sections[topSection].keys():
                    itemNum = self.__sectionToItemMap[subSection]

    ###################
    # Private Helpers:
    ###################
    def __Pull10KText(self, localDocPath, localSoupPath):
        """
        * Pull 10K text from local file or from SEC Edgar website.
        """
        # Pull from local file if path was specified:
        if localDocPath != None:
            # Pull from local file:
            self.LoadDocFromFile(path)
            return
        
        soup = None
        if localSoupPath != None:
            soup = self.LoadSoupFromFile(localSoupPath)
        else:
            # Pull from website:
            links = self.__GetLinks()[0]
            # Assuming that links have been output in descending order, and that 
            # the first link is the one we want.
            soup = Soup(requests.get(links[0]).text, "lxml")
            #soup = self.__Clean(links[0])
            
        # Pull file using BeautifulSoup library from link, extract all sections and
        # place into map:
        self.__ExtractFinancials(soup)
        self.__ExtractSections(soup)

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

    def __ExtractFinancials(self, soup):
        """
        * Extract all financials, store in mapping { TableName -> DataFrame() }
        """
        self.Financials = {}
        ticker = self.Ticker.lower()
        financialsPattern1 = re.compile(ticker + ':.+', re.UNICODE)
        financialsPattern2 = re.compile('us-gaap:.+', re.UNICODE)
        finPattern = re.compile(self.__FinancialsTag)
        #results = soup.find_all(finPattern)
        #results = soup.find_all(('xbrli', 'xbrl'))
        #results = soup.find_all((financialsPattern1, financialsPattern2))
        results = soup.find_all(('document'))
        for result in results:
            if re.compile('subsidiaries', 'i').match(result.text):
                # Example:
                self.__PullSubsidiaries(result)

            # Get parent table:
            parentTable = result.parent
            
            # Get the table date:
            
            #foundTables = parentTable.findChildren('xbrli:instant')
            # Pull in subsidiaries:

            tagName = result.name
            # ContextRef contains the period information. 
            period = result['contextref']
            lineItem = re.sub('<amzn:', '', result.name)
            
            # Find which table line item belongs to:    `
            table = result.parent
            # Get values:
            if 'decimals' in result.keys():
                dec = result.get('decimals')
                val = result.get_text()

    def __ExtractData(self, soup):
        """
        * Extract financials, footnotes and text.
        """
        # Extract filing date from document:
        dateTag = soup.find('acceptance-datetime')
        if dateTag:
            self.Date = Corporate10KDocument.__GetFilingDate(dateTag)

        docs = soup.find_all('document')
        for doc in docs:
            typeInfo = doc.find('type')
            if typeInfo.getText() == '10-K':
                # Pull in all items, load into table:
                pass
            else:
                # Pull in financials data:

                pass

    def __Load10KSections(self, doc):
        """
        * Load all items from 10K section (business description, risk factors, etc) into 
        tables.
        """
        tables = doc.find_all('table')
        results = [table for table in tables if 'Item' in table.get_text()]
        sectionTags = []
        for result in results:
            string = Corporate10KDocument.__CleanString(str(result))
            if len(Corporate10KDocument.__itemRE.findall(string)) == 1:
                sectionTags.append(result)

        for tag in sectionTags:
            # Extract section, subsection and item number strings:
            sectionName, itemNum, subSection = Corporate10KDocument.__PullSectionAttrs(str(result))
            # Create map in stored Sections dictionary:
            if itemNum in self.__ItemToSection.keys():
                # If at a subsection, then determine the super section name, and add
                # subsection title to map.
                topSection = self.__ItemToSection[itemNum]
                self.Sections[topSection][sectionName] = ''
                self.__SectionToItem[sectionName] = subSection
                self.__ItemToSection[subSection] = sectionName
            else:
                # Add super section, and create section to itself:
                topSection = sectionName
                subSection = sectionName
                self.__ItemToSection[itemNum] = sectionName
                self.__SectionToItem[sectionName] = itemNum
                self.Sections[sectionName] = {}
                self.Sections[sectionName][sectionName] = ''
            # Walk up through tree until node has div siblings (standard for Items sections):
            tag = self.__WalkSectionTag(result)

            # Pull in all text for section:
            currText = []
            tag = tag.nextSibling
            while tag and tag.name == 'div':
                currText.append(tag.get_text())
                tag = tag.nextSibling
            # Add text to the Sections map:
            self.Sections[topSection][subSection] = ' '.join(currText)

    def __LoadFinancials(self, doc):
        """
        * 
        """
        financialsPattern1 = re.compile(ticker + ':.+', re.UNICODE)
        financialsPattern2 = re.compile('us-gaap:.+', re.UNICODE)
        tables = doc.find_all('table')
        for table in tables:

            pass

    
    def __ExtractSections_2(self, soup):
        """
        * Map all { SectionName -> { SubSectionName -> Text } }using beautiful soup object.
        """
        tags = soup.find_all(('table'))
        results = [tag for tag in tags if 'Item' in tag.get_text()]
        sectionTags = []
        for result in results:
            string = Corporate10KDocument.__CleanString(str(result))
            if len(Corporate10KDocument.__itemRE.findall(string)) == 1:
                sectionTags.append(result)

        # Remove all tables with fewer than 2 div children:
        dateTag = soup.find_all(('acceptance-datetime'))
        # Extract filing date from document:
        if dateTag:
            dateTag = dateTag[0]
            self.Date = Corporate10KDocument.__GetFilingDate(dateTag)

        # We note that for each 'Item' section in the 10K, consists of <table>[Item # and Title]<\table><div>...<div>
        # until a non-'div' tag is hit.
        for result in sectionTags:
            # Extract section, subsection and item number strings:
            sectionName, itemNum, subSection = Corporate10KDocument.__PullSectionAttrs(str(result))
            # Create map in stored Sections dictionary:
            if itemNum in self.__ItemToSection.keys():
                # If at a subsection, then determine the super section name, and add
                # subsection title to map.
                topSection = self.__ItemToSection[itemNum]
                self.Sections[topSection][sectionName] = ''
                self.__SectionToItem[sectionName] = subSection
                self.__ItemToSection[subSection] = sectionName
            else:
                # Add super section, and create section to itself:
                topSection = sectionName
                subSection = sectionName
                self.__ItemToSection[itemNum] = sectionName
                self.__SectionToItem[sectionName] = itemNum
                self.Sections[sectionName] = {}
                self.Sections[sectionName][sectionName] = ''
            # Walk up through tree until node has div siblings (standard for Items sections):
            tag = self.__WalkSectionTag(result)

            # Pull in all text for section:
            currText = []
            tag = tag.nextSibling
            while tag and tag.name == 'div':
                currText.append(tag.get_text())
                tag = tag.nextSibling
            # Add text to the Sections map:
            self.Sections[topSection][subSection] = ' '.join(currText) 

    def __PullSectionAttrs(string):
        """
        * Extract the name of the section from the string.
        """
        # Remove all tricky characters from string:
        string = Corporate10KDocument.__CleanString(str(string))
        headerName = Corporate10KDocument.__headerRE.findall(string)
        sectionStr = ''
        item = None
        subSec = None
        if headerName:
            headerName = headerName[0]
            sectionStr = Corporate10KDocument.__itemTitleRE.findall(headerName)
            item = Corporate10KDocument.__itemRE.findall(headerName)
        if item:
            item = Corporate10KDocument.__TagText(item[0]).strip()
            subSec = re.findall('.[A-Z]', item)
            item = Corporate10KDocument.__NumbersOnly(item)
        else:
            item = None
        if subSec:
            subSec = Corporate10KDocument.__TagText(subSec[0]).strip()
        else:
            subSec = None
        if sectionStr:
            sectionStr = Corporate10KDocument.__TagText(sectionStr[0]).strip()
        else:
            sectionStr = None
        
        return (sectionStr, item, subSec)

    def __PullSubsidiaries(self, result):
        """
        * Pull all subsidiaries and information regarding them.
        """
        subs = p.DataFrame()
        divs = doc.find_all('div')
        headers = []
        row = 1
        tableVals = result.text.split('\xa0')
        for val in tableVals:
            if 'Exhibit' not in val:
                if row > 1:

                else:
                    '\nEX-21.1\n3\namzn-20181231xex211.htm\nEXHIBIT 21.1\n\n\n\n\nExhibit\n'
                'Exhibit 21.1 AMAZON.COM, INC. LIST OF SIGNIFICANT SUBSIDIARIES\xa0'
                'Legal Name\xa0Jurisdiction\xa0Percent\xa0Owned'
                'Amazon Services LLC\xa0Nevada\xa0100%Amazon Digital Services LLC\xa0Delaware\xa0100%
                'Amazon.com Services, Inc.\xa0Delaware\xa0100%Amazon.com Int’l Sales, Inc.\xa0Delaware\xa0100%Amazon Technologies, Inc.\xa0Nevada\xa0100%\xa0\n\n'

    #################
    # Static Helpers:
    #################
    @staticmethod
    def __WalkSectionTag(tag):
        """
        * Walk the section tag up until it has div siblings, which contain text for Item.
        """
        while tag.nextSibling is None or tag.nextSibling.name != 'div':
            tag = tag.parent
        return tag
    @staticmethod
    def __TagText(string):
        """
        * Pull text from tag.
        """
        return re.sub(r'<\/?\w+>', '', string)
    @staticmethod
    def __NumbersOnly(string):
        """
        * Return string with numbers only.
        """
        return re.sub(r'\D', '', string)
    @staticmethod
    def __NoPunctuation(string):
        """
        * Return string without punctuation.
        """
        return re.sub('.|,|;|:', '', string)

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
    def __CleanString(str):
        """
        * Clean all non-unicode characters.
        """
        return ''.join([ch if ord(ch) < 128 else ' ' for ch in str])

    @staticmethod
    def __CleanTag(tag):
        """
        * Return string with special characters replaced with space.
        """
        return re.sub('\xa0', str(tag), ' ')

    @staticmethod
    def __CheckIfTableOfContents(tag):
        """
        * Check if tag is sitting in table of contents.
        """
        pass

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
    @staticmethod
    def __GetFilingDate(tag):
        """
        * Extract the filing date from document given the <acceptance-datetime> tag:
        """
        filingExp = re.findall('FILED AS OF DATE:\s+\d{8}', str(tag))
        if filingExp:
            return re.findall('\d{8}', filingExp[0])[0]
        else:
            return None
    @staticmethod
    def __Clean(link):
        """
        * Clean all tags from document text.
        """
        data = requests.get(link).text
        soup = Soup(data, "lxml")
        
        # Remove all useless tags:
        for tag in soup.findAll():
            if tag.name.lower() in Corporate10KDocument.__blacklistTags.keys():
                tag.extract()
            if tag.name.lower() in Corporate10KDocument.__skiptags.keys():
                tag.replaceWithChildren()            
            for attribute in Corporate10KDocument.__attrlist.keys():
                del tag[attribute]
        return soup
