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
import numpy as n
import re
import requests
import string
import os
import unicodedata as uni

class Corporate10KDocument(object):
    """
    * Class pulls 10K for ticker from SEC website, divides text up into appropriate sections,
    and stores text and financials data in easily accessible tables.
    """
    ###############
    #
    ###############
    #__importantDocs = {'subsidaries' : re.compile('subsidiaries', 'i')}
    ###############
    # For reading raw xml data:
    ###############
    __blacklistTags = { "script" : 0, "style" : 0 }
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
    __tags = {'financial' : '<FinancialsTable:%s>', 'section' : '<TextExhibit:%s>'}
    __FinancialsTag = re.compile("<FinancialsTable:.*>")
    def __init__(self, ticker, yearEndDate, localDocPath = None, localSoupPath = None):
        """
        * Create new object. Pull from local file if localPath specified,
        (using predetermined format) or pull from 
        """
        self.Exhibits = {}
        self.Ticker = ticker
        self.Date = yearEndDate
        self.__itemMap = {}
        self.__sectionToItemMap = {}
        # Pull text from SEC Edgar website, load into object:
        self.ExtractData(localDocPath, localSoupPath)

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
    def TenK(self):
        """
        * Maps Item name -> Text line
        """
        return self.__tenK
    @property
    def Ticker(self):
        """
        * Return company ticker.
        """
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

    def PrintUniqueTagsWithCounts(self, soup, fileName):
        """
        * Print all unique tags that occur in xml object, with frequencies, to file at file path. 
        """
        errMsgs = []
        if not isinstance(soup, Soup):
            errMsgs.append('soup must be a BeautifulSoup object.')
        if not isinstance(fileName, str):
            errMsgs.append('fileName must be a string.')

        if len(errMsgs) > 0:
            raise Exception('\n'.join(errMsgs))

        uniqueElems = {}
        tag = soup.find()
        chars = [ch for ch in str(tag)]
        try:
            index = 0
            while index < len(chars):
                for index in range(index, len(chars)):
                    if chars[index] == '<' and index < len(chars) and chars[index + 1] != '/':
                        break
                firstIndex = index
                lastIndex = chars.index('>', firstIndex)
                tagStr = ''.join(chars[firstIndex:lastIndex + 1])
                if tagStr not in uniqueElems.keys():
                    uniqueElems[tagStr] = 0
                uniqueElems[tagStr] = uniqueElems[tagStr] + 1
                index = lastIndex + 1
        except Exception:
            pass

        # Write to file:
        with open(fileName, 'w', newline = '\n') as f:
            writer = csv.writer(f)
            writer.writerow(['Tag:', 'Freq:'])
            for key in uniqueElems.keys():
                writer.writerow([key, uniqueElems[key]])


    def WriteSoupToFile(self, soup, folderPath, fileType = '.html', fileName = None):
        """
        * Write soup object to local file.
        """
        if not os.path.exists(folderPath):
            raise Exception('folderPath at path does not exist.')
        if fileName is None:
            fileName = self.Name
        path= ''.join([folderPath, fileName, fileType])
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
            for topSection in self.Exhibits.keys():
                writer.writerow('SECTION ')
                for subSection in self.Sections[topSection].keys():
                    itemNum = self.__sectionToItemMap[subSection]

    ###################
    # Private Helpers:
    ###################
    def ExtractData(self, localDocPath, localSoupPath):
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
            links = self.__GetLinks()
            # Assuming that links have been output in descending order, and that 
            # the first link is the one we want.
            soup = Soup(requests.get(links[0]).text, "lxml") 
        
        # Pull information from html data:
        self.__ExtractData(soup)
        
    def __ExtractData(self, soup):
        """
        * Extract financials, footnotes and text.
        """
        # Extract filing date from document:
        self.Financials = {}
        self.Sections = {}
        # Tags denote that item is accounting line item:
        fin_1 = re.compile(self.Ticker.lower() + ':.+', re.UNICODE)
        fin_2 = re.compile('us-gaap:.+', re.UNICODE)
        finTags = (fin_1, fin_2, 'xbrli', 'xbrl')
        # Get the document date:
        dateTag = soup.find('acceptance-datetime')
        if dateTag:
            self.Date = Corporate10KDocument.__GetFilingDate(dateTag)
        docs = soup.find_all('document')
        for doc in docs:
            docType = doc.find('type')
            if docType:
                if '10-K' in docType.text:
                    # Pull in all items, load into table:
                    self.__Load10KSections(doc)
                elif doc.find(finTags):
                    # Pull in financials data:
                    self.__LoadFinancials(doc)

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

    def __Load10KSections(self, doc):
        """
        * Load all items from 10K section (business description, risk factors, etc) into 
        tables.
        """
        textFont = re.compile('^font-family:inherit;font-size:\d*pt;$')
        headerFont = re.compile('.*font-weight:bold;$')
        fonts = doc.find_all('font', {'style' : (headerFont, textFont) })
        itemMatch = re.compile('(Item \d+?\.?){1}')
        subSectionMatch = re.compile('(Item \d+[A-Z]?\.?){1}')
        #tablesWithItems = [table for table in tables if itemMatch.match(uni.normalize('NFKD', table.text))]
        lineNum = 1
        currFont = 0
        currTxt = []
        hitItem = False
        superSections = {}
        for font in fonts:
            text = uni.normalize('NFKD', font.text)
            if itemMatch.match(text):
                # Next font contains the name of the section:
                sectionName = uni.normalize('NFKD', fonts[currFont + 1]).strip()
                self.__tenK[sectionName][sectionName] = {}
                hitItem = True
            elif subSectionMatch.match(text):
                # Find the super-section name:
                itemNum = re.sub('')
                self.__tenK[sectionName][subSection] = {}
            elif hitItem:
                hitItem = False
            else:
                # Divide text into 188 character chunks for each line:
                if currTxt and len(text) > 188:
                    currTxt.append(text[0:188 - len(text)])
                    self.__tenK[sectionName] = ''.join(currTxt)
                    lineNum += 1
                    currTxt = [text[188 - len(text): len(text)]]
                elif len(text) > 188:
                    currTxt.append(text[0:188 - len(text)])
                else:
                    tenKTxt[lineNum] = ''.join(currTxt)

            currFont += 1

    def __MapSection(self, tag):
        sectionName, itemNum, subSection = Corporate10KDocument.__PullSectionAttrs(str(tag))
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
            self.Sections[sectionName][sectionName] = {}

        return (sectionName, itemNum, subSection)

    def __ExtractSections_2(self, soup):
        """
        * Map all { SectionName -> { SubSectionName -> Text }} using beautiful soup object.
        """
        tags = soup.find_all('table')
        results = [tag for tag in tags if 'Item' in tag.get_text()]
        sectionTags = []
        for result in results:
            string = Corporate10KDocument.__CleanString(str(result))
            if len(Corporate10KDocument.__itemRE.findall(string)) == 1:
                sectionTags.append(result)

        # Remove all tables with fewer than 2 div children:
        dateTag = soup.find_all('acceptance-datetime')
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

    def __PullSectionAttrs_2(string):
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
        * Clean all non-ascii characters.
        """
        return ''.join([ch if ord(ch) < 128 else ' ' for ch in str])
    @staticmethod
    def __CleanTag(tag):
        """
        * Return string with special characters replaced with space.
        """
        return re.sub('\xa0', str(tag), ' ')
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
    def __GetSectionNum(sectionText):
        """
        * Get the section number.
        """
        all = string.maketrans('','')
        nodigs = all.translate(all, string.digits)
        return sectionText.translate(all, nodigs)
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


    class SubDocument(object):
        """
        * Document within 10K.
        """
        def __init__(self, doc):
            self.__Load(doc)
        ###################################
        # Properties:
        ###################################
        @property
        def FootNotes(self):
            return self.__footnotes
        @property 
        def Name(self):
            return self.__name
        @property
        def Tables(self):
            return self.__tables
        @property
        def Text(self):
            return self.__text
        @FootNotes.setter
        def FootNotes(self, footnotes):
            return self.__footnotes
        @Name.setter
        def Name(self, name):
            return self.__name
        @Tables.setter
        def Tables(self, ):
            return self.__tables
        @Text.setter
        def Text(self):
            return self.__text
        ###################################
        # Properties:
        ###################################
        def __Load(self, doc):
            """
            * Load all data from document in easily accessible format.
            """
            pass