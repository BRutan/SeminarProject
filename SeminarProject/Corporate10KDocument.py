##########################################
# Corporate10KDocument.py
##########################################
# Description:
# * Class pulls 10K for ticker from SEC website, 
# cleans into usable form, then divides text up into
# appropriate sections.

# https://www.youtube.com/watch?v=2Oe9ZqXVGME

from bs4 import BeautifulSoup as Soup
import csv
from datetime import date, datetime
import pandas as p 
import numpy as n
import re
import requests
import string
import os
from xbrl import XBRLParser, GAAP
import unicodecsv as uniCSV
import unicodedata as uni
from unidecode import unidecode

class Corporate10KDocument(object):
    """
    * Class pulls 10K for ticker from SEC website, divides text up into appropriate sections,
    and stores text and financials data in easily accessible tables.
    """
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
    ###############
    # For outputting using custom tags:
    ###############
    __tags = {'financial' : '<FinancialsTable:%s>', 'section' : '<TextExhibit:%s>'}
    __FinancialsTag = re.compile("<FinancialsTable:.*>")

    def __init__(self, ticker, **args):
        """
        * Instantiate new document object, pull from SEC edgar website or local file 
        depending upon provided arguments.
        Potential Arguments:
        * 
        """
        errMsgs = []
        if not isinstance(ticker, str):
            raise Exception('ticker must be a string.')
        self.__ticker = ticker
        self.__itemMap = {}
        self.__sectionToItemMap = {}
        hasArg = False
        
        # Pull tags from html file if provided:
        if 'htmlPath' in args.keys():
            if not isinstance(args['htmlPath'], str):
                errMsgs.append('htmlPath must be a string.')
            elif not os.path.exists(args['htmlPath']):
                errMsgs.append('File at htmlPath does not exist.')
            elif not args['htmlPath'].endswith('.html'):
                errMsgs.append('File at htmlPath must have html extension.')
            else:
                hasArg = True
                self.ExtractData(htmlPath=args['htmlPath'])
        # Pull document from SEC Edgar website if document date was provided:
        if not hasArg and 'date' in args.keys():
            if isinstance(args['date'], datetime):
                errMsgs.append('date must be a datetime object.')
            else:
                hasArg = True
                self.ExtractData(date=args['date'])
        if not hasArg and 'customDocPath' in args.keys():
            if not isinstance(args['customDocPath'], str):
                errMsgs.append('customDocPath must be a string.')
            elif not os.path.exists(args['customDocPath']):
                errMsgs.append('File at customDocPath does not exist.')
            else:
                hasArg = True
                self.ExtractData(customDocPath=args['customDocPath'])
        # Handle exceptions if occurred:
        if errMsgs:
            raise Exception('\n'.join(errMsgs))
        elif not hasArg:
            raise Exception('Please provide at least one argument in (htmlPath, date, customDocPath).')

    ####################
    # Properties:
    ####################
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
        return self.__financials
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
    ###################
    # Interface Methods:
    ###################
    def __LoadDocFromFile(self, folderPath):
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

    def PrintFinancials(self, fileName, folderPath):
        """
        * Print financials only to file at folder path.
        """
        errMsgs = []
        if not isinstance(fileName):
            errMsgs.append('fileName must be a string.')
        if not isinstance(folderPath, str):
            errMsgs.append('folderPath must be a string.')
        elif not os.path.exists(folderPath):
            errMsgs.append('folderPath does not exist.')
        path = [folderPath.strip()]
        if not folderPath.endswith('\\'):
            path.append('\\')
        path.append(fileName)
        path = ''.join(path)
        with open(path, 'w') as f:
            writer = csv.writer(f)
            periods = self.Financials.keys()
            writer.writerow(list(periods))
            currRow = []
            for period in periods:
                pass

    def WriteToFile(self, fileName, folderPath, textChunkSize = None):
        """
        * Write cleaned text to local file, using custom
        tags to indicate sections and section names, that can be pulled in more easily.
        """
        errMsgs = []
        if not isinstance(fileName, str):
            errMsgs.append('fileName must be a string.')
        if not isinstance(folderPath, str):
            errMsgs.append('folderPath must be a string.')
        elif not os.path.exists(folderPath):
            errMsgs.append('folderPath does not exist.')
        if not textChunkSize:
            chunkSize = 108
        elif isinstance(textChunkSize, float) or isinstance(textChunkSize, int):
            chunkSize = textChunkSize
        else:
            errMsgs.append('textChunkSize must be numeric.')
        if errMsgs:
            raise Exception(''.join(errMsgs))
        path = [folderPath.strip()]
        if not folderPath.endswith('\\'):
            path.append('\\')
        path.append(fileName)
        path = ''.join(path)
        with open(path, 'w') as f:
            currRow = ['<filingdoc type: "10K;" corp: "', self.Ticker, ';" date:"', self.DateStr, '">', '\n']
            f.write(''.join(currRow))
            # Write text items:
            for section in self.__tenK.keys():
                currRow = ['<textsection name: "', section, ';">', '\n']
                f.write(''.join(currRow))
                for subSection in self.__tenK[section]:
                    currRow = ['<subsection name: "', subSection, ';">', '\n']
                    f.write(''.join(currRow))
                    sectionLen = len(self.TenK[section][subSection])
                    index = 0
                    while index < sectionLen:
                        if index + chunkSize < sectionLen:
                            f.write(self.TenK[section][subSection][index:index + chunkSize] + '\n')   
                        else:
                            f.write(self.TenK[section][subSection][index:sectionLen] + '\n')
                        index += chunkSize
                    f.write('</subsection>\n')
                f.write('</textsection>\n')
            # Write financials:
            f.write('</filingdoc>\n')
            
    ###################
    # Private Helpers:
    ###################
    def ExtractData(self, date = None, customDocPath = None, htmlPath = None):
        """
        * Pull 10K text from local file or from SEC Edgar website.
        """
        self.__financials = {}
        # Pull from local file if path was specified:
        if customDocPath != None:
            # Pull from local file:
            self.__LoadDocFromFile(customDocPath)
            return
        
        soup = None
        if htmlPath != None:
            soup = Soup(open(htmlPath, 'r'), "lxml")
        elif date:
            # Pull from website:
            links = self.__GetLinks(date)
            # Assuming that links have been output in descending order, and that 
            # the first link is the one we want.
            soup = Soup(requests.get(links[0]).text, "lxml") 
        
        # Pull information from html data:
        self.__ExtractData(soup)
        
    def __ExtractData(self, soup):
        """
        * Extract financials and text.
        """
        # Tags denote that item is accounting line item:
        fin_1 = re.compile(self.Ticker.lower() + ':.+', re.UNICODE)
        fin_2 = re.compile('us[-_]gaap:.+', re.UNICODE)
        # self.__finTags = (fin_1, fin_2, 'xbrli')
        finTags = (fin_1, fin_2)
        # Get the document date:
        dateTag = soup.find('acceptance-datetime')
        if dateTag:
            self.__date = Corporate10KDocument.__GetFilingDate(dateTag)
        # Document is divided into multiple <document> tags, containing both text and financial information:
        docs = soup.find_all('document')
        for doc in docs:
            financials = doc.find_all(finTags)
            if financials:
                # Pull in financials data:
                self.__LoadFinancials(financials)
            elif '10-K' in doc.find('type').text:
                # Pull in all items, load into table:
                self.__Load10KSections(doc)
                self.WriteToFile('Test.txt','D:\\Git Repos\\SeminarProject\\SeminarProject\\SeminarProject\\Notes')

    def __GetLinks(self, date):
        """
        * Pull all potential matching links from SEC website.
        """
        link = "http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK="+ \
            str(self.Ticker)+"&type=10-K&dateb="+ date.strftime('%Y%m%d') +"&owner=exclude&output=xml&count=1"
    
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
        # Map { SectionName -> { SubSectionName -> Text } }:
        self.__tenK = {}
        textFont = re.compile('^font-family:inherit;font-size:\d*pt;$')
        headerFont = re.compile('.*font-weight:bold;$')
        fonts = doc.find_all('font', {'style' : (headerFont, textFont) })
        itemMatch = re.compile('^(Item \d+\.){1}$')
        subSectionMatch = re.compile('^(Item \d+[A-Z]+.){1}$')
        lineNum = 1
        currFont = 0
        currTxt = []
        itemToSuperSection = {}
        hitItem = False
        skipLine = False
        for font in fonts:
            text = unidecode(font.text).strip()
            if skipLine:
                skipLine = False
            elif hitItem and 'Table of Contents' not in text and not itemMatch.match(text) and not subSectionMatch.match(text):
                text = re.sub('\n', ' ', text)
                currTxt.append(text)
            elif itemMatch.match(text):
                # Store previous text if loading previous section:
                if hitItem:
                    self.__tenK[sectionName][subSection] = ' '.join(currTxt)
                # Next font contains the name of the section:
                sectionName = unidecode(fonts[currFont + 1].text).strip()
                itemNum = re.search('[0-9]+', text)[0]
                subSection = sectionName
                self.__tenK[sectionName] = {}
                self.__tenK[sectionName][subSection] = ''
                itemToSuperSection[itemNum] = sectionName
                currTxt = []
                hitItem = True
                skipLine = True
            elif subSectionMatch.match(text):
                # Store previous text if loading previous section:
                if hitItem:
                    self.__tenK[sectionName][subSection] = ' '.join(currTxt)
                itemNum = re.search('[0-9]+', text)[0]
                sectionName = itemToSuperSection[itemNum]
                # Next font contains the name of the section:
                subSection = unidecode(fonts[currFont + 1].text).strip()
                self.__tenK[sectionName][subSection] = ''
                currTxt = []
                hitItem = True
                skipLine = True
            # Write last section's text if at end:
            if currFont == len(fonts):
                self.__tenK[sectionName][subSection] = ' '.join(currTxt)
            currFont += 1

    def __LoadFinancials(self, financials):
        """
        * Store all financials in current document.
        """
        hasFinancials = re.compile('^\d+$')
        fin_1 = re.compile(self.Ticker.lower() + ':.+', re.UNICODE)
        fin_2 = re.compile('us-gaap:.+', re.UNICODE)
        # finTags = [tag for tag in financials if fin_1.match(tag.name) or fin_2.match(tag.name)]
        for tag in financials:
            if hasFinancials.match(tag.text):
                # Get the line item name, period, and convert to appropriate format:
                lineItem = self.__ExtractLineItem(tag)
                amount = uni.normalize('NFKD', tag.text)
                period = tag['contextref']
                if period not in self.Financials.keys():
                    self.Financials[period] = {}
                self.Financials[period][lineItem] = int(amount)

    #####################
    # Helper for Helpers:
    #####################
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

    def __ExtractLineItem(self, tag):
        """
        * Extract the accounting line item from the tag name:
        """
        tagString = str(tag)
        return tagString[tagString.find(':') + 1:tagString.find(' ')]

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
            return datetime.strptime(re.findall('\d{8}', filingExp[0])[0], '%Y%m%d')
        else:
            return None
    @staticmethod
    def __GetSectionNum(sectionText):
        """
        * Get the section number.
        """
        pass
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
        * Document within financial document.
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
            self.__footnotes = footnotes
        @Name.setter
        def Name(self, name):
            self.__name = name
        @Tables.setter
        def Tables(self, table):
            self.__tables = table
        @Text.setter
        def Text(self, doc):
            self.__text = text
        ###################################
        # Private Helpers:
        ###################################
        def __LoadText(self, doc):
            """
            * Load all data from document in easily accessible format.
            """
            pass
        @property
        def __LoadFinancials(self, doc):
            """
            * 
            """
            items = doc.find_all('')
