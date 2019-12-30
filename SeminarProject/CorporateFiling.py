##########################################
# CorporateFiling.py
##########################################
# Description:
# * Class pulls 10K for ticker from SEC website, 
# cleans into usable form, then divides text up into
# appropriate sections.

# https://www.youtube.com/watch?v=2Oe9ZqXVGME

from bs4 import BeautifulSoup as Soup
import csv
from datetime import date, datetime
from enum import Enum
import pandas as p 
import numpy as n
import re
import requests
from sortedcontainers import SortedSet, SortedList
import string
import os
from xbrl import XBRLParser, GAAP
import unicodecsv as uniCSV
import unicodedata as uni
from unidecode import unidecode

class DocumentType(Enum):
    TENK = 1
    TENQ = 2
    EIGHTK = 3

class CorporateFiling(object):
    """
    * Class pulls 10K/10Q/8K for ticker from SEC website, divides text up into appropriate sections,
    and stores text and financials data in easily accessible tables.
    """
    ###############
    # For reading raw xml data:
    ###############
    __blacklistTags = { "script" : 0, "style" : 0 }
    __attrlist = {"class" : 0, "id" : 0, "name" : 0, "style": 0, 'cellpadding': 0, 'cellspacing': 0}
    __skiptags = {'font' : 0, 'a': 0, 'b': 0, 'i': 0, 'u': 0}
    __types = { DocumentType.TENK : '10-K', DocumentType.TENQ : '10-Q', DocumentType.EIGHTK : '8-K' }
    def __init__(self, ticker, type, **args):
        """
        * Instantiate new document object, pull from SEC edgar website or local file 
        depending upon provided arguments.
        Required Arguments:
        * ticker: Company ticker (string).
        * type: Document type. (DocumentType enum).
        Potential Arguments (only one possible):
        * customDocPath: If provided, will pull document from local '.fml' file that uses custom tags (string).
        * date: If provided, will pull document with filing date closest to passed date 
        from SEC Edgar website (datetime or string).
        * htmlPath: If provided, will pull document from local html file using xml tags (string).
        """
        errMsgs = []
        if not isinstance(ticker, str):
            errMsgs.append('ticker must be a string.')
        if not isinstance(type, DocumentType):
            errMsgs.append('type must be a DocumentType enumeration.')
        if errMsgs:
            raise Exception('\n'.join(errMsgs))
        self.__ticker = ticker
        self.__type = CorporateFiling.__types[type]
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
                self.__ExtractData(htmlPath=args['htmlPath'])
        # Pull document from SEC Edgar website if document date was provided:
        if not hasArg and 'date' in args.keys():
            if isinstance(args['date'], str):
                args['date'] = datetime.strptime(args['date'], '%Y%m%d')
            if not isinstance(args['date'], datetime):
                errMsgs.append('date must be a datetime object or a string.')
            else:
                hasArg = True
                self.__ExtractData(date=args['date'])
        if not hasArg and 'customDocPath' in args.keys():
            if not isinstance(args['customDocPath'], str):
                errMsgs.append('customDocPath must be a string.')
            elif not os.path.exists(args['customDocPath']):
                errMsgs.append('File at customDocPath does not exist.')
            elif not args['customDocPath'].endswith('.fml'):
                errMsgs.append('File at customDocPath must have .fml extension.')
            else:
                hasArg = True
                self.__ExtractData(customDocPath=args['customDocPath'])
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
    def DocumentType(self):
        return self.__type
    @property
    def FilePath(self):
        """
        * Return predetermined file path name for this object, to read from and output to.
        """
        return ''.join([self.Name + '.txt'])
    @property
    def Financials(self):
        """
        * Map { TableName -> numpy.array() }
        """
        return self.__financials
    @property
    def Name(self):
        """
        * Return name of object (for identifying in local files).
        """
        return ''.join([self.Ticker, '_', self.DocumentType, '_', self.DateStr])
    @property
    def TextSections(self):
        """
        * Maps { Section -> { SubSection -> Text } }.
        """
        return self.__textSections
    @property
    def Ticker(self):
        """
        * Return company ticker.
        """
        return self.__ticker
    ###################
    # Interface Methods:
    ###################
    def PrintFinancials(self, folderPath, fileName = None):
        """
        * Print financials only to csv file at folder path.
        Required Arguments:
        * folderPath: Folder path to write file (string).
        Optional Arguments:
        * fileName: Name of file to write to (string).
        """
        errMsgs = []
        if not isinstance(folderPath, str):
            errMsgs.append('folderPath must be a string.')
        elif not os.path.exists(folderPath):
            errMsgs.append('folderPath does not exist.')
        if not fileName:
            fileName = ''.join([self.Name, '_financials.csv'])
        elif not isinstance(fileName, str):
            errMsgs.append('fileName must be a string.')
        if errMsgs:
            raise Exception('\n'.join(errMsgs))
        path = [folderPath.strip()]
        if not folderPath.endswith('\\'):
            path.append('\\')
        path.append(fileName)
        path = ''.join(path)
        with open(path, 'w', newline = '') as f:
            writer = csv.writer(f)
            periods = SortedList(self.Financials.keys())
            uniqueItems = SortedSet()
            for period in periods:
                for key in self.Financials[period].keys():
                    uniqueItems.add(key)
            periods = list(periods)
            periods.insert(0, 'Line Item')
            writer.writerow(periods)
            periods.pop(0)
            for item in uniqueItems:
                currRow = [item]
                for period in periods:
                    if item not in self.Financials[period].keys():
                        currRow.append('NULL')
                    else:
                        currRow.append(str(self.Financials[period][item]))    
                writer.writerow(currRow)

    def WriteToFile(self, folderPath, fileName = None, textChunkSize = None):
        """
        * Write cleaned text to local file, using custom
        tags to indicate sections and section names, that can be pulled in more easily.
        Required Arguments:
        * folderPath: Path to output file. If fileName is None then will use default
        <company>_<type>_<filingdate>.fml as name of file (string).
        Optional Arguments:
        * fileName: Name + extension of file to output at folderPath (string).
        * textChunkSize: # of characters to include in single line for text sections (int/float, positive).
        """
        errMsgs = []
        if not isinstance(folderPath, str):
            errMsgs.append('folderPath must be a string.')
        elif not os.path.exists(folderPath):
            errMsgs.append('folderPath does not exist.')
        if not fileName:
            fileName = ''.join([self.Name, '.fml'])
        elif not isinstance(fileName, str):
            errMsgs.append('fileName must be a string.')
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
            currRow = ['<filingdoc type: "', self.DocumentType, '" corp: "', self.Ticker, '" date:"', self.DateStr, '">', '\n']
            f.write(''.join(currRow))
            # Write text items:
            for section in self.TextSections.keys():
                currRow = ['<textsection name: "', section, '">', '\n']
                f.write(''.join(currRow))
                for subSection in self.TextSections[section]:
                    currRow = ['<subsection name: "', subSection, '">', '\n']
                    f.write(''.join(currRow))
                    sectionLen = len(self.TextSections[section][subSection])
                    index = 0
                    while index < sectionLen:
                        if index + chunkSize < sectionLen:
                            f.write(self.TextSections[section][subSection][index:index + chunkSize] + '\n')   
                        else:
                            f.write(self.TextSections[section][subSection][index:sectionLen] + '\n')
                        index += chunkSize
                    f.write('</subsection>\n')
                f.write('</textsection>\n')
            # Write financials:
            f.write('</filingdoc>\n')
    
    ###################
    # Private Helpers:
    ###################
    def __ExtractData(self, date = None, customDocPath = None, htmlPath = None):
        """
        * Pull information from local file or SEC website.
        """
        self.__financials = {}
        self.__textSections = {}
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
        else:
            raise Exception('At least one argument must be provided.')

        # Tags denote that item is accounting line item:
        fin_1 = re.compile(self.Ticker.lower() + ':.+', re.UNICODE)
        fin_2 = re.compile('us-gaap:.+', re.UNICODE)
        finTags = (fin_1, fin_2)
        # Get the document date:
        dateTag = soup.find('acceptance-datetime')
        if dateTag:
            self.__date = CorporateFiling.__GetFilingDate(dateTag)
        else:
            self.__date = None

        # Document is divided into multiple <document> tags, containing both text and financial information:
        docs = soup.find_all('document')
        for doc in docs:
            financials = doc.find_all(finTags)
            if financials:
                # Pull in financials data:
                self.__LoadFinancials(financials)
            else:
                # Pull in all items, load into table:
                self.__LoadTextSections(doc)
                
    def __GetLinks(self, date):
        """
        * Pull all potential matching links from SEC website.
        """
        link = ["http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK="]
        link.append(self.Ticker)
        link.append("&type=")
        link.append(self.DocumentType)
        link.append("&dateb=")
        link.append(date.strftime('%Y%m%d'))
        link.append("&owner=exclude&output=xml&count=1")
    
        # Extract potential links to filing:
        data = requests.get(''.join(link)).text
        soup = Soup(data, "lxml")

        # If the link is .htm convert it to .html:
        return CorporateFiling.__ConvertHTMLinksToHTML(soup)

    def __LoadTextSections(self, doc):
        """
        * Load text from text document, depending upon document type.
        """
        if self.DocumentType == '10-K':
            self.__LoadSections10K(doc)
        elif self.DocumentType == '8-K':
            self.__LoadSections8K(doc)
        else:
            self.__LoadSections10K(doc)

    def __LoadSections8K(self, doc):
        """
        * Load all sections in 8K.
        """
        pass


    def __LoadSections10K(self, doc):
        """
        * Load all text/tables from text sections.
        """
        # Map { SectionName -> { SubSectionName -> Text } }:
        textFont = re.compile('^font-family:inherit;font-size:\d*pt;$')
        headerFont = re.compile('.*font-weight:bold;$')
        fonts = doc.find_all('font', {'style' : (headerFont, textFont) })
        itemMatch = re.compile('^(Item \d+\.){1}$')
        subSectionMatch = re.compile('^(Item \d+[A-Z]+.){1}$')
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
                text = re.sub('\n', ' ', font.text)
                currTxt.append(text)
            elif itemMatch.match(text):
                # Store previous text if loading previous section:
                if hitItem:
                    self.__textSections[sectionName][subSection] = ' '.join(currTxt)
                # Next font contains the name of the section:
                sectionName = unidecode(fonts[currFont + 1].text).strip()
                itemNum = re.search('[0-9]+', text)[0]
                subSection = sectionName
                self.__textSections[sectionName] = {}
                self.__textSections[sectionName][subSection] = ''
                itemToSuperSection[itemNum] = sectionName
                currTxt = []
                hitItem = True
                skipLine = True
            elif subSectionMatch.match(text):
                # Store previous text if loading previous section:
                if hitItem:
                    self.__textSections[sectionName][subSection] = ' '.join(currTxt)
                itemNum = re.search('[0-9]+', text)[0]
                sectionName = itemToSuperSection[itemNum]
                # Next font contains the name of the section:
                subSection = unidecode(fonts[currFont + 1].text).strip()
                self.__textSections[sectionName][subSection] = ''
                currTxt = []
                hitItem = True
                skipLine = True
            # Write last section's text if at end:
            if currFont == len(fonts):
                self.__textSections[sectionName][subSection] = ' '.join(currTxt)
            currFont += 1

    def __LoadFinancials(self, financials):
        """
        * Store all financials in current document.
        """
        periodRE = re.compile('^.*[0-9]{4}Q[0-9](YTD|QTD)?')
        hasFinancials = re.compile('^\d+$')
        for tag in financials:
            text = unidecode(tag.text.strip())
            if hasFinancials.match(text):
                # Get the line item name, period, and convert to appropriate format:
                lineItem = tag.name[tag.name.find(':') + 1:]
                period = periodRE.search(tag['contextref'])[0]
                if period not in self.Financials.keys():
                    self.Financials[period] = {}
                self.Financials[period][lineItem] = int(text)

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
    def __Clean(link):
        """
        * Clean all tags from document text.
        """
        data = requests.get(link).text
        soup = Soup(data, "lxml")
        
        # Remove all useless tags:
        for tag in soup.findAll():
            if tag.name.lower() in CorporateFiling.__blacklistTags.keys():
                tag.extract()
            if tag.name.lower() in CorporateFiling.__skiptags.keys():
                tag.replaceWithChildren()            
            for attribute in CorporateFiling.__attrlist.keys():
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

class SoupTesting(object):
    @staticmethod
    def PrintUniqueTagsWithCounts(soup, fileName):
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

    @staticmethod
    def WriteSoupToFile(soup, path):
        """
        * Write soup object to local file.
        """
        html = soup.prettify()
        with open(path,"w") as f:
            for i in range(0, len(html)):
                try:
                    f.write(html[i])
                except Exception:
                    pass