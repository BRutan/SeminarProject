##########################################
# CorporateFiling.py
##########################################
# Description:
# * Class pulls 10K for ticker from SEC website, 
# cleans into usable form, then divides text up into
# appropriate sections.

import bs4 as BSoup
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
    def Name(self):
        """
        * Return name of object (for identifying in local files).
        """
        return ''.join([self.Ticker, '_', self.DocumentType, '_', self.DateStr])
    @property
    def SubDocuments(self):
        """
        * Return all data contained within <document> tags
        { DocName -> SubDocument }.
        """
        return self.__subDocs
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
        subTag = re.compile('LIST OF SIGNIFICANT SUBSIDIARIES')
        headerFont = re.compile('.*font-weight:bold;$')
        # Get the document date:
        dateTag = soup.find('acceptance-datetime')
        if dateTag:
            self.__date = CorporateFiling.__GetFilingDate(dateTag)
        else:
            self.__date = None

        # Document is divided into multiple <document> tags, containing text, financials, :
        self.__subDocs = {}
        docs = soup.find_all('document')
        for doc in docs:
            subDoc = SubDocument(doc, self.Ticker)
            name = subDoc.Name
            self.__subDocs[name] = subDoc

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
    
    def __LoadSubsidiaries(self, doc):
        """
        * Pull in all significant subsidiaries.
        """
        # Get all of the column headers in the table:
        headerMatch = re.compile('.*solid \#000000.*')
        table = doc.find('table')
        rows = table.find_all('tr')
        columns = {}
        columnTypes = {}
        colNum = 0
        exclude = list(set(string.punctuation))
        exclude.append(' ')
        exclude = ''.join(exclude)
        nonHeaders = []
        numRows = 0
        for row in rows:
            cells = row.find_all('td')
            if columns:
                # Pull in row values:
                colNum = 0
                for cell in cells:
                    text = unidecode(cell.text).strip(exclude)
                    if text:
                        columns[colNames[colNum]].append(text)
                        colNum += 1
                numRows += 1
            elif not columns and row.find('td', { 'style' : headerMatch }):
                # Pull in all column headers:
                for cell in cells:
                    text = unidecode(cell.text).strip(exclude)
                    if text:
                        columns[text] = []
                colNames = list(columns.keys())

        # Load all table values:
        values = n.array([columns[col] for col in colNames])
        types = [col.dtype for col in values]
        dt = { 'names' : colNames, 'formats' : types }
        self.__subsidiaries = n.zeros(numRows, dtype = dt)
        for col in range(0, len(colNames)):
            self.__subsidiaries[colNames[col]] = values[col]

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

class SubDocument(object):
    """
    * Document within financial document.
    """
    __financialsPattern = re.compile('^\d+$')
    __footNotePattern = re.compile('', re.UNICODE)
    __periodPattern = re.compile('^.*[0-9]{4}Q[0-9](YTD|QTD)?')
    __titlePattern = '^font-family:inherit;font-size:\.pt;font-weight:bold$;'
    def __init__(self, doc, ticker):
        self.__Load(doc)
        if not SubDocument.__finDataRE:
            SubDocument.__finDataRE = (re.compile(self.Ticker.lower() + ':.+', re.UNICODE), re.compile('us-gaap:.+', re.UNICODE))
    ###################################
    # Properties:
    ###################################
    @property
    def FootNotes(self):
        """
        * All table footnotes in document.
        """
        return self.__footnotes
    @property 
    def Name(self):
        """
        * Name of document.
        """
        return self.__name
    @property
    def Tables(self):
        """
        * Maps { Name -> NumpyArray }.
        """
        return self.__tables
    @property
    def Text(self):
        """
        * All non-table text in document.
        """
        return self.__text
    ###################################
    # Interface Methods:
    ###################################
    def FindTable(self, exp):
        """
        * Find table with name that matches passed regular expression.
        Inputs:
        * exp: Expecting regular expression string or object (if exactMatch == False), or non-regexp string (if exactMatch == True),
        to match (string).
        * exactMatch: Put if want to find table with name that exactly matches, else False if want to use regular 
        expression (boolean).
        Outputs: Will return either the first table that matches the regular expression, 
        or None if no match.
        """
        errMsgs = []
        expression = None
        if not isinstance(exactMatch, bool):
            errMsgs.append("exactMatch must be a boolean.")
        if not isinstance(exp, TableItem.__reType) and not isinstance(exp, str):
            errMsgs.append("exp must be a string/regular expression.")
        elif isinstance(exp, TableItem.__reType) and exactMatch:
            errMsgs.append("exp must be a string if exactMatch = True.")
        elif isinstance(exp, str) and not exactMatch:
            errMsgs.append("exp must be a regular expression object if exactMatch = False.")
        if errMsgs:
            raise Exception('\n'.join(errMsgs))
        if not exactMatch and isinstance(exp, str):
            try:
                expression = re.compile(exp)
            except:
                raise Exception('Could not convert exp to regular expression.')
        for tableName in self.__tables.keys():
            if exactMatch and exp == tableName:
                return self.__tables[tableName]
            elif not exactMatch and expression.match(tableName):
                return self.__tables[tableName]

    ###################################
    # Private Helpers:
    ###################################
    def __Load(self, doc):
        """
        * Load all aspects of document into the object.
        """
        self.__ExtractDocName(doc.find('type'))
        self.__LoadText(doc)
        self.__LoadTables(doc)
        self.__LoadFinancials(doc)
    def __LoadText(self, doc):
        """
        * Load all text from document in easily accessible format.
        """
        pass
    def __LoadFinancials(self, doc):
        """
        * Load all xbrl-type financials in the document.
        """
        financials = doc.find_all(SubDocument.__finDataRE)
        for tag in financials:
            text = unidecode(tag.text.strip())
            if SubDocument.__financialsPattern.match(text):
                # Get the line item name, period, and convert to appropriate format:
                lineItem = tag.name[tag.name.find(':') + 1:]
                period = SubDocument.__periodPattern.search(tag['contextref'])[0]
                if period not in self.Financials.keys():
                    self.Financials[period] = {}
                self.Financials[period][lineItem] = int(text)

    def __LoadTables(self, doc):
        """
        * Structure all tables in the document.
        """
        self.__tables = {}
        tables = doc.find_all('table')
        for table in tables:
            tableData = TableItem(table)
            tableName = tableData.Name
            tableCount = 2
            while tableName in self.__tables.keys():
                tableName = ''.join([tableData.Name, '_', tableCount])
                tableCount += 1
            self.__tables[tableName] = tableData

    def __ExtractDocName(self, doc):
        """
        * Pull document name from text.
        """
        divs = doc.find('text').find_all('div', recursive = False)
        titleText = []
        for div in divs:
            boldFont = div.find('font', { 'style' : SubDocument.__titlePattern })
            if boldFont:
                titleText.append(boldFont.text.strip())
        self.__name = ' '.join(titleText)
            
class TableItem(object):
    """
    * Loads and stores data contained in html table for easy access.
    """
    __reType = type(re.compile(''))
    __headerMatch = re.compile('.*solid \#000000.*')
    __excludeChars = ''.join(list(set(string.punctuation)).append(' '))
    #####################
    # Constructors:
    #####################
    def __init__(self, doc):
        self.__LoadData(doc)
    #####################
    # Properties:
    #####################
    @property
    def ColumnNames(self):
        """
        * Return names of columns in table.
        """
        return self.__data.dtype.names
    @property
    def Data(self):
        """
        * Return access to numpy table.
        """
        return self.__data
    @property
    def Name(self):
        """
        * Title of table.
        """
        return self.__name
    #####################
    # Interface Methods:
    #####################
    def FindColumn(self, exp, exactMatch = False):
        """
        * Find column with name that matches passed regular expression.
        Inputs:
        * exp: Expecting regular expression string or object (if exactMatch == False), or non-regexp string (if exactMatch == True),
        to match (string).
        * exactMatch: Put if want to find exact column match, else False if want to use regular 
        expression (boolean).
        Outputs: Will return either the first column that matches the regular expression, 
        or None if no match.
        """
        errMsgs = []
        expression = None
        if not isinstance(exactMatch, bool):
            errMsgs.append("exactMatch must be a boolean.")
        if not isinstance(exp, TableItem.__reType) and not isinstance(exp, str):
            errMsgs.append("exp must be a string/regular expression.")
        elif isinstance(exp, TableItem.__reType) and exactMatch:
            errMsgs.append("exp must be a string if exactMatch = True.")
        elif isinstance(exp, str) and not exactMatch:
            errMsgs.append("exp must be a regular expression object if exactMatch = False.")
        if errMsgs:
            raise Exception('\n'.join(errMsgs))
        rows, cols = self.__data.shape
        if not exactMatch and isinstance(exp, str):
            try:
                expression = re.compile(exp)
            except:
                raise Exception('Could not convert exp to regular expression.')
        elif not exactMatch:
            expression = exp

        for col in range(0, cols):
            colName = self.__data.dtype.names[col]
            if exactMatch and exp == colName:
                return self.__data[:,col]
            elif not exactMatch and expression.match(colName):
                return self.__data[:,col]
        return None

    #####################
    # Private Helpers:
    #####################
    def __LoadData(self, table):
        """
        * Set the table data using passed BeautifulSoup <document> tag from SEC Edgar html 
        document.
        Inputs:
        * table: Expecting BeautifulSoup <table> tag.
        """
        columns = {}
        columnTypes = {}
        colNum = 0
        nonHeaders = []
        numRows = 0
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if columns:
                # Pull in row values after getting column headers:
                colNum = 0
                for cell in cells:
                    text = unidecode(cell.text).strip(TableItem.__excludeChars)
                    if text:
                        columns[colNames[colNum]].append(text)
                        colNum += 1
                numRows += 1
            elif not columns and row.find('td', { 'style' : TableItem.__headerMatch }):
                # Pull in all column headers:
                for cell in cells:
                    text = unidecode(cell.text).strip(TableItem.__excludeChars)
                    if text:
                        columns[text] = []
                colNames = list(columns.keys())
        # Load all table values:
        values = n.array([columns[col] for col in colNames])
        types = [col.dtype for col in values]
        dt = { 'names' : colNames, 'formats' : types }
        self.__data = n.zeros(numRows, dtype = dt)
        for col in range(0, len(colNames)):
            self.__data[colNames[col]] = values[col]

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