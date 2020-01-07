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

class PullingSteps(object):
    def __init__(self, text, tables, financials):
        """
        * Denote the steps to perform with the CorporateFiling object.
        """
        self.PullText = text
        self.PullTables = tables
        self.PullFinancials = financials
    @property
    def PullText(self):
        return self.__pullText
    @property
    def PullTables(self):
        return self.__pullTables
    @property
    def PullFinancials(self):
        return self.__pullFinancials
    @PullText.setter
    def PullText(self, pull):
        if not isinstance(pull, bool):
            raise Exception('PullText must be boolean.')
        self.__pullText = pull
    @PullTables.setter
    def PullTables(self, pull):
        if not isinstance(pull, bool):
            raise Exception('PullTables must be boolean.')
        self.__pullTables = pull
    @PullFinancials.setter
    def PullFinancials(self, pull):
        if not isinstance(pull, bool):
            raise Exception('PullFinancials must be boolean.')
        self.__pullFinancials = pull


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
    __reType = type(re.compile(''))
    __skiptags = {'font' : 0, 'a': 0, 'b': 0, 'i': 0, 'u': 0}
    __types = { DocumentType.TENK : '10-K', DocumentType.TENQ : '10-Q', DocumentType.EIGHTK : '8-K' }
    def __init__(self, ticker, type, steps, **args):
        """
        * Instantiate new document object, pull from SEC edgar website or local file 
        depending upon provided arguments.
        Required Arguments:
        * ticker: Company ticker (string).
        * type: Document type. (DocumentType enum).
        * steps: Actual steps to perform (PullingSteps object).
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
        if not isinstance(steps, PullingSteps):
            errMsgs.append('steps must be a PullingSteps object.')
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
                self.__ExtractData(type, steps, htmlPath=args['htmlPath'])
        # Pull document from SEC Edgar website if document date was provided:
        if not hasArg and 'date' in args.keys():
            if isinstance(args['date'], str):
                args['date'] = datetime.strptime(args['date'], '%Y%m%d')
            if not isinstance(args['date'], datetime):
                errMsgs.append('date must be a datetime object or a string.')
            else:
                hasArg = True
                self.__ExtractData(type, steps, date=args['date'])
        if not hasArg and 'customDocPath' in args.keys():
            if not isinstance(args['customDocPath'], str):
                errMsgs.append('customDocPath must be a string.')
            elif not os.path.exists(args['customDocPath']):
                errMsgs.append('File at customDocPath does not exist.')
            elif not args['customDocPath'].endswith('.fml'):
                errMsgs.append('File at customDocPath must have .fml extension.')
            else:
                hasArg = True
                self.__ExtractData(type, steps, customDocPath=args['customDocPath'])
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
    def FindSubDocument(self, exp, exactMatch):
        """
        * Find SubDocument object with name that matches passed regular expression.
        Inputs:
        * exp: Expecting regular expression string or object (if exactMatch == False), or non-regexp string (if exactMatch == True),
        to match (string).
        * exactMatch: Put True if want to find SubDocument object with name that exactly matches regular expression 
        (i.e. re.match() or str == name) or string, else False if want to determine if partial match 
        the regular expression/string (i.e. re.search() or str in name).
        Outputs: 
        * Will return either the first table that matches the regular expression or None if no match.
        """
        errMsgs = []
        expression = None
        if not isinstance(exactMatch, bool):
            errMsgs.append("exactMatch must be a boolean.")
        if not isinstance(exp, CorporateFiling.__reType) and not isinstance(exp, str):
            errMsgs.append("exp must be a string/regular expression.")
        if errMsgs:
            raise Exception('\n'.join(errMsgs))

        subDocs = self.__subDocs
        for docName in subDocs.keys():
            if isinstance(exp, CorporateFiling.__reType):
                if exactMatch and exp.match(docName):
                    return subDocs[docName]
                elif not exactMatch and exp.search(docName):
                    return subDocs[docName]
            elif isinstance(exp, str):
                if exactMatch and exp == docName:
                    return subDocs[docName]
                elif not exactMatch and exp in docName:
                    return subDocs[docName]
        return None

    def FindTable(self, exp, exactMatch):
        """
        * Find first table within all SubDocuments that matches passed regular expression or string.
        Inputs:
        * exp: Expecting regular expression object or string.
        * exactMatch: Put True if want to find table with name that exactly matches regular expression or string (i.e. exp.match(name) or exp == name)
        else False if partially match the regular expression/string (i.e. exp.search(name) or exp in name).
        Outputs: 
        * Will return tuple containing (SubDocument, TableItem) for first table that matches the regular expression, 
        or (None, None) if no match.
        """
        errMsgs = []
        expression = None
        if not isinstance(exactMatch, bool):
            errMsgs.append("exactMatch must be a boolean.")
        if not isinstance(exp, CorporateFiling.__reType) and not isinstance(exp, str):
            errMsgs.append("exp must be a string/regular expression.")
        if errMsgs:
            raise Exception('\n'.join(errMsgs))

        for key in self.SubDocuments.keys():
            doc = self.SubDocuments[key]
            table = doc.FindTable(exp, exactMatch)
            if table:
                return (doc, table)

        return (None, None)

    def PrintTables(self, folderPath, excel = False, fileName = None):
        """
        * Print tables only to csv/xlsx file at folder path.
        Required Arguments:
        * folderPath: Folder path to write file (string). If fileName is None
        then will use default name.
        Optional Arguments:
        * excel: Put True if want to put all tables in single xlsx file, one table per sheet (boolean).
        * fileName: Name of file to write to (string).
        """
        errMsgs = []
        if not isinstance(folderPath, str):
            errMsgs.append('folderPath must be a string.')
        elif not os.path.exists(folderPath):
            errMsgs.append('folderPath does not exist.')
        if not isinstance(excel, bool):
            errMsgs.append('excel must be boolean.')
        if fileName and not isinstance(fileName, str):
            errMsgs.append('fileName must be a string.')
        if errMsgs:
            raise Exception('\n'.join(errMsgs))
        if not fileName:
            fileName = [self.Name, '_tables']
            if excel:
                fileName.append('.xlsx')
            else:
                fileName.append('.csv')
            fileName = ''.join(fileName)
        path = [folderPath.strip()]
        if not folderPath.endswith('\\'):
            path.append('\\')
        path.append(fileName)
        path = ''.join(path)
        if excel:
            # Put tables into excel workbook:
            pass
        else:
            with open(path, 'w', newline = '') as f:
                writer = csv.writer(f)
                for subDoc in self.__subDocs:
                    for name in subDoc.Tables.keys():
                        table = subDoc.Tables[name]
                        writer.writerow(['<Table ' + name + '>'])
                        # Write headers:
                        writer.writerow(table.dtype.names)
                        row = []
                        for col in cols:
                            row.append(col)
                        writer.writerow(row)
                        row = []
                        # Write data:
                        n.apply_along_axis(f.write, axis=1, arr=subDoc.Tables[name])

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
    def __ExtractData(self, type, steps, date = None, customDocPath = None, htmlPath = None):
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

        # Get the document date:
        dateTag = soup.find('acceptance-datetime')
        if dateTag:
            self.__date = CorporateFiling.__GetFilingDate(dateTag)
        else:
            self.__date = None

        # Document is divided into multiple <document> tags, containing text, financials, tables with footnotes:
        self.__subDocs = {}
        docs = soup.find_all('document')
        for doc in docs:
            subDoc = SubDocument(type, steps, doc, self.Ticker)
            name = subDoc.Name
            if name:
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
    __namePattern = re.compile('.+\n')
    __financialsPattern = re.compile('^\d+$')
    __footerTextMatch = re.compile('\([0-9]+\)')
    __headerTextPattern = re.compile('.*font-weight:bold;$')
    __normalTextPattern = re.compile('^font-family:inherit;font-size:\d*pt;$')
    __periodPattern = re.compile('^.*[0-9]{4}Q[0-9](YTD|QTD)?')
    __reType = type(re.compile(''))
    __titlePattern = re.compile('^font-family:inherit;font-size:\d+pt;font-weight:bold$;')
    def __init__(self, docType, steps, doc, ticker):
        self.__type = docType
        self.__finDataRE = (re.compile(ticker.lower() + ':.+', re.UNICODE), re.compile('us-gaap:.+', re.UNICODE))
        self.__Load(steps, doc)

    ###################################
    # Properties:
    ###################################
    @property
    def Financials(self):
        """
        * Return { LineItem -> { Period -> FinancialsData }} mapping.
        """
        return self.__financials
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
        * Maps { Name -> [NumpyArray] }.
        """
        return self.__tables
    @property
    def TextSections(self):
        """
        * Return { SectionName -> Text } mapping.
        """
        return self.__textSections
    ###################################
    # Interface Methods:
    ###################################
    def FindTable(self, exp, exactMatch):
        """
        * Find table with name that matches passed regular expression or string.
        Inputs:
        * exp: Expecting regular expression object or string.
        * exactMatch: Put True if want to find table with name that exactly matches regular expression or string (i.e. exp.match(name) or exp == name)
        else False if partially match the regular expression/string (i.e. exp.search(name) or exp in name).
        Outputs: Will return either the first table that matches the regular expression, 
        or None if no match.
        """
        errMsgs = []
        expression = None
        if not isinstance(exactMatch, bool):
            errMsgs.append("exactMatch must be a boolean.")
        if not isinstance(exp, SubDocument.__reType) and not isinstance(exp, str):
            errMsgs.append("exp must be a string/regular expression.")
        if errMsgs:
            raise Exception('\n'.join(errMsgs))

        tables = self.__tables
        for tableName in tables.keys():
            if isinstance(exp, SubDocument.__reType):
                if exactMatch and exp.match(tableName):
                    return tables[tableName]
                elif not exactMatch and exp.search(tableName):
                    return tables[tableName]
            elif isinstance(exp, str):
                if exactMatch and exp == tableName:
                    return tables[tableName]
                elif not exactMatch and exp in tableName:
                    return tables[tableName]
        return None

    ###################################
    # Private Helpers:
    ###################################
    def __Load(self, steps, doc):
        """
        * Load all aspects of document into the object.
        """
        self.__ExtractDocName(doc)
        if not self.__name:
            return
        if steps.PullText:
            self.__LoadText(doc)
        if steps.PullTables:
            self.__LoadTables(doc)
        if steps.PullFinancials:
            self.__LoadFinancials(doc)

    def __LoadText(self, doc):
        """
        * Load all text from document in easily accessible format.
        """
        self.__textSections = {}
        if self.__type == DocumentType.TENK:
            self.__LoadSections10K(doc)
        elif self.__type == DocumentType.EIGHTK:
            self.__LoadSections8K(doc)
        else:
            self.__LoadSections10Q(doc)

    def __LoadFinancials(self, doc):
        """
        * Load all xbrl-type financials in the document.
        """
        financials = doc.find_all(self.__finDataRE)
        self.__financials = {}
        for tag in financials:
            text = unidecode(tag.text.strip())
            if SubDocument.__financialsPattern.match(text):
                # Get the line item name, period, and convert to appropriate format:
                lineItem = tag.name[tag.name.find(':') + 1:]
                period = SubDocument.__periodPattern.search(tag['contextref'])[0]
                if period not in self.__financials.keys():
                    self.__financials[period] = {}
                self.__financials[period][lineItem] = int(text)

    def __LoadTables(self, doc):
        """
        * Structure all tables in the document.
        """
        self.__tables = {}
        tables = doc.find_all('table')
        for table in tables:
            text = unidecode(table.text).strip()
            if text and TableItem.HasColumnHeaders(table):
                tableData = TableItem(table)
                if tableData.Name:
                    tableName = tableData.Name 
                    tableCount = 2
                    while tableName in self.__tables.keys():
                        tableName = ''.join([tableData.Name, '_', str(tableCount)])
                        tableCount += 1
                    self.__tables[tableName] = tableData
    ##########
    # Helper to Helper Functions:
    ##########
    def __LoadSections10K(self, doc):
        """
        * Do custom pulling for 10K.
        """
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

    def __LoadSections8K(self, doc):
        """
        * Do custom pulling procedure for 8K.
        """
        pass

    def __LoadSections10Q(self, doc):
        """
        * Do custom pulling procedure for 10Q.
        """
        pass

    def __ExtractDocName(self, doc):
        """
        * Get name/description of document.
        """
        self.__name = ''
        desc = doc.find('description')
        if desc:
            self.__name = unidecode(desc.text[0:desc.text.find('\n')])
        else:
            desc = desc

class TableItem(object):
    """
    * Loads and stores data contained in html table for easy access.
    """
    #### Store all tables with irregular info:
    irregTables = []
    __reType = type(re.compile(''))
    __divHeaderMatch = re.compile('line-height:\d\d\d%;text-align:center;font-size:\d\dpt;')
    #__divHeaderMatch = re.compile('.*padding-top:\d+px.*')
    __excludeChars = ''.join(list(set(string.punctuation + ' '))).replace('(', '').replace(')', '').replace('-', '')
    __excludeNames = { 'page' : True, 'table of contents' : True }
    __footerPattern = re.compile('__(_)+')
    __footerTextPattern = re.compile('\([0-9]+\)')
    __headerMatch = re.compile('.*solid \#000000.*')
    __monthMatch = re.compile('(Year Ended)? (Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|(Nov|Dec)(?:ember)?) [1-3]?[0-9]?', re.IGNORECASE)
    __tableStripPattern = re.compile('item \d+\.', re.IGNORECASE)
    __tableTitlePattern = re.compile('^font-family:inherit;font-size:\d+pt;font-weight:bold;$')
    __yearMatch = re.compile('(19|20)[0-9][0-9]')
    __yearEndMatch = re.compile('')
    #####################
    # Constructors:
    #####################
    def __init__(self, table):
        self.__ExtractTableName(table)
        if self.__name:
            self.__LoadData(table)
            self.__GetFootNotes(table)
    #####################
    # Properties:
    #####################
    @property
    def ColumnNames(self):
        """
        * Return names of columns in table, in list format if multiple
        tables exist, or None.
        """
        cols = {}
        if not self.__data:
            return None
        else:
            return self.__data.dtype.names
    @property
    def Data(self):
        """
        * Return access to numpy arrays containing row information. 
        """
        if not self.__data:
            return None
        else:
            return self.__data
    @property
    def FootNotes(self):
        """
        * Return table footnotes.
        """
        return self.__footnotes
    @property
    def Name(self):
        """
        * Title of table.
        """
        return self.__name
    #####################
    # Interface Methods:
    #####################
    @staticmethod
    def HasColumnHeaders(table):
        """
        * Determine if passed table has at least one column header.
        """
        return not table.find('td', {'style' : TableItem.__headerMatch }) is None
    @staticmethod
    def IsExcludedName(name):
        """
        * Determine if name is excluded.
        """
        return name.lower() in TableItem.__excludeNames.keys()
    def FindColumn(self, exp, exactMatch):
        """
        * Find column in this table with name that matches passed regular expression.
        Inputs:
        * exp: Expecting regular expression object or string.
        * exactMatch: Put True if want to find column with name that exactly matches regular expression or string (i.e. exp.match(name) or exp == name)
        else False if partially match the regular expression/string (i.e. exp.search(name) or exp in name).
        Outputs: 
        * Will return either the first table that matches the regular expression, or None if no match.
        """
        errMsgs = []
        expression = None
        if not isinstance(exactMatch, bool):
            errMsgs.append("exactMatch must be a boolean.")
        if not isinstance(exp, TableItem.__reType) and not isinstance(exp, str):
            errMsgs.append("exp must be a string/regular expression.")
        if errMsgs:
            raise Exception('\n'.join(errMsgs))

        table = self.__data
        columns = table.dtype.names
        for column in columns:
            if isinstance(exp, TableItem.__reType):
                if exactMatch and exp.match(column):
                    return table[column]
                elif not exactMatch and exp.search(column):
                    return table[column]
            elif isinstance(exp, str):
                if exactMatch and exp == column:
                    return table[column]
                elif not exactMatch and exp in column:
                    return table[column]
        return None

    
    #####################
    # Private Helpers:
    #####################
    def __LoadData(self, table):
        """
        * Set the table data using passed BeautifulSoup <table> tag from SEC Edgar html 
        document.
        Inputs:
        * table: Expecting BeautifulSoup <table> tag.
        """
        if self.__name == "CONSOLIDATED STATEMENTS OF STOCKHOLDERS' EQUITY":
            self.__name = self.__name
            TableItem.irregTables.append(table)
        self.__data = None
        colNum = 0
        rows = table.find_all('tr')
        # Get column headers for table:
        headerRows = [] 
        for row in rows:
            td = row.find('td', { 'style' : TableItem.__headerMatch })
            if td and not 'background-color:' in str(td):
                headerRows.append(row)
        # Exit data loading if no column headers were found:
        if not headerRows:
            TableItem.irregTables.append(table)
            return
        prefix = ''
        columns = []
        colNames = []
        tableStarts = []
        try:
            for headerRow in headerRows:
                text = unidecode(headerRow.text)
                match = TableItem.__monthMatch.search(unidecode(text))
                if match:
                    prefix = match[0].strip() + ', '
                else:
                    cells = headerRow.find_all('td')
                    tableStarts.append(headerRow)
                    columns.append({})
                    for cell in cells:
                        text = unidecode(cell.text).strip(TableItem.__excludeChars)
                        if text:
                            columns[currColSet][prefix + text] = []
                    colNames.append(list(columns[currColSet].keys()))
                    prefix = ''
            # Pull in row data after getting column headers:
            for headerRow in tableStarts:
                rowCount = 0
                nextRow = headerRow.nextSibling
                regularRowTD = ''
                if nextRow:
                    while '(in ' in str(nextRow):
                        nextRow = nextRow.nextSibling
                    if nextRow:
                        regularRowTD = nextRow.find('td')
                # End pulling in rows when hit column header:
                while nextRow and 'background-color' in str(regularRowTD):
                    cells = nextRow.find_all('td')
                    currRowStrs = []
                    for cell in cells:
                        text = unidecode(cell.text).strip(TableItem.__excludeChars)
                        if text:
                            currRowStrs.append(text)
                    if currRowStrs:
                        # If working with uneven tables, set the first column as the 'Line Item':
                        if len(currRowStrs) > len(columns[currColSet]):
                            columns[currColSet]['Line Item'] = []
                        # Append blank cell values if fewer cells than number of columns for current row
                        # (ex: to accomodate 'Total' columns):
                        while len(currRowStrs) < len(colNames[currColSet]):
                            currRowStrs.append('')
                        for colNum in range(0, len(colNames[currColSet])):
                            data = currRowStrs[colNum]
                            columns[currColSet][colNames[currColSet][colNum]].append(data)
                    nextRow = nextRow.nextSibling

                firstKey = columns[currColSet].keys()
                numRows = 0
                if firstKey:
                    firstKey = list(firstKey)[0]
                    numRows = len(columns[currColSet][firstKey])
                    if numRows > 0:
                        # Load all table values:
                        values = n.array([n.asarray(columns[currColSet][colName]) for colName in colNames[currColSet]])
                        types = [col.dtype for col in values]
                        dt = { 'names' : colNames[currColSet], 'formats' : types }
                        self.__data = n.zeros(numRows, dtype = dt)
                        for col in range(0, len(colNames[currColSet])):
                            self.__data[currColSet][colNames[currColSet][col]] = values[col]
        except:
            TableItem.irregTables.append(table)

    def __ExtractTableName(self, table):
        """
        * Get the name of the table.
        """
        tableName = []
        tag = table
        while tag and tag.parent.name != 'text':
            tag = tag.parent

        boldFont = re.compile('.*font-weight:bold.*')
        # Find the first div tag with bold font text:
        div = tag.find_previous('div')
        while div and not div.find('font' , { 'style' : boldFont }) and div.parent.name == 'text':
            div = div.find_previous('div')
            if div.find('font' , { 'style' : boldFont }) and not unidecode(div.find('font' , { 'style' : boldFont }).text).strip():
                div = div.find_previous('div')
        if div and div.find('font', { 'style' : boldFont}):
            font = div.find('font', { 'style' : boldFont})
            tableName.append(unidecode(font.text).strip())
            tableName = ''.join(tableName)
            #while tag and boldFont.search(str(tag)) and tag.previousSibling and tag.previousSibling.name == 'div':
            #    tableName.append(unidecode(tag.text).strip())
                #tag = tag.find_previous('div' : { 'style' : '' })

        if tableName and tableName.lower() != 'index':
            self.__name = TableItem.__tableStripPattern.sub('', tableName)
        else:
            self.__name = ''

    def __GetFootNotes(self, table):
        """
        * Get footnotes for table (if present).
        """
        self.__footnotes = {}
        tag = table.parent
        while tag.parent.name == 'div':
            tag = tag.parent
        footnote = tag.nextSibling
        if str(type(footnote)) == "<class 'bs4.element.NavigableString'>":
            return
        # We assume that if table is immediately followed by div tag with underscores, then is a footnote table:
        if TableItem.__footerPattern.match(unidecode(footnote.text).strip()):
            table = footnote.nextSibling
            num = 1
            while table.name == 'table':
                if TableItem.__footerTextPattern.search(unidecode(table.text)):
                    rows = table.find_all('tr')
                    for row in rows:
                        rowText = TableItem.__footerTextPattern.sub('', unidecode(row.text).strip())
                        if rowText:
                            self.__footnotes[num] = rowText
                            num += 1
                table = table.nextSibling

class SoupTesting(object):
    @staticmethod
    def PrintUniqueTagsWithCounts(soup, fileName, tagName = None):
        """
        * Print all unique tags that occur in xml object, with frequencies, to file at file path.
        Required Arguments:
        * soup: Expecting BeautifulSoup object.
        * fileName: Expecting string path to output file with tags and unique counts.
        Optional Arguments:
        * tagName: Expecting string or list/tuple of strings to denote which tag(s) to perform counts upon.
        """
        errMsgs = []
        if not isinstance(soup, Soup):
            errMsgs.append('soup must be a BeautifulSoup object.')
        if not isinstance(fileName, str):
            errMsgs.append('fileName must be a string.')
        if not tagName is None and not isinstance(tagName,(str,tuple,list)):
            errMsgs.append('tagName must be a string or container if specified.')

        if len(errMsgs) > 0:
            raise Exception('\n'.join(errMsgs))

        uniqueElems = {}
        if tagName and isinstance(tagName, str):
            tags = soup.find_all(tagName)
            tag = ''.join([str(tag) for tag in tags])
            tagRE = re.compile('<' + tagName + '.+>')
        elif tagName and isinstance(tagName, (tuple, list)):
            tags = soup.find_all(tagName)
            tag = ''.join([str(tag) for tag in tags])
            tagRES = []
            for tagType in tagName:
                tagRES.append(re.compile('<' + tagType + '.+>'))
        elif not tagName:
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
                if isinstance(tagName,str) and tagRE.match(tagStr) and tagStr not in uniqueElems.keys():
                    uniqueElems[tagStr] = 0
                elif isinstance(tagName, (tuple, list)) and [tagRE.match(tagStr) for tagRE in tagRES] and tagStr not in uniqueElems.keys():
                    uniqueElems[tagStr] = 0    
                elif not tagName and tagStr not in uniqueElems.keys():
                    uniqueElems[tagStr] = 0
                if tagStr in uniqueElems.keys():
                    uniqueElems[tagStr] += 1
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
    def PrintTags(tags, path):
        """
        * Print all tags to file.
        """
        with open(path, 'w') as f:
            for tag in tags:
                html = tag.prettify()
                for i in range(0, len(html)):
                    try:
                        f.write(html[i])
                    except:
                        pass
                f.write('____________SEPARATOR____________')
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