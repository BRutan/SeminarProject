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
import _locale
import pandas as p 
import numpy as n
import re
import requests
from sortedcontainers import SortedSet, SortedList
import string
import sys
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
    AllDescriptions = {}
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
        Optional Arguments:
        * corpName: Official company name (string).
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
        self.__corpName = ''
        if 'corpName' in args.keys():
            self.__corpName = args[corpName]
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
    def CompanyName(self):
        return self.__corpName
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
        * Return name of object, containing document type and filing date information 
        (i.e. <Ticker>_<DocumentType>_<FilingDate>).
        """
        return ''.join([self.Ticker, '_', self.DocumentType.replace('-','_'), '_', self.DateStr])
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
    # Testing:
    @staticmethod
    def InsertUniqueDescriptions(db):
        if db.TableExists('UniqueDescriptions'):
            data = {'description' : [], 'type' : []}
            for key in CorporateFiling.UniqueDescriptions.keys():
                data['description'].append(key)
                data['type'].append(CorporateFiling.UniqueDescriptions[key])
            db.InsertValues('UniqueDescriptions', data)
    @staticmethod
    def GetUniqueDescriptions(db):
        if db.TableExists('UniqueDescriptions'):
            query = ''
            for key in CorporateFiling.UniqueDescriptions.keys():
                data['description'].append(key)
                data['type'].append(CorporateFiling.UniqueDescriptions[key])
            db.ExecuteQuery('UniqueDescriptions', data)

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
        * Print all tables in CorporateFiling with data to csv/xlsx file at folder path.
        Required Arguments:
        * folderPath: Folder path to write file (string). If fileName is None
        then will use default name.
        Optional Arguments:
        * excel: Put True if want to put all tables in single xlsx file, one table per sheet (boolean).
        * fileName: Name of file to write to, excluding extension (string).
        """
        errMsgs = []
        if not isinstance(folderPath, str):
            errMsgs.append('folderPath must be a string.')
        elif not os.path.exists(folderPath):
            # Create folder path if does not exist already:
            os.mkdir(folderPath)
        if not isinstance(excel, bool):
            errMsgs.append('excel must be boolean.')
        if fileName and not isinstance(fileName, str):
            errMsgs.append('fileName must be a string.')
        if errMsgs:
            raise Exception('\n'.join(errMsgs))
        if not fileName:
            fileName = [self.Name, '_tables']
        else:
            # Remove extension if included:
            fileName = [fileName[0:fileName.find('.') if fileName.find('.') > -1 else len(fileName)]]
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
        # Exit immediately if file with name already exists:
        if os.path.exists(path):
            return
        # Write all tables to file:
        if excel:
            maxSheetTitleLen = 255
            # Put tables into excel workbook:
            pass
        else:
            with open(path, 'w', newline = '') as f:
                writer = csv.writer(f)
                for subDoc in self.__subDocs.keys():
                    # Only write subdocument attributes if it has tables:
                    if not self.__subDocs[subDoc].HasTables:
                        continue
                    writer.writerow(['-' * 10])
                    writer.writerow(['Document:', subDoc])
                    writer.writerow(['-' * 10])
                    for tableName in self.__subDocs[subDoc].Tables.keys():
                        table = self.__subDocs[subDoc].Tables[tableName]
                        # Only write table attributes if data was loaded:
                        if not table.HasData:
                            continue
                        writer.writerow(['Table Title:', tableName])
                        colnames = table.ColumnNames
                        # Write headers:
                        writer.writerow(colnames)
                        rows = table.Data.shape[0]
                        for row in range(0, rows):
                            currRow = []
                            for col in colnames:
                                currRow.append(str(table.Data[col][row]))
                            writer.writerow(currRow)
                        if table.HasFootNotes:
                            # Write all table footnotes:
                            writer.writerow(['FootNotes:'])
                            for row in range(1, len(table.FootNotes.keys()) + 1):
                                writer.writerow([row, table.FootNotes[row]])
                        writer.writerow([])
                        writer.writerow([])

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
        links = []

        if htmlPath != None:
            soup = Soup(open(htmlPath, 'r'), "lxml")
        elif date:
            # Pull from website:
            links = self.__GetDocumentLinks(date, steps.PullFinancials)
        else:
            raise Exception('At least one argument must be provided.')

        # Document is divided into multiple <document> tags, containing text, financials, tables with footnotes:
        self.__subDocs = {}
        if links:
            # Testing:
            rawTables = []
            cleanTables = []
            path = 'D:\\Git Repos\\SeminarProject\\SeminarProject\\SeminarProject\\Notes\\TableNames\\'
            filePath = ''.join([path, self.Ticker, '_Tables.html'])
            for link in links:
                soup = Soup(requests.get(link).text, 'lxml')
                # Testing:
                rawTables.extend(soup.find_all('table'))
                self.__CleanSoup(soup)
                cleanTables.extend(soup.find_all('table'))
                SoupTesting.PrintTableHTML(tables = rawTables, filePath = ''.join([path, 'Company HTML Tables Raw\\', self.Ticker, '_RawHTML.html']))
                SoupTesting.PrintTableHTML(tables = cleanTables, filePath = ''.join([path, 'Company HTML Tables Unwrapped\\', self.Ticker, '_UnwrappedHTML.html']))
                docs = soup.find_all('document')
                for doc in docs:
                    subDoc = SubDocument(type, steps, doc, self.Ticker, self.CompanyName)
                    if subDoc.Name:
                        self.__subDocs[subDoc.Name] = subDoc
        elif not soup is None:
            self.__CleanSoup(soup)
            # Pull data from single Soup object (loaded from .html document or custom .brl document):
            docs = soup.find_all('document')
            for doc in docs:
                subDoc = SubDocument(type, steps, doc, self.Ticker, self.CompanyName)
                if subDoc.Name:
                    self.__subDocs[subDoc.Name] = subDoc

        # Testing:
        #SoupTesting.PrintTableAttributes(self, path)

        # Print all irregular tables:
        #if TableItem.irregTables:
        #    fileName = ''.join([path, 'IrregTables_', self.Ticker, '.html'])
        #    SoupTesting.PrintTableHTML(tables = TableItem.irregTables, filePath = fileName)
        #    TableItem.irregTables = []

    def __GetDocumentLinks(self, date, pullFinancials):
        """
        * Pull filing link with closest filing date to passed date.
        """
        link = ["http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK="]
        link.append(self.Ticker)
        link.append("&type=")
        link.append(self.DocumentType)
        link.append("&dateb=")
        link.append(date.strftime('%Y%m%d'))
        link.append("&owner=exclude&output=xml&count=1")
    
        # Extract links to document with nearest filing date:
        targetDate = datetime(year = date.year, day = date.day, month = date.month)
        minDays = -1
        try:
            data = requests.get(''.join(link)).text
        except Exception:
            raise Exception(message='\n'.join(['CorporateFiling::__GetDocumentLinks()','Could not grab data from link:', link]))

        soup = Soup(data, "lxml")
        link = ''
        filingTags = [tag for tag in soup.find_all('filing') if tag.find('datefiled') and tag.find('type') and unidecode(tag.find('type').text) == self.__type]
        docFilingDate = None
        # Select tag that is closest to the target date (later date):
        for tag in filingTags:
            filingDate = datetime.strptime(unidecode(tag.find('datefiled').text), '%Y-%m-%d')
            dayDiff = (filingDate - targetDate).days
            if minDays == -1:
                minDays = abs(dayDiff)
                link = unidecode(tag.find('filinghref').text).strip()
                docFilingDate = filingDate
            elif dayDiff < 0 and abs(dayDiff) < minDays:
                minDays = abs(dayDiff)
                link = unidecode(tag.find('filinghref').text).strip()
                docFilingDate = filingDate

        # If the link is .htm convert it to .html:
        if link.endswith('.htm'):
            link += 'l'

        # Pull approriate link to document from searched link:
        try:
            soup = Soup(requests.get(link).text, 'lxml')
        except Exception:
            raise Exception(message='\n'.join(['Could not grab data from link:', link]))
        # Get filing date:
        self.__date = docFilingDate
        targetLinks = []
        # Attempt to get company name from website:
        if not self.__corpName and soup.find('span', {'class' : re.compile('companyName')}):
            nameText = unidecode(soup.find('span', {'class' : re.compile('companyName')}).contents[0]).strip()
            self.__corpName = re.sub('\(.+\)', '', nameText).strip()
        # We do not want to pull in links to graphics or PDFs:
        exp = ''.join(['(', self.__type, '|EX-\d+(\..+)?)'])
        xbrlMatch = re.compile('xml', re.IGNORECASE)
        docMatch = re.compile(exp, re.IGNORECASE)
        tables = soup.find_all('table' , {'class' : re.compile('tablefile', re.IGNORECASE)})
        if tables:
            for table in tables:
                # Skip pulling in financial related documents if not directed to do so by the PullingSteps class:
                if not pullFinancials and re.match('data files', table['summary'], re.IGNORECASE):
                    continue
                rows = table.find_all('tr')
                for row in rows:
                    headers = row.find_all('th')
                    cells = [unidecode(td.text).strip() for td in row.find_all('td')]
                    if headers:
                        # Find the 'Description' and 'Document' column:
                        descCol, typeCol = (None, None)
                        for col, header in enumerate(headers):
                            if descCol is None and re.match('description', unidecode(header.text), re.IGNORECASE):
                                descCol = col
                            if typeCol is None and re.match('type', unidecode(header.text), re.IGNORECASE):
                                typeCol = col
                    elif re.match('data files', table['summary'], re.IGNORECASE) and xbrlMatch.match(cells[typeCol]):
                        linkText = unidecode(row.find('a')['href']).strip()
                        link = ['https://www.sec.gov']
                        link.append(linkText)
                        targetLinks.append(''.join(link))
                    elif docMatch.match(cells[typeCol]) and not re.search('pdf', cells[descCol], re.IGNORECASE):
                        linkText = unidecode(row.find('a')['href']).strip()
                        link = ['https://www.sec.gov']
                        link.append(linkText)
                        targetLinks.append(''.join(link))
        return targetLinks

    def __CleanSoup(self, soup):
        """
        * Unwrap all critical tags (font, rows, cells, tables) from 'div' tags, 
        unwrap fonts from tables that have no header columns (i.e. are not really tables).
        """
        tags = soup.find_all('div')
        for tag in tags:
            tag.unwrap()
        tables = soup.find_all('table')
        headerMatch = re.compile('.+solid \#\d+.+')
        footnoteMatch = re.compile('^(\(\d+\)|\*)[ ]?.+\.$', re.IGNORECASE) 
        # Unwrap font from tables that do not have headers in them:
        for table in tables:
            skip = False
            rows = table.find_all('tr')
            for row in rows:
                if row.find_all('td', {'style' : headerMatch}):
                    skip = True
                    break
                elif footnoteMatch.match(unidecode(row.text).strip()):
                    skip = True
                    break
            if not skip:
                # Unwrap all font tags if not a table or footer table:
                for cell in table.find_all('td'):
                    cell.unwrap()
                for row in table.find_all('tr'):
                    row.unwrap()
                table.unwrap()
    #################
    # Static Helpers:
    #################
    @staticmethod
    def __GetFilingDate(soup):
        """
        * Extract the filing date from document given the <acceptance-datetime> tag:
        """
        infos = soup.find_all('div' , {'class' : 'infoHead'})
        filingDateRE = re.compile('filing date', re.IGNORECASE)
        infoRE = re.compile('info', re.IGNORECASE)
        for info in infos:
            text = unidecode(info.text).strip()
            if filingDateRE.match(text):
                dateTag = info.find_next('div', {'class' : infoRE } )
                if dateTag:
                    dateText = unidecode(dateTag.text).strip()
                    return datetime.strptime(dateText, '%Y-%m-%d')
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
    def __init__(self, docType, steps, doc, ticker, corpName):
        self.__type = docType
        self.__name = ''
        self.__financials = {}
        self.__tables = {}
        self.__textSections = {}
        self.__finDataRE = (re.compile(ticker.lower() + ':.+', re.UNICODE), re.compile('us-gaap:.+', re.UNICODE))
        self.__Load(steps, doc, corpName)

    ###################################
    # Properties:
    ###################################
    @property
    def Financials(self):
        """
        * Return { Period -> { LineItem -> Amount }} mapping.
        """
        return self.__financials
    @property
    def HasTables(self):
        """
        * Indicate that tables have been found in sub document.
        """
        return len(self.__tables.keys()) > 0
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
    def __Load(self, steps, doc, corpName):
        """
        * Load all aspects of document into the object.
        """
        self.__ExtractDocName(doc)
        if not self.__name:
            return
        if steps.PullText:
            self.__LoadText(doc)
        if steps.PullTables:
            self.__LoadTables(doc, corpName)
        if steps.PullFinancials:
            self.__LoadFinancials(doc)

    def __LoadText(self, doc):
        """
        * Load all text from document in easily accessible format.
        """
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
        for tag in financials:
            text = unidecode(tag.text.strip())
            if SubDocument.__financialsPattern.match(text):
                # Get the line item name, period, and convert to appropriate format:
                lineItem = tag.name[tag.name.find(':') + 1:]
                period = SubDocument.__periodPattern.search(tag['contextref'])[0]
                if period not in self.__financials.keys():
                    self.__financials[period] = {}
                self.__financials[period][lineItem] = int(text)

    def __LoadTables(self, doc, corpName):
        """
        * Structure all tables in the document.
        """
        tables = doc.find_all('table')
        for table in tables:
            text = unidecode(table.text).strip()
            if text and TableItem.HasColumnHeaders(table):
                tableData = TableItem(table, corpName)
                if tableData.HasData:
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
    __excludeChars = ''.join(list(set(string.punctuation + ' '))).replace('(', '').replace(')', '').replace('-', '')
    __excludeNames = { 'page' : True, 'table of contents' : True }
    __footerPattern = re.compile('__(_)+')
    __footerTextPattern = re.compile('(\([0-9]+\)|\*){1}')
    __headerMatch = re.compile('.*solid \#000000.*')
    __tableSubPattern = re.compile('(item \d+(\.[A-Z]?)|note)', re.IGNORECASE)
    __tableTitlePattern = re.compile('^font-family:inherit;font-size:\d+pt;font-weight:bold;$')
    __titleStripChars = ''.join(list(set(string.punctuation + '= '))).replace('(', '').replace(')', '')
    __yearMatch = re.compile('(19|20)[0-9][0-9]')
    #####################
    # Constructors:
    #####################
    def __init__(self, table, corpName):
        self.__data = []
        self.__name = ''
        self.__footnotes = {}
        self.__ExtractTableName(table, corpName)
        if self.__name:
            self.__LoadData(table, corpName)
            self.__GetFootNotes(table, corpName)
    #####################
    # Properties:
    #####################
    @property
    def ColumnNames(self):
        """
        * Return names of columns in table, in list format if multiple
        tables exist, or None.
        """
        if not self.HasData:
            return []
        else:
            return list(self.__data.dtype.names)
    @property
    def ColCount(self):
        """
        * Return number of columns in table.
        """
        return len(self.ColumnNames)
    @property
    def Data(self):
        """
        * Return access to numpy arrays containing row information. 
        """
        return self.__data
    @property
    def FootNotes(self):
        """
        * Return table footnotes.
        """
        return self.__footnotes
    @property
    def HasData(self):
        """
        * Is True if data has been loaded.
        """
        if isinstance(self.__data, list):
            return False 
        else:
            return self.__data.shape[0] > 0
    @property
    def HasFootNotes(self):
        """
        * Is True if footnotes have been found for this table.
        """
        return len(self.__footnotes.keys()) > 0
    @property
    def Name(self):
        """
        * Title of table.
        """
        return self.__name
    @property
    def RowCount(self):
        """
        * Number of rows in table.
        """
        if self.HasData:
            return self.__data.shape[0]
        else:
            return 0
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
        
        if not self.HasData:
            return None
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
    def __LoadData(self, table, corpName):
        """
        * Set the table data using passed BeautifulSoup <table> tag from SEC Edgar html 
        document.
        Inputs:
        * table: Expecting BeautifulSoup <table> tag.
        """
        colNum = 0
        rows = table.find_all('tr')
        yearMatch = re.compile('(19|20)[0-9][0-9]')
        prefixMatch = re.compile('(((Year|Month|Quarter) Ended ){0,1}[A-Z]{3,9} [0-9]+,?|Fiscal)', re.IGNORECASE)
        hasLettersMatch = re.compile('[A-Z]+', re.IGNORECASE)
        dateMatch = re.compile('[A-Z]{3,9} [0-9]+', re.IGNORECASE)
        boldFontMatch = re.compile('.*font-weight:bold.*')
        stripAll = re.compile('(\(\d+\)|\(in.+\))', re.IGNORECASE)
        negMatch = re.compile('^\(\d+[,.]{0,1}\d?\)$')
        zeroMatch = re.compile('^-+$')
        hasDigitsMatch = re.compile('\d+')
        swap = {'year ended' : 'YE', 'month ended' : 'ME', 'fiscal' : 'YE', 'quarter ended' : 'QE'}
        # Get column headers for table:
        headerRows = [] 
        dataRows = []
        columns = {}
        colNames = []
        prefix = []
        # Determine which column header has maximum length:
        maxLen = 0
        boldRows = [row for row in rows if row.find('font', {'style': boldFontMatch})]
        for row in boldRows:
            tds = [td for td in row.find_all('td') if unidecode(td.text).strip(TableItem.__excludeChars).strip()]
            if len(tds) > maxLen:
                maxLen = len(tds)
        for row in rows:
            text = unidecode(row.text).strip()
            boldFont = row.find('font', {'style': boldFontMatch})
            # Determine if need to include a 'prefix' (ex: "Year Ended December 31", appears in column header encircling multiple column headers):
            if not headerRows and text and boldFont:
                if not prefix and prefixMatch.match(text):
                    text = text.strip().lower()
                    prefKey = ''
                    for key in swap.keys():
                        if key in text:
                            prefKey = swap[key]
                            break
                    if not prefKey:
                        prefKey = prefKey
                    dateStr = dateMatch.search(text)[0] if dateMatch.search(text) else ''
                    dateStr += ',' if dateStr and not dateStr.endswith(',') else ''
                    prefix = [prefKey, dateStr.strip(), ' ', '']
                elif len([td for td in row.find_all('td') if unidecode(td.text).strip(TableItem.__excludeChars).strip()]) == maxLen:
                    headerRows.append(row)
            elif text and headerRows:
                dataRows.append(row)
        # Exit data loading if no column headers were found:
        if not headerRows or not dataRows:
            return
        # Get the column names:
        cells = headerRows[0].find_all('td')
        cellCount = 0
        for cell in cells:
            text = unidecode(cell.text).strip(TableItem.__excludeChars)
            text = stripAll.sub('', text).strip()
            if not text and cellCount == 0:
                columns['Line Item'] = []
            elif text:
                if prefix and yearMatch.match(text):
                    prefix[len(prefix) - 1] = text
                    dateVal = ''
                    if prefix[1]:
                        try:
                            dateVal = datetime.strptime(''.join([prefix[1], prefix[2], prefix[3]]), '%B %d, %Y')
                            dateVal = dateVal.strftime('%m/%d/%Y')
                        except:
                            dateVal = ''
                    else:
                        dateVal = text
                    columns[''.join([prefix[0], ' ', dateVal])] = []
                else:
                    columns[text] = []
            cellCount += 1
        colNames = list(columns.keys())
        # Exit if could not find column names:
        if not colNames:
            return
        
        # Pull in row data after getting column headers:
        for row in dataRows:
            cells = row.find_all('td')
            currRowStrs = []
            for col, cell in enumerate(cells):
                text = unidecode(cell.text).strip()
                text = text if not hasLettersMatch.search(text) else stripAll.sub('', text)
                if text and hasDigitsMatch.search(text):
                    # Check for orphan ')' in adjacent cells:
                    if text.startswith('(') and not text.endswith(')') and col < len(cells) - 1 and unidecode(cells[col + 1].text).strip() == ')':
                        text += ')'
                    if negMatch.match(text):
                        currRowStrs.append('-' + text.strip('()'))
                    elif text.strip(TableItem.__excludeChars + ')'):
                        currRowStrs.append(text.strip(TableItem.__excludeChars + ')'))
                elif zeroMatch.match(text):
                    # Convert "--" to 0:
                    currRowStrs.append('0')
                elif text.strip('()') and (hasLettersMatch.search(text) or hasDigitsMatch.search(text)):
                    currRowStrs.append(text.strip())
            if currRowStrs:
                # Append blank cell values if fewer cells than number of columns for current row
                # (ex: to accomodate 'Total' columns):
                while len(currRowStrs) < len(colNames):
                    currRowStrs.append('')
                for colNum in range(0, len(colNames)):
                    data = currRowStrs[colNum]
                    columns[colNames[colNum]].append(data)
        # Store table data in numpy array:
        firstKey = list(columns.keys())[0]
        numRows = len(columns[firstKey])
        if numRows > 0:
            # Load all table values:
            values = n.array([n.asarray(columns[colName]) for colName in colNames])
            types = [col.dtype for col in values]
            dt = { 'names' : colNames, 'formats' : types }
            self.__data = n.zeros(numRows, dtype = dt)
            for col in range(0, len(colNames)):
                self.__data[colNames[col]] = values[col]

    def __ExtractTableName(self, table, corpName):
        """
        * Extract table title.
        """
        # Skip loading table if is table of contents (contains 'Items'):
        for row in table.find_all('tr'):
            text = unidecode(row.text).strip()
            if text and re.search('item \d', text, re.IGNORECASE):
                return
        tableName = []
        tag = table
        # Get to outermost tag that is child to <text> tag (contains all information for <document>):
        while tag and tag.parent.name != 'text':
            tag = tag.parent
        boldFontRE = re.compile('.*font-weight:bold.*')
        corpNameRE = re.compile(corpName, re.IGNORECASE) if corpName else None
        indexRE = re.compile('index', re.IGNORECASE)
        italicFontRE = re.compile('.*font-style:italic.*')
        noteRE = re.compile('note \d+( |-)? ', re.IGNORECASE)
        unitRE = re.compile('\(in .+\)', re.IGNORECASE)
        # Find the title of the table
        # (assume that is first div tag with bold/italic font text, that does not match with any above
        # regular expressions):
        font = tag.find_previous('font', {'style' : (boldFontRE, italicFontRE) })
        while font:
            # If landed in a table, climb out of table back to <text> tag:
            if font.parent.name != 'text':
                while font and font.parent and font.parent.name != 'text':
                    font = font.parent
                font = font.find_previous('font', {'style' : (boldFontRE, italicFontRE) })
            else:
                text = unidecode(font.text).strip() if font else ''
                if font and font.name == 'font' and text and not unitRE.match(text) and not (corpNameRE.match(text) if corpNameRE else True) and not indexRE.match(text):
                    break
                font = font.find_previous('font', {'style' : (boldFontRE, italicFontRE) })
        if font:
            # We assume table titles have bold and/or italic font, and are contiguous if split over
            # multiple <font> tags:
            isBold = True if boldFontRE.search(font['style']) else False
            isItalic = True if italicFontRE.search(font['style']) and not isBold else False
            tableName.append(unidecode(font.text).strip())
            prevSib = font.previousSibling
            while prevSib and prevSib.name == 'font' and hasattr(prevSib, 'style') and (boldFontRE.search(prevSib['style']) if isBold else italicFontRE.search(prevSib['style'])):
                tableName.append(unidecode(prevSib.text).strip())
                prevSib = prevSib.previousSibling
            tableName = ' '.join(tableName)
            tableName = noteRE.sub('', tableName)
            
            self.__name = TableItem.__tableSubPattern.sub('', tableName).strip(TableItem.__titleStripChars)
        
    def __GetFootNotes(self, table, corpName):
        """
        * Get footnotes for table (if present).
        """
        if re.search('properties', self.Name, re.IGNORECASE):
            self.Name == 'properties'
        footnote = table.nextSibling
        if str(type(footnote)) != "<class 'bs4.element.NavigableString'>" and footnote.name != 'table' and TableItem.__footerPattern.match(unidecode(footnote.text).strip()):
            footnote = footnote.nextSibling
        if not footnote.name == 'table':
            return
        # We assume that if table is immediately followed by div tag with underscores, then is a footnote table:
        num = 1
        while footnote.name == 'table':
            if TableItem.__footerTextPattern.match(unidecode(footnote.text)):
                rows = footnote.find_all('tr')
                for row in rows:
                    rowText = TableItem.__footerTextPattern.sub('', unidecode(row.text).strip())
                    if rowText:
                        self.__footnotes[num] = rowText
                        num += 1
                footnote = footnote.nextSibling
            else:
                break

class SoupTesting(object):
    @staticmethod
    def TestTableName_New(table, corpName):
        """
        * Get the name of the table.
        """
        # Skip loading table if is table of contents (contains 'Items'):
        for row in table.find_all('tr'):
            text = unidecode(row.text).strip()
            if text and re.search('item \d', text, re.IGNORECASE):
                return
        tableName = []
        tag = table
        # Get to outermost tag that is child to <text> tag (contains all information for <document>):
        while tag and tag.parent.name != 'text':
            tag = tag.parent
        boldFontRE = re.compile('.*font-weight:bold.*')
        corpNameRE = re.compile(corpName, re.IGNORECASE) if corpName else None
        indexRE = re.compile('index', re.IGNORECASE)
        italicFontRE = re.compile('.*font-style:italic.*')
        noteRE = re.compile('note \d+( |-)? ', re.IGNORECASE)
        unitRE = re.compile('\(in .+\)', re.IGNORECASE)
        # Find the title of the table
        # (assume that is first div tag with bold/italic font text, that does not match with any above
        # regular expressions):
        font = tag.find_previous('font', {'style' : (boldFontRE, italicFontRE) })
        while font:
            # If landed in a table, climb out of table back to <text> tag:
            if font.parent.name != 'text':
                while font and font.parent and font.parent.name != 'text':
                    font = font.parent
                font = font.find_previous('font', {'style' : (boldFontRE, italicFontRE) })
            else:
                text = unidecode(font.text).strip() if font else ''
                if font and font.name == 'font' and text and not unitRE.match(text) and not (corpNameRE.match(text) if corpNameRE else True) and not indexRE.match(text):
                    break
                font = font.find_previous('font', {'style' : (boldFontRE, italicFontRE) })
        if font:
            # We assume table titles have bold and/or italic font, and are contiguous if split over
            # multiple <font> tags:
            isBold = True if boldFontRE.search(font['style']) else False
            isItalic = True if italicFontRE.search(font['style']) and not isBold else False
            tableName.append(unidecode(font.text).strip())
            prevSib = font.previousSibling
            while prevSib and prevSib.name == 'font' and hasattr(prevSib, 'style') and (boldFontRE.search(prevSib['style']) if isBold else italicFontRE.search(prevSib['style'])):
                tableName.append(unidecode(prevSib.text).strip())
                prevSib = prevSib.previousSibling
            tableName = ' '.join(tableName)
            tableName = noteRE.sub('', tableName)
            
            __name = TableItem.__tableSubPattern.sub('', tableName).strip(TableItem.__titleStripChars)

    @staticmethod
    def TestLoad_New(table):
        """
        * Test loading particular table.
        """
        colNum = 0
        rows = table.find_all('tr')
        excludeChars = ''.join(list(set(string.punctuation + ' '))).replace('(', '').replace(')', '').replace('-', '')
        yearMatch = re.compile('(19|20)[0-9][0-9]')
        prefixMatch = re.compile('(((Year|Month|Quarter) Ended ){0,1}[A-Z]{3,9} [0-9]+,?|Fiscal)', re.IGNORECASE)
        hasLettersMatch = re.compile('[A-Z]+', re.IGNORECASE)
        dateMatch = re.compile('[A-Z]{3,9} [0-9]+', re.IGNORECASE)
        boldFontMatch = re.compile('.*font-weight:bold.*')
        stripAll = re.compile('(\(\d+\)|\(in.+\))', re.IGNORECASE)
        negMatch = re.compile('^\(\d+[,.]{0,1}\d?\)$')
        zeroMatch = re.compile('^-+$')
        hasDigitsMatch = re.compile('\d+')
        swap = {'year ended' : 'YE', 'month ended' : 'ME', 'fiscal' : 'YE', 'quarter ended' : 'QE'}
        # Get column headers for table:
        headerRows = [] 
        dataRows = []
        columns = {}
        colNames = []
        prefix = []
        # Determine which column header has maximum length:
        maxLen = 0
        boldRows = [row for row in rows if row.find('font', {'style': boldFontMatch})]
        for row in boldRows:
            tds = [td for td in row.find_all('td') if unidecode(td.text).strip(excludeChars).strip()]
            if len(tds) > maxLen:
                maxLen = len(tds)
        for row in rows:
            text = unidecode(row.text).strip()
            boldFont = row.find('font', {'style': boldFontMatch})
            # Determine if need to include a 'prefix' (ex: "Year Ended December 31", appears in column header encircling multiple column headers):
            if not headerRows and text and boldFont:
                if not prefix and prefixMatch.match(text):
                    text = text.strip().lower()
                    prefKey = ''
                    for key in swap.keys():
                        if key in text:
                            prefKey = swap[key]
                            break
                    if not prefKey:
                        prefKey = prefKey
                    dateStr = dateMatch.search(text)[0] if dateMatch.search(text) else ''
                    dateStr += ',' if dateStr and not dateStr.endswith(',') else ''
                    prefix = [prefKey, dateStr.strip(), ' ', '']
                elif len([td for td in row.find_all('td') if unidecode(td.text).strip(excludeChars).strip()]) == maxLen:
                    headerRows.append(row)
            elif text and headerRows:
                dataRows.append(row)
        # Exit data loading if no column headers were found:
        if not headerRows or not dataRows:
            return
        # Get the column names:
        cells = headerRows[0].find_all('td')
        cellCount = 0
        for cell in cells:
            text = unidecode(cell.text).strip(excludeChars)
            text = stripAll.sub('', text).strip()
            if not text and cellCount == 0:
                columns['Line Item'] = []
            elif text:
                if prefix and yearMatch.match(text):
                    prefix[len(prefix) - 1] = text
                    dateVal = ''
                    if prefix[1]:
                        try:
                            dateVal = datetime.strptime(''.join([prefix[1], prefix[2], prefix[3]]), '%B %d, %Y')
                            dateVal = dateVal.strftime('%m/%d/%Y')
                        except:
                            dateVal = ''
                    else:
                        dateVal = text
                    columns[''.join([prefix[0], ' ', dateVal])] = []
                else:
                    columns[text] = []
            cellCount += 1
        colNames = list(columns.keys())
        # Exit if could not find column names:
        if not colNames:
            return
        
        # Pull in row data after getting column headers:
        for row in dataRows:
            cells = row.find_all('td')
            currRowStrs = []
            for col, cell in enumerate(cells):
                text = unidecode(cell.text).strip()
                text = text if not hasLettersMatch.search(text) else stripAll.sub('', text)
                if text and hasDigitsMatch.search(text):
                    # Check for orphan ')' in adjacent cells:
                    if text.startswith('(') and not text.endswith(')') and col < len(cells) - 1 and unidecode(cells[col + 1].text).strip() == ')':
                        text += ')'
                    if negMatch.match(text):
                        currRowStrs.append('-' + text.strip('()'))
                    elif text.strip(excludeChars + ')'):
                        currRowStrs.append(text.strip(excludeChars + ')'))
                elif zeroMatch.match(text):
                    # Convert "--" to 0:
                    currRowStrs.append('0')
                elif text.strip('()') and (hasLettersMatch.search(text) or hasDigitsMatch.search(text)):
                    currRowStrs.append(text.strip())
            if currRowStrs:
                # Append blank cell values if fewer cells than number of columns for current row
                # (ex: to accomodate 'Total' columns):
                while len(currRowStrs) < len(colNames):
                    currRowStrs.append('')
                for colNum in range(0, len(colNames)):
                    data = currRowStrs[colNum]
                    columns[colNames[colNum]].append(data)
        # Store table data in numpy array:
        firstKey = list(columns.keys())[0]
        numRows = len(columns[firstKey])
        if numRows > 0:
            # Load all table values:
            values = n.array([n.asarray(columns[colName]) for colName in colNames])
            types = [col.dtype for col in values]
            dt = { 'names' : colNames, 'formats' : types }
            self.__data = n.zeros(numRows, dtype = dt)
            for col in range(0, len(colNames)):
                self.__data[colNames[col]] = values[col]


    @staticmethod
    def PrintTableHTML(tables, filePath):
        """
        * Print all table HTML for all tables in passed list.
        """
        # Exit immediately if file already present:
        if os.path.exists(filePath) or not tables:
            return
        enclosingFolder = filePath[0:filePath.rfind('\\')]
        if not os.path.exists(enclosingFolder):
            os.mkdir(enclosingFolder)
        if '.html' not in filePath or '.' not in filePath:
            filePath = ''.join([filePath[0:filePath.find('.')], '.html'])
        with open(filePath, 'w', newline = '') as f:
            for table in tables:
                html = table.prettify()
                for i in range(0, len(html)):
                    try:
                        f.write(html[i])
                    except:
                        pass

    @staticmethod
    def PrintTableAttributes(tables, filePath):
        """
        * Print all table names, rows and columns for all tables in passed list.
        """
        # Exit immediately if file already present:
        if os.path.exists(filePath):
            return
        enclosingFolder = filePath[0:filePath.rfind('\\')]
        if not os.path.exists(enclosingFolder):
            os.mkdir(enclosingFolder)
        if '.csv' not in filePath or '.' not in filePath:
            filePath = ''.join([filePath[0:filePath.find('.')], '.csv']) 
        with open(filePath, 'w', newline = '') as f:
            writer = csv.writer(f)
            # Write headers:
            writer.writerow(['Name:', 'Rows:', 'Columns:', 'ColumnNames:'])
            for table in tables:
                if table.HasData:
                    rows, cols, columnNames = (table.Data.shape[0], len(table.ColumnNames), ' '.join(table.ColumnNames))
                else:
                    rows, cols, columnNames = (0, 0, ' ')
                writer.writerow([table.Name, rows, cols, columnNames])

    @staticmethod
    def PrintTableAttributes(doc, folderPath):
        """
        * Print all table names, number of rows, columns for all tables in passed CorporateFiling object.
        """
        # Exit immediately if file already present:
        if not os.path.exists(folderPath):
            os.mkdir(folderPath)
        path = ''.join([folderPath, doc.Ticker, '_TableAttributes.csv'])
        # Exit immediately if file already exists:
        if os.path.exists(path):
            return
        with open(path, 'w', newline = '') as f:
            writer = csv.writer(f)
            writer.writerow(['SubDoc:', 'Name:', 'Rows:', 'ColumnCount:', 'ColumnNames:'])
            if not doc.SubDocuments:
                return
            for subDoc in doc.SubDocuments.keys():
                if doc.SubDocuments[subDoc].Tables:
                    printDoc = False
                    for tableName in doc.SubDocuments[subDoc].Tables.keys():
                        table = doc.SubDocuments[subDoc].Tables[tableName]
                        currRow = []
                        if not printDoc:
                            currRow.append(subDoc)
                            printDoc = True
                        else:
                            currRow.append('')
                        currRow.append(tableName)
                        if table.HasData:
                            rows, cols, colNames = (table.Data.shape[0], len(table.ColumnNames), ', '.join(table.ColumnNames)) 
                        else:
                            rows, cols, colNames = (0, 0, '')
                        currRow.append(str(rows))
                        currRow.append(str(cols))
                        currRow.append(colNames)
                        writer.writerow(currRow)

    @staticmethod
    def PrintUniqueTagsWithCounts(soup, filePath, tagName = None):
        """
        * Print all unique tags that occur in xml object, with frequencies, to file at file path.
        Required Arguments:
        * soup: Expecting BeautifulSoup object.
        * filePath: Expecting string path to output file with tags and unique counts.
        Optional Arguments:
        * tagName: Expecting string or list/tuple of strings to denote which tag(s) to perform counts upon.
        """
        errMsgs = []
        if not isinstance(soup, Soup):
            errMsgs.append('soup must be a BeautifulSoup object.')
        if not isinstance(filePath, str):
            errMsgs.append('filePath must be a string.')
        if not tagName is None and not isinstance(tagName,(str,tuple,list)):
            errMsgs.append('tagName must be a string or container if specified.')

        if len(errMsgs) > 0:
            raise Exception('\n'.join(errMsgs))

        # Exit immediately if file already present:
        if os.path.exists(filePath):
            return

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
        with open(filePath, 'w', newline = '\n') as f:
            writer = csv.writer(f)
            writer.writerow(['Tag:', 'Freq:'])
            for key in uniqueElems.keys():
                writer.writerow([key, uniqueElems[key]])

    @staticmethod
    def PrintTagsHTML(tags, path, prettify = False):
        """
        * Print all tags to file.
        """
        # Exit immediately if file already exists:
        if os.path.exists(path):
            return 
        with open(path, 'w') as f:
            for tag in tags:
                if prettify:
                    html = tag.prettify()
                else:
                    html = str(tag)
                for i in range(0, len(html)):
                    try:
                        f.write(html[i])
                    except:
                        pass
    @staticmethod
    def WriteSoupToFile(soup, path, prettify = False):
        """
        * Write soup object to local file.
        """
        path = path.replace('-', '_')
        if '.html' not in path:
            path = path[0:path.rfind('.')] + '.html'
        elif '.' not in path:
            path = path + '.html'
        # Exit immediately if file already present at path:
        if os.path.exists(path):
            return
        enclosingFolder = path[0:path.rfind('\\')]
        if not os.path.exists(enclosingFolder):
            os.mkdir(enclosingFolder)
        if prettify:
            html = soup.prettify()
        else:
            html = str(soup)
        with open(path,"w") as f:
            for i in range(0, len(html)):
                try:
                    f.write(html[i])
                except Exception:
                    pass

class SECEdgarDistinctInfoQueries(object):
    """
    * Object loads information regarding exhibit information/descriptions
    and other 
    """
    def __init__(self, db, targetSchema):
        pass

