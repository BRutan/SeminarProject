#################################################
# DataBase.py
#################################################
# Description:
# * 

import csv
import re
import os
import mysql.connector as msc
from mysql.connector import errorcode

__all__ = ['MYSQLDatabase']

class MYSQLDatabase(object):
    """
    * Object wraps functionality of mysql connector.
    """
    #######################################
    # Static Variables:
    #######################################
    # Map appropriate wrapping in insert query when dealing with these variable types
    __TypeWrapMap = { "varchar" : "\"", "text" : "\"", "char" : "\"", "blob" : "\"", "binary" : "\"", "varbinary" : "\"",
                     "date" : "\"", "time" : "\"", "datetime" : "\"", "timestamp" : "\""}
    __InvalidOrds = [43, 64]
    # TODO: Determine invalid characters in text strings: https://dev.mysql.com/doc/refman/8.0/en/string-literals.html

    #######################################
    # Constructors/Destructors:
    #######################################
    def __init__(self, userName, password, hostName, schema):
        """
        * Connect to schema at passed hostName, using provided credentials.
        Get all existing tables, with column attributes, in the schema.
        """
        errMsgs = []
        # Validate all input parameters:
        if not isinstance(userName, str):
            errMsgs.append("userName must be a string.")
        if not isinstance(password, str):
            errMsgs.append("password must be a string.")
        if not isinstance(hostName, str):
            errMsgs.append("hostName must be a string.")
        if not isinstance(schema, str):
            errMsgs.append("schema must be a string.")
        # Raise exception if issues occur:
        if len(errMsgs) > 0:
            raise Exception('\n'.join(errMsgs))
        self.__user = userName
        self.__pw = password
        self.__host = hostName
        # Attempt to initially open connection:
        connect = self.__Reconnect(schema)
        # Map {table -> {columns -> typeInfo}}:
        self.__tables = {}
        # Get all existing tables in the schema:
        cursor = connect.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        for table in tables:
            table = table[0].lower()
            # Pull column information from table:
            cursor.execute("SHOW COLUMNS FROM " + table)
            columns = cursor.fetchall()
            columnEntries = {}
            for column in columns:
                columnEntries[column[0].lower()] = self.__ColToDictKey(column)
            self.__tables[table] = columnEntries
        
        # Create map for existing schemas:
        self.__schemas = { }
        # Get all existing schemas in the database:
        cursor.execute("SHOW DATABASES")
        dbs = cursor.fetchall()
        for db in dbs:
            self.__schemas[db[0].lower()] = True
        
        cursor.close()
        # Set the active schema that gets switched whenever a different schema is queried:
        self.__activeSchema = schema

    def __del__(self):
        """
        * Close the connection to the database.
        """
        try:
            connect.close()
        except:
            pass

    #######################################
    # Mutators:
    #######################################
    def CreateSchema(self, schema):
        """
        * Create schema if does not exist for current mysql instance.
        """
        if not isinstance(schema, str):
            raise Exception("schema must be a string.")
        elif not schema:
            raise Exception("Please provide schema name.")

        schema = schema.strip()
        
        connect = self.__Reconnect(schema)
        cursor = connect.cursor()
        try:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS " + schema + ";")
            schema  = schema.lower()
            self.__schemas[schema] = 0
            cursor.close()
        except Exception:
            pass

    def CreateTable(self, tableName, columns, schema = None):
        """
        * Create table with name for given database, with columns and types specified as strings
        in the columnNameToType map. If table exists then will skip creation. 
        Inputs:
        * tableName: String to name table.
        * schema: String with name of database (if omitted uses ActiveSchema).
        * columns: Dictionary mapping { ColumnName -> (Type [Str], IsPKey [Bool], FKeyRef [Str]) }.
        If IsPKey is true then column will be primary key of table. If FKeyRef is not 
        blank then column will be foreign key referencing passed table (as RefTable(Column).
        """
        # Validate function inputs:
        if not schema:
            schema = self.ActiveSchema
        errMsgs = self.__CheckParams(tableName, schema, columns)
        if len(errMsgs) > 0:
            raise Exception('\n'.join(errMsgs))

        # Ensure that schema exists already and table does not exist yet:
        tableName = tableName.lower()
        schema = schema.lower()
        # Ensure that schema exists in database, table does not exist yet:
        errMsgs = self.__CheckDBObjectsExist(tableName, schema, False)
        if len(errMsgs) > 0:
            raise Exception('\n'.join(errMsgs))

        # Ensure that at least one column is specified:
        numColumns = columns.keys()
        if numColumns == 0:
            raise Exception("At least one column must be specified.")
    
        # Reopen the connection:
        connect = self.__Reconnect(schema)
        cursor = connect.cursor()
        
        fKeyStrings = []
        variables = []
        pKeyStr = ""
        createTableStrings = ["USE ", schema, "; CREATE TABLE "]
        createTableStrings.append(tableName)
        createTableStrings.append("(")
        ##############
        # Append all column names, type declaration strings:
        ##############
        for columnName in columns.keys():
            variables.append(columnName + " " + columns[columnName][0])
            pKey = columns[columnName][1]
            # Check if column is the primary key, skip if one was already specified:
            if pKey and not pKeyStr:
                pKeyStr = "PRIMARY KEY(" + columnName + ")"
            # Check if column should use foreign key relationship with another table:
            if columns[columnName][2]:
                fKeyStrings.append("FOREIGN KEY(" + columnName + ") REFERENCES " + columns[columnName][2])
        if pKeyStr:
            variables.append(pKeyStr)
        if len(fKeyStrings) > 0:
            variables.append(','.join(fKeyStrings))
        createTableStrings.append(','.join(variables))
        createTableStrings.append(");")

        ##############
        # Execute table creation command:
        ##############
        cursor.execute(''.join(createTableStrings))
        tableName = tableName.lower()
        self.__tables[tableName] = columns
        cursor.close()
        
    def ExecuteQuery(self, query, schema = None, getResults = False, shouldCommit = False):
        """
        * Execute query on passed table for given schema.
        Input:
        * schema: Expecting string naming existing schema in mysql instance.
        * query: Expecting string with query for schema.
        """
        if not schema:
            schema = self.ActiveSchema
        query = query.lower()
        connect = self.__Reconnect(schema)
        cursor = connect.cursor()
        cursor.execute(query)
        # Commit transaction if requested:
        if shouldCommit:
            connect.commit()
        # Return results if expecting any.
        if getResults:
            # Return nothing if a select statement wasn't entered:
            if "select" not in query:
                return None
            # Output results as dictionary mapping column name to value.
            # Extract table name and selected columns from select stmt:
            tokens = str.split(query, ' ')
            fromIndex = tokens.index("from")
            potTableTokens = tokens[fromIndex + 1: len(tokens)]
            potColumnTokens = tokens[tokens.index("select") + 1: fromIndex]
            tableName = ''
            output = {}
            for token in potTableTokens:
                if tableName:
                    break
                for key in self.__tables.keys():
                    if token in str(key):
                        tableName = key
                        break

            for token in potColumnTokens:
                if token in self.__tables[tableName].keys():
                    output[token] = []
            
            # Output data as dictionary mapping column name to data:
            rawResults = cursor.fetchall() 
            for result in rawResults:
                columnCounter = 0
                for column in output.keys():
                    output[column].append(result[columnCounter])
                    columnCounter += 1

            return output

    def InsertValues(self, tableName, columns, schema = None):
        """
        * Insert all values into the create database. 
        Inputs:
        * tableName: Expecting a string that refers to target table to insert data.
        * columns: Expecting { ColumnName -> Values[] } map. Len(Values) must be
        uniform for all columns.
        """
        # Validate function parameters:
        if not schema:
            schema = self.ActiveSchema
        errMsgs = self.__CheckParams(tableName, schema, columns)
        if len(errMsgs) > 0:
            raise Exception('\n'.join(errMsgs))
        # Raise exception if schema and table do not exist:
        errMsgs = self.__CheckDBObjectsExist(tableName, schema)
        if len(errMsgs) > 0:
            raise Exception('\n'.join(errMsgs))

        # Reopen the connection:
        connect = self.__Reconnect(schema)
        cursor = connect.cursor()

        insertVals = []
        # Get total number of rows (must be uniform for all columns):
        firstColName = list(columns.keys())[0]
        numRows = len(columns[firstColName])
        # Exit if no data was provided for columns:
        if numRows == 9:
            return
        colString = "(" + ','.join(list(columns.keys())) + ")"
        tableInsertQuery = ["INSERT INTO " , tableName,  " ", colString, " VALUES "]
        # Determine appropriate wrapping for data given column type:
        wrapMap = {}
        for column in columns.keys():
            wrapMap[column] = self.__GetDataWrap(self.__tables[tableName][column][0])
            
        ###########################
        # Generate full insert query string using passed data:
        ###########################
        currRow = 0
        rows = []
        while currRow < numRows:
            currRowValue = []
            for column in columns.keys():
                wrap = wrapMap[column]
                # Convert values to list if not already:
                if not isinstance(columns[column], list):
                    columns[column] = list(columns[column])
                currRowValue.append(wrap + str(columns[column][currRow]) + wrap)
            rows.append("(" + ",".join(currRowValue) + ")")
            currRow += 1
        tableInsertQuery.append(','.join(rows))
        cursor.execute(''.join(tableInsertQuery))
        connect.commit()
        cursor.close()

    def PrintSelectToCSV(self, query, csvPath, schema = None):
        """
        * Print select statement to specified CSV.
        """
        errMsgs = []
        if not schema:
            schema = self.ActiveSchema
        if os.path.exists(csvPath):
            errMsgs.append("csv at csvPath already exists.")
        if "select" not in query or "SELECT" not in query:
            errMsgs.append("query must be a select statement.")
        if schema not in self.__schemas.keys():
            errMsgs.append("schema does not exist in database.")

        if len(errMsgs) > 0:
            msg = '\n'.join(errMsgs)
            raise Exception(msg)

        # Pull data from select statement:
        data = self.ExecuteQuery(schema, query, True)
        for column in data.keys():
            pass

    def TableExists(self, tableName):
        """
        * Return True if table exists in current schema.
        """
        return tableName.lower() in self.__tables.keys()

    #######################
    # Accessors:
    #######################
    @property
    def ActiveSchema(self):
        """
        * Return the schema most recently connected to.
        """
        return self.__activeSchema
    @property
    def Tables(self):
        """
        * Return copy of tables in database (as dictionary mapping of tables to column attributes).
        """
        return self.__tables.copy()
    @property
    def Schemas(self):
        """
        * Return copy of schemas in the database (as list containing all schema names).
        """
        return list(self.__schemas.keys()).copy()
    @ActiveSchema.setter
    def ActiveSchema(self, schema):
        pass
    #######################
    # Helper Functions:
    #######################
    def __CheckParams(self, tableName, schema, columns):
        """
        * Ensure that parameters to Create and Insert functions are valid.
        Returns list containing all error strings.
        """
        errMsgs = []
        if not isinstance(tableName, str):
            errMsgs.append("tableName must be a string.")
        if not isinstance(schema, str):
            errMsgs.append("schema must be a string.")
        if not isinstance(columns, dict):
            errMsgs.append("columns must be a dictionary with appropriate mapping.")
        return errMsgs

    def __CleanString(string):
        """
        * Clean string of all non-unicode characters.
        """
        pass

    def __CheckDBObjectsExist(self, tableName, schema, tableExists = True):
        """
        * Ensure that schema and table exists.
        Inputs:
        * tableName: Expecting string indicating table in schema.
        * schema: Expecting string indicating schema in current MySQL instance.
        * tableExists: Put True if want to check that table exists, otherwise put false.
        Output:
        * Returns list containing all error strings.
        """
        tableName = tableName.lower()
        schema = schema.lower()
        errMsgs = []
        if schema not in self.__schemas.keys():
            errMsgs.append("Schema does not exist yet in database (please create with CreateSchema()).")
        # Ensure that schema exists, passed table does/does not exist (depending on tableExists):
        if tableExists and not tableName in self.__tables.keys():
            pass
            #errMsgs.append("Table already exists in database.")
        elif not tableExists and tableName in self.__tables.keys():
            errMsgs.append("Table already exists in schema.")

        return errMsgs

    def __GetDataWrap(self, columnType):
        """
        * Return wrap for data for INSERT queries.
        """
        # Remove quantity specification from columnType string:
        parenthIndex = columnType.find('(')
        parenthIndex = len(columnType) if parenthIndex == -1 else parenthIndex
        columnType = columnType[0 : parenthIndex]
        if columnType in MYSQLDatabase.__TypeWrapMap.keys():
            return MYSQLDatabase.__TypeWrapMap[columnType]
        else:
            return ''

    def __Reconnect(self, schema):
        """
        * Return connection to mysql instance using stored parameters, at schema.
        """
        schema = schema.lower()
        return msc.connect(user=self.__user, password=self.__pw, host = self.__host, database = schema)

    def __ColToDictKey(self, columnList):
        """
        * Convert list containing column information to a dictionary key
        suitable for __tables object mapping.
        """
        colEntry = [columnList[1]]
        pKey = True if columnList[3] == 'PRI' else False
        fKey = True if columnList[3] == 'MUL' else False
        colEntry.append(pKey)
        if not pKey and columnList[2] == 'NO':
            colEntry[0] += ' NOT NULL'

        return colEntry