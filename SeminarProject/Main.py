#################################
# Main.py
#################################
# Description:
# * Perform all key steps in the 

import DataBase
from datetime import datetime
from SeminarProject import SeminarProject
    
if __name__ == '__main__':
    #testPath = 'D:\\Git Repos\\SeminarProject\\SeminarProject\\SeminarProject\\Notes\\TableNames\\Company HTML Tables Raw\\AZO_RawHTML.html'
    #soup = Soup(open(testPath, 'w'), 'lxml')
    #tables = soup.find_all('table')
    #for table in tables:
    #    SoupTesting.TestLoad_New(table)
    tickerPath = "D:\\Git Repos\\SeminarProject\\SeminarProject\\SeminarProject\\XLY_All_Holdings.csv"
    db = DataBase.MYSQLDatabase("root", "Correlation$", "127.0.0.1", "Research_Seminar_Project")
    startDate = datetime.strptime('2019-01-01', '%Y-%m-%d')
    endDate = datetime.strptime('2020-01-01', '%Y-%m-%d')
    seminar = SeminarProject(startDate, endDate, tickerPath, db)
    seminar.ExecuteAll()
