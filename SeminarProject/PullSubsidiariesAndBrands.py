#################################
# PullSubsidiariesAndBrands.py
#################################
# Description:
# * Pull all subsidiaries and brands
# for corporation.

from DataBase import MYSQLDatabase
from argparser import ArgumentParser
from SeminarProject import SeminarProject
import os

def IsDate(string):
    try:
        return datetime.strptime(string, '%Y-%m-%d')
    except:
        raise argparse.ArgumentTypeError

def PullSubsidiariesAndBrands():
    parser = ArgumentParser(prog="PullSubsidiariesAndBrands")
    # Required arguments:
    parser.add_argument("ticker", type = str, help="Company ticker to pull subsidiary information.")
    parser.add_argument('username', type = str, help="Username for MYSQL instance.")
    parser.add_argument('pw', type = str, help="Password to MYSQL instance.")
    parser.add_argument('schema', type = str, help="Name of schema containing all tables.")
    parser.add_argument('startdate', type = IsDate, help="Begin date for pulling all data (YYYY-MM-DD).")
    parser.add_argument('enddate', type = IsDate, help="End date for pulling all data (YYYY-MM-DD).")
    parser.add_argument('outputfile', type = str, help="Output file for subsidiary and brand data.")
    # Optional args:
    parser.add_argument('--host', type = str, help="IP Address of MYSQL instance.")

    args = parser.parse_args()
    errs = []

    if os.path.exists(args.outputfile):
        errs.append('outputfile path already exists.')

    if not args.host:
        host = "127.0.0.1"
    else:
        host = args.host.strip()
    try:
        db = DataBase.MYSQLDatabase(args.username, args.pw, host, args.schema)
    except:
        errs.append('Could not connect to MYSQL database instance.')

    if errs:
        raise Exception('\n'.join(errs))

    startDate = args.startdate
    endDate = args.enddate

    seminar = SeminarProject(startDate, endDate, db)

    # Perform key steps for single ticker if was specified, else perform for all tickers listed in XLY_ALL_Holdings.csv:
    if args.ticker:
        seminar.CreateTables(args.ticker)
        seminar.GetSubsidiaries(args.ticker)
        seminar.GetBrands(args.ticker)

    






if __name__ == "__main__":
    PullSubsidiariesAndBrands()
