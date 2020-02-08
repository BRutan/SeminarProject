#################################
# PullSubsidiariesAndBrands.py
#################################
# Description:
# * Pull all subsidiaries and brands
# for corporation.

from argparser import ArgumentParser

from TargetedWebScraping import BrandQuery

def PullSubsidiariesAndBrands():
    parser = ArgumentParser(prog="PullSubsidiariesAndBrands")
    # Required arguments:
    parser.add_argument("ticker", type = str, help="Company ticker to pull subsidiary information.")
    parser.add_argument('username', type = str, help="Username for MYSQL instance.")
    parser.add_argument('pw', type = str, help="Password to MYSQL instance.")
    parser.add_argument('schema', type = str, help="Name of schema containing all tables.")
    parser.add_argument('startdate', type = str, help="Begin date for pulling all data.")
    parser.add_argument('enddate', type = str, help="End date for pulling all data.")
    parser.add_argument('outputfile', type = str, help="Output file for subsidiary and brand data.")

    args = parser.parse_args()

    






if __name__ == "__main__":
    PullSubsidiariesAndBrands()
