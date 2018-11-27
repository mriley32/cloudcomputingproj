import sys
import json
import pymysql

from datetime import datetime, date, time, timedelta
import dateutil.parser as parser

import multiprocessing as mp

from iexfinance import get_historical_data
from iexfinance import Stock

#Load credentials and config
json_data=open("config.json").read()
config = json.loads(json_data)

#DB Credentials
USERNAME = config['RDS']['USERNAME']
PASSWORD = config['RDS']['PASSWORD']
DB_NAME = config['RDS']['DB_NAME']
HOST = config['RDS']['HOST']

#Read parameters to set start and stop time
if len(sys.argv) == 2:
    dataBegin = datetime.strptime(sys.argv[1],'%Y-%m-%d').date()
    dataEnd = datetime.strptime(sys.argv[1],'%Y-%m-%d').date()  
elif len(sys.argv) > 2:
    dataBegin = datetime.strptime(sys.argv[1],'%Y-%m-%d').date()
    dataEnd = datetime.strptime(sys.argv[2],'%Y-%m-%d').date()
else:
    dataBegin = date.today()
    dataEnd = date.today()

#Read in list of industries
industries = {}
data=json.loads(open('stock_codes.json').read())
for industry in data:
    for stock in data[industry]:
        industries[stock] = industry

#Get stock data
data = get_historical_data(industries.keys(),dataBegin,dataEnd, output_format='json')

def processData(stock):
    conn = pymysql.connect (host = HOST,
                user = USERNAME,
                passwd = PASSWORD,
                db = DB_NAME, 
		        port = 3306, charset='utf8mb4', use_unicode=True)
    
    cursor = conn.cursor()

    for d in data[stock].keys():
        statement = 'SELECT * FROM financial_data WHERE ticker_symbol="%s" AND date_time="%s"' % (stock, d)
        cursor.execute(statement)
        if cursor.fetchone() == None:
            statement = 'INSERT INTO financial_data (ticker_symbol, industry, stock_price, date_time) VALUES ("%s", "%s", %f, "%s")' % (stock, industries[stock], data[stock][d]['close'], d)
            print statement
            cursor.execute(statement)
        else:
            print '%s @ %s is already in table.' % (stock, d)
    
    conn.commit()
    cursor.close()
    conn.close() 

stock_pool = mp.Pool(processes=25)

def main():
    #print industries.keys()[0]
    #processData(industries.keys()[0])

    stock_pool.map_async(processData, industries.keys()).get(9999999)

    stock_pool.close()
    stock_pool.join()

if __name__ == '__main__':
    main()