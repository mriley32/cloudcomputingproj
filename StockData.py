from iexfinance import get_historical_data
from iexfinance import Stock
import datetime

#You should input the date you would like to see changed from
#For example, if you input Feb 2, this will tell you how much the high on Feb 3 - high on Feb 2 is

#You must pass in a datetiem object for the date
def getPriceChangeforDate(stockName,date):
	nextdate = date
	nextdate += datetime.timedelta(days=1)
	#we need the previous date so it will return data for this date and the next one
	prevdate = date - datetime.timedelta(days=1)
	print(prevdate)
	print(nextdate)
	mydata = get_historical_data(stockName,prevdate,nextdate)
	print(len(mydata[stockName]))
	while(len(mydata[stockName]) < 2):
		prevdate = prevdate - datetime.timedelta(days=1)
		mydata  = get_historical_data(stockName,prevdate,nextdate)
	print(mydata)
	#I could implement change here, but for now we'll just return data
	return mydata

def getPriceToday(stockName):
	stock = Stock(stockName)
	return stock.get_price()

name = "AMZN"
start = datetime.datetime(2018,1,1)
print(start)
answer = getPriceChangeforDate(name,start)
print(getPriceToday(name))