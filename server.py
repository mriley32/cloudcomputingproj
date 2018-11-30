from flask import Flask
from flask import request, render_template, jsonify
import urllib2
import pymysql
import json

app = Flask(__name__)
from datetime import date
today = date.today()

#Load credentials and config
json_data=open("config.json").read()
config = json.loads(json_data)

#DB Credentials
USERNAME = config['RDS']['USERNAME']
PASSWORD = config['RDS']['PASSWORD']
DB_NAME = config['RDS']['DB_NAME']
HOST = config['RDS']['HOST']


telcom = [
    	"VZ",
    	"T",
        "CHL",
        "TMUS",
        "VOD"
    ]

consumer = [
        "PG",
        "KO",
        "PEP",
        "BUD",
        "WMT"
    ]

technology = [
        "GOOGL",
        "FB",
        "IBM",
        "BABA",
        "INTC",
        "CSCO",
        "AAPL",
        "NFLX",
        "TSLA"
	]

energy = [
        "CVX",
        "BP",
        "DUK",
        "BTU",
        "EC",
        "XOM"
    ]

finance = [
        "HSBC",
        "BAC",
        "JPM",
        "BRK.A",
        "BRK.B",
        "AIG",
        "WFC",
        "CB"
    ]
industrials = [
		"BA",
		"MMM",
		"HON"
	]

json_data = open('stock_codes.json').read()
company_data=json.loads(json_data)
current_time = date.today()

twitter_handles_data = open('twitter_handles.json').read()
twitter_handles = json.loads(twitter_handles_data)


def retreiveData(key):

	conn = pymysql.connect (host = HOST, user = USERNAME,passwd = PASSWORD,db = DB_NAME, port = 3306, charset='utf8mb4', use_unicode=True)	 

	cursor = conn.cursor()

    #statement = 'SELECT * FROM financial_data WHERE ticker_symbol="%s" AND date_time="%s"' % (stock, d)
	statement = 'SELECT * FROM financial_data'
	cursor.execute(statement)
	result = cursor.fetchall()

	companies = company_data[key]
	current_price = []
	prediction = [] ##fill in later
	key_handle = []

	for code in company_data[key]:
		code = code.encode('utf-8')
		stock = Stock(code)
		current_price.append(stock.get_price())

	cursor.close()
	conn.close()

	#need to convert lists into tuples for easy indexing in jinja
	zip(companies)
	zip(current_price)
	zip(prediction)
	zip(key_handle)


	table_data = {"companies": companies,"current_price":current_price,"prediction":prediction,"key_handle":key_handle}

	return table_data


@app.route('/', methods = ['GET'])
def home_page():

	return render_template('index.html', handles = twitter_handles['handles'])


@app.route('/technology',methods=['GET'])
def tech_page():
	table = retreiveData("Technology")
	return render_template('predictions.html',tech = table, companies = company_data["Technology"])


@app.route('/finance', methods = ['GET'])
def finance_page():
	table = retreiveData("Financial")
	return render_template('finance.html', tech = table, companies = company_data["Financial"])


@app.route('/energy',methods = ['GET'])
def energy_page():
	table = retreiveData("Energy")
	return render_template('energy.html', tech = table,companies = company_data["Energy"])


@app.route('/consumer',methods = ['GET'])
def consumer_page():
	table = retreiveData("Consumer")
	return render_template('consumer.html', tech = table, comapnies = company_data["Consumer"])

@app.route('/industrials',methods = ['GET'])
def industrials_page():
	table = retreiveData("Industrials")
	return render_template('industrials.html', tech = table, comapnies = company_data["Industrials"])

@app.route('/telecom',methods = ['GET'])
def telecom_page():
	table = retreiveData("Telecommunication")
	return render_template('telecom.html', tech = table, comapnies = company_data["Telecommunication"])

@app.route('/healthcare', methods = ['GET'])
def healthcare_page():
	table = retreiveData("Healthcare")
	return render_template('healthcare.html', tech = table, comapnies = company_data["Healthcare"])


if __name__ == '__main__':
    app.debug=True
    app.run(host='0.0.0.0', port=5000)