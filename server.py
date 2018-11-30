from flask import Flask
from flask import request, render_template, jsonify
import urllib2
import pymysql
import json
import boto3
import matplotlib.pyplot as plt
import numpy as np

app = Flask(__name__)
from datetime import date
today = date.today()

#Load credentials and config
json_data=open("config.json").read()
config = json.loads(json_data)

AWS_KEY= config['AWS']['AWS_KEY']
AWS_SECRET= config['AWS']['AWS_SECRET']
REGION=config['AWS']['REGION']
BUCKET = config['S3']['Bucket']

s3 = boto3.client('s3', aws_access_key_id=AWS_KEY,
                            aws_secret_access_key=AWS_SECRET)
                           
get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))

objs = s3.list_objects_v2(Bucket=BUCKET)['Contents']
last_added = [obj['Key'] for obj in sorted(objs, key=get_last_modified)][0]

response = s3.get_object(Bucket=BUCKET,
                         Key=last_added)

result = json.loads(response['Body'].read())
companies_keys = result.keys()



@app.route('/', methods = ['GET'])
def home_page():
	return render_template('index.html', tableData = result, companies = companies_keys )


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

#@app.route('/R2.png', methods = ['GET'])
#def plotR2():
	#companies = result.keys()
	#R2 = [result[i].R2 for i in companies]

	#fig, ax = plt.subplots()
	#ind = np.arange(1, len(companies) + 1)

	#plt.bar(ind, R2)

	#ax.set_xticks(ind)
	#ax.set_xticklabels(companies)
	#ax.set_ylim([0, 100])
	#ax.set_ylabel('R Squared Value')
	#ax.set_title('Prediction Accuracy (R Squared)')

	#return fig

if __name__ == '__main__':
    app.debug=True
    app.run(host='0.0.0.0', port=5000)