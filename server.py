from flask import Flask
from flask import request, render_template, jsonify
import urllib2
import pymysql
import json
import boto3

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

@app.route('/', methods = ['GET'])
def home_page():

	return render_template('index.html', handles = twitter_handles['handles'])


# @app.route('/R2.png', methods = ['GET'])
# def plotR2():
# 	companies = result.keys()
# 	R2 = [result[i].R2 for i in companies]

# 	fig, ax = plt.subplots()
# 	ind = np.arange(1, len(companies) + 1)

# 	plt.bar(ind, R2)

# 	ax.set_xticks(ind)
# 	ax.set_xticklabels(companies)
# 	ax.set_ylim([0, 100])
# 	ax.set_ylabel('R Squared Value')
# 	ax.set_title('Prediction Accuracy (R Squared)')

# 	return fig

if __name__ == '__main__':
    app.debug=True
    app.run(host='0.0.0.0', port=5000)