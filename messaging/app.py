import os
import time
import json
import subprocess
import swiftclient.client
from flask import Flask, jsonify
from tasks import calcPro
from subprocess import call
from celery import Celery, subtask, group

app = Flask(__name__)

def summering():
	startTime = time.time()
	pronouns = ['han', 'hon', 'den', 'det', 'denna', 'denne', 'hen', 'time']
	result = [0,0,0,0,0,0,0]

	parse_list = [calcPro.s('tweets_{}.txt'.format(x)) for x in xrange(0,20)]
	job = group(parse_list)

	calc = job.apply_async()

	while calc.ready() == False:
		print "sleeping.."
		time.sleep(2)

	antal = calc.get()

	endTime = time.time()
	totTime = endTime - startTime

	for i in range(len(result)):
		for x in range(len(antal)):
			result[i] = result[i] + antal[x][i]
	result.append(totTime)

	data = {}
	for x in range(len(result)):
		#print pronouns[x] + ": " + str(result[x])
		data[pronouns[x]] = result[x]
	json_data = json.dumps(data)
	return json_data

@app.route('/get', methods=['GET'])
def downloadTweets():
	for i in range(0,20):
		objName = 'tweets_' + str(i) + '.txt'
		objPath = 'curl -O http://smog.uppmax.uu.se:8080/swift/v1/tweets/'
		objNamePath = objPath + objName
		print "getting object " + objName + "..."
		subprocess.call(objNamePath, shell=True)
		print "object downloaded"

	json_data = summering()
	return json_data, 200

@app.route('/run', methods=['GET'])
def runSummering():
	return summering(), 200


if __name__ == '__main__':
	app.run(host='0.0.0.0',debug= True)
