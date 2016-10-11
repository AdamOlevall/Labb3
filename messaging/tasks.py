import json
import ujson
import subprocess
from celery import Celery

app = Celery('tweet', backend='amqp', broker='amqp://ad:ol@130.238.29.10:5672/adol')

@app.task
def calcPro(objName):
	pronouns = ['han', 'hon', 'den', 'det', 'denna', 'denne', 'hen']
	antal = [0,0,0,0,0,0,0]

	tweetObj = 'curl -O http://smog.uppmax.uu.se:8080/swift/v1/tweets/' + objName
	print "getting object " + tweetObj + "..."
	subprocess.call(tweetObj, shell=True)

	print "parsing file: " + objName
	textJSON = open(objName, 'r')

	for t in textJSON:
		try:
			tweet = ujson.loads(t)
			if 'retweeted_status' not in tweet:
				for i in range(len(pronouns)):
					if pronouns[i] in tweet['text'] and not pronouns[i] + 'n' in tweet['text']:
						antal[i] += 1
		except:
			pass
	return antal
