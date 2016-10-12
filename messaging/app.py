#!flask/bin/python

from celery import Celery
from celery import group
from tasks import parseTweets
from flask import Flask, jsonify
import subprocess
import sys
import os
import swiftclient.client
import json
import time
import urllib2
from collections import Counter

app = Flask(__name__)

@app.route('/test', methods=['GET'])
def print_hello():
	return 'Tjo Valle! Allt bra? :)', 200

@app.route('/Labb3/messaging', methods=['GET'])
def cow_say():
	tweets = []
	req = urllib2.Request("http://smog.uppmax.uu.se:8080/swift/v1/tweets")
	response = urllib2.urlopen(req)
	tweetsObject = response.read().split()
	

	job = group(parseTweets.s(i) for i in tweetsObject)
	tweetTask = job.apply_async()
	print "Celery is working..."
	
	while (tweetTask.ready() == False):
		#print "... %i s" %(counter)
		int counter = 0
	print "The task is done!"

	toReturn = tweetTask.get()

	c = Counter()
	for d in toReturn:
		c.update(d)
	
	display = dict(c)
	return jsonify(display), 200

if __name__ == '__main__':
	app.run(host='0.0.0.0',debug=True)
