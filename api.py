from flask import Flask, jsonify
from flask_restful import Api
from flask import request
import requests
import grequests


import pickle
import os
import threading

class NodeState:

	def __init__(self):
		self.port = os.getenv('PORT','8080')
		self.neighbors = os.getenv('MEMBERS',[(1,'9999'), (2,'9998'), (3,'9997')]).split(',')
		self.createNeighborNumbers()
		self.leader = None 
		self.setLeader()
		self.nodeIp = os.getenv('IP', 'http://0.0.0.0')
		self.number = str(self.nodeIp.split('.')[3])
		self.isLeader = False
		self.identifier = self.nodeIp+':'+self.port

		if self.identifier == self.leader: 
			self.isLeader = True

	def createNeighborNumbers(self):
		self.neighborsAndNumbers = []
		for neighbor in self.neighbors:
			self.neighborsAndNumbers.append((self.getMemberNumber(neighbor),neighbor))
		self.neighborsAndNumbers.sort(key=lambda x: x[0])

	def getLeaderURL(self):
		return 'http://'+self.leader

	def getMemberNumber(self, member):
		return str(member.split('.')[3])

	def setLeader(self):
		for neighbor in self.neighbors:
			try:
				r = requests.get('http://'+neighbor+'/are_you_leader/')
				val = r.json()['val']
				if val:
					self.leader = neighbor
					break
				else:
					print('not leader')
					continue

			except :
				print ('neighbor is dead')

		if not self.leader:
			self.findNewLeader()

	def backupNewData(self,key,payload):

		for neighbor in self.neighbors:
			print (neighbor)
			if neighbor == self.identifier: continue

			try:
				r = requests.put('http://'+neighbor+'/new_data_backup/' +key, {'val':payload}, timeout=0.2)
			except:
				self.retryRequest(neighbor,'/new_data_backup/',key,payload)

	def retryRequest(self,neighbor, url, key, payload):
			try:
				r = requests.put('http://'+neighbor+url +key, {'val':payload}, timeout=0.2)
			except:
				print ('failed again')

	def backupDeleteData(self,key):

		for neighbor in self.neighbors:
			print (neighbor)
			if neighbor == self.identifier: continue

			try:
				r = requests.delete('http://'+neighbor+'/new_data_delete/' +key, timeout=0.2)
			except:
				print ('faccccc')

	def findNewLeader(self):
		print ('Finding our new leader !')
		for number,neighbor in self.neighborsAndNumbers:
			try:
	
				print('trying neighbor {} as leader'.format(neighbor))
				url = 'http://'+neighbor+'/are_you_down/'
				print (url)

				r = requests.get(url,data = {'identifier_number':self.number},timeout=1)
				value = r.json()['val']
				print(value)
				if value:

					self.leader = neighbor

					print ('New leader is {}'.format(self.leader))
					break
				else:
					print ('leader is NOT DOWN')
					continue
			except :
				print ('leader is dead')
				continue


	def heartBeatLeader(self):
		print('sending heart beat')
		if not self.isLeader:
			threading.Timer(5.0,self.heartBeatLeader).start()
			try:
				print('about to hb leader')
				r = requests.get(self.getLeaderURL()+'/ping/',timeout=5)
				print ('LEADER IS ALIVE')

			except :
				print ('Leader is Dead')
				self.findNewLeader()
		else:
			print('nb')
			print('I AM THE LEADER X)')


def exception_handler(request, exception):
	print ("Request failed",request.body())

app = Flask(__name__)
api = Api(app)

# all data is stored in a database dictionary
myDb = {}

# all data is saved to a file called db.pickle
# using the pickle encoding
if os.path.isfile('db.pickle'):
	with open('db.pickle', 'rb') as handle:
  		myDb = pickle.load(handle)
  		print ('LOADED DB IS {}'.format(myDb))

# this function writes the database to the db file
def writeToFile():
	with open('db.pickle', 'wb') as outfile:
		pickle.dump(myDb, outfile)

# this function will accept any call from the allowed 
# HTTP methods. It follows the /kvs/<key> protocol

@app.route('/ping/', methods=['GET'])
def ping():
	return 'good job',201


@app.route('/are_you_down/', methods=['GET'])
def are_you_down():
	identifier_number = request.values.get('identifier_number')
	print(networkState.number,identifier_number)
	if networkState.number < identifier_number or networkState.isLeader:
		networkState.isLeader = True
		return jsonify({ 'val':True}),201
	else:
		return jsonify({ 'val': False}), 202



@app.route('/are_you_leader/', methods=['GET'])
def are_you_leader():

		if networkState.isLeader:
			return jsonify({ 'val':True}),201
		else:
			return jsonify({ 'val':False}), 202


@app.route('/new_data_backup/<key>', methods=['PUT'])
def new_data_backup(key):
	value = request.values.get('val')

	if not value:
		return jsonify({'msg':'error','value':'key does not exist'}),404
	if key in myDb:
		myDb[key] = value
		writeToFile()
		return jsonify({'replaced':1,'msg':'success'}),201
	else:
		myDb[key] = value
		writeToFile()
		return jsonify({'replaced':0,'msg':'success'}),201


@app.route('/new_data_delete/<key>', methods=['DELETE'])
def new_data_delete(key):
	if key in myDb:
		del myDb[key]
		writeToFile()
		return jsonify({'msg':'success'}),201
	else :
		return jsonify({'msg':'error','error':'key does not exist'}),404


@app.errorhandler(404)
@app.route('/kvs/<key>', methods=['GET', 'PUT','DELETE'])
def kvs(key):
	# Here the GET request checks if the value is in the dict
	if request.method == 'GET':
		print(networkState.port)

		if key in myDb:
			return jsonify({'msg':'success','value':myDb[key]}),201
		else :
			return jsonify({'msg':'error','error':'key does not exist'}),404

	# Here the PUT request adds or updates a new value in the dict
	# it also writes the new changes to the db.pickle
	if request.method == 'PUT':

		if len(key) >250 : return jsonify({'msg':'error','value':'key is too long'}),404


		value = request.values.get('val')

		if not networkState.isLeader:
			if networkState.leader:

				url = networkState.getLeaderURL()+'/kvs/'+key
			
				print(url)
				try:
					r = requests.put(url,data= {'val':value},timeout=1)
					print (r.json())
					code = 0
					if r.json()['msg'] == 'success':
						code = 201
					else:
						code = 404
					return jsonify(dict(r.json())),code
				except:
					print ('failed')
			

		else:
			if not value:
				return jsonify({'msg':'error','value':'key does not exist'}),404
			if key in myDb:
				myDb[key] = value
				writeToFile()
				networkState.backupNewData(key,value)
				return jsonify({'replaced':1,'msg':'success'}),201
			else:
				myDb[key] = value
				writeToFile()
				networkState.backupNewData(key,value)
				return jsonify({'replaced':0,'msg':'success'}),201

	# Here the DELETE request deletes the key and all the values associated with that key
	# it also writes the new changes to the db.pickle
	if request.method == 'DELETE':

		if not networkState.isLeader:
			if networkState.leader:

				url = networkState.getLeaderURL()+'/kvs/'+key
			
				print(url)
				try:
					r = requests.delete(url,timeout=1)
					print (r.json())
					code = 0
					if r.json()['msg'] == 'success':
						code = 201
					else:
						code = 404
					return jsonify(dict(r.json())),code
				except:
					print ('failed')
			

		else:
			if key in myDb:
				del myDb[key]
				writeToFile()

				networkState.backupDeleteData(key)

				return jsonify({'msg':'success'}),201
			else :
				return jsonify({'msg':'error','error':'key does not exist'}),404


	return jsonify({'msg':'error','value':'request invalid'}),404

import sys


if __name__ == '__main__':
	networkState = NodeState()

	if not networkState.isLeader:
		networkState.heartBeatLeader()

	app.run(host='0.0.0.0', port =networkState.port)




