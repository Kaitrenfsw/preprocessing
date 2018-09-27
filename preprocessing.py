import json
import re, string, unicodedata, copy
import nltk
import contractions
import json
import os
import request
import glob
import errno
import urllib3
import sys
import pika
import datetime
from nltk import word_tokenize, sent_tokenize
from nltk.corpus import stopwords
from nltk.stem import LancasterStemmer, WordNetLemmatizer
from pprint import pprint


def callback(ch, method, properties, body):
	pprint(" [x] Received %r" % body)

	#data = json.loads(body)
	new_id = body.decode("utf-8")
	pprint(new_id)

	# create request filters

	r_filter = {"type": "match", "field": "_id"}
	r_filter['value'] = new_id
	filters = [r_filter]

	# GET document from RAW_DATA with new_id

	http = urllib3.PoolManager()
	r = http.request('GET', 'http://raw_data:4000/api/documents', fields = {"filters": filters})

	json_response = json.loads(r.data)
	new_document = json_response['documents']['records'][0]

	print(new_document['title'])

	# preprocess text from document

	text = new_document['raw_text']
	words = nltk.word_tokenize(text)
	words = pre_processing_text(words)
	lemmas = lemmatize_verbs(words)

	text = " ".join(lemmas)

	# create document with clean text

	document = {}
	document['title'] = new_document['title']
	document['url'] = new_document['url']
	document['source_name'] = new_document['source_name']
	document['source_id'] = new_document['source_id']
	document['published'] = new_document['published']
	document['main_image'] = new_document['main_image']
	document['summary'] = new_document['summary']

	document['clean_text'] = text

	message = {}
	message['document'] = document

	# POST clean document to CORPUS

	json_message = json.dumps(message)

	r = http.request('POST', 'http://corpus_data:4000/api/documents', body=json_message, headers={'Content-Type': 'application/json'})

	# get id from saved document

	json_response = json.loads(r.data)
	saved_doc = json_response['document']
	saved_id = saved_doc['id']

	id_message = {}
	id_message['new_id'] = saved_id
	json_id_message = json.dumps(id_message)

	# POST saved id to Processing

	r = http.request('POST', 'http://processing:8000/newclassification/', body=json_id_message, headers={'Content-Type': 'application/json'})

	#n_docs = data["doc_count"]

	#for doc in data["documents"]:
	#	text = doc["text"]
	#	words = nltk.word_tokenize(text)
	#	words = pre_processing_text(words)
	#	lemmas = lemmatize_verbs(words)

	#	text = " ".join(lemmas)
	#	doc["text"] = text

	#json_data = json.dumps(data)
	#http = urllib3.PoolManager()
	#r = http.request('POST', 'http://procesamiento:8000/ldamodel/', body=json_data, headers={'Content-Type': 'application/json'})

	#pprint(r.status)
	#pprint(r.data)	


def main():

	connected = False

	while(not connected):
		try:
			connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq-docker'))
			connected = True
		except:
			pass

	channel = connection.channel()

	channel.queue_declare(queue='preprocessing_queue')

	channel.basic_consume(callback,
	                      queue='preprocessing_queue',
	                      no_ack=True)

	print(' [*] Waiting for messages. To exit press CTRL+C')
	channel.start_consuming()


	print("This is the Text Preprocessing Script")
	print(sys.version)
		

#Remove non ASCII words from a tokenized list.
def remove_non_ascii(words):
	new_words = []
	for word in words:
		new_word = unicodedata.normalize('NFKD', word).encode('ascii', 'ignore').decode('utf-8', 'ignore')
		new_words.append(new_word)
	return new_words

#Transform all words in a tokenized list to lowercase.
def to_lowercase(words):
	new_words = []
	for word in words:
		new_word = word.lower()
		new_words.append(new_word)
	return new_words

#Reove punctuation from a list of tokenized words.
def remove_punctuation(words):
	new_words = []
	for word in words:
		new_word = re.sub(r'[^\w\s]', '', word)
		if new_word != '':
			new_words.append(new_word)
	return new_words

#Remove stop words from list of tokenized words.
def remove_stopwords(words):
	new_words = []
	for word in words:
		if word not in stopwords.words('english'):
			new_words.append(word)
	return new_words

#Stem words in list of tokenized words
def stem_words(words):
	stemmer = LancasterStemmer()
	stems = []
	for word in words:
		stem = stemmer.stem(word)
		stems.append(stem)
	return stems

#Lemmatize verbs in list of tokenized words
def lemmatize_verbs(words):
	lemmatizer = WordNetLemmatizer()
	lemmas = []
	for word in words:
		lemma = lemmatizer.lemmatize(word, pos='v')
		lemmas.append(lemma)
	return lemmas

#Remove verbs from tokenized list of words
def remove_verbs(words):
	tagged = nltk.pos_tag(words)
	new_words = []
	for tag_word in tagged:
		if not tag_word[1].startswith('V'):
			new_words.append(tag_word[0])

	return new_words

#Remove numbers from tokenized list of words (full digit words only)
def remove_numbers(words):
	new_words = []
	for word in words:
		if not word.isdigit():
			new_words.append(word)

	return new_words

#Execute all the preprocessing steps in a list of tokenized words.
def pre_processing_text(words):
	words = remove_non_ascii(words)
	words = to_lowercase(words)
	words = remove_punctuation(words)
	words = remove_stopwords(words)
	words = remove_verbs(words)
	words = remove_numbers(words)
	return words

if __name__ == "__main__":
	main()
