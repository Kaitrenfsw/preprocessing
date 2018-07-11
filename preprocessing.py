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

# Test variables:
example_message = '''
{
	"date": "2018-7-10 00:41:35",
	"doc_count": 1,
	"documents": [
		{
			"title": "Document Title",
			"url": "https://www.google.com/search?q=hola+mundo",
			"site": "https://www.google.com",
			"site_name": "Google",
			"published": "2018-7-10 00:41:35",
			"main_image": "image url",
			"text": "Twitter has shut down up to 70 million fake and suspicious accounts since May, according to the Washington Post. The suspensions and shutdowns were part of a concerted effort by Twitter to clear up the platform, said the paper. Many of the accounts are thought to be used by trolls or remotely controlled bots that abuse the service.\nTwitter declined to comment on the Post story but said it was making an effort to improve public conversation on the social network.\nCat and mouse\nJuan Guzman, a researcher at UCL who has exposed hundreds of thousands of bots on social media, said Twitter had neglected tackling automatic tweet generators for years.\nUntil recently, Twitter did not think bots were a problem on its platform and did not lead a strong bot-detection effort, he told the BBC.\nIt was only after Brexit and the 2016 election, where these bots became a liability and Twitter, as well as Facebook began taking them seriously.\nDel Harvey, Twitter's head of trust and safety, told the paper that it was now erring more on the side of preserving safety rather than supporting free speech at all costs.\nOne of the biggest shifts is in how we think about balancing free expression versus the potential for free expression to chill someone else's speech, said Ms Harvey.\nFree expression doesn't really mean much if people don't feel safe, she said."
		}
	]
}
'''

def new_main():
	data = json.loads(example_message.replace('\n', ' '))

	text = data["documents"][0]["text"]
	words = nltk.word_tokenize(text)
	words = pre_processing_text(words)
	lemmas = lemmatize_verbs(words)

	text = " ".join(lemmas)
	data["documents"][0]["text"] = text
	json_data = json.dumps(data)

	pprint(json_data)

	http = urllib3.PoolManager()
	r = http.request('POST', 'http://procesamiento:8000/ldamodel/', body=json_data, headers={'Content-Type': 'application/json'})

	pprint(r.status)
	pprint(r.data)	


def callback(ch, method, properties, body):
    pprint(" [x] Received %r" % body)

    data = json.loads(body)
    n_docs = data["doc_count"]
    
    for doc in data["documents"]:
    	text = doc["text"]
    	words = nltk.word_tokenize(text)
		words = pre_processing_text(words)
		lemmas = lemmatize_verbs(words)

		text = " ".join(lemmas)
		doc["text"] = text

	json_data = json.dumps(data)
	http = urllib3.PoolManager()
	r = http.request('POST', 'http://procesamiento:8000/ldamodel/', body=json_data, headers={'Content-Type': 'application/json'})

	pprint(r.status)
	pprint(r.data)	


def main():

	connected = False

	while(not connected):
		try:
			connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq-docker'))
			connected = True
		except:
			pass

	channel = connection.channel()

	channel.queue_declare(queue='hello')

	channel.basic_consume(callback,
	                      queue='hello',
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
