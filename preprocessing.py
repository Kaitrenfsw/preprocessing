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
from nltk import word_tokenize, sent_tokenize
from nltk.corpus import stopwords
from nltk.stem import LancasterStemmer, WordNetLemmatizer
from pprint import pprint

def main():
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
