import glob
import errno
import json
import urllib3
import re
import unicodedata
from datetime import datetime
from nltk import word_tokenize, pos_tag
from nltk.corpus import stopwords
from nltk.stem import LancasterStemmer, WordNetLemmatizer


# Remove non ASCII words from a tokenized list.
def remove_non_ascii(words):
    new_words = []
    for word in words:
        new_word = unicodedata.normalize('NFKD', word).encode('ascii', 'ignore').decode('utf-8', 'ignore')
        new_words.append(new_word)
    return new_words


# Transform all words in a tokenized list to lowercase.
def to_lowercase(words):
    new_words = []
    for word in words:
        new_word = word.lower()
        new_words.append(new_word)
    return new_words


# Reove punctuation from a list of tokenized words.
def remove_punctuation(words):
    new_words = []
    for word in words:
        new_word = re.sub(r'[^\w\s]', '', word)
        if new_word != '':
            new_words.append(new_word)
    return new_words


# Remove stop words from list of tokenized words.
def remove_stopwords(words):
    new_words = []
    for word in words:
        if word not in stopwords.words('english'):
            new_words.append(word)
    return new_words


# Stem words in list of tokenized words
def stem_words(words):
    stemmer = LancasterStemmer()
    stems = []
    for word in words:
        stem = stemmer.stem(word)
        stems.append(stem)
    return stems


# Lemmatize verbs in list of tokenized words
def lemmatize_verbs(words):
    lemmatizer = WordNetLemmatizer()
    lemmas = []
    for word in words:
        lemma = lemmatizer.lemmatize(word, pos='v')
        lemmas.append(lemma)
    return lemmas


# Remove verbs from tokenized list of words
def remove_verbs(words):
    tagged = pos_tag(words)
    new_words = []
    for tag_word in tagged:
        if not tag_word[1].startswith('V'):
            new_words.append(tag_word[0])

    return new_words


# Remove numbers from tokenized list of words (full digit words only)
def remove_numbers(words):
    new_words = []
    for word in words:
        if not word.isdigit():
            new_words.append(word)

    return new_words


# Execute all the preprocessing steps in a list of tokenized words.
def pre_processing_text(words):
    words = remove_non_ascii(words)
    words = to_lowercase(words)
    words = remove_punctuation(words)
    words = remove_stopwords(words)
    words = remove_verbs(words)
    words = remove_numbers(words)
    return words

# START - LOAD DUMMY DATA:

http = urllib3.PoolManager()

noticia = dict()
noticia["document"] = dict()
noticia["document"]["title"] = "dummy"
noticia["document"]["url"] = "dummy"
noticia["document"]["source_id"] = 1
noticia["document"]["source_name"]= "DZone"
noticia["document"]["published"] = "06/11/2018"
noticia["document"]["summary"]= "dummy"
noticia["document"]["main_image"] = "dummy"
noticia["document"]["text"] = "dummy"

values = [[1,2,5,6,7,8,9,14,17,25,16,15,12,6,7,7,7,10,11,9,5,4,2,1], # containers
           [10,12,15,16,17,18,19,14,17,25,16,15,12,16,17,17,17,10,24,29,15,24,22,21], # aws
           [17,18,15,16,17,18,16,14,17,29,30,35,29,16,17,18,20,20,25,29,25,28,22,25], # nosql
           [15,12,5,6,7,8,9,14,22,25,26,30,12,6,7,7,7,5,11,9,5,4,8,10], # java framework
           [10,10,15,16,14,12,16,24,12,25,14,12,12,26,27,32,25,12,11,3,6,3,2,1], # mobile dev
           [1,2,0,0,0,0,0,0,0,2,3,1,1,1,2,4,0,0,0,1,15,14,12,11], # red hat ibm
           [1,0,0,0,0,0,1,2,0,0,2,2,3,3,2,0,0,0,0,0,4,2,1], # h2o aws
           [100,201,254,265,173,108,201,154,179,181,165,153,121,126,157,99,95,104,118,168,150,184,142,205], # tdd
           [1,0,0,0,0,0,0,0,0,0,2,0,3,0,0,0,0,0,0,0,4,2,0], # amazon corretto
           [2,3,5,9,7,8,9,9,9,5,1,5,2,6,3,4,1,1,9,5,5,3,4], # ocr tool
           [11,12,15,16,17,12,9,14,6,7,8,10,4,5,2,1,1,0,0,9,5,0,2,1], # iot health
           [10,12,12,16,17,28,19,12,15,17,18,25,25,6,9,2,1,8,13,19,25,24,22,33], # iot analitycs
           [40,43,31,38,37,35,39,44,47,25,12,15,12,34,36,29,25,13,12,36,42,43,31,54], # blockchain
           [20,23,15,17,17,18,9,4,2,1,6,5,2,6,2,1,0,0,11,9,5,4,1,4], # cloud and mobile
           [10,11,25,26,37,28,19,14,27,35,26,15,12,19,17,24,80,70,30,20,76,91,103,122], # ways of working
           [0,0,0,0,0,3,2,0,1,1,1,1,0,0,0,0,0,8,7,7,2,3,2,5], # saas
           [10,14,15,10,16,18,9,0,0,15,22,30,77,67,90,57,70,100,80,120,140,200,212,244], #bi
           [10,10,15,16,14,2,6,4,2,25,14,12,12,26,27,12,15,12,11,3,6,3,2,1], # android dev
           [1,0,0,3,4,8,3,4,17,5,10,15,18,17,2,3,5,10,11,20,16,14,12,10], # cloud
           [0,3,4,2,1,2,0,0,0,0,0,0,0,9,4,6,3,0,1,10,2,5,1,2], # tech time
           [0,2,2,2,7,8,15,19,7,5,6,1,0,0,0,2,5,7,10,20,15,14,12,15], # learning courses
           [0,0,0,0,0,2,4,1,1,2,6,5,2,6,7,6,8,9,19,15,18,14,22,25], # azure
           [11,9,13,10,15,20,21,24,22,21,20,22,28,24,23,23,19,22,27,22,25,26,28,24],
           [17,21,25,16,13,12,13,15,11,20,21,29,30,26,27,28,32,25,26,23,20,22,21,26],
           [1,0,0,0,0,1,0,0,0,0,1,1,2,0,0,1,0,0,1,2,2,4,2,1],
           [132,156,154,169,217,208,199,141,176,183,210,215,192,162,137,124,167,159,171,191,235,274,302,291],
           [2, 2, 5, 3, 1, 8, 4, 0, 0, 0, 1, 4, 9, 6, 2, 3, 0, 1, 0, 2, 6, 1, 5, 3],
           [1, 0, 0, 1, 0, 2, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 1, 2, 0, 0, 2, 4, 2, 1],
           [12, 21, 9, 12, 17, 18, 23, 21, 28, 31, 30, 15, 21, 30, 18, 22, 23, 19, 17, 19, 15, 14, 20, 15],
           [0, 2, 3, 0, 1, 1, 0, 0, 0, 0, 0, 1, 2, 0, 1, 0, 0, 1, 0, 1, 0, 0, 2, 1]]

topic_id = 1
for topic in values:
    int_published = 440
    for week in topic:
        noticia["document"]["int_published"] = int_published
        noticia["document"]["topics"] = [{"id": topic_id,
                                          "weight": str(1)}]
        encoded_data = json.dumps(noticia).encode('utf-8')
        print("int_published: " + str(int_published))
        print("week: " + str(week))
        for num in range(1, week + 1):
            print(".")
            request_3 = http.request('POST', 'http://categorized_data:4000/api/documents/',
                                     body=encoded_data,
                                     headers={'Content-Type': 'application/json'})

        noticia["document"].pop("int_published")
        noticia["document"].pop("topics")
        int_published = int_published + 1
    topic_id = topic_id + 1

# FINISH - LOAD DUMMY DATA:

# START - LOAD NEW DATASET:

path = 'auto_dataset/*.json'
files = glob.glob(path)

news = []
new = {}
counter = 0
http = urllib3.PoolManager()

for file_path in files:
    try:
        with open(file_path, mode='r') as n:
            data = json.load(n)
            
            text = data["document"]["raw_text"]
            words = word_tokenize(text)
            words = pre_processing_text(words)
            lemmas = lemmatize_verbs(words)
            text = " ".join(lemmas)
            data["document"]["clean_text"] = text
            data["document"].pop("raw_text")
	
	

            json_body = json.dumps(data)
            r = http.request('POST', 'http://corpus_data:4000/api/documents/', body=json_body,  headers={'Content-Type': 'application/json'})

            json_response = json.loads(r.data.decode('utf-8'))
            saved_doc = json_response['document']
            saved_id = saved_doc['id']

            id_message = dict()
            id_message['new_id'] = saved_id
            json_id_message = json.dumps(id_message)

            # POST saved id to Processing

            r = http.request('POST', 'http://processing:8000/newclassification/', body=json_id_message,
                             headers={'Content-Type': 'application/json'})
            new = {}
            counter = counter + 1
            print(counter)

    except IOError as exc:
        if exc.errno != errno.EISDIR:  # Do not fail if a directory is found, just ignore it.
            raise

# FINISH - LOAD NEW DATASET:

