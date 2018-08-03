'''
A sample code usage of the python package stanfordcorenlp to access a Stanford CoreNLP server.
Written as part of the blog post: https://www.khalidalnajjar.com/how-to-setup-and-use-stanford-corenlp-server-with-python/ 
'''

from stanfordcorenlp import StanfordCoreNLP
import logging

import requests
import json

diseases = ["coronary" ,
            "stroke",
            "flu",
            "pneumonia",
            "bronchitis",
            "diabetes",
            "alzheimer’s" ,
            "tuberculosis",
            "cirrhosis",
            "cancer",
            "aids",
            "malaria",
            "depressive",
            "Measlesv"
            "mers",
 ]

class StanfordNLP:
    def __init__(self, host='http://localhost', port=9000):
        self.nlp = StanfordCoreNLP(host, port=port,
                                   timeout=30000)  # , quiet=False, logging_level=logging.DEBUG)
        self.props = {
            'annotators': 'tokenize,ssplit,pos,lemma,ner,parse,depparse,dcoref,relation',
            'pipelineLanguage': 'en',
            'outputFormat': 'json'
        }

    def word_tokenize(self, sentence):
        return self.nlp.word_tokenize(sentence)

    def pos(self, sentence):
        return self.nlp.pos_tag(sentence)

    def ner(self, sentence):
        return self.nlp.ner(sentence)

    def parse(self, sentence):
        return self.nlp.parse(sentence)

    def dependency_parse(self, sentence):
        return self.nlp.dependency_parse(sentence)

    def annotate(self, sentence):
        return json.loads(self.nlp.annotate(sentence, properties=self.props))

    @staticmethod
    def tokens_to_dict(_tokens):
        tokens = defaultdict(dict)
        for token in _tokens:
            tokens[int(token['index'])] = {
                'word': token['word'],
                'lemma': token['lemma'],
                'pos': token['pos'],
                'ner': token['ner']
            }
        return tokens

if __name__ == '__main__':
    sNLP = StanfordNLP()

    url = 'https://sour-stingray-0.localtunnel.me/query/service'

    r = requests.post(url, data={
        'statement': 'use feeds; select * from Tweets where lang="en" order by created_at desc limit 10;'})

    json_data = r.json()
    data = json_data['results']

    #lower_words = []
    cnt = 0

    # for d in data :
    #     words = sNLP.word_tokenize(d['Tweets']['text'])
    #     for word in words :
    #         lower_words.append(word.lower())


    for d in data:
        for disease in diseases:
            words = sNLP.word_tokenize(d['Tweets']['text'])
            lower_words = []
            for word in words :
                lower_words.append(word.lower())
            if disease in lower_words :
                cnt = cnt+1
                print("\nDISEASES DETECTED = ",cnt,"\n", d['Tweets']['text'])
        # print(d['Tweets']['text'])



    # text = 'The number of 25-34 year olds who died annually from alcohol-related liver disease nearly tripled between 1999 and 2016.'
    # print ("Annotate:", sNLP.annotate(text))
    # print ("POS:", sNLP.pos(text))
    # print ("Tokens:", sNLP.word_tokenize(text))
    # print ("NER:", sNLP.ner(text))
    # print ("Parse:", sNLP.parse(text))
    # print ("Dep Parse:", sNLP.dependency_parse(text))


