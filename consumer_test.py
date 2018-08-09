from kafka import KafkaConsumer

from stanfordcorenlp import StanfordCoreNLP
import json
import datetime
import time

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

# from flask import Flask
# app = Flask(__name__)

# @app.route("/")
# def hello():
#     return "Hello World!"

if __name__ == '__main__':
    sNLP = StanfordNLP()
    print("Hello")

cnt=0
total=0


consumer = KafkaConsumer('test_topic', bootstrap_servers='daeho.local:9092')
for message in consumer:
    # print(message.value)
    total=total+1

    msg = message.value.decode('utf-8')

    # print(type(msg))

    if msg[0] == 1 :
        msg = msg[1:]
        new_msg = msg.split('/')
        text = new_msg[0]
        time = new_msg[1]
        name = new_msg[2]
        geo = new_msg[3]

    else :
        text = msg[1:]
        new_msg = msg.split('/')
        text = new_msg[0]
        time = new_msg[1]
        name = new_msg[2]

    tokenized_text = sNLP.word_tokenize(text)

    # print('\n', datetime.datetime.now())
    # print("Total = ",total, "MSG = ",msg, "\n---------")
    for disease in diseases :
        if disease in tokenized_text :
            cnt = cnt+1
            print("\n------------------------------------------------------------------------")
            print("Total Tweets : ",total, "  Current time : ",datetime.datetime.now())
            print("DISEASES DETECTED = ", cnt, "   DISEASE = ", disease, "\n", msg)

