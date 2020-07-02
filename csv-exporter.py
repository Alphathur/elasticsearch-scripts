from elasticsearch import Elasticsearch
import csv
import sys
import json
import codecs

reload(sys)
sys.setdefaultencoding('utf-8')
# localhost:9200 or https://your_remote_es_url/
es = Elasticsearch(["localhost:9200"], timeout=9999)
es_index = "server-log"
es_type = "doc"
csv_header = ["time", "api",  "parameterMap", "response", "sessionId", "page"]
# Replace the following Query with your own Elastic Search Query
# If you want to export all fields, remove "_source" field, and set line 46 as "w = csv.DictWriter(f, fields)"
res = es.search(index=es_index, doc_type=es_type, body={
    "query": {
        "match_all": {}
    },
    "_source": {
        "includes": csv_header,
        "excludes": []
    },
}, size=1000)

def export(file_name):
    """
    export es documents to csv file
    :param file_name: the file name you wish to export
    :return: None
    """
    mapping = es.indices.get_mapping(index=es_index, doc_type=es_type)
    fields = []
    for field in mapping[es_index]['mappings'][es_type]['properties']:
        fields.append(field)
    with open(file_name, 'w') as f:
        f.write(codecs.BOM_UTF8)
        header_present = False
        i = 0
        for doc in res['hits']['hits']:
            i = i+1
            my_dict = doc['_source']
            if not len(my_dict):
                continue
            if not header_present:
                w = csv.DictWriter(f, csv_header)
                w.writeheader()
                header_present = True
            deal_chinese_words(my_dict)
            w.writerow(my_dict)


def deal_chinese_words(my_dict):
    if my_dict.get('parameterMap'):
        # in case chinese character garbled by utf8 encoding
        my_dict['parameterMap'] = json.dumps(my_dict['parameterMap']).decode('unicode_escape')
    if my_dict.get('response'):
        my_dict['response'] = json.dumps(my_dict['response']).decode('unicode_escape')


export("/Users/zhuhuiyuan/Downloads/data.csv")
