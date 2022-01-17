import jsonlines
import json
import os
from tqdm import tqdm


def nq():
    table_path = './data/tables'
    docu_table = {}
    #nq_file = 'simplified-nq-train.jsonl'
    url_table = {}
    for file in tqdm(os.listdir(table_path)):
        file_path = os.path.join(table_path, file)
        with open(file_path, 'r') as f:
            table_file = json.load(f)
            url = table_file['url']
            uid = table_file['uid']
            if url not in url_table:
                url_table[url] = []
            url_table[url].append([file, uid])
    with open('url_table_all.json', 'w') as f:
        json.dump(url_table, f)

def wiki():
    table_path = './data/Wiki/tables'
    docu_table = {}
    url_table = {}
    for file in tqdm(os.listdir(table_path)):
        file_path = os.path.join(table_path, file)
        with open(file_path, 'r') as f:
            table_file = json.load(f)
            url = table_file['url']
            uid = table_file['uid']
            if url not in url_table:
                url_table[url] = []
            url_table[url].append([file, uid])
    with open('url_table_wiki.json', 'w') as f:
        json.dump(url_table, f)

if __name__ == '__main__':
    wiki()

