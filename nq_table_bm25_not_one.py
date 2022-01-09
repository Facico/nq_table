from rank_bm25 import BM25Okapi
import json
import os
import jsonlines
from tqdm import tqdm
import numpy as np
import gzip
from multiprocessing import Pool
import multiprocessing
import time

def search_bm25(tokenized_query, i_query, bm25, k=100):
    #tokenized_query = x[0]
    #i_query = x[1]
    #bm25 = x[2]
    result = {}
    scores = bm25.get_scores(tokenized_query)
    top_n = np.argsort(scores)[::-1][:k]
    score = [scores[i] for i in top_n]
    result[query_id[i_query]] = {'candidate_doc': [corpus_name[i] for i in top_n], 'score': score}
    return result
if __name__ == '__main__':
    cores = multiprocessing.cpu_count()
    pool = Pool(cores)

    tabfact_file_path = '/data1/fch123/OTT-QA/table_crawling/data/tabfact_process'
    corpus = []

    it = 0
    corpus_name = []
    for file in tqdm(os.listdir(tabfact_file_path)):
        file_path = os.path.join(tabfact_file_path, file)
        with open(file_path, 'r') as f:
            table = json.load(f)
            corpus.append(table['table_text_horizontal'])
            corpus_name.append(table['file_name'])

    print('all corpus: {}'.format(len(corpus)))

    tokenized_corpus = [doc.split(" ") for doc in corpus]

    bm25 = BM25Okapi(tokenized_corpus)


    not_one_data_path = '/data1/fch123/OTT-QA/table_crawling/data/data_not_one.json'
    print('deal with the not one url data')
    query = []
    query_id = []
    id_num = 0
    with open(not_one_data_path, 'r') as f:
        tables = json.load(f)
        lens = len(tables)
        for i in range(lens):
            query.append(tables[i]['question_text'])
            query_id.append(tables[i]['example_id'])
            
            if((i + 1) % 1000 == 0 or i == lens - 1):
                print('all query: {}'.format(len(query)))
                tokenized_query = [(doc.split(" "), i, bm25) for i, doc in enumerate(query)]
                bm25_result = []
                time_start=time.time()
                """with Pool(cores) as p:
                    _result = list((tqdm(p.starmap(search_bm25, tokenized_query), total=len(tokenized_query), desc='bm25')))
                    for i in _result:
                        bm25_result.append(i)"""
                
                for tokenized_query_x, i_query, bm25 in tqdm(tokenized_query):
                    bm25_result.append(search_bm25(tokenized_query_x, i_query, bm25))
                #bm25_result = pool.starmap(search_bm25, tokenized_query)
                #for i in tqdm(tokenized_query):
                #    bm25_result.append(search_bm25(i))
                #print(bm25_result)
                end_start=time.time()
                print('cost time {:.5f} min'.format((end_start - time_start)/ 60 ))
                
                print('id {}'.format(id_num))
                not_one_result_path = './data/bm25_result_not_one_{}.json'.format(id_num)
                with open(not_one_result_path, 'w') as f:
                    json.dump(bm25_result, f)
                query = []
                query_id = []
                id_num += 1
    

    print('start to merge not one')
    bm25_result = []
    for i in range(id_num):
        file_path = './data/bm25_result_not_one_{}.json'.format(i)
        if not os.path.exists(file_path):
            break
        with open(file_path, 'r') as f:
            table = json.load(f)
            bm25_result += table

    print('start to merge one and not one')

    result_all = bm25_result
    one_result_path = './data/bm25_result_one.json'
    with open(one_result_path, 'r') as f:
        one_bm25_result = json.load(f)
        result_all += one_bm25_result
    
    all_result_path = './data/bm25_result_all.json'
    print('length of all is {}'.format(len(result_all)))
    with open(all_result_path, 'w') as f:
        json.dump(result_all, f)

    pool.close()
    pool.join()
    
