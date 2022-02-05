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
    result[query_url[i_query]] = {'candidate_doc': [corpus_name[i] for i in top_n], 'score': score}
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


    nq_train_data_path = '/data1/fch123/OTT-QA/data/v1.0/train/'
    for it, file in enumerate(os.listdir(nq_train_data_path)):
        file_path = os.path.join(nq_train_data_path, file)
        it_num = int(file.split('-')[-1].split('.')[0])
        query = []
        query_url = []
        
        num_num = 0
        with gzip.open(file_path, 'r') as f:
            for i in tqdm(f):
                table = json.loads(i)
                query.append(table['question_text'])
                query_url.append(table['document_url'])
                num_num += 1
            
        print('all query: {}'.format(len(query)))

        tokenized_query = [(doc.split(" "), i, bm25) for i, doc in enumerate(query)]
        print('{} th file to search split number {}'.format(it, it_num))
        bm25_result = []
        time_start=time.time()
        with Pool(cores) as p:
            _result = list((tqdm(p.starmap(search_bm25, tokenized_query), total=len(tokenized_query), desc='bm25')))
            for i in _result:
                bm25_result.append(i)
        #bm25_result = pool.starmap(search_bm25, tokenized_query)
        #for i in tqdm(tokenized_query):
        #    bm25_result.append(search_bm25(i))
        #print(bm25_result)
        end_start=time.time()
        print('cost time {:.5f} min'.format((end_start - time_start)/ 60 ))
        
        result_path = './data/bm25_result_split{}.json'.format(it_num)
        with open(result_path, 'w') as f:
            json.dump(bm25_result, f)
    
    pool.close()
    pool.join()

    print('start')
    """k = 5
    bm25_result = {}
    for it, i in enumerate(tqdm(tokenized_query)):
        scores = bm25.get_scores(i)
        top_n = np.argsort(scores)[::-1][:k]
        #print(top_n)
        bm25_result[query_url[it]] = [corpus_name[i] for i in top_n]"""
        #print(u)
    #print(bm25_result)
    
