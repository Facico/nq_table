from table_utils.utils import *
import os
import json
from tqdm import tqdm
import multiprocessing
import gzip
#from try_all_nq import *
def hyper_try():
    hyper_table_dict_path = './data/hyper_table_list_add_wiki.json'
    print('get hyper...')
    with open(hyper_table_dict_path, 'r') as f:
        hyper_table_dict = json.load(f)
    
    tot = 0
    tot_wiki = 0
    tot_nq = 0
    for k, v in hyper_table_dict.items():
        tot += len(v)
        for i in v:
            if i[0] == 'not find':
                continue
            elif i[0] == 'wiki':
                tot_wiki += 1
            else:
                tot_nq += 1
    print('tot: {},  tot_wiki:{},  tot_nq:{}'.format(tot, tot_wiki, tot_nq))

    hyper_need = 0
    result_all = []
    all_result_path = './data/bm25_result_all.json'
    with open(all_result_path, 'r') as f:
        one_bm25_result = json.load(f)
        result_all = one_bm25_result    
    
    print('get need')
    need = 0
    need_wiki = 0
    need_nq = 0
    for i in tqdm(range(len(result_all))):
        example_id = ''
        for k, v in result_all[i].items():
            example_id = k
        c_set = {}
        for j in result_all[i][example_id]["candidate_doc"]:
            need += 1
            if j not in c_set:
                c_set[j] = 1
        for j in result_all[i][example_id]["candidate_doc"]:   
            for k in hyper_table_dict[j]:
                lei = 0
                id = ''
                if k[0] == 'not find':
                    lei = 0
                    id = k[1]
                elif k[0] == 'wiki':
                    lei = 1
                    id = k[1]
                else:
                    lei = 2
                    id = k[0]
                    
                if id not in c_set:
                    c_set[id] = 1
                    hyper_need += 1
                    if lei == 1:
                        need_wiki += 1
                    elif lei == 2:
                        need_nq += 1
                else:
                    continue
    
    print('need:{},  hyper_need: {}, need_wiki:{},  need_nq:{}'.format(need, hyper_need, need_wiki, need_nq))

def bm25_try():
    id_to_answer = {}
    num_chong = 0
    num_c = 0
    nq_train_data_path = '/data1/fch123/OTT-QA/data/v1.0/train/'
    for id in range(50):
        print(id)
        if(id>=10):
            file_path = os.path.join(nq_train_data_path, 'nq-train-{}.jsonl.gz'.format(id))
        else:
            file_path = os.path.join(nq_train_data_path, 'nq-train-0{}.jsonl.gz'.format(id))
        with gzip.open(file_path, 'r') as f:
            for i in tqdm(f):
                table = json.loads(i)
                #print(table['question_text'])
                if table['example_id'] not in id_to_answer:
                    id_to_answer[table['example_id']] = 1
                    num_c += 1
                else:
                    num_chong += 1
                    print(table['example_id'])
    
    print(num_c, num_chong)

if __name__ == '__main__':
    #print(tokenize('Music from the Motion Picture Alternate Mixes'))
    #print(os.path.exists(r'./data/tables/...Baby_One_More_Time_(album)_825954529_8.json'))
    #tokenization_tab(r'./data/tables/.380_ACP_850240454_0.json')
    #print(os.path.exists(r'./data/tables_tok/.380_ACP_850240454_0.json'))
    #print(url2dockey("You_Can_Dance_\u2013_Po_Prostu_Ta\u0144cz!_(season_1)_20"))
    #print(multiprocessing.cpu_count())
    bm25_try()