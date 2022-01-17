import json
import os
from tqdm import tqdm
import time
import gzip
from multiprocessing import Pool
import multiprocessing
import re
from functools import partial

def get_text(token_text):
    str_list = []
    for i in token_text:
        str_list.append(i['token'])
    return str_list
def get_html_mask(token_text):
    str_list = []
    for i in token_text:
        str_list.append(i['html_token'])
    return str_list

def get_short_from_htmlmask(text_token, mask_text):
    short_answer = []
    now = 0
    now_answer = []
    for i in range(len(text_token)):
        if(mask_text[i] == True):
            if now == 0: 
                continue
            else:
                now = 0
                if(now_answer != []):
                    short_answer.append(now_answer)
                now_answer = []
        else:
            now_answer.append(text_token[i])
            now = 1
    return short_answer
def get_no_html(text_token, mask_text):
    no_html_answer = []
    for i in range(len(text_token)):
        if mask_text[i] == False:
            no_html_answer.append(text_token[i])
    return no_html_answer
def get_answer(table):
    token_text = get_text(table['document_tokens'])
    mask_text = get_html_mask(table['document_tokens'])
    answers = []
    html_mask = []
    for answer_x in table['long_answer_candidates']:
        start_t, end_t = answer_x['start_token'], answer_x['end_token']
        answers.append(token_text[start_t : end_t])
        html_mask.append(mask_text[start_t : end_t])
    return answers, html_mask

def cell_match(cellx, answer):
    for answerx in answer:
        if(cellx == ' '.join(answerx)):
            return True
    return False

def table_match(table, answer):
    datax = table['data']
    header = table['header']
    n_row, n_column = len(datax), len(datax[0])
    for i in range(len(header)):
        if(cell_match(header[i][0], answer) == True):
                return True
    for i in range(n_row):
        for j in range(n_column):
            if(cell_match(datax[i][j][0], answer) == True):
                return True
    
    return False

def match_hyper_table(table_name, hyper_table_dict, table_dict, answer, wiki_table_path):
    if table_name not in hyper_table_dict:
        return []
    p_list = []
    for i in hyper_table_dict[table_name]:
        if i[0] == 'not find':
            continue
        elif i[0] == 'wiki':
            wiki_table_path_x = os.path.join(wiki_table_path, i[1])
            with open(wiki_table_path_x, 'r') as f:
                wiki_tablex = json.load(f)
            y_n = table_match(tablex, answer)
            if y_n == True:
                p_list.append(['wiki', i[0]])
        else:
            tablex = table_dict(i[0])
            y_n = table_match(tablex, answer)
            if y_n == True:
                p_list.append(['nq', i[0]])
    
    return p_list

def get_pn(result_all_x, id_to_answer):
    example_id = ''
    ans_pn = {}
    for k, v in result_all_x.items():
        example_id = k
    
    if(example_id not in id_to_answer):
        return
    
    answers = id_to_answer[example_id]['no_html_answers']
    p_n = []
    positive_num = 0
    for answer_doc_file_name in result_all[i][example_id]["candidate_doc"]:
        tablex = table_dict[answer_doc_file_name]
        y_n = table_match(tablex, answers)
        if y_n == True:
            p_n.append(1)
            positive_num += 1
        else:
            p_n.append(0)
    ans_pn[example_id] = {'positive_num': positive_num, 'pn_list': p_n}
    return ans_pn
if __name__ == '__main__':
    cores = multiprocessing.cpu_count()

    step = ['2', '3', '4'] #['1', '2', '3']
    nq_train_data_path = '/data1/fch123/OTT-QA/data/v1.0/train/'
    
    if '1' in step:
        for id in range(50):
            id_to_answer = {}
            id_to_answer_file = './data/id_to_answer_split{}.json'.format(id)
            if(id>=10):
                file_path = os.path.join(nq_train_data_path, 'nq-train-{}.jsonl.gz'.format(id))
            else:
                file_path = os.path.join(nq_train_data_path, 'nq-train-0{}.jsonl.gz'.format(id))
            with gzip.open(file_path, 'r') as f:
                for i in tqdm(f):
                    table = json.loads(i)
                    #print(table['question_text'])
                    u = get_answer(table)
                    answers_no_html = []
                    for answers, html_masks in list(zip(u[0], u[1])):
                        no_html_example = get_no_html(answers, html_masks)
                        answers_no_html.append(no_html_example)
                    id_to_answer[table['example_id']] = {'no_html_answers': answers_no_html}
        
            with open(id_to_answer_file, 'w') as f:
                json.dump(id_to_answer, f)
    
    if '2' in step:
        result_all = []
        all_result_path = './data/bm25_result_all.json'
        with open(all_result_path, 'r') as f:
            one_bm25_result = json.load(f)
            result_all = one_bm25_result
        print('len of all is {}'.format(len(result_all)))
        
        table_path = './data/tables_tok'
        wiki_table_path = './data/Wiki/tables_tok'

        hyper_table_dict_path = './data/hyper_table_list_add_wiki.json'

        print('get hyper...')
        with open(hyper_table_dict_path, 'r') as f:
            hyper_table_dict = json.load(f)

        table_dict = {}
        print('get file...')
        for file in tqdm(os.listdir(table_path)):
            file_path = os.path.join(table_path, file)
            with open(file_path, 'r') as f:
                table_file = json.load(f)
                table_dict[file] = table_file
        
        
        """# multi process
        for id in range(50):
            print('deal file id {}'.format(id))
            start_time = time.time()

            id_to_answer = {}
            id_to_answer_file = './data/id_to_answer_split{}.json'.format(id)
            with open(id_to_answer_file, 'r') as f:
                id_to_answer = json.load(f)
            
            ans_pn = []
            _get_pn = partial(get_pn, id_to_answer=id_to_answer)
            with Pool(cores) as p:
                _result = list((tqdm(p.starmap(_get_pn, result_all), total=len(result_all), desc='bm25')))
                for i in _result:
                    ans_pn.append(i)
            
            end_start = time.time()
            print('cost time {:.5f} min'.format((end_start - start_time)/ 60 ))
            pn_file_path = './data/bm25_pn_split{}.json'.format(id)
            with open(pn_file_path, 'w') as f:
                json.dump(ans_pn, f)"""

        for id in range(50):
            print('deal file id {}'.format(id))
            start_time = time.time()
            id_to_answer = {}
            id_to_answer_file = './data/id_to_answer_split{}.json'.format(id)
            with open(id_to_answer_file, 'r') as f:
                id_to_answer = json.load(f)
            ans_pn = {}
            hyper_answer = {}
            for i in range(len(result_all)):
                example_id = ''
                for k, v in result_all[i].items():
                    example_id = k
                
                if(example_id not in id_to_answer):
                    continue
                
                answers = id_to_answer[example_id]['no_html_answers']
                p_n = []
                hyper_answer_list = []
                positive_num = 0
                for answer_doc_file_name in result_all[i][example_id]["candidate_doc"]:
                    tablex = table_dict[answer_doc_file_name]
                    y_n = table_match(tablex, answers)
                    # get hyper
                    table_hyper_list = match_hyper_table(answer_doc_file_name, hyper_table_dict, table_dict, answers, wiki_table_path=wiki_table_path)
                    if (len(table_hyper_list) != 0):
                        y_n = True
                        hyper_answer_list = hyper_answer_list + table_hyper_list
                    if y_n == True:
                        p_n.append(1)
                        positive_num += 1
                    else:
                        p_n.append(0)
                ans_pn[example_id] = {'positive_num': positive_num, 'pn_list': p_n}
                hyper_answer[example_id] = {'hyper_list': table_hyper_list}
                #print(positive_num)
                #break
            end_start = time.time()
            print('cost time {:.5f} min'.format((end_start - start_time)/ 60 ))
            pn_file_path = './data/bm25_pn_add_wiki_split{}.json'.format(id)
            with open(pn_file_path, 'w') as f:
                json.dump(ans_pn, f)
            
            hyper_file_path = './data/bm25_hyper_list_split{}.json'.format(id)
            with open(hyper_file_path, 'w') as f:
                json.dump(hyper_answer, f)
    if '3' in step:
        ans_pn_all = {}
        for id in tqdm(range(50)):
           # print('deal file id {}'.format(id))
            pn_file_path = './data/bm25_pn_add_wiki_split{}.json'.format(id)
            with open(pn_file_path, 'r') as f:
                ans_pn_x = json.load(f)
                for k, v in ans_pn_x.items():
                    ans_pn_all[k] = v
        pn_all_path = './data/bm25_pn_add_wiki_all.json'    
        with open(pn_all_path, 'w') as f:
            json.dump(ans_pn_all, f)
    
    if '4' in step:
        ans_pn_all = {}
        pn_all_path = './data/bm25_pn_add_wiki_all.json'  
        with open(pn_all_path, 'r') as f:
            ans_pn_all = json.load(f)
        AP_id = [1, 5, 10, 20, 50, 100]
        for j in AP_id:
            acc_j = 0
            all_j = 0
            for k, v in ans_pn_all.items():
                acc = 0
                for i in range(j):
                    if v['pn_list'][i] == True:
                        acc += 1
                acc_j += acc / j
                all_j += 1
            print('p@{}: {:.3f}'.format(j, acc_j / all_j))

        acc = 0
        all_j = 0
        for k, v in ans_pn_all.items():
            if v['positive_num'] > 0:
                acc += 1
            all_j += 1
        print('have positive {} / {} : {}'.format(acc, all_j, acc / all_j))

        """result_all = []
        all_result_path = './data/bm25_result_all.json'
        with open(all_result_path, 'r') as f:
            one_bm25_result = json.load(f)
            result_all = one_bm25_result
        print(len(result_all))"""
    
    