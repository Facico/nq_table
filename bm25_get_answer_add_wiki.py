import json
import os
from tqdm import tqdm
import time
import gzip
from multiprocessing import Pool
import multiprocessing
import re
from functools import partial
import sys

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

"""def table_dict(filename):
    table_path = './data/tables_tok'
    with open(os.path.join(table_path, filename), 'r') as f:
        tablex = json.load(f)
    return tablex"""

def match_hyper_table(table_name, hyper_table_dict, answer):
    if table_name not in hyper_table_dict:
        return []
    p_list = []
    for i in hyper_table_dict[table_name]:
        if i[0] == 'not find':
            continue
        elif i[0] == 'wiki':
            wiki_tablex = table_wiki[i[1]]
            y_n = table_match(wiki_tablex, answer)
            if y_n == True:
                p_list.append(['wiki', i[0]])
        else:
            #tablex = table_dict(i[0])
            tablex = table_dict[i[0]]
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
        #tablex = table_dict(answer_doc_file_name)
        tablex = table_dict[answer_doc_file_name]
        y_n = table_match(tablex, answers)
        if y_n == True:
            p_n.append(1)
            positive_num += 1
        else:
            p_n.append(0)
    ans_pn[example_id] = {'positive_num': positive_num, 'pn_list': p_n}
    return ans_pn

def main_2(id):
    print('deal file id {}'.format(id))
    start_time = time.time()
    id_to_answer = {}
    id_to_answer_file = './data/id_to_answer_split{}.json'.format(id)
    #id_to_answer_file = './data/id_to_answer_all.json'
    with open(id_to_answer_file, 'r') as f:
        id_to_answer = json.load(f)
    ans_pn = {}
    hyper_answer = {}
    for i in tqdm(range(len(result_all))):
        example_id = ''
        for k, v in result_all[i].items():
            example_id = k
        
        if(example_id not in id_to_answer):
            continue
        
        answers = id_to_answer[example_id]['no_html_answers']
        posi_list = []
        positive_num = 0
        c_set = {}
        for answer_doc_file_name in result_all[i][example_id]["candidate_doc"]:
            c_set[answer_doc_file_name] = 1
        for answer_doc_file_name in result_all[i][example_id]["candidate_doc"]:
            #y_n = table_match(tablex, answers)
            # get hyper
            for i in hyper_table_dict[answer_doc_file_name]:
                if i[0] == 'not find':
                    continue
                elif i[0] == 'wiki':
                    if(i[1] in c_set):
                        continue
                    c_set[i[1]] = 1
                    wiki_tablex = table_wiki[i[1]]
                    y_n = table_match(wiki_tablex, answers)
                    if y_n == True:
                        posi_list.append(['wiki', i[1]])
                else:
                    #tablex = table_dict(i[0])
                    if(i[0] in c_set):
                        continue
                    c_set[i[0]] = 1
                    if i[0] not in table_dict:
                        print('not find    ', i[0])
                        continue
                    tablex = table_dict[i[0]]
                    y_n = table_match(tablex, answers)
                    if y_n == True:
                        posi_list.append(['nq', i[0]])
            
        ans_pn[example_id] = {'positive_list': posi_list}
        #print(positive_num)
        #break
    end_start = time.time()
    print('cost time {:.5f} min'.format((end_start - start_time)/ 60 ))
    pn_file_path = './data/bm25_pn_add_wiki_split{}.json'.format(id)
    with open(pn_file_path, 'w') as f:
        json.dump(ans_pn, f)
if __name__ == '__main__':
    cores = 5#multiprocessing.cpu_count()

    step = ['3']#['3.5', '4']#['2', '3', '4'] #['1', '2', '3']
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
    
    if '1.5' in step:
        # merge id_to_answer
        id_to_answer_all = {}
        for id in range(50):
            #print('deal file id {}'.format(id))
            start_time = time.time()
            id_to_answer_file = './data/id_to_answer_split{}.json'.format(id)
            with open(id_to_answer_file, 'r') as f:
                id_to_answer_x = json.load(f)
                for k, v in id_to_answer_x.items():
                    id_to_answer_all[k] = v
        id_to_answer_all_file = './data/id_to_answer_all.json'            
        with open(id_to_answer_all_file, 'w') as f:
            json.dump(id_to_answer_all, f)
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
        
        table_wiki = {}
        print('get wiki ...')
        for file in tqdm(os.listdir(wiki_table_path)):
            wiki_table_path_x = os.path.join(wiki_table_path, file)
            with open(wiki_table_path_x, 'r') as f:
                wiki_tablex = json.load(f)
                table_wiki[file] = wiki_tablex
        
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

        # simple multi process
        id = [i for i in range(50)]
        _ = Pool(cores).map(main_2, id)
        """for id in range(50):
            print('deal file id {}'.format(id))
            start_time = time.time()
            id_to_answer = {}
            id_to_answer_file = './data/id_to_answer_split{}.json'.format(id)
            #id_to_answer_file = './data/id_to_answer_all.json'
            with open(id_to_answer_file, 'r') as f:
                id_to_answer = json.load(f)
            ans_pn = {}
            hyper_answer = {}
            for i in tqdm(range(len(result_all))):
                example_id = ''
                for k, v in result_all[i].items():
                    example_id = k
                
                if(example_id not in id_to_answer):
                    continue
                
                answers = id_to_answer[example_id]['no_html_answers']
                posi_list = []
                positive_num = 0
                c_set = {}
                for answer_doc_file_name in result_all[i][example_id]["candidate_doc"]:
                    c_set[answer_doc_file_name] = 1
                for answer_doc_file_name in result_all[i][example_id]["candidate_doc"]:
                    #y_n = table_match(tablex, answers)
                    # get hyper
                    for i in hyper_table_dict[answer_doc_file_name]:
                        if i[0] == 'not find':
                            continue
                        elif i[0] == 'wiki':
                            if(i[1] in c_set):
                                continue
                            c_set[i[1]] = 1
                            wiki_tablex = table_wiki[i[1]]
                            y_n = table_match(wiki_tablex, answers)
                            if y_n == True:
                                posi_list.append(['wiki', i[1]])
                        else:
                            #tablex = table_dict(i[0])
                            if(i[0] in c_set):
                                continue
                            c_set[i[0]] = 1
                            tablex = table_dict[i[0]]
                            y_n = table_match(tablex, answers)
                            if y_n == True:
                                posi_list.append(['nq', i[0]])
                    
                ans_pn[example_id] = {'positive_list': posi_list}
                #print(positive_num)
                #break
            end_start = time.time()
            print('cost time {:.5f} min'.format((end_start - start_time)/ 60 ))
            pn_file_path = './data/bm25_pn_add_wiki_split{}.json'.format(id)
            with open(pn_file_path, 'w') as f:
                json.dump(ans_pn, f)"""

    if '3' in step:
        ans_pn_all = {}
        k_v_num = 0
        k_v_chong = 0
        for id in tqdm(range(50)):
           # print('deal file id {}'.format(id))
            pn_file_path = './data/bm25_pn_add_wiki_split{}.json'.format(id)
            with open(pn_file_path, 'r') as f:
                ans_pn_x = json.load(f)
                for k, v in ans_pn_x.items():
                    if k not in ans_pn_all:
                        ans_pn_all[k] = v
                        k_v_num += 1
                    else:
                        k_v_chong += 1
                        print(k, v, ans_pn_all[k])
        pn_all_path = './data/bm25_pn_add_wiki_all.json'    
        print('len of all {}'.format(k_v_num))
        with open(pn_all_path, 'w') as f:
            json.dump(ans_pn_all, f)
    
    if '3.5' in step:
        result_all = []
        all_result_path = './data/bm25_result_all.json'
        with open(all_result_path, 'r') as f:
            one_bm25_result = json.load(f)
            result_all = one_bm25_result
        print('len of all is {}'.format(len(result_all)))

        hyper_ans_pn_all = {}
        pn_all_path = './data/bm25_pn_add_wiki_all.json'  
        with open(pn_all_path, 'r') as f:
            hyper_ans_pn_all = json.load(f)
        
        ans_pn_all = {}
        pn_all_path = './data/bm25_pn_all.json'  
        with open(pn_all_path, 'r') as f:
            ans_pn_all = json.load(f)
        result_all_new = {}
        pn_new = {}
        for i in tqdm(range(len(result_all))):
            example_id = ''
            for k, v in result_all[i].items():
                example_id = k
            top100 = result_all[i][example_id]["candidate_doc"]
            top100_pn = ans_pn_all[example_id]['pn_list']
            result_all_new_x = []
            pn_new_x = []
            stack_hyper = hyper_ans_pn_all[example_id]['positive_list']
            o = len(stack_hyper)
            c_set = {}
            for j in range(len(top100)-1, -1, -1):
                if top100_pn[j] == False:
                    if o > 0:
                        #print(stack_hyper)
                        #print(o)
                        x = stack_hyper[o - 1]
                        o -= 1
                        if x[0] == 'wiki':
                            result_all_new_x = result_all_new_x + [x]
                        else:
                            result_all_new_x = result_all_new_x + [x[1]]
                        pn_new_x = pn_new_x + [1]
                    else:
                        result_all_new_x = result_all_new_x + [top100[j]]
                        pn_new_x = pn_new_x + [0]
                else:
                    result_all_new_x = result_all_new_x + [top100[j]]
                    #print(top100[j])
                    pn_new_x = pn_new_x + [1]
                
                now_x = ''
                if len(result_all_new_x[-1]) == 2:
                    now_x = result_all_new_x[-1][1]
                else:
                    now_x = result_all_new_x[-1]

                if now_x not in c_set:
                    c_set[now_x] = 1
                else:
                    #print('error!!!')
                    print(now_x, j, top100[j], result_all_new_x[-1])
                    sys.exit(0)
                    pass
            
            result_all_new[example_id] = result_all_new_x
            positive_num = 0
            for i in pn_new_x:
                if i == 1:
                    positive_num += 1
            pn_new[example_id] = {'pn_list': pn_new_x, 'positive_num': positive_num}
        with open('./data/bm25_result_merge_all.json', 'w') as f:
            json.dump(result_all_new, f)
        with open('./data/bm25_pn_merge_all.json', 'w') as f:
            json.dump(pn_new, f)
    if '4' in step:
        ans_pn_all = {}
        pn_all_path = './data/bm25_pn_merge_all.json'  
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
    
    