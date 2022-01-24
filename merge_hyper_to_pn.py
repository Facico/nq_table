import json
from tqdm import tqdm
import os

if __name__ == '__main__':
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
    result_all_new = []
    pn_new = []
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
                pn_new_x = pn_new_x + [1]
        
        result_all_new.append(result_all_new_x)
        pn_new.append(pn_new_x)
    with open('./data/bm25_result_merge_all.json', 'w') as f:
        json.dump(result_all_new, f)
    with open('./data/bm25_pn_merge_all.json', 'w') as f:
        json.dump(pn_new, f)