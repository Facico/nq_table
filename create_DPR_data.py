import os
import json
import gzip

def make_example(query, answers, table_list, pn_list):
    datax = {}
    datax["question"] = query
    datax["answers"] = [" ".join(i) for i in answers]
    datax["positive_ctxs"] = []
    datax["negative_ctxs"] = []
    for i in range(len(pn_list)):
        passagex = {}
        passagex["title"] = table_list[i]['file_name']
        passagex["text"] = table_list[i]['table_text_horizontal']
        if pn_list[i] == 0:
            datax["positive_ctxs"].append(passagex)
        else:
            datax["positive_ctxs"].append(passagex)
    return datax

def make_table_list(example_id):
    table_list = []
    pn_list = []
    for k in hyper_table_dict[example_id]:
        tablex = ''
        if k[0] == 'not find':
            continue
        elif k[0] == 'wiki':
            tablex = table_dict[k[1]]
        else:
            tablex = table_dict[k[0]]
        table_list.append(tablex)
        pn_list.append(1)
    for k in range(len(table_list_dict[example_id])):
        table_list.append(table_list_dict[example_id][k])
        pn_list.append(ans_pn_all[example_id]['pn_list'][k])
    
    return table_list, pn_list

if __main__ == '__name__':
    hyper_table_dict_path = './data/hyper_table_list_add_wiki.json'
    print('get hyper...')
    with open(hyper_table_dict_path, 'r') as f:
        hyper_table_list_dict = json.load(f)
    
    print('get answer list ...')
    result_all = []
    all_result_path = './data/bm25_result_all.json'
    table_list_dict = {}
    with open(all_result_path, 'r') as f:
        one_bm25_result = json.load(f)
        result_all = one_bm25_result
        for i in tqdm(range(len(result_all))):
            example_id = ''
            for k, v in result_all[i].items():
                example_id = k
            table_list_dict[example_id] = result_all[i][example_id]["candidate_doc"]    

    print('get pn list ...')
    ans_pn_all = {}
    pn_all_path = './data/bm25_pn_all.json'  
    with open(pn_all_path, 'r') as f:
        ans_pn_all = json.load(f)

    table_dict = {}
    table_path = './data/tabfact_process'
    wiki_table_path = './data/Wiki/tabfact_process'
    print('get file horizontal...')
    for file in tqdm(os.listdir(table_path)):
        file_path = os.path.join(table_path, file)
        with open(file_path, 'r') as f:
            table_file = json.load(f)
            table_dict[file] = table_file
    
    table_wiki = {}
    print('get wiki horizontal...')
    for file in tqdm(os.listdir(wiki_table_path)):
        wiki_table_path_x = os.path.join(wiki_table_path, file)
        with open(wiki_table_path_x, 'r') as f:
            wiki_tablex = json.load(f)
            table_wiki[file] = wiki_tablex
    
    nq_train_data_path = '/data1/fch123/OTT-QA/data/v1.0/train/'
    for it, file in enumerate(os.listdir(nq_train_data_path)):
        print(it)
        file_path = os.path.join(nq_train_data_path, file)
        it_num = int(file.split('-')[-1].split('.')[0])

        id_to_answer = {}
        id_to_answer_file = './data/id_to_answer_split{}.json'.format(it_num)
        with open(id_to_answer_all_file, 'r') as f:
            id_to_answer = json.load(f)

        query = []
        query_url = []
        query_id = []
        num_num = 0
        dpr_file_path = './data/dpr/'
        if not os.path.exists(dpr_file_path):
            os.mkdir(dpr_file_path)
        dpr_data = []
        with gzip.open(file_path, 'r') as f:
            for i in tqdm(f):
                table = json.loads(i)
                # create
                example_id = table['example_id']
                table_listx, pn_listx = make_table_list(example_id)
                dpr_data.append(make_example(table['question_text'], id_to_answer[example_id]['no_html_answers'], table_listx, pn_listx))
            
        print('all query: {}'.format(len(query)))
        with open(os.path.join(dpr_file_path, 'train_{}.json'.format(it_num))) as f:
            json.dump(dpr_data, f)

