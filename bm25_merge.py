import json
import os
from tqdm import tqdm
import gzip

if __name__ == '__main__':
    bm25_split_path = './data/'
    nq_train_data_path = '/data1/fch123/OTT-QA/data/v1.0/train/'
    bm25_result = []
    result_one_all = {}
    data_not_one = []
    for id in range(50):
        print(id)
        split_filex = os.path.join(bm25_split_path, 'bm25_result_split{}.json'.format(id))
        url = {}
        result_one = {}
        not_one = 0
        with open(split_filex, 'r') as f:
            table = json.load(f)
            #bm25_result += table
            for i in range(len(table)):
                for k, v in table[i].items():
                    if k not in url:
                        url[k] = 1
                    else:
                        url[k] += 1
            for i in range(len(table)):
                for k, v in table[i].items():
                    if(url[k] == 1):
                        result_one[k] = v
        if(id>=10):
            file_path = os.path.join(nq_train_data_path, 'nq-train-{}.jsonl.gz'.format(id))
        else:
            file_path = os.path.join(nq_train_data_path, 'nq-train-0{}.jsonl.gz'.format(id))
        with gzip.open(file_path, 'r') as f:
            for i in tqdm(f):
                table = json.loads(i)
                if table['document_url'] in result_one:
                    result_one_all[table['example_id']] = result_one[table['document_url']]
                else:
                    data_not_one.append(table)


    output_path = './data/bm25_result_one.json'
    with open(output_path, 'w') as f:
        json.dump(result_one_all, f)

    """print('not ont are {}'.format(len(data_not_one)))
    output_path_one = './data/data_not_one.json'
    with open(output_path_one, 'w') as f:
        json.dump(data_not_one, f)"""