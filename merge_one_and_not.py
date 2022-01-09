import json
import os

if __name__ == '__main__':
    print('start to merge not one')
    bm25_result = []
    for i in range(10):
        file_path = './data/bm25_result_not_one_{}.json'.format(i)
        if not os.path.exists(file_path):
            print('not {}'.format(i))
            break
        with open(file_path, 'r') as f:
            table = json.load(f)
            bm25_result += table

    print('not one {}'.format(len(bm25_result)))
    print('start to merge one and not one')

    result_all = bm25_result
    one_result_path = './data/bm25_result_one.json'
    one_bm25_result = []
    with open(one_result_path, 'r') as f:
        table = json.load(f)
        for k, v in table.items():
            one_bm25_result.append({k: v})
        print('one {}'.format(len(one_bm25_result)))
        result_all += one_bm25_result
    
    all_result_path = './data/bm25_result_all.json'
    print('length of all is {}'.format(len(result_all)))
    with open(all_result_path, 'w') as f:
        json.dump(result_all, f)