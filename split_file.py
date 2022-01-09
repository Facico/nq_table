import json
import jsonlines
import os
from multiprocessing import Pool
import multiprocessing
from tqdm import tqdm

if __name__ == '__main__':
    """cores = multiprocessing.cpu_count()
    pool = Pool(cores)
    print("Initializing the pool of cores")"""

    #big_file = '/home/fch/OTT-QA/table_crawling/simplified-nq-train.jsonl'
    big_file = '/data1/fch123/OTT-QA/table_crawling/simplified-nq-train.jsonl'
    filename = os.path.basename(big_file)
    filename_no_suffix = filename.split('.')[0]
    count = 0
    for line in tqdm(jsonlines.open(big_file, 'r')):
        count += 1

    print("the line number of the file {}".format(count))
    split = count // 50000
    nums = [ (count * i // split) for i in range(1, split+1)]
    print(nums)
    
    if not os.path.exists('./data/data_split'):
        os.makedirs('./data/data_split')
    with jsonlines.open(big_file, 'r') as f:
        count_now = 0
        data = []
        split_number = 0
        for i in tqdm(f, total=count):
            count_now += 1
            data.append(i)
            if count_now in nums:
                file_split_name = './data/data_split/{}_split{}.jsonl'.format(filename_no_suffix, split_number)
                with jsonlines.open(file_split_name, 'w') as f:
                    f.write_all(data)
                print('Successful save file as {}'.format(file_split_name))
                count_now = 0
                data = []
                split_number += 1