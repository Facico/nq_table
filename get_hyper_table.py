import os
import json
from tqdm import tqdm
import multiprocessing
from functools import partial
from multiprocessing import Pool
import time

def parse_url_title(url):
    title_pos = url.find('title=')
    amp_pos = url.find('&amp;')
    if(amp_pos == -1):
        amp_pos = url.find(';')
        if(amp_pos == -1):
            amp_pos = 0
    if title_pos == -1:
        title_pos = url.find(r'/wiki/')
        return url[title_pos+len(r'/wiki/'):].strip(r'/')
    return url[title_pos+len('title='): amp_pos].strip(r'/')

def get_hyper_list(tablex, title_url):
    table_header = tablex['header']
    datax = tablex['data']
    n_row, n_column = len(datax), len(datax[0])
    url_list = []
    for j in range(n_column):
        if(table_header[j][1] != []):
            url_list.append(table_header[j][1][0])
        for i in range(n_row):
            if(datax[i][j][1] != []):
                url_list.append(datax[i][j][1][0])
    hyper_table = []
    for urlx in url_list:
        #print(urlx)
        url_n = urlx
        if 'en.wikipedia.org' not in urlx:
            url_n = r'https://en.wikipedia.org' + urlx
        page_title = parse_url_title(url_n)
        if page_title in title_url:
            hyper_table = hyper_table + title_url[page_title]
        else:
            hyper_table = hyper_table + [['not find', urlx]]
    return {'table_name':tablex['file_name'], 'hyper_table': hyper_table}

def get_table_from_wiki(urlx, wiki_title_url):
    url_n = urlx
    if 'en.wikipedia.org' not in urlx:
        url_n = r'https://en.wikipedia.org' + urlx
    page_title = parse_url_title(url_n)
    if page_title in wiki_title_url:
        return wiki_title_url[page_title]
    else:
        return None

if __name__ == "__main__":
    cores = multiprocessing.cpu_count()
    steps = ['2']#['1', '2']
    if '1' in steps:
        # 用nq里处理出的table来附加
        table_path = "/data1/fch123/OTT-QA/table_crawling/data/tables"
        table_url_path = '/data1/fch123/OTT-QA/table_crawling/url_table_all.json'
        table_data = []
        print('get file...')
        tot = 0
        for file in tqdm(os.listdir(table_path)):
            file_path = os.path.join(table_path, file)
            with open(file_path, 'r') as f:
                table_file = json.load(f)
                table_file['file_name'] = file
                table_data.append(table_file)
        
        table_url = {}
        title_url = {}
        with open(table_url_path, 'r') as f:
            table_url = json.load(f)
            for k, v in table_url.items():
                title = parse_url_title(k)
                title_url[title] = v
        
        hyper_dict = {}

        for i in tqdm(table_data):
            ans = get_hyper_list(i, title_url)
            table_name, hyper_table = ans['table_name'], ans['hyper_table']
            hyper_dict[table_name] = hyper_table
        
        with open('./data/hyper_table_list.json', 'w') as f:
            json.dump(hyper_dict, f)
    
    if '2' in steps:
        with open('./data/hyper_table_list.json', 'r') as f:
            hyper_dict = json.load(f)
        
        wiki_table_path = '/data1/fch123/OTT-QA/table_crawling/data/Wiki/tables/'
        print('get wiki file...')
        tot = 0
        table_data = []
        for file in tqdm(os.listdir(wiki_table_path)):
            file_path = os.path.join(wiki_table_path, file)
            with open(file_path, 'r') as f:
                table_file = json.load(f)
                table_file['file_name'] = file
                table_data.append(table_file)
        
        wiki_table_url_path = '/data1/fch123/OTT-QA/table_crawling/url_table_wiki.json'
        wiki_title_url = {}
        with open(wiki_table_url_path, 'r') as f:
            table_url = json.load(f)
            for k, v in table_url.items():
                title = parse_url_title(k)
                wiki_title_url[title] = v

        hyper_dict_new = {}
        for k, v in hyper_dict.items():
            if len(v) == 0:
                hyper_dict_new[k] = v
                continue
            hyper_listx = []
            for i in v:
                if i[0] == 'not find':
                    url = i[1]
                    json_name = get_table_from_wiki(i[1], wiki_title_url)
                    if json_name is not None:
                        for j in json_name:
                            hyper_listx.append(['wiki', j[0]])
                    else:
                        hyper_listx.append(i)
                else:
                    hyper_listx.append(i)
            
            hyper_dict_new[k] = hyper_listx
        
        with open('./data/hyper_table_list_add_wiki.json', 'w') as f:
            json.dump(hyper_dict_new, f, indent=2)
    