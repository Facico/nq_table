import time
import os
import sys
import json
import jsonlines
import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from multiprocessing import Pool
import multiprocessing
import copy
from shutil import copyfile
from nltk import text
from nltk.tokenize import word_tokenize, sent_tokenize
import glob
import sys
import os
from table_utils.utils import *
from tqdm import tqdm
#sys.path.append('/home/fch/OTT-QA/table_crawling/')

#local_path = '/home/fch/OTT-QA/table_crawling/'
local_path = '/data1/fch123/OTT-QA/table_crawling/'
input_htmls = local_path + 'htmls'
output_folder = local_path + 'data/'
#default setting
#default_setting = {'miniumu_row': 8, 'ratio': 0.7, 'max_header': 6, 'min_header': 2}
default_setting = {'miniumu_row': 5, 'ratio': 0.85, 'max_header': 20, 'min_header': 2}

def parse_url_title(url):
    title_pos = url.find('title=')
    amp_pos = url.find('&amp;')
    if(amp_pos == -1):
        amp_pos = url.find(';')
    return url[title_pos+len('title='): amp_pos]
def parse_url_id(url):
    title_pos = url.find('oldid=')
    return url[title_pos+len('oldid='):]
def harvest_tables_nq(f_url, noise=None):
    f, url = f_url
    results = []
    soup = BeautifulSoup(f, 'html.parser')
    tmp = soup.find_all('table')
    rs = []
    rest = []
    for _ in tmp:
        rs.append(_)
    rs = rs + rest
    for it, r in enumerate(rs):
        heads = []
        rows = []
        replicate = {}
        for i, t_row in enumerate(r.find_all('tr')):
            if i == 0:
                for h in t_row.find_all(['th', 'td']):
                    h = remove_ref(h)
                    if len(h.find_all('a')) > 0:
                        tmp = (h.get_text(separator=" ").strip(), process_link(h))
                    else:
                        tmp = (h.get_text(separator=" ").strip(), [])
                    assert len(tmp) and isinstance(tmp[0], str) and isinstance(tmp[1], list)
                    heads.append(tmp)
            else:
                row = []
                for h in t_row.find_all(['th', 'td']):
                    col_idx = len(row)
                    if col_idx in replicate:
                        row.append(replicate[col_idx][1])
                        replicate[col_idx][0] = replicate[col_idx][0] - 1
                        if replicate[col_idx][0] == 0:
                            del replicate[col_idx]
                    h = remove_ref(h)
                    if len(h.find_all('a')) > 0:
                        tmp = (h.get_text(separator=" ").strip(), process_link(h))
                    else:
                        tmp = (h.get_text(separator=" ").strip(), [])
                    if 'rowspan' in h.attrs:
                        # Identify row span cases
                        try:
                            num = int(h['rowspan'])
                            replicate[len(row)] = [num - 1, tmp]
                        except Exception:
                            pass
                    assert len(tmp) and isinstance(tmp[0], str) and isinstance(tmp[1], list)
                    row.append(tmp)
                if noise is None and all([len(cell[0]) == 0 for cell in row]):
                    continue
                else:
                    rows.append(row)
        rows = rows[:20]
        if noise is None and (any([len(row) != len(heads) for row in rows]) or len(rows) < default_setting['miniumu_row']):
            continue
        else:
            try:
                section_title = get_section_title(r)
            except Exception:
                section_title = ''
            try:
                section_text = get_section_text(r)
            except Exception:
                section_text = ''
            title = parse_url_title(url)
            title = re.sub(' - Wikipedia', '', title)
            #url = 'https://en.wikipedia.org/wiki/{}'.format('_'.join(title.split(' ')))
            table_name = title  #os.path.split(f_name)[1].replace('.html', '')
            title = re.sub('_', ' ', title)
            oldid = parse_url_id(url)
            uid = table_name + "_{}_{}".format(oldid, it)
            results.append({'url': url, 'title': title, 'header': heads, 'data': rows,
                            'section_title': section_title, 'section_text': section_text,
                            'uid': uid})
    return results

def inplace_postprocessing(tables, default_setting=default_setting, all_tables=None):
    deletes = []
    for i, table in enumerate(tables):
        # Remove sparse columns
        to_remove = []
        for j, h in enumerate(table['header']):
            if 'Coordinates' in h[0] or 'Image' in h[0]:
                to_remove.append(j)
                continue
            
            count = 0
            total = len(table['data'])
            for d in table['data']:
                if d[j][0] != '':
                    count += 1
            
            if count / total < 0.5:
                to_remove.append(j)
        
        bias = 0
        for r in to_remove:
            del tables[i]['header'][r - bias]
            for _ in range(len(table['data'])):
                del tables[i]['data'][_][r - bias]
            bias += 1
        
        # Remove sparse rows
        to_remove = []
        for k in range(len(table['data'])):
            non_empty = [1 if _[0] != '' else 0 for _ in table['data'][k]]
            if sum(non_empty) < 0.5 * len(non_empty):
                to_remove.append(k)
        
        bias = 0
        for r in to_remove:        
            del tables[i]['data'][r - bias]
            bias += 1

        if all_tables == True:
            continue
        elif len(table['header']) > default_setting['max_header']:
            deletes.append(i)
        elif len(table['header']) <= default_setting['min_header']:
            deletes.append(i)
        else:
            pass
            """count = 0
            total = 0
            for row in table['data']:
                for cell in row:
                    if len(cell[0]) != '':
                        if cell[1] == []:
                            count += 1                    
                        total += 1
            if count / total >= default_setting['ratio']:
                deletes.append(i)"""

    print('out of {} tables, {} need to be deleted'.format(len(tables), len(deletes)))

    bias = 0
    for i in deletes:
        del tables[i - bias]
        bias += 1
    

def distribute_to_separate_files(table):
    if table['uid'] == "Premier_League_4":
        print(table)
    if url2dockey(table['uid']) == "Premier_League_4":
        print(table)
    for row_idx, row in enumerate(table['data']):
        for col_idx, cell in enumerate(row):
            table['data'][row_idx][col_idx][0] = clean_cell_text(cell[0])
    
    for col_idx, header in enumerate(table['header']):
        table['header'][col_idx][0] = clean_cell_text(header[0])

    headers = table['header']
    if headers[0] == '':
        for i in range(len(table['data'])):
            del table['data'][i][0]
        del headers[0]

    if any([_[0] == 'Rank' for _ in headers]):
        if table['data'][0][0][0] == '':
            for i in range(len(table['data'])):
                if table['data'][i][0][0] == '':
                    table['data'][i][0][0] = str(i + 1)

    if any([_[0] == 'Place' for _ in headers]):
        if table['data'][0][0][0] == '':
            for i in range(len(table['data'])):
                if table['data'][i][0][0] == '':
                    table['data'][i][0][0] = str(i + 1)

    """name = url2dockey(re.sub(r'.+\.org', '', table['url']))
    summary = merged_unquote[name]
    table['intro'] = summary

    local_dict = {}
    for i, cell in enumerate(table['header']):
        assert isinstance(cell[0], str) and isinstance(cell[1], list)
        j = 0
        while j < len(cell[1]):
            url = cell[1][j]
            linked_content = merged_unquote[url2dockey(url)]
            if len(linked_content.split(' ')) <= 6:
                del cell[1][j]
            else:
                local_dict[url2dockey(url)] = linked_content
                j += 1

    for i, row in enumerate(table['data']):
        for j, cell in enumerate(row):
            assert isinstance(cell[0], str) and isinstance(cell[1], list)
            k = 0
            while k < len(cell[1]):
                url = cell[1][k]
                linked_content = merged_unquote[url2dockey(url)]
                if len(linked_content.split(' ')) <= 6:
                    del cell[1][k]
                else:
                    local_dict[url2dockey(url)] = linked_content
                    k += 1"""

    table['uid'] = url2dockey(table['uid'])
    f_id = '{}/tables/{}.json'.format(output_folder, table['uid'])
    with open(f_id, 'w') as f:
        json.dump(table, f, indent=2)

    """request_file = f_id.replace('/tables/', '/request/')
    with open(request_file, 'w') as f:
        json.dump(local_dict, f, indent=2)"""

def tokenization_tab(f_n):
    try:
        with open(f_n) as f:
            table = json.load(f)
    except:
        print(f_n)
    with open(f_n) as f:
        table = json.load(f)
    
    for col_idx, cell in enumerate(table['header']):
        table['header'][col_idx][0] = tokenize(cell[0], True)
        for i, ent in enumerate(cell[1]):
            table['header'][col_idx][1][i] = url2dockey(table['header'][col_idx][1][i])

    for row_idx, row in enumerate(table['data']):
        for col_idx, cell in enumerate(row):
            table['data'][row_idx][col_idx][0] = tokenize(cell[0], True)
            for i, ent in enumerate(cell[1]):
                table['data'][row_idx][col_idx][1][i] = url2dockey(table['data'][row_idx][col_idx][1][i])
    
    f_n = f_n.replace('/tables/', '/tables_tok/')
    with open(f_n, 'w') as f:
        json.dump(table, f, indent=2)


def harvest_tables(f_name):
    results = []
    with open(f_name, 'r') as f:
        soup = BeautifulSoup(f, 'html.parser')
        tmp = soup.find_all(class_='wikitable')
        rs = []
        rest = []
        for _ in tmp:
            if _['class'] == ['wikitable', 'sortable']:
                rs.append(_)
            else:
                rest.append(_)
        rs = rs + rest
        for it, r in enumerate(rs):
            heads = []
            rows = []
            replicate = {}
            for i, t_row in enumerate(r.find_all('tr')):
                if i == 0:
                    for h in t_row.find_all(['th', 'td']):
                        h = remove_ref(h)
                        if len(h.find_all('a')) > 0:
                            tmp = (h.get_text(separator=" ").strip(), process_link(h))
                        else:
                            tmp = (h.get_text(separator=" ").strip(), [])
                        assert len(tmp) and isinstance(tmp[0], str) and isinstance(tmp[1], list)
                        heads.append(tmp)
                else:
                    row = []
                    for h in t_row.find_all(['th', 'td']):
                        col_idx = len(row)
                        if col_idx in replicate:
                            row.append(replicate[col_idx][1])
                            replicate[col_idx][0] = replicate[col_idx][0] - 1
                            if replicate[col_idx][0] == 0:
                                del replicate[col_idx]

                        h = remove_ref(h)
                        if len(h.find_all('a')) > 0:
                            tmp = (h.get_text(separator=" ").strip(), process_link(h))
                        else:
                            tmp = (h.get_text(separator=" ").strip(), [])

                        if 'rowspan' in h.attrs:
                            # Identify row span cases
                            try:
                                num = int(h['rowspan'])
                                replicate[len(row)] = [num - 1, tmp]
                            except Exception:
                                pass
                        assert len(tmp) and isinstance(tmp[0], str) and isinstance(tmp[1], list)
                        row.append(tmp)

                    if all([len(cell[0]) == 0 for cell in row]):
                        continue
                    else:
                        rows.append(row)

            rows = rows[:20]
            if any([len(row) != len(heads) for row in rows]) or len(rows) < default_setting['miniumu_row'] or len(heads) == 0:
                continue
            else:
                try:
                    section_title = get_section_title(r)
                except Exception:
                    section_title = ''
                try:
                    section_text = get_section_text(r)
                except Exception:
                    section_text = ''
                title = soup.title.string
                title = re.sub(' - Wikipedia', '', title)
                url = 'https://en.wikipedia.org/wiki/{}'.format('_'.join(title.split(' ')))
                table_name = os.path.split(f_name)[1].replace('.html', '')
                uid = table_name + "_{}".format(it)
                results.append({'url': url, 'title': title, 'header': heads, 'data': rows,
                                'section_title': section_title, 'section_text': section_text,
                                'uid': uid})
    return results

def proccess_data(filename, steps, split_number=None):
    train_data = []
    # '/home/fch/OTT-QA/table_crawling/simplified-nq-train.jsonl'
    with jsonlines.open(filename, 'r') as f:
        print('start load json data')
        for i in tqdm(f):
            train_data.append(i)
    if '1' in steps:
        # Step1: Harvesting the tables
        text_url_list = []
        table_num_dict = {}
        print('start reading data for table and url')
        for i in tqdm(range(len(train_data))):
            text_url_list.append((train_data[i]["document_text"], train_data[i]["document_url"]))
            title = parse_url_title(train_data[i]["document_url"])
            table_num_dict[title] = 0
        rs = pool.map(harvest_tables_nq, text_url_list)
        tables = []
        for r in rs:
            tables = tables + r
        print('start get table')
        inplace_postprocessing(tables, all_tables=False)
        have_table_num = 0
        for table_x in tables:
            if(table_x == []): continue
            title = parse_url_title(table_x["url"])
            if(table_num_dict[title] == 0): have_table_num += 1
            table_num_dict[title] += 1
        if split_number is not None:
            with open('{}/processed_new_table_postfiltering_split{}.json'.format(output_folder, split_number), 'w') as f:
                json.dump(tables, f, indent=2)
            with open('{}/processed_new_table_num_split{}.json'.format(output_folder, split_number), 'w') as f:
                json.dump(table_num_dict, f, indent=2)
        else:
            with open('{}/processed_new_table_postfiltering.json'.format(output_folder), 'w') as f:
                json.dump(tables, f, indent=2)
            with open('{}/processed_new_table_num.json'.format(output_folder), 'w') as f:
                json.dump(table_num_dict, f, indent=2)
        print('have table data: {} / {}'.format(have_table_num, len(text_url_list)))
        print("Step1-2: Finsihing postprocessing the tables")
    if '3' in steps:
        # Step 4: distribute the tables into separate files
        with open('{}/processed_new_table_postfiltering_split{}.json'.format(output_folder, split_number), 'r') as f:
            tables = json.load(f)
        
        print('strat distributing the tables')
        pool.map(distribute_to_separate_files, tables)
        print("Step3: Finishing distributing the requests")

    if '4' in steps:
        # Step 6: tokenize the tables and request
        print("Step4: Starting tokenizing")
        if not os.path.exists('{}/request_tok'.format(output_folder)):
            os.mkdir('{}/request_tok'.format(output_folder))
        if not os.path.exists('{}/tables_tok'.format(output_folder)):
            os.mkdir('{}/tables_tok'.format(output_folder))

        #pool.map(tokenization_req, glob.glob('{}/request/*.json'.format(output_folder)))
        pool.map(tokenization_tab, glob.glob('{}/tables/*.json'.format(output_folder)))
        print("Step4: Finishing tokenization")
    
    if '6' in steps:
        # Step7: Generate tables without hyperlinks
        table_set = {}
        for file_name in glob.glob('{}/tables_tok/*.json'.format(output_folder)):
            with open(file_name, 'r') as f:
                table = json.load(f)
            for i, h in enumerate(table['header']):
                table['header'][i] = table['header'][i][0]

            for i, row in enumerate(table['data']):
                for j, cell in enumerate(row):
                    table['data'][i][j] = table['data'][i][j][0]
            file_name = os.path.basename(file_name)
            file_name = os.path.splitext(file_name)[0]
            table_set[file_name] = table
        with open('../data/all_plain_tables_split{}.json'.format(split_number), 'w') as f:
            json.dump(table_set, f)
        print("Step6: Finished generating plain tables")

def merge_split(split_file_path):
    tables = {}
    table_num_dict = {}
    table_plain = {}
    print('start merge json file from split json')
    for file in os.listdir(split_file_path):
        file_path = os.path.join(split_file_path, file)
        split_number = int(file.split('split')[1].split('.')[0])
        print(split_number)
        with open('{}/processed_new_table_postfiltering_split{}.json'.format(output_folder, split_number), 'w') as f:
            json.load(f)
            tables.update(f)
        with open('{}/processed_new_table_num_split{}.json'.format(output_folder, split_number), 'w') as f:
            json.load(f)
            table_num_dict.update(f)
        with open('../data/all_plain_tables_split{}.json'.format(split_number), 'w') as f:
            json.load(f)
            table_plain.update(f)
    
    with open('{}/processed_new_table_postfiltering.json'.format(output_folder), 'w') as f:
        json.dump(tables, f)
    with open('{}/processed_new_table_num.json'.format(output_folder), 'w') as f:
        json.dump(table_num_dict, f)
    with open('../data/all_plain_tables.json', 'w') as f:
        json.dump(table_plain, f)

if __name__ == '__main__':
    steps = ['1', '3', '4', '6']#['1', '3', '4']
    cores = multiprocessing.cpu_count()
    pool = Pool(cores)
    print("Initializing the pool of cores")

    if not os.path.exists(output_folder):
        os.mkdir(output_folder)
    if not os.path.exists('{}/tables'.format(output_folder)):
        os.mkdir('{}/tables'.format(output_folder))
    if not os.path.exists('{}/request'.format(output_folder)):
        os.mkdir('{}/request'.format(output_folder))

    filenames = glob.glob('{}/*.html'.format(input_htmls))
    file_dict = {}
    for i in filenames:
        file_dict[i] = 1

    split_file_path = output_folder + 'data_split/'
    for file in os.listdir(split_file_path):
        file_path = os.path.join(split_file_path, file)
        split_number = int(file.split('split')[1].split('.')[0])
        print(split_number)
        proccess_data(file_path, steps=steps, split_number=split_number)
    """with jsonlines.open('/home/fch/OTT-QA/table_crawling/nq-small-train-1000.json', 'r') as f:
        for i in f:
            train_data.append(i)"""
    
    """for i in range(len(train_data)):
        text, url = train_data[i]["document_text"], train_data[i]["document_url"]
        title = parse_url_title(url)
        if(input_htmls + '/' + title + '.html' in file_dict):
            results = harvest_tables_nq(text, url, noise=None)
            print(results)
            print(len(results))

            results_ott = harvest_tables(input_htmls + '/' + title + '.html')
            print(results_ott)
            print(len(results_ott))
            if(results_ott != []):
                break"""
    