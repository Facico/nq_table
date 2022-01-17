from __future__ import division
import random
import sys
import io
import os
import logging
import re
import pandas as pd
import os.path as op
from tqdm import tqdm
from collections import Counter, OrderedDict
import argparse
from multiprocessing import Pool
import multiprocessing
import json

entity_linking_pattern = re.compile('#.*?;-*[0-9]+,(-*[0-9]+)#')
fact_pattern = re.compile('#(.*?);-*[0-9]+,-*[0-9]+#')
unk_pattern = re.compile('#([^#]+);-1,-1#')
TSV_DELIM = "\t"
TBL_DELIM = " ; "

def join_unicode(delim, entries):
    #entries = [_.decode('utf8') for _ in entries]
    return delim.join(entries)

def parse_fact(fact):
    fact = re.sub(unk_pattern, '[UNK]', fact)
    chunks = re.split(fact_pattern, fact)
    output = ' '.join([x.strip() for x in chunks if len(x.strip()) > 0])
    return output

def tabfact_template(tablex, scan = 'horizontal'):
    #print(tablex['file_name'])
    table_header = tablex['header']
    datax = tablex['data']
    n_row, n_column = len(datax), len(datax[0])
    #print(n_row, n_column)
    table_cells = []
    if scan == 'horizontal':
        for i in range(n_row):
            table_cells.append('row {} is :'.format(i + 1))
            this_row = []
            for j in range(n_column):
                this_row.append('{} is {}'.format(table_header[j][0], datax[i][j][0]))
            this_row = join_unicode(TBL_DELIM, this_row)
            table_cells.append(this_row)
            table_cells.append('.')
    elif scan == 'vertical':
        for j in range(n_column):
            table_cells.append('{} are :'.format(table_header[j][0]))
            this_column = []
            for i in range(n_row):
                this_column.append('row {} is {}'.format(i, datax[i][j][0]))
            this_column = join_unicode(TBL_DELIM, this_column)
            table_cells.append(this_column)
            table_cells.append('.')
    else:
        pass

    table_str = ' '.join(table_cells)
    #print(table_str)
    results = {'table_text': table_str}
    for k, v in tablex.items():
        results[k] = v
    return results

def tabfact_template_all(tablex):
    #print(tablex['file_name'])
    table_header = tablex['header']
    datax = tablex['data']
    n_row, n_column = len(datax), len(datax[0])
    #print(n_row, n_column)
    table_cells = []
    results = {}
    for i in range(n_row):
        table_cells.append('row {} is :'.format(i + 1))
        this_row = []
        for j in range(n_column):
            this_row.append('{} is {}'.format(table_header[j][0], datax[i][j][0]))
        this_row = join_unicode(TBL_DELIM, this_row)
        table_cells.append(this_row)
        table_cells.append('.')

    table_str = ' '.join(table_cells)
    results['table_text_horizontal'] = table_str
    table_cells = []
    for j in range(n_column):
        table_cells.append('{} are :'.format(table_header[j][0]))
        this_column = []
        for i in range(n_row):
            this_column.append('row {} is {}'.format(i, datax[i][j][0]))
        this_column = join_unicode(TBL_DELIM, this_column)
        table_cells.append(this_column)
        table_cells.append('.')

    table_str = ' '.join(table_cells)
    results['table_text_vertical'] = table_str
    table_str = ' '.join(table_cells)
    results['file_name'] = tablex['file_name']
    results['uid'] = tablex['uid']

    return results
if __name__ == '__main__':
    #table_path = './data/tables_tok'
    #output_path = './data/tabfact_process'

    """
    wiki html
    """
    table_path = './data/Wiki/tables_tok'
    output_path = './data/Wiki/tabfact_process'

    if not os.path.exists(output_path):
        os.mkdir(output_path)

    table_data = []
    print('get file...')
    for file in tqdm(os.listdir(table_path)):
        file_path = os.path.join(table_path, file)
        with open(file_path, 'r') as f:
            table_file = json.load(f)
            table_file['file_name'] = file
            table_data.append(table_file)
    
    
    cores = 2 #multiprocessing.cpu_count()
    pool = Pool(cores)
    rs = pool.map(tabfact_template_all, table_data)
    print('processing...')
    for i in tqdm(range(len(rs))):
        tablex = rs[i]
        file_name = tablex['file_name']
        file_path = os.path.join(output_path, file_name)
        with open(file_path, 'w') as f:
            json.dump(tablex, f)
