- **bm25_get_answer.py**：从bm25检索结果中划分answer
  - 1、在所有candidate answer中，只留下非html的文本部分，分成50个文件`'./data/id_to_answer_split{}.json'.format(id)`
  - 2、从bm25检索分数文件`'./data/bm25_result_all.json'`，对top100的进行精确匹配，得到50个匹配文件`'./data/bm25_pn_split{}.json'.format(id)`
  - 3、合并这50个文件为`'./data/bm25_pn_all.json'   `
  - 4、计算`p@1(5,10,20,50,100)`的得分



- **bm25_merge.py**：把`'bm25_result_split{}.json'.format(id)`合并，为`'./data/bm25_result_one.json'`或`'./data/data_not_one.json'`
-  **merge_one_and_not.py**：合并网址出现一次的和多次的

- **nq_table_bm25.ipynb**：bm25检索的实验部分

-  **nq_table_bm25.py**：bm25检索，保留文件为`'./data/bm25_result_split{}.json'.format(it_num)`。使用的文件是tabfact_process中的`'table_text_horizontal'`字段来对query检索

-  **nq_table_bm25_not_one.py**：对网址出现多次的检索

-  **tabfact_preprocess.py**：将提取出的table文件处理出horizontal、vertical、template的方式

-  **search_table.py**：得到url到uid的字典`'url_table.json'`可能对应多个
-  **try_all_nq.py**：使用OTT的方式处理完整的nq数据
-  **try_simple.py**：使用OTT的方式处理nq simple的数据
-  **split_file.py**：把simple nq的文件分割
- **shiyan.py**：不用管它
- 其他：原OTT脚本

## 超链接部分
-  **get_hyper_table.py**：得到对bm25 top100搜索出的超链接table，要考虑wiki中的table，得到文件`'hyper_table_list_add_wiki.json'`
-  **bm25_get_answer_add_wiki.py**: 将超链接搜索出的table部分匹配
-  **merge_hyper_to_pn.py**： 将超链接搜索出的结果融合进原来的top100中
-  **try_wikepedia.py**： 用OTT提供的wiki的数据得到table
