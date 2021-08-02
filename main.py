import pandas as pd
import threading
from db_operations import *
from confusion import *
from check_data import *

excluded_columns = ['id','given_label']
distinct_columns = []
indice_limit = 100

df = pd.read_csv('project_data.csv', index_col=None)

column_list = list(df.columns.values.tolist())

for column in column_list:
    if column in excluded_columns:
        print(column)
    else:
       distinct_model = column.split("_", 1) 
       print(distinct_model)
       t = (distinct_model[0], distinct_model[1])
       print(t)
       distinct_columns.append(t)
       print(distinct_columns)

thread = threading.Thread(target=check_availability)
thread.start()

print('Main process running....')

insert_file(df, distinct_columns, indice_limit)

if thread.is_alive():
    thread.join()
