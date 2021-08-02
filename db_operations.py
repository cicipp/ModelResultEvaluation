import pyodbc
import pandas as pd

def insert_file(df, model_column_list, indice_limit):
    cnxn = pyodbc.connect(r'Driver=SQL Server;Server=.;Database=melis;Trusted_Connection=yes;')
    list_df = [df[i:i+indice_limit] for i in range(0,df.shape[0],indice_limit)]
    cursor = cnxn.cursor()
    
    for k in range(len(list_df)):
        query_string_list = []
        for i in range(indice_limit):
            model_indice = 0
            while model_indice < len(model_column_list):
                query_string = """
                            INSERT INTO 
                                dbo.MODEL_RESULT(
                                        ID, 
                                        GIVEN_LABEL, 
                                        MODEL_NUMBER, 
                                        MODEL_LABEL, 
                                        MODEL_VALUE
                                                )
                                VALUES(
                                    {id},
                                    '{given_label}',
                                    {model_number},
                                    '{model_label}',
                                    {model_value}
                                    )    
        """.format(
                    id = list_df[k]['id'].iloc[i],
                    given_label = list_df[k]['given_label'].iloc[i],
                    model_number = model_column_list[model_indice][0][5:],
                    model_label = model_column_list[model_indice][1].upper(),
                    model_value = list_df[k][str(model_column_list[model_indice][0] + '_' + model_column_list[model_indice][1])].iloc[i]
                )
                query_string_list.append(query_string)
                model_indice += 1
        for query in query_string_list:
            cursor.execute(query)
        cnxn.commit()
  
    cnxn.close()

def fetch_maximums(starting_point):
    cnxn = pyodbc.connect(r'Driver=SQL Server;Server=.;Database=melis;Trusted_Connection=yes;')
    df = pd.read_sql_query('EXEC CALCULATE_MAXIMUM {start_id}, {end_id}'.format(start_id = starting_point, end_id = starting_point + 1000), cnxn)
    cnxn.close()
    return df

def check_count():
    cnxn = pyodbc.connect(r'Driver=SQL Server;Server=.;Database=melis;Trusted_Connection=yes;')
    cursor = cnxn.cursor()
    query = """
            SELECT
                COUNT(*)
            FROM
                dbo.MODEL_RESULT
            """
    cursor.execute(query)
    row = cursor.fetchone()
    print(row)
    cnxn.close()
    return row
    

def save_confusion(starting_index, result_list):
    cnxn = pyodbc.connect(r'Driver=SQL Server;Server=.;Database=melis;Trusted_Connection=yes;')
    cursor = cnxn.cursor() 
    query = """INSERT INTO
                dbo.CONFUSION_MATRIX(
                    [START_ID], 
                    [A_A], 
                    [A_B], 
                    [B_A], 
                    [B_B]
                )
            VALUES
                (
                    {starting_id},
                    {a_a},
                    {a_b},
                    {b_a},
                    {b_b}
                ) 
    """.format( 
        starting_id = starting_index, 
        a_a = result_list[0], 
        a_b = result_list[1], 
        b_a = result_list[2], 
        b_b = result_list[3])
    cursor.execute(query)
    cnxn.commit()
    cnxn.close()

