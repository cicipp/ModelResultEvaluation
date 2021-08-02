import pandas as pd
from db_operations import *

def check_available_confusion():
    if check_count()[0] >= 1000:
        return True
    else:
        return False

def calculate_confusion(df):

    a_a = 0
    a_b = 0
    b_a = 0
    b_b = 0
    

    for i in range(1000):
        if df['GIVEN_LABEL'].iloc[i] == df['MODEL_LABEL'].iloc[i]:
            if df['GIVEN_LABEL'].iloc[i] == 'A':
                a_a += 1
            else:
                b_b += 1
        else:
            if df['GIVEN_LABEL'].iloc[i] == 'A':
                a_b += 1
            else:
                b_a += 1

    result_list = [a_a, a_b, b_a, b_b]
    return result_list