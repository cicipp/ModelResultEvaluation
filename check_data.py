import time
from db_operations import *
from confusion import *
from check_data import *

def check_availability():
    starting_index = 1
    available_confusion = False

    while available_confusion == False:
        available_confusion = check_available_confusion()
        print('Confusion check running...')
        time.sleep(5)
        

    for _ in range(50001):
        current_df = fetch_maximums(starting_index)
        result_list = calculate_confusion(current_df)
        save_confusion(starting_index, result_list)
        starting_index += 1