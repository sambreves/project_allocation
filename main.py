from app.utils.read_data import detect_enconding

import pandas as pd

# table_billing = DataProcessor(pd.read_csv('data/files/KE24_2024.txt', encoding='utf-8', delimiter='/t'))

detect_enconding('data/files/KE24_2024.txt')

