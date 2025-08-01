import datetime
import pandas as pd

def save_local_file_csv(file, name):
    path_local = f'./data/files/{name}.csv'
    
    try:
        file.to_csv(path_local, sep=';', decimal=',', index=False)
        print(f'Save: {path_local}')
    except (IndexError, TypeError) as error:
        print('Error:', error)

def save_local_file_xlsx(file, name):
    path_local = f'./data/files/{name}.xlsx'
    
    try:
        file.to_excel(path_local, index=False)
        print(f'Save: {path_local}')
    except (IndexError, TypeError) as error:
        print('Error:', error)


def save_daily_allocation_OV(table):
    DATA_HOJE = datetime.datetime.now().strftime('%Y-%m-%d')
    count = 1

    try:
        table_published_1 = pd.read_excel(f'./data/priorizations/Priorizações_{DATA_HOJE}_{count}.xlsx')

        data = table.copy()

        columns_to_convert = [
            'CC', 'SKU', 'CD', 'REGIONAL', 'Customer Group 1', 'OV', 'GrupoKAM', 'Nome 1', 'Item SO'
        ]
        
        for col in data.columns:
            if col in columns_to_convert:
                data[col] = data[col].astype(str)

        for col in table_published_1.columns:
            if col in columns_to_convert:
                table_published_1[col] = table_published_1[col].astype(str)

        table_concat = pd.concat([table_published_1, data], axis = 0, ignore_index = True)
        table_concat = table_concat[table_concat.duplicated(keep=False)]

        if (table_concat.duplicated(subset=['OV', 'SKU']).sum()) > 0:
            table_published_2 = pd.concat([table_concat, data], axis = 0, ignore_index = True)
        else:
            table_published_2 = data

        table_published_2 = table_published_2.drop_duplicates(subset=['OV', 'SKU', 'Item SO'], keep=False)
        
        count = 2

        table_published_2.to_excel(f'./data/priorizations/Priorizações_{DATA_HOJE}_{count}.xlsx', index=False)
        print(f'Priorização {count}, salva com sucesso!')
    except FileNotFoundError as error:
        table.to_excel(f'./data/priorizations/Priorizações_{DATA_HOJE}_{1}.xlsx', index=False)
        print('Priorização 1, salva com sucesso!')