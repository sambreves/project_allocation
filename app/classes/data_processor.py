from app.utils.read_file import process_data_csv, process_data_excel
import pandas as pd
import datetime

class DataProcessor:
    def __init__(self, data):
        self.data = data

    @classmethod
    def type_columns(cls, file_content, sheet_name=None):
        data = (process_data_csv(file_content) if sheet_name==None else process_data_excel(file_content, sheet_name))

        columns_to_convert = [
            'CC', 'SKU', 'CD', 'REGIONAL', 'Customer Group 1', 'Status verificações', 
            'OV', 'GrupoKAM', 'Nome 1', 'Num Linha', 'Item SO', 'Denominação_2',
            'Tipo de pedido', 'Classificacao', 'Cidade', 'UF'
        ]
        for col in data.columns:
            if col in columns_to_convert:
                data[col] = data[col].astype(str)

        return cls(data)
    
    @classmethod
    def concat_table_billing(cls, table_24, table_25):
        actual_month = datetime.date.today().month
        actual_year = datetime.date.today().year
        last_year = (actual_year - 1)
        last_month = (actual_month - 1)

        month_last_year = 12 - actual_month
        # Filtra os dados do último ano
        table_billing_last_year = table_24[(table_24['Ano'] == last_year)&(table_24['Mês'] >= month_last_year)]
        table_billing_actual_year = table_25[(table_25['Ano'] == actual_year)&(table_25['Mês'] <= last_month)]

        table_billing = pd.concat([table_billing_last_year, table_billing_actual_year])

        table_billing['data'] = pd.to_datetime(table_billing['Ano'].astype(str) + '-' + table_billing['Mês'].astype(str) + '-01')

        return cls(table_billing)
