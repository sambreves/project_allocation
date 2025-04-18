from app.utils.read_file import process_data_csv, process_data_excel

class DataProcessor:
    def __init__(self, data):
        self.data = data

    @classmethod
    def type_columns(cls, file_content, sheet_name=None):
        data = (process_data_csv(file_content) if sheet_name==None else process_data_excel(file_content, sheet_name))

        columns_to_convert = [
            'CC', 'SKU', 'CD', 'REGIONAL', 'Customer Group 1', 'Status verificações', 
            'OV', 'GrupoKAM', 'Nome 1', 'Num Linha', 'Item SO'
        ]
        for col in data.columns:
            if col in columns_to_convert:
                data[col] = data[col].astype(str)

        return cls(data)
    
