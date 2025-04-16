import pandas as pd
import io

def process_sap_report(file_content):
    # Processar o conteúdo do arquivo
    lines = file_content.split('\n')
    
    # Encontrar o início da tabela de dados
    start_indices = [i for i, line in enumerate(lines) if line.startswith('| Tp.operaç.|Dt.lçto.  |Criado em |')]
    
    if not start_indices:
        return pd.DataFrame()  # Retorna DataFrame vazio se não encontrar os cabeçalhos
    
    # Coletar todas as linhas de dados
    data_lines = []
    for start_idx in start_indices:
        # Pegar linhas até a próxima linha divisória longa
        end_idx = start_idx + 1
        while end_idx < len(lines) and not lines[end_idx].startswith('--------------------------------------------------------------------------------------------------------------------------------'):
            end_idx += 1
        
        # Adicionar as linhas de dados (ignorando linhas de cabeçalho e divisórias)
        data_lines.extend([line for line in lines[start_idx+1:end_idx] 
                          if line.startswith('|') and not line.startswith('|--')])
    
    # Juntar todas as linhas de dados e criar um DataFrame
    data_str = '\n'.join(data_lines)
    
    # Ler como CSV usando pipe como delimitador
    df = pd.read_csv(io.StringIO(data_str), 
                    delimiter='|', 
                    header=None,
                    skipinitialspace=True,
                    dtype=str,
                    na_values=['', ' '],
                    keep_default_na=False)
    
    # Remover colunas vazias (geradas pelos pipes no início e fim de cada linha)
    df = df.dropna(how='all', axis=1)
    
    # Definir os nomes das colunas (baseado no cabeçalho original)
    columns = [
        'Tp.operaç.', 'Dt.lçto.', 'Criado em', 'CDst', 'CtgI', 'TpDV', 'TipFt', 
        'Contract', 'OrdCliente', 'Nº doc.ref', 'Artigo', 'Cliente', 'Dom./Exp.', 
        'IC / TP', 'Quant_BUM', 'Sales revenue', 'Freight bi', 'Insurance', 
        'IC Com Inc', 'ContMarkup', 'Discount', 'A_Sales_de', 'COGS Cux.o', 
        'c_MOC', 'F.prod.cos', 'A_Freight', 'O_Revenue', 'V.prod.cos', 
        'Other Disc', 'UMB1', 'UMB2', 'Qtd.vendas'
    ]
    
    # Verificar se o número de colunas bate
    if len(df.columns) == len(columns):
        df.columns = columns
    else:
        # Se não bater, usar as colunas genéricas
        df.columns = [f'Col_{i}' for i in range(len(df.columns))]
    
    # Limpeza dos dados - remover espaços em branco
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    
    # Converter colunas numéricas
    numeric_cols = ['Quant_BUM', 'Sales revenue', 'Freight bi', 'Insurance', 
                   'IC Com Inc', 'ContMarkup', 'Discount', 'A_Sales_de', 
                   'COGS Cux.o', 'c_MOC', 'F.prod.cos', 'A_Freight', 
                   'O_Revenue', 'V.prod.cos', 'Other Disc', 'Qtd.vendas']
    
    for col in numeric_cols:
        if col in df.columns:
            # Substituir vírgula por ponto e converter para float
            df[col] = df[col].str.replace('.', '').str.replace(',', '.').str.strip()
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

# Exemplo de uso:
with open('./data/files/KE24_2024.TXT', 'r', encoding='latin1') as f:
    file_content = f.read()

df = process_sap_report(file_content)
# df.to_csv('Amostra.csv')
print(df.head())
