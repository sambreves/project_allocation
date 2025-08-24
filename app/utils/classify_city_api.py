import pandas as pd
from unidecode import unidecode

def classificar_cidades_por_arquivo(df_cidades: pd.DataFrame, path_populacao_csv: str) -> pd.DataFrame:
    """
    Adiciona uma coluna de classificação logística a um DataFrame de cidades
    usando o arquivo 'populacao_municipios_ibge_2022.xlsx - Municípios.csv'.

    Args:
        df_cidades (pd.DataFrame): DataFrame com as colunas 'Cidade' e 'UF'.
        path_populacao_csv (str): O caminho para o arquivo de população que você forneceu.

    Returns:
        pd.DataFrame: O DataFrame original com a nova coluna 'Classificacao'.
    """
    # --- Validação e Carregamento ---
    if not all(col in df_cidades.columns for col in ['Cidade', 'UF']):
        print("Erro: O DataFrame de entrada precisa conter as colunas 'Cidade' e 'UF'.")
        return pd.DataFrame()
    try:
        # Lê o arquivo CSV fornecido diretamente
        df_ibge = pd.read_csv(path_populacao_csv)
    except FileNotFoundError:
        print(f"Erro: Arquivo de população não encontrado em '{path_populacao_csv}'.")
        print("Certifique-se de que o nome do arquivo e o caminho estão corretos.")
        return pd.DataFrame()

    # --- Dicionário de Capitais e Listas de Classificação ---
    CAPITAIS_BRASIL = {
        'AC': 'Rio Branco', 'AL': 'Maceió', 'AP': 'Macapá', 'AM': 'Manaus', 'BA': 'Salvador',
        'CE': 'Fortaleza', 'DF': 'Brasília', 'ES': 'Vitória', 'GO': 'Goiânia', 'MA': 'São Luís',
        'MT': 'Cuiabá', 'MS': 'Campo Grande', 'MG': 'Belo Horizonte', 'PA': 'Belém',
        'PB': 'João Pessoa', 'PR': 'Curitiba', 'PE': 'Recife', 'PI': 'Teresina',
        'RJ': 'Rio de Janeiro', 'RN': 'Natal', 'RS': 'Porto Alegre', 'RO': 'Porto Velho',
        'RR': 'Boa Vista', 'SC': 'Florianópolis', 'SP': 'São Paulo', 'SE': 'Aracaju', 'TO': 'Palmas'
    }
    CAPITAIS_NORM = {uf: unidecode(capital).title() for uf, capital in CAPITAIS_BRASIL.items()}
    acesso_remoto_ufs = ["AM", "AC", "RR", "AP", "RO"]

    # --- Preparação dos Dados para Junção (Merge) ---
    df_classificado = df_cidades.copy()
    # Normaliza os dados do seu DataFrame de vendas
    df_classificado['Cidade_Norm'] = df_classificado['Cidade'].apply(lambda x: unidecode(str(x)).strip().title())
    df_classificado['UF_Norm'] = df_classificado['UF'].str.strip().upper()

    # Normaliza os dados da planilha do IBGE
    df_ibge['Cidade_Norm'] = df_ibge['Cidade'].apply(lambda x: unidecode(str(x)).strip().title())
    df_ibge['UF_Norm'] = df_ibge['UF'].str.strip().upper()
    
    # Junta o DataFrame do usuário com a base do IBGE
    df_merged = pd.merge(
        df_classificado,
        df_ibge[['Cidade_Norm', 'UF_Norm', 'Populacao']],
        on=['Cidade_Norm', 'UF_Norm'],
        how='left'
    )

    # --- Lógica de Classificação ---
    def classificar_cidade(row):
        capital_do_estado = CAPITAIS_NORM.get(row['UF_Norm'])
        populacao = row['Populacao'] if pd.notna(row['Populacao']) else 0

        if (row['Cidade_Norm'] == capital_do_estado) or (populacao > 700000):
            return "Polo Estratégico"
        if populacao > 100000:
            return "Eixo Regional"
        if row['UF_Norm'] in acesso_remoto_ufs:
            return "Acesso Remoto"
        return "Interior Conectado"

    df_merged['Classificacao'] = df_merged.apply(classificar_cidade, axis=1)

    # Limpeza final
    colunas_finais = list(df_cidades.columns) + ['Classificacao']
    return df_merged[colunas_finais]