import pandas as pd
from statsmodels.tsa.stattools import adfuller, grangercausalitytests

# ============================
# Função para testar estacionaridade
def testar_estacionaridade(serie):
    resultado = adfuller(serie.dropna())
    p_valor = resultado[1]
    return p_valor

# ============================
# Função principal para rodar toda a análise
def analisar_causalidade(df, max_lag=7):
    resultados = {}

    for sku in df['SKU'].unique():
        df_sku = df[df['SKU'] == sku].sort_values('Data')

        data_granger = df_sku[['Saída', 'Entrada + Estoque']].dropna()

        # Se a série for muito pequena, pula
        if len(data_granger) < (max_lag + 5):
            print(f"Poucos dados para SKU {sku}. Pulando.\n")
            continue

        # # Testar estacionaridade
        # p_valor_cancelada = testar_estacionaridade(data_granger['Saída'])
        # p_valor_entrada = testar_estacionaridade(data_granger['Entrada + Estoque'])

        # # Se não for estacionária, diferenciar
        # if p_valor_cancelada >= 0.05:
        #     data_granger['Saída'] = data_granger['Saída'].diff()
        
        # if p_valor_entrada >= 0.05:
        #     data_granger['Entrada + Estoque'] = data_granger['Entrada + Estoque'].diff()

        # Remover NaNs gerados pela diferenciação
        data_granger = data_granger.dropna()

        # Rodar o teste de Granger
        try:
            granger_result = grangercausalitytests(data_granger, maxlag=max_lag)
            
            p_values = [granger_result[i+1][0]['ssr_ftest'][1] for i in range(max_lag)]
            melhor_p = min(p_values)
            melhor_lag = p_values.index(melhor_p) + 1
            causalidade = 'Sim' if melhor_p < 0.05 else 'Não'
            
            resultados[sku] = {
                'Melhor Lag (dias)': melhor_lag,
                'p-valor': melhor_p,
                'Causalidade Entrada -> Perda': causalidade
            }

            print(f"SKU: {sku} | Melhor lag: {melhor_lag} | p-valor: {melhor_p:.4f} | Causalidade: {causalidade}\n")

        except Exception as e:
            print(f"Erro no SKU {sku}: {e}\n")

    # Criar DataFrame final
    tabela_resultados = pd.DataFrame(resultados).T.reset_index()
    tabela_resultados.rename(columns={'index': 'SKU'}, inplace=True)

    return tabela_resultados

# ============================
# Carregar seu arquivo Excel
df = pd.read_excel('./data/files/analise_estoque_2024.xlsx', sheet_name='entrada-estoque')

# Garantir que a coluna Data é datetime
df['Data'] = pd.to_datetime(df['Data'])

# Rodar a análise
tabela_final = analisar_causalidade(df, max_lag=30)

# Mostrar a tabela
print("\nTabela final de causalidade:")
print(tabela_final)






