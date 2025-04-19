from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.decomposition import PCA

import pandas as pd

class TableProcessor:
    def __init__(self, data):
        self.data = data

    @classmethod
    def create_params(cls, table):
        table_params = table

        table_params['percent_recorrencia'] = (table_params['qtd_meses_faturados'] / 12)
        table_params['sales_IV FLUIDS & IRRIGATION/Hopistal Care'] = (
                    table_params['sales_revenue_IV FLUIDS & IRRIGATION_ytd'] / table_params['sales_revenue_hp_ytd'])
        table_params['sales_DRUGS/Hopistal Care'] = (
                    table_params['sales_revenue_DRUGS_ytd'] / table_params['sales_revenue_hp_ytd'])
        table_params['consumo_recorrencia'] = (table_params['Qtd_YTD'] / table_params['qtd_meses_faturados'])

        table_params[[
            'percent_recorrencia', 'sales_IV FLUIDS & IRRIGATION/Hopistal Care', 'sales_DRUGS/Hopistal Care', 'consumo_recorrencia'
        ]] = table_params[[
            'percent_recorrencia', 'sales_IV FLUIDS & IRRIGATION/Hopistal Care', 'sales_DRUGS/Hopistal Care', 'consumo_recorrencia'
        ]].fillna(0)

        return cls(table_params)

    @classmethod
    def create_coefficient_PCA(cls, table):
        table_coefficient = table[
            [
                'CC',
                'SKU',
                'sales_revenue_IV FLUIDS & IRRIGATION_ytd',
                'sales_revenue_DRUGS_ytd',
                'portfolio_strategic_IV FLUIDS & IRRIGATION',
                'portfolio_strategic_DRUGS',
                'PercentGPS+_YTD',
                'percent_recorrencia',
                'sales_IV FLUIDS/Hopistal Care'
            ]
        ]

        # Normalizar as variáveis
        scaler = StandardScaler()
        table_coefficient_normalized = scaler.fit_transform(table_coefficient)

        # Aplicar o PCA - Análise de componentes principais
        pca = PCA(n_components=1)
        principal_components = pca.fit_transform(table_coefficient_normalized)

        # Criar table coefficients
        table_final = table
        table_final['coefficient_PCA'] = principal_components

        return cls(table_final)
    
    @classmethod
    def create_coefficient_normalized(cls, table, weights=None, invert=True, sbas=['IV FLUIDS & IRRIGATION', 'DRUGS']):
        result = pd.DataFrame()

        # Cria o coefficient de clientes para cada SBA
        for sba in sbas:
            # Colunas padrão que serão utilizadas
            columns_coefficient = [
                f'sales_revenue_{sba}_ytd',
                f'portfolio_strategic_{sba}',
                f'PercentGPS+_YTD',
                f'percent_recorrencia',
                f'sales_{sba}/Hopistal Care'
            ]
            # Verificação de colunas
            for col in columns_coefficient:
                if col not in table.columns:
                    raise ValueError(f'Coluna ausente no DataFrame: {col}')
                
            default_weights = {
                f'sales_revenue_{sba}_ytd': 0.15,
                f'portfolio_strategic_{sba}': 0.17,
                f'PercentGPS+_YTD': 0.20,
                f'percent_recorrencia': 0.35,
                f'sales_{sba}/Hopistal Care': 0.13,
            }

            # Se weights não forem passados, usa 1 para todos
            if weights is None:
                weights = default_weights
            else:
                # Verifica se todos os weights foram fornecidos
                missing_cols = [col for col in columns_coefficient if col not in weights]
                if missing_cols:
                    raise ValueError(f'weights faltando para as colunas: {missing_cols}')

            table_sba = table[table['Terapia']==sba]

            # Normalização min-max
            scaler = MinMaxScaler()
            table_normalized = scaler.fit_transform(table_sba[columns_coefficient])

            # Aplicar weights
            table_weighted = table_normalized * [weights[col] for col in columns_coefficient]

            # Soma ponderada
            coefficients = table_weighted.sum(axis=1)

            # Inverte para que menor coef seja maior prioridade
            if invert:
                coefficients = 1 - coefficients

            # Adiciona a coluna 'coefficient'
            table_sba = table_sba.copy()
            table_sba['coefficient_NM'] = coefficients

            if result.empty:
                result = table_sba
            else:
                result = pd.concat([result, table_sba], ignore_index=True)
            
            weights = None

        result = result.fillna(0)

        return cls(result)