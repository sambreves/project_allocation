from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

class TableProcessor:
    def __init__(self, data):
        self.data = data

    @classmethod
    def create_params(cls, table):
        table_params = table

        table_params['percent_recorrencia'] = (table_params['qtd_meses_faturados'] / 12)
        table_params['sales_IV FLUIDS/Hopistal Care'] = (
                    table_params['sales_revenue_IV FLUIDS & IRRIGATION_ytd'] / table_params['sales_revenue_hp_ytd'])
        table_params['consumo_recorrencia'] = (table_params['Qtd_YTD'] / table_params['qtd_meses_faturados'])

        table_params[[
            'percent_recorrencia', 'sales_IV FLUIDS/Hopistal Care', 'consumo_recorrencia'
        ]] = table_params[[
            'percent_recorrencia', 'sales_IV FLUIDS/Hopistal Care', 'consumo_recorrencia'
        ]].fillna(0)

        return cls(table_params)

    @classmethod
    def create_coefficient(cls, table):
        table_coefficient = table[
            [
                'CC',
                'SKU',
                # 'sales_revenue_IV FLUIDS & IRRIGATION_ytd',
                # 'sales_revenue_DRUGS_ytd',
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
        table_final['coefficient'] = principal_components

        return cls(table_final)