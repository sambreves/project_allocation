from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.decomposition import PCA
from sklearn.linear_model import LinearRegression
from pulp import LpProblem, LpVariable, lpSum, LpMinimize, PULP_CBC_CMD
from unidecode import unidecode

import pandas as pd
import numpy as np

NUM_CAIXAS_LIPURO_50ML = 10
NUM_CAIXAS_LIPURO_20ML = 5
NUM_CAIXAS_LIPURO_100ML = 10
LIMITE_MAX_CONSUMO_ESTOQUE = 0.81

SKU_DRUGS = ['3547817', '3547825', '3547833']
GRUPO_CAM = ["REDE D'OR", 'DASA', 'AMERICAS', 'AMIL', 'Oncoclínicas']

class TableProcessor:
    def __init__(self, data):
        self.data = data

    @classmethod
    def create_params(cls, table):
        table_params = table

        table_params['pendent/stock_today'] = np.where(
            (
                table_params['Estoque'] > 0
            ),
            table_params.groupby(['SKU', 'CD'])['Pendente'].transform('sum') / table_params['Estoque'],
            0
        )

        table_params['percent_recorrencia'] = (table_params['qtd_meses_faturados'] / 12)
        
        table_params['consumo_recorrencia'] = (table_params['Qtd_YTD'] / table_params['qtd_meses_faturados'])

        table_params[[
            'percent_recorrencia',
            'sales_hp_with_IV FLUIDS & IRRIGATION',
            'sales_hp_with_DRUGS', 
            'consumo_recorrencia',
            'portfolio_strategic_IV FLUIDS & IRRIGATION',
            'portfolio_strategic_DRUGS',
        ]] = table_params[[
            'percent_recorrencia',
            'sales_hp_with_IV FLUIDS & IRRIGATION',
            'sales_hp_with_DRUGS', 
            'consumo_recorrencia',
            'portfolio_strategic_IV FLUIDS & IRRIGATION',
            'portfolio_strategic_DRUGS',
        ]].fillna(0)

        table_params['novo'] = np.where(
            (
                table_params['Qtd_YTD'] == 0
            ) & (
                table_params['Pendente'] > 0
            ),
            1,
            0
        )

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
                f'sales_hp_with_{sba}',
                f'portfolio_strategic_{sba}',
                f'PercentGPS+_YTD',
                f'percent_recorrencia',
                f'novo'
            ]
            # Verificação de colunas
            for col in columns_coefficient:
                if col not in table.columns:
                    raise ValueError(f'Coluna ausente no DataFrame: {col}')
                
            default_weights = {
                f'portfolio_strategic_{sba}': 0.30,
                f'PercentGPS+_YTD': 0.20,
                f'percent_recorrencia': 0.30,
                f'sales_hp_with_{sba}': 0.10,
                f'novo': 0.10
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
    
    @classmethod
    def allocate(cls, table):

        table = table.copy()

        skus = table['SKU'].drop_duplicates().tolist()
        cds = table['CD'].drop_duplicates().tolist()

        listOV = []
        listSKU = []
        alocationVolume = []

        dictTableAlocation = {
            'OV': listOV,
            'SKU': listSKU,
            'AllocatedVolume': alocationVolume
        }

        for cd in cds:
            for sku in skus:
                sumPendente = table.loc[(table['SKU'] == sku) & (table['CD'] == cd), 'pending_analysis'].sum()
                sumEstoque = table.loc[(table['SKU'] == sku) & (table['CD'] == cd), 'Estoque'].max()

                branch = ['CD0']
                warehouse = table.loc[(table['SKU'] == sku) & (table['CD'] == cd), 'OV'].tolist()

                if len(warehouse) > 0:
                    if sumPendente > sumEstoque:
                        branch = ['CD0', 'DUMMY']

                        diff = sumPendente - sumEstoque

                        stockCD0 = table.loc[(table['SKU'] == sku) & (table['CD'] == cd), ['Estoque']].drop_duplicates().values.tolist()[0][0]
                        stockDUMMY = diff

                        supply = {
                            'CD0': stockCD0,
                            'DUMMY': stockDUMMY
                        }

                        infoTable = table.loc[(table['SKU'] == sku) & (table['CD'] == cd), ['OV', 'SKU', 'pending_analysis',
                                                                                            'coefficient_NM']].reset_index().drop('index', axis=1)
                        infoTable['CoefficientDummy'] = 0

                        demand = {}
                        coefficient_NM = {}
                        coefficientDummy = {}
                        coefficient = {
                            'CD0': coefficient_NM,
                            'DUMMY': coefficientDummy
                        }

                        for info in infoTable.itertuples():
                            demand[info.OV] = info.pending_analysis
                            coefficient_NM[info.OV] = info.coefficient_NM
                            coefficientDummy[info.OV] = info.CoefficientDummy

                        prob = LpProblem('Transportation', LpMinimize)

                        routes = [(i, j) for i in branch for j in warehouse]

                        amount_vars = LpVariable.dicts('Amountships', (branch, warehouse), 0)

                        prob += lpSum([amount_vars[i][j] * coefficient[i][j] for (i, j) in routes])                      
                        
                        # # Variáveis binárias para controle de atendimento total
                        # y_vars = LpVariable.dicts('AttendOV', warehouse, cat='Binary')

                        # # Garante que a OV só será atendida se puder ser completamente alocada
                        # for j in warehouse:
                        #     prob += lpSum(amount_vars[i][j] for i in branch) == demand[j] * y_vars[j]

                        for j in warehouse:
                            prob += lpSum(amount_vars[i][j] for i in branch) >= demand[j]

                        for i in branch:
                            prob += lpSum(amount_vars[i][j] for j in warehouse) <= supply[i]

                        prob.solve(PULP_CBC_CMD(msg=0))

                        for v in prob.variables():
                            listOV.append(v.name)
                            listSKU.append(sku)
                            alocationVolume.append(v.varValue)

                    if sumPendente < sumEstoque:
                        diff = sumEstoque - sumPendente

                        tableOVDummy = pd.DataFrame({'OV': ['0000000'], 'SKU': [sku], 'pending_analysis': [diff], 'coefficient_NM': [0]})

                        stockCD0 = table.loc[(table['SKU'] == sku) & (table['CD'] == cd), ['Estoque']].drop_duplicates().values.tolist()[0][0]

                        supply = {
                            'CD0': stockCD0,
                        }

                        infoTable = table.loc[(table['SKU'] == sku) & (table['CD'] == cd), ['OV', 'SKU', 'pending_analysis',
                                                                                            'coefficient_NM']].reset_index().drop('index', axis=1)
                        infoTable = pd.concat([infoTable, tableOVDummy], axis=0).reset_index(drop=True)

                        demand = {}
                        coefficient_NM = {}
                        coefficient = {
                            'CD0': coefficient_NM,
                        }

                        for info in infoTable.itertuples():
                            demand[info.OV] = info.pending_analysis
                            coefficient_NM[info.OV] = info.coefficient_NM

                        prob = LpProblem('Transportation', LpMinimize)

                        routes = [(i, j) for i in branch for j in warehouse]

                        amount_vars = LpVariable.dicts('Amountships', (branch, warehouse), 0)

                        prob += lpSum([amount_vars[i][j] * coefficient[i][j] for (i, j) in routes])

                        for j in warehouse:
                            prob += lpSum(amount_vars[i][j] for i in branch) >= demand[j]

                        for i in branch:
                            prob += lpSum(amount_vars[i][j] for j in warehouse) <= supply[i]

                        prob.solve(PULP_CBC_CMD(msg=0))

                        for v in prob.variables():
                            listOV.append(v.name)
                            listSKU.append(sku)
                            alocationVolume.append(v.varValue)

                    if sumPendente == sumEstoque:
                        stockCD0 = table.loc[(table['SKU'] == sku) & (table['CD'] == cd), ['Estoque']].drop_duplicates().values.tolist()[0][0]

                        supply = {
                            'CD0': stockCD0,
                        }

                        infoTable = table.loc[(table['SKU'] == sku) & (table['CD'] == cd), ['OV', 'SKU', 'pending_analysis',
                                                                                            'coefficient_NM',]].reset_index().drop('index', axis=1)
                        demand = {}
                        coefficient_NM = {}
                        coefficient = {
                            'CD0': coefficient_NM,
                        }

                        for info in infoTable.itertuples():
                            demand[info.OV] = info.pending_analysis
                            coefficient_NM[info.OV] = info.coefficient_NM

                        prob = LpProblem('Transportation', LpMinimize)

                        routes = [(i, j) for i in branch for j in warehouse]

                        amount_vars = LpVariable.dicts('Amountships', (branch, warehouse), 0)

                        prob += lpSum([amount_vars[i][j] * coefficient[i][j] for (i, j) in routes])

                        for j in warehouse:
                            prob += lpSum(amount_vars[i][j] for i in branch) >= demand[j]

                        for i in branch:
                            prob += lpSum(amount_vars[i][j] for j in warehouse) <= supply[i]

                        prob.solve(PULP_CBC_CMD(msg=0))

                        for v in prob.variables():
                            listOV.append(v.name)
                            listSKU.append(sku)
                            alocationVolume.append(v.varValue)

        tableTransportProblemStock = pd.DataFrame(dictTableAlocation)
        tableTransportProblemStock['Branch'] = tableTransportProblemStock['OV'].str[:15]
        tableTransportProblemStock['Branch'] = tableTransportProblemStock['Branch'].str[12:]
        tableTransportProblemStock['OV'] = tableTransportProblemStock['OV'].str[16:]
        tableTransportProblemStock = tableTransportProblemStock.loc[tableTransportProblemStock['Branch'] == 'CD0', ['OV', 'SKU', 'AllocatedVolume']].reset_index(drop=True)

        table = table.merge(tableTransportProblemStock, left_on=['OV', 'SKU'], right_on=['OV', 'SKU'], how='left')
        table['OV'] = table['OV'].str[:7]
        table = table.fillna(0)

        return cls(table)
    
    @classmethod
    def treat_allocation_table(cls, table):

        tableMain = table.copy()

        #Zerar as OV's que receberam alocação parcial
        tableMain['AllocatedVolume'] = np.where(
            (
                tableMain['AllocatedVolume'] > 0
            ) & (
                tableMain['AllocatedVolume'] < tableMain['pending_analysis']
            ),
            0,
            tableMain['AllocatedVolume']
        )

        tableMain['AllocatedVolume'] = np.where(tableMain['SKU'] == '3547817',
                                            tableMain['AllocatedVolume'] / NUM_CAIXAS_LIPURO_50ML,
                                            tableMain['AllocatedVolume'])
        tableMain['AllocatedVolume'] = np.where(tableMain['SKU'] == '3547825',
                                                tableMain['AllocatedVolume'] / NUM_CAIXAS_LIPURO_20ML,
                                                tableMain['AllocatedVolume'])
        tableMain['AllocatedVolume'] = np.where(tableMain['SKU'] == '3547833',
                                                tableMain['AllocatedVolume'] / NUM_CAIXAS_LIPURO_100ML,
                                                tableMain['AllocatedVolume'])
        
        tableMain['Pendente'] = np.where(tableMain['SKU'] == '3547817', tableMain['Pendente'] / NUM_CAIXAS_LIPURO_50ML,
                                     tableMain['Pendente'])
        tableMain['Pendente'] = np.where(tableMain['SKU'] == '3547825', tableMain['Pendente'] / NUM_CAIXAS_LIPURO_20ML,
                                        tableMain['Pendente'])
        tableMain['Pendente'] = np.where(tableMain['SKU'] == '3547833', tableMain['Pendente'] / NUM_CAIXAS_LIPURO_100ML,
                                        tableMain['Pendente'])
        
        tableMain['pending_analysis'] = np.where(tableMain['SKU'] == '3547817', tableMain['pending_analysis'] / NUM_CAIXAS_LIPURO_50ML,
                                     tableMain['pending_analysis'])
        tableMain['pending_analysis'] = np.where(tableMain['SKU'] == '3547825', tableMain['pending_analysis'] / NUM_CAIXAS_LIPURO_20ML,
                                        tableMain['pending_analysis'])
        tableMain['pending_analysis'] = np.where(tableMain['SKU'] == '3547833', tableMain['pending_analysis'] / NUM_CAIXAS_LIPURO_100ML,
                                        tableMain['pending_analysis'])
        conditions = [
            tableMain['Status verificações'] == 'B',
            tableMain['Estoque'] <= 0,
            tableMain['Volume'] <= 0,
            tableMain['AllocatedVolume'] == 0,
            tableMain['AllocatedVolume'] < tableMain['pending_analysis'],
        ]

        choices = [
            'Bloqueio de Crédito',
            'Estoque Indisponível',
            'Volume Regional Indisponível',
            'Liberação Adiada - Estoque Crítico',
            'Liberação Parcial - Estoque Crítico',
        ]

        tableMain['Denominação_2'] = np.where(
            tableMain['Denominação_2'] == 'nan',
            np.select(conditions, choices, default=''),
            tableMain['Denominação_2']
        )

        tableMain.rename(columns={'Denominação_2': 'Status'}, inplace=True)

        tableMain = tableMain.rename(columns={'Num Linha': 'Item SO'})
        
        return cls(tableMain)
    
    @classmethod
    def create_minimum_order(cls, table):
        table_minimum_order = table.copy()

        mascara_invalidos = (table_minimum_order['Limite_Alerta_%'] <= 0)

        table_minimum_order['Limite_Alerta_%_Corrigido'] = table_minimum_order['Limite_Alerta_%'].where(~mascara_invalidos, np.nan)

        medias_grupo = table_minimum_order.groupby(['UF', 'Classificacao'])['Limite_Alerta_%_Corrigido'].transform('mean')

        table_minimum_order['Limite_Alerta_%_Corrigido'] = table_minimum_order['Limite_Alerta_%_Corrigido'].fillna(medias_grupo)

        table_minimum_order['Limite_Alerta_%'] = table_minimum_order['Limite_Alerta_%_Corrigido']

        table_minimum_order['AllocatedVolumeCC'] = table_minimum_order.groupby(['CC'])['AllocatedVolume'].transform('sum')
        table_minimum_order['frete_cc'] = table_minimum_order['Custo_Unitario_Medio'] * table_minimum_order['AllocatedVolumeCC']

        ratio = table_minimum_order['frete_cc'] / table_minimum_order['sales_revenue_cc']

        table_minimum_order['frete_cc/sales_revenue_cc'] = np.nan_to_num(ratio, nan=0.0, posinf=0.0, neginf=0.0)

        mascara = (
            (table_minimum_order['Customer Group 1'] != 'Público') &
            (table_minimum_order['Classificacao'] != 'Polo Estratégico') &
            ~(table_minimum_order['GrupoKAM'].isin(GRUPO_CAM)) &
            ~(table_minimum_order['SKU'].isin(SKU_DRUGS)) &
            (table_minimum_order['frete_cc/sales_revenue_cc'] > table_minimum_order['Limite_Alerta_%'])
        )
        
        table_minimum_order['AllocatedVolumeValidated'] = table_minimum_order['AllocatedVolume'].where(~mascara, 0)
        table_minimum_order['Status'] = table_minimum_order['Status'].where(~mascara, 'Aguardando Revisão: Custo de Frete Elevado')

        table_minimum_order['AllocatedVolume'] = table_minimum_order['AllocatedVolumeValidated']

        table_minimum_order = table_minimum_order.reindex(['OV','SKU', 'Item SO', 'AllocatedVolume', 'DataPreparo', 'CD', 'CC', 'Nome 1', 'Pendente',
                                    'Customer Group 1', 'GrupoKAM', 'REGIONAL', 'Valor item OV', 'Status', 'Tipo de pedido', 'Cidade', 'UF',
                                    'coefficient_NM', 'Limite_Alerta_%', 'ConsumoEstoque', 'ConsumoSaldoRegional', 'frete_cc/sales_revenue_cc'], axis=1)

        return cls(table_minimum_order)
    
    @classmethod
    def estimate_base_cost_per_group(cls, table_frete):
        """
        Estima o custo base de envio (custo fixo) para cada grupo de UF e Classificação
        usando regressão linear sobre os dados históricos.
        """        
        # Classifica as cidades da base de faturamento
        # (Usando a função offline que já criamos)
        df_faturamento_classificado = table_frete.copy()

        # Agrupa por UF e Classificação para a análise
        grupos = df_faturamento_classificado.groupby(['UF', 'Classificacao'])
        resultados_custo_base = []

        for nome, grupo in grupos:
            # A regressão linear precisa de pelo menos 2 pontos para ser calculada
            if len(grupo) < 2:
                continue

            X = grupo[['Quantidade']]
            y = grupo['AFrete']

            model = LinearRegression()
            model.fit(X, y)

            # O intercepto é o nosso custo base estimado
            intercepto = model.intercept_
            
            # O custo base não pode ser negativo, então pegamos o máximo entre o resultado e 0
            custo_base_estimado = max(0, intercepto)
            
            resultados_custo_base.append({
                'UF': nome[0],
                'Classificacao': nome[1],
                'Custo_Base_Estimado': custo_base_estimado
            })
        
        df_custo_base = pd.DataFrame(resultados_custo_base)
        return cls(df_custo_base)
        
    @classmethod
    def suggest_dynamic_minimum_quantity(cls, df_validado, df_custo_base, coluna_preco_unitario):
        df_result = df_validado.copy()

        # Junta o custo base estimado na tabela principal
        df_result = pd.merge(
            df_result,
            df_custo_base,
            on=['UF', 'Classificacao'],
            how='left'
        )
        # Para grupos que não tiveram custo estimado, usa um valor padrão (mediana geral)
        fallback_custo_base = df_result['Custo_Base_Estimado'].median()
        df_result['Custo_Base_Estimado'] = df_result['Custo_Base_Estimado'].fillna(fallback_custo_base)

        # O resto da lógica é a mesma, mas usando o Custo_Base_Estimado
        mascara_invalidacao_frete = (
            (df_result['Customer Group 1'] != 'Público') &
            (df_result['Classificacao'] != 'Polo Estratégico') &
            ~(df_result['GrupoKAM'].isin(GRUPO_CAM)) &
            ~(df_result['SKU'].isin(SKU_DRUGS)) &
            (df_result['frete_cc/sales_revenue_cc'] > df_result['Limite_Alerta_%'])
        )
        linhas_para_calcular = (df_result['AllocatedVolumeValidated'] == 0) & mascara_invalidacao_frete

        F_base = df_result['Custo_Base_Estimado'] # <- Usando o custo dinâmico
        F_var = df_result['Custo_Unitario_Medio']
        L = df_result['Limite_Alerta_%']
        P = df_result[coluna_preco_unitario]
        P = P.replace(0, np.nan).ffill().fillna(0.01)

        denominador = (L * P) - F_var
        
        quantidade_minima = np.where(
            (denominador > 0) & linhas_para_calcular,
            np.ceil(F_base / denominador),
            np.nan
        )

        df_result['Sugestao_Quantidade_Minima'] = quantidade_minima
        return cls(df_result)