import pandas as pd
import numpy as np
import datetime
from tabulate import tabulate

LIMITE_MAX_CONSUMO_ESTOQUE = 0.81
LIMITE_MAX_CONSUMO_SALDO_REGIONAL = 1.33
LIMITE_MAX_SKU_ESTRATEGICO_FLUIDS = 0.85
LIMITE_MAX_SKU_ESTRATEGICO_DRUGS= 0.97
PESO_CAM_FLUIDS = 0.5
PESO_CAM_DRUGS = 1.7
PESO_DISTRIBUIDOR_FLUIDS = 0.5
PESO_DISTRIBUIDOR_DRUGS = 0.3
PESO_PUBLICO_FLUIDS = 5
PESO_PUBLICO_DRUGS = 1.7
PESO_GRUPOS_IMPORTANTES_DRUGS = 1.8
DATA_HOJE = datetime.datetime.now().strftime('%Y-%m-%d')
HORA_ATUAL = datetime.datetime.now().strftime('%H.%M.%S')
NUM_CAIXAS_LIPURO_50ML = 10
NUM_CAIXAS_LIPURO_20ML = 5
NUM_CAIXAS_LIPURO_100ML = 10

SKU = ['200102', '200104', '200105', '200110', '200111', '200112', '200114', '200115', '200124', '200125',
        '200130', '200131', '200132', '200134', '200135','200142', '200154', '200164',
        '200165', '200181', '200182', '200183', '3547825', '3547833', '3547817']

GRUPOS_PRIORIDADE_LIPURO_50 = ['Oswaldo Cruz', 'HCOR', 'BP SP', 'Albert Einstein', 'São Camilo',
                            'Real Hospital Portugues de', 'Santa Casa de Misericórdia da Bahia',
                            'Erechim', 'Divina Providencia', "REDE D'OR", 'DASA', 'AMERICAS', 'AMIL']

actual_month = datetime.date.today().month
actual_year = datetime.date.today().year
last_year = (actual_year - 1)
last_month = (actual_month - 1)

class TableCreator:

    def __init__(self, data):
        self.data = data

    @classmethod
    def create_table_main(cls, table_customers, table_products):
        # Filtrar e obter listas únicas de CC e SKU
        list_cc = table_customers['CC'].drop_duplicates()
        list_sku = table_products['SKU'].drop_duplicates()

        # Criar DataFrame de permutações usando pandas
        table_main = pd.DataFrame([(cc, sku) for cc in list_cc for sku in list_sku], columns=['CC', 'SKU'])

        # Filtrar colunas relevantes de table_customers e table_products
        # table_customers = table_customers[['CC', 'REGIONAL', 'CD', 'ClasseABC']]
        # table_sba = table_products[
        #     table_products['Terapia'].isin(['IV FLUIDS & IRRIGATION', 'DRUGS', 'PARENTERAL NUTRITION'])
        # ][['SKU', 'Terapia']]

        return cls(table_main)

    @classmethod
    def create_table_customers(cls, table):
        table_customers = table.loc[:, ['CC', 'REGIONAL', 'ClasseABC', 'Customer Group 1', 'GrupoKAM', 'Nome 1', 'CD']]

        return cls(table_customers)

    @classmethod
    def create_table_products(cls, table):
        table_products = table.loc[:, ['SKU', 'Terapia']]

        return cls(table_products)

    @classmethod
    def create_table_billing_ytd(cls, table):
        table_billing_ytd = table.copy()

        # Agrupa e soma os valores necessários
        aggregation_columns = ['Quantidade', 'SalesRevenue', 'ASalesDe', 'COGS', 'ContMarkup',
                               'Discount', 'FprodCos', 'FreteBi', 'Insurance', 'MOC', 'VprodCos', 
                               'AFrete', 'ICComInc']
        
        table_billing_ytd = table_billing_ytd.groupby([
            'CC', 'SKU', 'Terapia'
        ])[aggregation_columns].sum().reset_index()

        # Calcula os custos e receitas
        table_billing_ytd['Costs_YTD'] = table_billing_ytd[['FprodCos', 'VprodCos', 'COGS', 'MOC']].sum(axis=1)
        table_billing_ytd['Sales_YTD'] = table_billing_ytd['SalesRevenue'] - table_billing_ytd['Discount'] + table_billing_ytd[[
            'FreteBi', 'Insurance', 'ContMarkup', 'ICComInc'
        ]].sum(axis=1) - table_billing_ytd['ASalesDe']
        table_billing_ytd['Sales+_YTD'] = table_billing_ytd['Sales_YTD'] - table_billing_ytd['AFrete']
        table_billing_ytd['GPS_YTD'] = table_billing_ytd['Sales_YTD'] - table_billing_ytd['Costs_YTD']
        table_billing_ytd['GPS+_YTD'] = table_billing_ytd['Sales+_YTD'] - table_billing_ytd['Costs_YTD']

        # Calcula as porcentagens
        table_billing_ytd['PercentGPS+_YTD'] = np.where(
            table_billing_ytd['Sales+_YTD'] == 0, 0, table_billing_ytd['GPS+_YTD'] / abs(table_billing_ytd['Sales+_YTD'])
        )
        table_billing_ytd['PercentGPS_YTD'] = np.where(
            table_billing_ytd['Sales_YTD'] == 0, 0, table_billing_ytd['GPS_YTD'] / abs(table_billing_ytd['Sales_YTD'])
        )

        # Seleciona e renomeia as colunas
        selected_columns = ['CC', 'SKU', 'Terapia', 'Quantidade', 'SalesRevenue', 'AFrete', 'Costs_YTD', 
                            'Sales_YTD', 'Sales+_YTD', 'GPS_YTD', 'PercentGPS_YTD', 'GPS+_YTD', 
                            'PercentGPS+_YTD']
        table_billing_ytd = table_billing_ytd[selected_columns]
        table_billing_ytd.columns = ['CC', 'SKU', 'Terapia', 'Qtd_YTD', 'SalesRevenue_YTD', 'AFrete_YTD', 
                                     'Costs_YTD', 'Sales_YTD', 'Sales+_YTD', 'GPS_YTD', 'PercentGPS_YTD', 'GPS+_YTD', 
                                     'PercentGPS+_YTD']

        # Preenche valores nulos com zero
        table_billing_ytd = table_billing_ytd.fillna(0)

        return cls(table_billing_ytd)

    @classmethod
    def create_table_pareto_customers(cls, table):
        list_terapia = ['IV FLUIDS & IRRIGATION', 'DRUGS', 'PARENTERAL NUTRITION']
        result = pd.DataFrame()

        for terapia in list_terapia:
            table_cum_sum_sales = table.loc[table['Terapia'] == terapia, ['CC', 'SalesRevenue_YTD']].groupby('CC')['SalesRevenue_YTD'].sum().reset_index()
            table_cum_sum_sales = table_cum_sum_sales.sort_values('SalesRevenue_YTD', ascending=False).reset_index(drop=True)
            table_cum_sum_sales['sales_revenue_cum'] = table_cum_sum_sales['SalesRevenue_YTD'].cumsum().astype(float).round(1)
            sum_sales = table_cum_sum_sales['SalesRevenue_YTD'].sum()
            table_cum_sum_sales[f'%pareto_cc_{terapia}'] = table_cum_sum_sales['sales_revenue_cum'] / sum_sales
            table_cum_sum_sales = table_cum_sum_sales.drop(['SalesRevenue_YTD', 'sales_revenue_cum'], axis=1)

            if result.empty:
                result = table_cum_sum_sales
            else:
                result = result.merge(table_cum_sum_sales, on='CC', how='outer')

        result = result.fillna(0)
        return cls(result)

    @classmethod
    def create_table_pareto_private_customers(cls, table, table_customers):
        list_terapia = ['IV FLUIDS & IRRIGATION', 'DRUGS', 'PARENTERAL NUTRITION']
        result = pd.DataFrame()
        for terapia in list_terapia:
            table_cum_sum_sales_private = table.loc[table['Terapia'] == terapia, ['CC', 'SalesRevenue_YTD']].groupby('CC')['SalesRevenue_YTD'].sum().reset_index()
            table_cum_sum_sales_private = table_cum_sum_sales_private.merge(table_customers[['CC', 'Customer Group 1']], on='CC', how='inner')
            table_cum_sum_sales_private = table_cum_sum_sales_private[table_cum_sum_sales_private['Customer Group 1'] == 'Privados']
            table_cum_sum_sales_private = table_cum_sum_sales_private.sort_values('SalesRevenue_YTD', ascending=False).reset_index(drop=True)
            table_cum_sum_sales_private['sales_revenue_cum'] = table_cum_sum_sales_private['SalesRevenue_YTD'].cumsum().astype(float).round(1)
            sum_sales_private = table_cum_sum_sales_private['SalesRevenue_YTD'].sum()
            table_cum_sum_sales_private[f'%pareto_private_cc_{terapia}'] = table_cum_sum_sales_private['sales_revenue_cum'] / sum_sales_private
            table_cum_sum_sales_private = table_cum_sum_sales_private.drop(['SalesRevenue_YTD', 'sales_revenue_cum', 'Customer Group 1'], axis=1)

            if result.empty:
                result = table_cum_sum_sales_private
            else:
                result = result.merge(table_cum_sum_sales_private, on='CC', how='outer')

        result = result.fillna(0)
        return cls(result)

    @classmethod
    def create_table_pareto_products(cls, table):
        list_terapia = ['IV FLUIDS & IRRIGATION', 'DRUGS', 'PARENTERAL NUTRITION']
        result = pd.DataFrame()

        for terapia in list_terapia:
            table_cum_sum_sku = table.loc[table['Terapia'] == terapia, ['SKU', 'SalesRevenue_YTD']].groupby('SKU')['SalesRevenue_YTD'].sum().reset_index()
            table_cum_sum_sku = table_cum_sum_sku.sort_values('SalesRevenue_YTD', ascending=False).reset_index(drop=True)
            table_cum_sum_sku['sales_revenue_cum'] = table_cum_sum_sku['SalesRevenue_YTD'].cumsum().astype(float).round(1)
            sum_sales_sku = table_cum_sum_sku['SalesRevenue_YTD'].sum()
            table_cum_sum_sku[f'%pareto_sku_{terapia}'] = table_cum_sum_sku['sales_revenue_cum'] / sum_sales_sku
            table_cum_sum_sku = table_cum_sum_sku.drop(['SalesRevenue_YTD', 'sales_revenue_cum'], axis=1)

            if result.empty:
                result = table_cum_sum_sku
            else:
                result = result.merge(table_cum_sum_sku, on='SKU', how='outer')

        result = result.fillna(0)
        return cls(result)

    @classmethod
    def create_table_portfolio(cls, table_billing_ytd, table_pareto_products):
        list_terapia = ['IV FLUIDS & IRRIGATION', 'DRUGS', 'PARENTERAL NUTRITION']
        table = table_billing_ytd.merge(table_pareto_products, on='SKU', how='left')

        table_portfolio = table.loc[
            (table['Terapia'] == 'IV FLUIDS & IRRIGATION') & (table['%pareto_sku_IV FLUIDS & IRRIGATION'] <= LIMITE_MAX_SKU_ESTRATEGICO_FLUIDS), ['CC', 'SKU']
        ]
        table_portfolio = table_portfolio.groupby('CC')['SKU'].count().reset_index()
        table_portfolio.columns = ['CC', 'portfolio_strategic_IV FLUIDS & IRRIGATION']

        count_sku = table.loc[
            (table['%pareto_sku_IV FLUIDS & IRRIGATION'] <= LIMITE_MAX_SKU_ESTRATEGICO_FLUIDS) & (table['%pareto_sku_IV FLUIDS & IRRIGATION'] > 0), 'SKU'
        ].nunique()

        table_portfolio['portfolio_strategic_IV FLUIDS & IRRIGATION'] /= count_sku

        for terapia in list_terapia[1:]:
            table_portfolio_terapia = table.loc[
                (table['Terapia'] == terapia) & (table[f'%pareto_sku_{terapia}'] <= LIMITE_MAX_SKU_ESTRATEGICO_DRUGS), ['CC', 'SKU']
            ]
            table_portfolio_terapia = table_portfolio_terapia.groupby('CC')['SKU'].count().reset_index()
            table_portfolio_terapia.columns = ['CC', f'portfolio_strategic_{terapia}']
            table_portfolio = table_portfolio.merge(table_portfolio_terapia, on='CC', how='outer')

            count_sku_terapia = table.loc[
                (
                    table[f'%pareto_sku_{terapia}'] <= LIMITE_MAX_SKU_ESTRATEGICO_DRUGS
                ) & (
                    table[f'%pareto_sku_{terapia}'] > 0
                ), 'SKU'
            ].nunique()

            table_portfolio[f'portfolio_strategic_{terapia}'] /= count_sku_terapia

        table_portfolio = table_portfolio.fillna(0)
        return cls(table_portfolio)

    @classmethod
    def create_table_billing_customers_sba(cls, table):
        list_terapia = ['IV FLUIDS & IRRIGATION', 'DRUGS', 'PARENTERAL NUTRITION']
        result = pd.DataFrame()

        for terapia in list_terapia:
            table_billing_cc = table.loc[table['Terapia'] == terapia, ['CC', 'Terapia', 'SalesRevenue_YTD']].groupby(['CC', 'Terapia'])['SalesRevenue_YTD'].sum().reset_index()
            table_billing_cc.columns = ['CC', 'Terapia', f'sales_revenue_{terapia}_ytd']

            if result.empty:
                result = table_billing_cc
            else:
                result = result.merge(table_billing_cc, on=['CC', 'Terapia'], how='outer')

        result = result.fillna(0)
        return cls(result)

    @classmethod
    def create_table_billing_customers_hospital_care(cls, table):
        table_billing_cc_hospital_care = table.groupby('CC')['SalesRevenue_YTD'].sum().reset_index()
        table_billing_cc_hospital_care.columns = ['CC', 'sales_revenue_hp_ytd']
        return cls(table_billing_cc_hospital_care)

    @classmethod
    def create_table_purchase_frequency(cls, table):
        table_purchase_frequency = table.loc[table['Quantidade'] != 0].groupby(['CC', 'SKU', 'Mês'])['Quantidade'].sum().reset_index()
        table_purchase_frequency = table_purchase_frequency.groupby(['CC', 'SKU'])['Mês'].count().reset_index()
        table_purchase_frequency.columns = ['CC', 'SKU', 'qtd_meses_faturados']
        return cls(table_purchase_frequency)
    
    @classmethod
    def create_table_purchase_frequency_customers(cls, table):
        table_purchase_frequency_customers = table.loc[table['Quantidade'] != 0].groupby(['CC', 'Mês'])['Quantidade'].sum().reset_index()
        table_purchase_frequency_customers = table_purchase_frequency_customers.groupby(['CC'])['Mês'].count().reset_index()
        table_purchase_frequency_customers.columns = ['CC', 'qtd_meses_faturados_clientes']
        return cls(table_purchase_frequency_customers)

    @classmethod
    def create_table_last_month_purchase(cls, table):
        table_last_month_billing = table.groupby(['CC', 'SKU'])['Mês'].max().reset_index()
        table_last_month_billing.columns = ['CC', 'SKU', 'ultimo_mes_compra']
        return cls(table_last_month_billing)

    @classmethod
    def create_table_volume_sku(cls, table):
        table_volume_sku = table.loc[
            (table['Mês n']==actual_month),
            ['SKU', 'REGIONAL', 'Volume']].reset_index(drop=True)

        return cls(table_volume_sku)
    
    @classmethod
    def create_table_pending_customers(cls, table):
        table_pending = table.copy()

        table_pending['DataPreparo'] = pd.to_datetime(table_pending['DataPreparo'], format='mixed')
        table_pending = table_pending.loc[(table_pending['ano'] == datetime.datetime.now().year), :]
        table_pending = table_pending.loc[table_pending['DataPreparo'] <= DATA_HOJE, :]
        table_pending = table_pending.loc[table_pending['Status verificações'] != 'B', :]
        table_pending = table_pending.groupby(['CC', 'SKU'])['Pendente'].sum().reset_index()

        table_pending['Pendente'] = np.where(table_pending['SKU'] == '3547817', table_pending['Pendente'] * NUM_CAIXAS_LIPURO_50ML, table_pending['Pendente'])
        table_pending['Pendente'] = np.where(table_pending['SKU'] == '3547825', table_pending['Pendente'] * NUM_CAIXAS_LIPURO_20ML, table_pending['Pendente'])
        table_pending['Pendente'] = np.where(table_pending['SKU'] == '3547833', table_pending['Pendente'] * NUM_CAIXAS_LIPURO_100ML, table_pending['Pendente'])

        return cls(table_pending)
    
    @classmethod
    def create_table_pending(cls, table):
        table_pending = table.copy()

        table_pending['DataPreparo'] = pd.to_datetime(table_pending['DataPreparo'], format='mixed')
        table_pending = table_pending.loc[(table_pending['ano'] == datetime.datetime.now().year), :]
        table_pending = table_pending.loc[table_pending['DataPreparo'] <= DATA_HOJE, :]
        table_pending['OV'] = table_pending['OV'] + '_' + table_pending['Num Linha']
        table_pending = table_pending.groupby(['OV', 'Num Linha', 'CC', 'SKU', 'CD', 'REGIONAL', 'Status verificações', 'DataPreparo', 'Valor item OV'])['Pendente'].sum().reset_index()

        table_pending['Pendente'] = np.where(table_pending['SKU'] == '3547817', table_pending['Pendente'] * NUM_CAIXAS_LIPURO_50ML, table_pending['Pendente'])
        table_pending['Pendente'] = np.where(table_pending['SKU'] == '3547825', table_pending['Pendente'] * NUM_CAIXAS_LIPURO_20ML, table_pending['Pendente'])
        table_pending['Pendente'] = np.where(table_pending['SKU'] == '3547833', table_pending['Pendente'] * NUM_CAIXAS_LIPURO_100ML, table_pending['Pendente'])

        return cls(table_pending)

    @classmethod
    def create_table_billing_actual_year(cls, table):
        # Filtra os dados do ano atual
        table_billing = table[table['Ano'] == actual_year]

        # Agrupa e soma os valores necessários
        aggregation_columns = ['Quantidade', 'SalesRevenue', 'ASalesDe', 'COGS', 'ContMarkup', 
                               'Discount', 'FprodCos', 'FreteBi', 'Insurance', 'MOC', 'VprodCos', 
                               'AFrete', 'ICComInc']
        table_billing = table_billing.groupby([
            'CC', 'SKU'
        ])[aggregation_columns].sum().reset_index()

        # Calcula os custos e receitas
        table_billing['Costs_YTD'] = table_billing[['FprodCos', 'VprodCos', 'COGS', 'MOC']].sum(axis=1)
        table_billing['Sales_YTD'] = table_billing['SalesRevenue'] - table_billing['Discount'] + table_billing[[
            'FreteBi', 'Insurance', 'ContMarkup', 'ICComInc'
        ]].sum(axis=1) - table_billing['ASalesDe']
        table_billing['Sales+_YTD'] = table_billing['Sales_YTD'] - table_billing['AFrete']
        table_billing['GPS_YTD'] = table_billing['Sales_YTD'] - table_billing['Costs_YTD']
        table_billing['GPS+_YTD'] = table_billing['Sales+_YTD'] - table_billing['Costs_YTD']

        # Calcula as porcentagens
        table_billing['PercentGPS+_YTD'] = np.where(
            table_billing['Sales+_YTD'] == 0, 0, table_billing['GPS+_YTD'] / abs(table_billing['Sales+_YTD'])
        )
        table_billing['PercentGPS_YTD'] = np.where(
            table_billing['Sales_YTD'] == 0, 0, table_billing['GPS_YTD'] / abs(table_billing['Sales_YTD'])
        )

        # Seleciona e renomeia as colunas
        selected_columns = ['CC', 'SKU', 'Quantidade', 'SalesRevenue', 'PercentGPS+_YTD']
        table_billing = table_billing[selected_columns]
        table_billing.columns = ['CC', 'SKU', 'Qtd_AY', 'SalesRevenue_AY', 'PercentGPS+_AY']

        # Preenche valores nulos com zero
        table_billing = table_billing.fillna(0)

        return cls(table_billing)
    
    @classmethod
    def create_table_stock(cls, table):
        table_stock = table.copy()
        table_stock = table.groupby(['SKU', 'CD'])[['Estoque']].sum().reset_index()
        return cls(table_stock)
    
    @classmethod
    def create_table_volume_reg(cls, table):
        table_volume_reg = table.copy()
        table_volume_reg = table.loc[(table['Mês n']==actual_month), ['REGIONAL', 'SKU', 'Volume']]

        return cls(table_volume_reg)

    @classmethod
    def create_table_billing_actual_month(cls, table):
        # Filtra os dados do ano atual
        table_billing = table[table['Ano'] == actual_year]
        table_billing = table[table['Mês']== actual_month]

        # Agrupa e soma os valores necessários
        aggregation_columns = ['Quantidade']

        table_billing = table_billing.groupby([
            'CC', 'SKU'
        ])[aggregation_columns].sum().reset_index()

        # Seleciona e renomeia as colunas
        selected_columns = ['CC', 'SKU', 'Quantidade',]
        table_billing = table_billing[selected_columns]
        table_billing.columns = ['CC', 'SKU', 'Qtd_AM',]

        # Preenche valores nulos com zero
        table_billing = table_billing.fillna(0)

        return cls(table_billing)
 
    @classmethod
    def create_table_business_rules(cls, table):
        # - Sem atendimento para distribuidor do produtos 3547817
       
        # 1 Analisar apenas produtos Pharma
        # 2 Sem atendimento para produtos com estoque zero
        # 3 Sem atendimento para produtos com volume zero
        # 4 Sem atendimento para clientes com bloqueio
        # 5 Sem atendimento para clientes com coefficient zero
        # 6 Sem atendimento para clientes com consumo de volume ultrapassado
        # 7 Sem atendimento para distribuidor quando consumo de estoque for maior que 80%
        # 8 Sem atendimento para clientes que não são GRUPOS_PRIORIDADE do produto 3547817.

        table_business_rules = table.copy()

        # Criar coluna de consumo estoque e consumo volume
        table_business_rules['sumPendente'] = table_business_rules.groupby(['SKU', 'CD'])['Pendente'].transform('sum')
        table_business_rules['FaturamentoRegional'] = table_business_rules.groupby(['REGIONAL', 'SKU'])['Qtd_AM'].transform('sum')

        table_business_rules['ConsumoEstoque'] = np.where(
            table_business_rules['Estoque'] <= 0,
            0,
            (table_business_rules['sumPendente'] / table_business_rules['Estoque'])
        )

        table_business_rules['ConsumoSaldoRegional'] = np.where(
            table_business_rules['Volume'] <= 0,
            0,
            (table_business_rules['FaturamentoRegional'] / table_business_rules['Volume'])
        )

        # Regra 1
        table_business_rules = table_business_rules.loc[table_business_rules['SKU'].isin(SKU), :]

        # Regra 2, 3, 4, 5, 6
        table_business_rules['pending_analysis'] = np.where(
            (
                table_business_rules['ConsumoSaldoRegional'] > LIMITE_MAX_CONSUMO_SALDO_REGIONAL
            ) | (
                table_business_rules['Estoque'] <= 0
            ) | (
                table_business_rules['Volume'] <= 0
            ) | (
                table_business_rules['Status verificações'] == 'B'
            ) | (
                table_business_rules['coefficient_NM'] == 0
            ),
            0,
            table_business_rules['Pendente']
        )

        # Regra 7
        table_business_rules['pending_analysis'] = np.where(
            (
                table_business_rules['ConsumoEstoque'] > LIMITE_MAX_CONSUMO_ESTOQUE
            ) & (
                table_business_rules['Customer Group 1'] == 'Distribuidor'
            ),
            0,
            table_business_rules['pending_analysis']
        )

        table_business_rules['pending_analysis'] = np.where(
            (
                table_business_rules['SKU'] == '3547817'
            ) & (
                ~table_business_rules['GrupoKAM'].isin(GRUPOS_PRIORIDADE_LIPURO_50)
            ),
            0,
            table_business_rules['pending_analysis']
        )


        return cls(table_business_rules)

    @classmethod
    def create_table_representation_sales_sba(cls, table):
        table_sba = table.copy()
        list_sba = ['IV FLUIDS & IRRIGATION', 'DRUGS', 'PARENTERAL NUTRITION']
        result = pd.DataFrame()

        for sba in list_sba:
            list_cc = table_sba.loc[(table_sba['Terapia']==sba) & (table_sba['SalesRevenue']>0), 'CC'].unique().tolist()

            table_hp_with_sba = table_sba.loc[table_sba['CC'].isin(list_cc), ['CC', 'Terapia', 'SalesRevenue']]
            table_hp_with_sba = table_hp_with_sba.groupby(['CC', 'Terapia'])['SalesRevenue'].sum().reset_index()
            table_hp_with_sba.columns = ['CC', 'Terapia', f'sales_hp_with_{sba}']
            
            if result.empty:
                result = table_hp_with_sba
            else:
                result = result.merge(table_hp_with_sba, on=['CC', 'Terapia'], how='outer')

        result = result.fillna(0)


        return cls(result)

    @classmethod
    def create_table_unit_price(cls, table):
        
        table_revenue = table.copy()
        table_revenue = table_revenue.loc[(table_revenue['Quantidade'] > 0), :]

        table_revenue.loc[:, 'unit_price'] = table_revenue['SalesRevenue'] / table_revenue['Quantidade']

        table_revenue = table_revenue.sort_values(['CC', 'SKU', 'data'])

        current_price = table_revenue.groupby(['CC', 'SKU']).tail(1)[['CC', 'SKU', 'unit_price']]
        current_price = current_price.rename(columns={'unit_price': 'current_price'})
        current_price['current_price'] = current_price['current_price'].map(lambda x: round(x, 2))

        last_price = table_revenue.groupby(['CC', 'SKU']).nth(-2).reset_index()
        last_price = last_price.loc[:, ['CC', 'SKU', 'unit_price', 'Mês', 'Ano']]
        last_price = last_price.rename(columns={'unit_price': 'last_price', 'Mês': 'Mês_last_price', 'Ano': 'Ano_last_price'})
        last_price['last_price'] = last_price['last_price'].map(lambda x: round(x, 2))

        result = pd.merge(current_price, last_price, on=['CC', 'SKU'], how='left')

        result['percent_price_variation'] = (result['current_price'] / result['last_price']) - 1
        
        return cls(result)