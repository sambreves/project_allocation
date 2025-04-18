import pandas as pd
import numpy as np
import datetime

LIMITE_MAX_SKU_ESTRATEGICO_FLUIDS = 0.85
LIMITE_MAX_SKU_ESTRATEGICO_DRUGS= 0.97
PESO_CAM_FLUIDS = 0.5
PESO_CAM_DRUGS = 1.7
PESO_DISTRIBUIDOR_FLUIDS = 0.5
PESO_DISTRIBUIDOR_DRUGS = 0.3
PESO_PUBLICO_FLUIDS = 5
PESO_PUBLICO_DRUGS = 1.7
PESO_GRUPOS_IMPORTANTES_DRUGS = 1.8
actual_month = datetime.date.today().month
last_year = (datetime.date.today().year - 1)

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
        table_customers = table_customers[['CC', 'REGIONAL', 'CD', 'ClasseABC']]
        # table_sba = table_products[
        #     table_products['Terapia'].isin(['IV FLUIDS & IRRIGATION', 'DRUGS', 'PARENTERAL NUTRITION'])
        # ][['SKU', 'Terapia']]

        return cls(table_main)

    @classmethod
    def create_table_customers(cls, table):
        table_customers = table.loc[:, ['CC', 'REGIONAL', 'ClasseABC', 'Customer Group 1', 'GrupoKAM', 'Nome 1', 'CD']]

        return cls(table_customers)

    @classmethod
    def create_table_billing_ytd(cls, table):
        # Calcula qual o ano passado
        actual_year = datetime.date.today().year
        last_year = actual_year - 1
        # Filtra os dados do último ano
        table_billing_ytd = table[table['Ano'] == last_year]

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
    def create_table_last_month_purchase(cls, table):
        table_last_month_billing = table.groupby(['CC', 'SKU'])['Mês'].max().reset_index()
        table_last_month_billing.columns = ['CC', 'SKU', 'ultimo_mes_compra']
        return cls(table_last_month_billing)

    @classmethod
    def create_table_volume_sku(cls, table):
        table_volume_sku = table.loc[
            (table['Mês n']==actual_month),
            ['SKU', 'REGIONAL', 'Mês n', 'Volume']].reset_index(drop=True)

        return cls(table_volume_sku)