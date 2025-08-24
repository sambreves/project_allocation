class TableMerger:
    def __init__(self, data):
        self.data = data

    @classmethod
    def merge_table_general(
            cls,
            table_main,
            table_billing_ytd,
            table_customers,
            table_products,
            table_volume_sku,
            table_pareto_customers,
            table_pareto_private_customers,
            table_pareto_products,
            table_portfolio,
            table_billing_customers_sba,
            table_billing_customers_hospital_care,
            table_purchase_frequency,
            table_purchase_frequency_customers,
            table_last_month_purchase,
            table_billing_ay,
            table_pending_customers,
            table_representation_sales_sba,
            table_stock,
            table_billing_am,
            table_unit_price,
            filter=['IV FLUIDS & IRRIGATION']
    ):
        table = table_main.merge(table_customers[['CC', 'CD', 'Customer Group 1', 'REGIONAL', 'ClasseABC', 'GrupoKAM']], on='CC', how='left')
        table = table.merge(table_products[['SKU', 'Terapia']], on='SKU', how='left')
        table = table[table['Terapia'].isin(filter)][:]
        table = table.merge(table_billing_ytd.drop('Terapia', axis=1), on=['CC', 'SKU'], how='left')
        table = table.merge(table_volume_sku, on=['SKU', 'REGIONAL'], how='left')
        table = table.merge(table_pareto_customers, on='CC', how='left')
        table = table.merge(table_pareto_private_customers, on='CC', how='left')
        table = table.merge(table_pareto_products, on='SKU', how='left')
        table = table.merge(table_portfolio, on='CC', how='left')
        table = table.merge(table_billing_customers_sba, on=['CC', 'Terapia'], how='left')
        table = table.merge(table_billing_customers_hospital_care, on='CC', how='left')
        table = table.merge(table_purchase_frequency, on=['CC', 'SKU'], how='left')
        table = table.merge(table_purchase_frequency_customers, on=['CC'], how='left')
        table = table.merge(table_last_month_purchase, on=['CC', 'SKU'], how='left')
        table = table.merge(table_billing_ay, on=['CC', 'SKU'], how='left')
        table = table.merge(table_pending_customers, on=['CC', 'SKU'], how='left')
        table = table.merge(table_representation_sales_sba, on=['CC', 'Terapia'], how='left')
        table = table.merge(table_stock, on=['SKU', 'CD'], how='left')
        table = table.merge(table_billing_am, on=['CC', 'SKU'], how='left')
        table = table.merge(table_unit_price, on=['CC', 'SKU'], how='left')

        table = table.fillna(0)

        return cls(table)

    @classmethod
    def merge_table_allocation(
        cls,
        table_pending,
        table_general,
        table_customers,
        table_billing_am,
        table_volume_reg,
        table_stock,
        table_frete,
        table_alert_limits
    ):
        table = table_pending.merge(table_general[['CC', 'SKU', 'current_price', 'coefficient_NM']], on=['CC', 'SKU'], how='left')
        table = table.merge(table_customers[['CC', 'Customer Group 1', 'GrupoKAM', 'Nome 1', 'Cidade', 'UF']], on=['CC'], how='left')
        table = table.merge(table_billing_am, on=['CC', 'SKU'], how='left')
        table = table.merge(table_volume_reg, on=['SKU', 'REGIONAL'], how='left')
        table = table.merge(table_stock, on=['SKU', 'CD'], how='left')
        table = table.merge(table_frete.loc[:, ['Cidade', 'UF', 'Classificacao', 'Custo_Unitario_Medio']], on=['Cidade', 'UF'], how='left')
        table = table.merge(table_alert_limits, on=['Cidade', 'UF'], how='left')

        table = table.fillna(0)

        return cls(table)