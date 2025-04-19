class TableMerger:
    def __init__(self, data):
        self.data = data

    @classmethod
    def merge_table_main(
            cls,
            table_main,
            table_billing_ytd,
            table_customers,
            table_volume_sku,
            table_pareto_customers,
            table_pareto_private_customers,
            table_pareto_products,
            table_portfolio,
            table_billing_customers_sba,
            table_billing_customers_hospital_care,
            table_purchase_frequency,
            table_last_month_purchase,
            filter=['IV FLUIDS & IRRIGATION']
    ):
        table = table_main.merge(table_customers[['CC', 'CD', 'Customer Group 1', 'REGIONAL', 'ClasseABC', 'GrupoKAM']], on='CC', how='left')
        table = table.merge(table_billing_ytd, on=['CC', 'SKU'], how='left')
        table = table[table['Terapia'].isin(filter)][:]
        table = table.merge(table_volume_sku, on=['SKU', 'REGIONAL'], how='left')
        table = table.merge(table_pareto_customers, on='CC', how='left')
        table = table.merge(table_pareto_private_customers, on='CC', how='left')
        table = table.merge(table_pareto_products, on='SKU', how='left')
        table = table.merge(table_portfolio, on='CC', how='left')
        table = table.merge(table_billing_customers_sba, on=['CC', 'Terapia'], how='left')
        table = table.merge(table_billing_customers_hospital_care, on='CC', how='left')
        table = table.merge(table_purchase_frequency, on=['CC', 'SKU'], how='left')
        table = table.merge(table_last_month_purchase, on=['CC', 'SKU'], how='left')        

        table = table.fillna(0)

        return cls(table)
