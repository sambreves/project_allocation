from app.classes.data_processor import DataProcessor
from app.classes.table_creator import TableCreator
from app.classes.table_merger import TableMerger
from app.classes.table_processor import TableProcessor
from app.utils.save_file import save_local_file_csv

from tabulate import tabulate

file_content_billing = './data/files/billing_hp_2024.csv'
file_content_general_data = './data/files/general_data.xlsx'

data_billing_ytd = DataProcessor.type_columns(file_content_billing).data
data_customers = DataProcessor.type_columns(file_content_general_data, sheet_name="Customers").data
data_volume = DataProcessor.type_columns(file_content_general_data, sheet_name="Volume").data
data_products = DataProcessor.type_columns(file_content_general_data, sheet_name="Products").data

table_main = TableCreator.create_table_main(data_customers, data_products).data
table_customers = TableCreator.create_table_customers(data_customers).data
table_billing_ytd = TableCreator.create_table_billing_ytd(data_billing_ytd).data
table_pareto_customers = TableCreator.create_table_pareto_customers(table_billing_ytd).data
table_volume_sku = TableCreator.create_table_volume_sku(data_volume).data
table_pareto_private_customers = TableCreator.create_table_pareto_private_customers(table_billing_ytd, table_customers).data
table_pareto_products = TableCreator.create_table_pareto_products(table_billing_ytd).data
table_portfolio = TableCreator.create_table_portfolio(table_billing_ytd, table_pareto_products).data
table_billing_customers_sba = TableCreator.create_table_billing_customers_sba(table_billing_ytd).data
table_billing_customers_hospital_care = TableCreator.create_table_billing_customers_hospital_care(table_billing_ytd).data
table_purchase_frequency = TableCreator.create_table_purchase_frequency(data_billing_ytd).data
table_last_month_purchase = TableCreator.create_table_last_month_purchase(data_billing_ytd).data

table_merge = TableMerger.merge_table_main(
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
).data

table_params = TableProcessor.create_params(table_merge).data
table_coefficient = TableProcessor.create_coefficient(table_params).data

save_local_file_csv(table_coefficient, name='table_iv_fluids')

print(tabulate(table_coefficient.info(), headers='keys', tablefmt='psql'))




