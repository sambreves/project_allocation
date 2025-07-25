from app.classes.data_processor import DataProcessor
from app.classes.table_creator import TableCreator
from app.classes.table_merger import TableMerger
from app.classes.table_processor import TableProcessor
from app.utils.save_file import save_local_file_xlsx, save_daily_allocation_OV, save_local_file_csv
from app.utils.treat_table import format_float_2_decimal

from tabulate import tabulate

file_content_billing = './data/files/billing_hp_2024.csv'
file_content_general_data = './data/files/general_data.xlsx'

data_billing_ytd = DataProcessor.type_columns(file_content_general_data, sheet_name="billing_hp_2024").data
data_billing_ay = DataProcessor.type_columns(file_content_general_data, sheet_name="Billing").data

data_billing = DataProcessor.concat_table_billing(data_billing_ytd, data_billing_ay).data

data_customers = DataProcessor.type_columns(file_content_general_data, sheet_name="Customers").data
data_products = DataProcessor.type_columns(file_content_general_data, sheet_name="Products").data
data_volume = DataProcessor.type_columns(file_content_general_data, sheet_name="Volume").data
data_pending = DataProcessor.type_columns(file_content_general_data, sheet_name="OV").data
data_volume_reg = DataProcessor.type_columns(file_content_general_data, sheet_name='Volume').data
data_stock = DataProcessor.type_columns(file_content_general_data, sheet_name='Stock').data

table_main = TableCreator.create_table_main(data_customers, data_products).data
table_customers = TableCreator.create_table_customers(data_customers).data
table_products = TableCreator.create_table_products(data_products).data
table_billing_ytd = TableCreator.create_table_billing_ytd(data_billing).data
table_pareto_customers = TableCreator.create_table_pareto_customers(table_billing_ytd).data
table_volume_sku = TableCreator.create_table_volume_sku(data_volume).data
table_pareto_private_customers = TableCreator.create_table_pareto_private_customers(table_billing_ytd, table_customers).data
table_pareto_products = TableCreator.create_table_pareto_products(table_billing_ytd).data
table_portfolio = TableCreator.create_table_portfolio(table_billing_ytd, table_pareto_products).data
table_billing_customers_sba = TableCreator.create_table_billing_customers_sba(table_billing_ytd).data
table_billing_customers_hospital_care = TableCreator.create_table_billing_customers_hospital_care(table_billing_ytd).data
table_purchase_frequency = TableCreator.create_table_purchase_frequency(data_billing).data
table_purchase_frequency_customers = TableCreator.create_table_purchase_frequency_customers(data_billing).data
table_last_month_purchase = TableCreator.create_table_last_month_purchase(data_billing).data
table_billing_ay = TableCreator.create_table_billing_actual_year(data_billing_ay).data
table_pending_customers = TableCreator.create_table_pending_customers(data_pending).data
table_representation_sales_sba = TableCreator.create_table_representation_sales_sba(data_billing).data
table_unit_price = TableCreator.create_table_unit_price(data_billing).data

table_pending = TableCreator.create_table_pending(data_pending).data
table_stock = TableCreator.create_table_stock(data_stock).data
table_volume_reg = TableCreator.create_table_volume_reg(data_volume_reg).data
table_billing_am = TableCreator.create_table_billing_actual_month(data_billing_ay).data

table_general_merge = TableMerger.merge_table_general(
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
    filter=['IV FLUIDS & IRRIGATION', 'DRUGS', 'PARENTERAL NUTRITION']
).data

table_general_params = TableProcessor.create_params(table_general_merge).data
table_general = TableProcessor.create_coefficient_normalized(table_general_params).data

table_allocation_merge = TableMerger.merge_table_allocation(
    table_pending,
    table_general,
    table_customers,
    table_billing_am,
    table_volume_reg,
    table_stock
).data

table_allocation_rules = TableCreator.create_table_business_rules(table_allocation_merge).data

table_allocation = TableProcessor.allocate(table_allocation_rules).data

table_allocation_treated = TableProcessor.treat_allocation_table(table_allocation).data

format_float_2_decimal(table_general, 2)
format_float_2_decimal(table_allocation_treated, 2)

# save_local_file_csv(table_general, name='table_general')
save_local_file_xlsx(table_allocation_treated, name='table_daily_allocation_ov')
save_daily_allocation_OV(table_allocation_treated)

# print(tabulate(table_general.info(), headers='keys', tablefmt='psql'))
# print(tabulate(table_allocation_treated.info(), headers='keys', tablefmt='psql'))






