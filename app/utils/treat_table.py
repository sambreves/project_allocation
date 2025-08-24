def format_float_2_decimal(table, decimal=2):
    #Formatar decimais das colunas igual a float
    float_columns = table.select_dtypes(include=['float64']).columns
    table[float_columns] = table[float_columns].map(lambda x: round(x, decimal))