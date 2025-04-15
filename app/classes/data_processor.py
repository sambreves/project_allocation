import pandas as pd
import numpy as np
import datetime
from pulp import LpProblem, LpMinimize, LpVariable, lpSum, PULP_CBC_CMD
from tabulate import tabulate

LIMITE_MAX_CONSUMO_ESTOQUE = 0.91
LIMITE_MAX_CONSUMO_SALDO_REGIONAL = 1.33
LIMITE_MAX_CONSUMO_PREMISSA = 1.33
LIMITE_MIN_PEDIDO_MINIMO = 2000
LIMITE_MAX_CONSUMO_SALDO_GRUPO = 1.0
PERCENTUAL_MAX_PEDIDO_PARCIAL = 0.5
DATE_FIRST = '2020-12-30'
DATA_HOJE = datetime.datetime.now().strftime('%Y-%m-%d')
HORA_ATUAL = datetime.datetime.now().strftime('%H.%M.%S')
NUM_CAIXAS_LIPURO_50ML = 10
NUM_CAIXAS_LIPURO_20ML = 5
NUM_CAIXAS_LIPURO_100ML = 10
SKU = ['200102', '200104', '200105', '200110', '200111', '200112', '200114', '200115', '200124', '200125',
        '200130', '200131', '200132', '200134', '200135', '200142', '200144', '200145', '200154', '200164',
        '200165', '200181', '200182', '200183', '3547825', '3547833', '3547817']

class DataProcessor:
    def __init__(self, table):
        self.table = table

    def type_columns(self):
        columns_to_convert = [
            'CC', 'SKU', 'CD', 'REGIONAL', 'Customer Group 1', 'Status verificações', 
            'OV', 'GrupoKAM', 'Nome 1', 'Num Linha', 'Item SO'
        ]
        for col in self.table.columns:
            if col in columns_to_convert:
                self.table[col] = self.table[col].astype(str)
        return self.table
    def treat_table(self):
        ...
