class TableCreator:
    def __init__(self, table):
        self.table = table

    def create_table_pending(self):
        table_pending = self.table.copy()
        table_pending['DataPreparo'] = pd.to_datetime(table_pending['DataPreparo'], format='mixed')
        table_pending = table_pending.loc[(table_pending['ano'] == datetime.datetime.now().year), :]
        table_pending = table_pending.loc[table_pending['DataPreparo'] <= datetime.datetime.now(), :]
        table_pending['OV'] = table_pending['OV'] + '_' + table_pending['Num Linha']
        table_pending = table_pending.groupby(
            ['OV', 'CC', 'SKU', 'CD', 'Status verificações', 'DataPreparo', 'Valor item OV']
        )['Pendente'].sum().reset_index()

        table_pending['Pendente'] = np.where(
            table_pending['SKU'] == '3547817', table_pending['Pendente'] * NUM_CAIXAS_LIPURO_50ML, table_pending['Pendente']
        )
        table_pending['Pendente'] = np.where(
            table_pending['SKU'] == '3547825', table_pending['Pendente'] * NUM_CAIXAS_LIPURO_20ML, table_pending['Pendente']
        )
        table_pending['Pendente'] = np.where(
            table_pending['SKU'] == '3547833', table_pending['Pendente'] * NUM_CAIXAS_LIPURO_100ML, table_pending['Pendente']
        )

        return table_pending

    def create_table_stock(self):
        table_stock = self.table.groupby(['SKU', 'CD'])[['Estoque', 'Transito']].sum().reset_index()
        return table_stock

    def create_table_customers(self):
        table_customers = self.table.loc[:, ['CC', 'REGIONAL', 'ClasseABC', 'Customer Group 1', 'GrupoKAM', 'Nome 1']]
        return table_customers

    def create_table_billing_am(self, actual_month, actual_year):
        table_billing_am = self.table.loc[
            (self.table['Mês'] == actual_month) & (self.table['Ano'] == actual_year), ['CC', 'SKU', 'Quantidade']
        ]
        table_billing_am = table_billing_am.groupby(['CC', 'SKU'])[['Quantidade']].sum().reset_index()
        table_billing_am.columns = ['CC', 'SKU', 'Qtd_AM']
        return table_billing_am

    def create_table_premises(self):
        table_premises = self.table.loc[:, ['CC', 'SKU', 'Premissa', 'Coefficient', 'Qtd_YTD']]
        table_premises = table_premises.drop_duplicates(subset=['CC', 'SKU'], keep='last')
        return table_premises

    def create_table_volume_reg(self, actual_month):
        table_volume_reg = self.table.loc[(self.table['Mês n'] == actual_month), ['Regional', 'SKU', 'Volume']]
        return table_volume_reg

    def create_table_volume_group(self):
        table_volume_group = self.table.loc[:, ['GrupoKAM', 'SKU', 'VolumeGrupo']]
        return table_volume_group

    def create_table_weight_customers(self):
        table_weight_customers = self.table.loc[:, ['CC', 'SKU', 'Peso']]
        return table_weight_customers

class TableMerger:
    def __init__(self, table_pending, table_stock, table_customers, table_billing, table_params, table_volume_reg, table_volume_group, table_weight_customers):
        self.table_pending = table_pending
        self.table_stock = table_stock
        self.table_customers = table_customers
        self.table_billing = table_billing
        self.table_params = table_params
        self.table_volume_reg = table_volume_reg
        self.table_volume_group = table_volume_group
        self.table_weight_customers = table_weight_customers

    def merge_tables(self):
        table = self.table_pending
        table = table.merge(self.table_customers, on='CC', how='left')
        table = table.merge(self.table_stock, on=['SKU', 'CD'], how='left')
        table = table.merge(self.table_billing, on=['CC', 'SKU'], how='left')
        table = table.merge(self.table_params, on=['CC', 'SKU'], how='left')
        table = table.merge(self.table_volume_reg, left_on=['REGIONAL', 'SKU'], right_on=['Regional', 'SKU'], how='left')
        table = table.merge(self.table_volume_group, on=['GrupoKAM', 'SKU'], how='left')
        table = table.merge(self.table_weight_customers, on=['CC', 'SKU'], how='left')
        table = table.fillna(0)
        return table

class TableProcessor:
    def __init__(self, table):
        self.table = table
    def create_params(self):
        self.table['Peso'] = np.where(
            (self.table['Qtd_YTD'] == 0) & (self.table['Peso'] == 0) & (self.table['Customer Group 1'] != 'Distribuidor'),
            1,
            self.table['Peso']
        )
        self.table['Coefficient'] = np.where(
            (self.table['Qtd_YTD'] == 0) & (self.table['Customer Group 1'] != 'Distribuidor'),
            (self.table.loc[(self.table['Coefficient'] > 0), 'Coefficient'].min()),
            self.table['Coefficient']
        )
        self.table['Coefficient'] = self.table['Peso'] * self.table['Coefficient']
        table_main = self.table.loc[self.table['SKU'].isin(SKU), :]
        table_main_bloq = table_main.loc[
            (table_main['Estoque'] <= 0) & (table_main['Volume'] <= 0) & (table_main['Status verificações'] == 'B') & (table_main['Coefficient'] == 0),
            :
        ].reset_index(drop=True)
        table_main_bloq['ConsumoEstoque'] = 0
        table_main = table_main.loc[
            (table_main['Estoque'] > 0) & (table_main['Volume'] > 0) & (table_main['Status verificações'] != 'B') & (table_main['Coefficient'] > 0),
            :
        ].reset_index(drop=True)
        table_main['sumPendente'] = table_main.groupby(['SKU', 'CD'])['Pendente'].transform('sum')
        table_main['FaturamentoRegional'] = table_main.groupby(['REGIONAL', 'SKU'])['Qtd_AM'].transform('sum')
        table_main['FaturamentoGrupo'] = table_main.groupby(['GrupoKAM', 'SKU'])['Qtd_AM'].transform('sum')
        table_main['ConsumoEstoque'] = (table_main['sumPendente'] / table_main['Estoque'])
        table_main['ConsumoSaldoRegional'] = (table_main['FaturamentoRegional'] / table_main['Volume'])
        table_main['ConsumoSaldoGrupo'] = np.where(table_main['VolumeGrupo'] > 0, (table_main['FaturamentoGrupo'] / table_main['VolumeGrupo']), 0)
        table_main['ConsumoSaldoPremissa'] = np.where(table_main['Premissa'] > 0, ((table_main['Qtd_AM'] + table_main['Pendente']) / table_main['Premissa']), 10)
        table_main['PendenteCrit'] = np.where(
            (table_main['ConsumoSaldoRegional'] > LIMITE_MAX_CONSUMO_SALDO_REGIONAL) & (table_main['Customer Group 1'] == 'Distribuidor'),
            0,
            table_main['Pendente']
        )
        table_main['PendenteNotCrit'] = np.where(
            table_main['ConsumoSaldoRegional'] <= LIMITE_MAX_CONSUMO_SALDO_REGIONAL,
        )
