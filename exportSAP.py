from pyrfc import Connection, ABAPApplicationError, ABAPRuntimeError, LogonError, CommunicationError
from tkinter import filedialog, messagebox

import sys
import pandas as pd
import os
import datetime

date_today = datetime.datetime.now().strftime('%Y-%m-%d')

login = str(os.getenv('SAP_LOGIN'))
passw = str(os.getenv('SAP_PASSW'))

# Test SAP base -- Ambiente BRB
# sapBase = 'bbmags2.bbmag.bbraun.com'
# client = '100'

sapBase = 'bbmagsh.bbmag.bbraun.com'
client = '100'

####### SAP PART #######
ASHOST = sapBase
CLIENT = client
SYSNR = '00'
USER = login
PASSWD = passw

try:
    # file_path = filedialog.askopenfilename(
    #     title="Select an Excel file",
    #     filetypes=(
    #         ("Excel files", "*.xlsx;*.xls"),
    #         ("All files", "*.*")
    #     )
    # )

    file_path = f'./data/priorizations/Priorizações_{date_today}_1.xlsx'

    df = pd.read_excel(file_path)

    df = df[df['AllocatedVolume'] != 0]
    ### BAPI parameters ###
    # transaction header
    T_ITEM_ALLOC = []

    T_ITEM_ALLOC_LINE = {}

    i = 0
    for index, row in df.iterrows():
        # print(row)
        OV = str(df.iloc[i, 0]).split('.')[0]
        SKU = str(df.iloc[i, 1]).split('.')[0]
        ItemNumber = str(df.iloc[i, 2]).split('.')[0]
        allocatedVolume = str(df.iloc[i, 3]).split('.')[0]
        DataPreparo = str(df.iloc[i, 4]).split('.')[0]
        # Convert date format
        date_obj = datetime.datetime.strptime(DataPreparo, '%Y-%m-%d %H:%M:%S')
        formatted_date = date_obj.strftime('%Y%m%d')
        fabrica = str(df.iloc[i, 5]).split('.')[0]
        codigoCliente = str(df.iloc[i, 6]).split('.')[0]
        Nome1 = str(df.iloc[i, 7]).split('.')[0]
        pendente = str(df.iloc[i, 8]).split('.')[0]
        customerGroup = str(df.iloc[i, 9]).split('.')[0]
        grupoKAM = str(df.iloc[i, 10]).split('.')[0]
        regional = str(df.iloc[i, 11]).split('.')[0]
        value = str(df.iloc[i, 12]).split('.')[0]

        # sales document
        T_ITEM_ALLOC_LINE['VBELN'] = OV
        # sales document item
        T_ITEM_ALLOC_LINE['POSNR'] = ItemNumber
        # material number
        T_ITEM_ALLOC_LINE['MATNR'] = SKU
        # quantity - formato 00,00
        T_ITEM_ALLOC_LINE['ALLOC'] = allocatedVolume
        # material availibility date - formato yyyymmdd
        T_ITEM_ALLOC_LINE['DTPREP'] = formatted_date
        # plant
        T_ITEM_ALLOC_LINE['WERKS'] = fabrica
        # sold-to party
        T_ITEM_ALLOC_LINE['KUNNR'] = codigoCliente
        # name 1
        T_ITEM_ALLOC_LINE['NAME1'] = Nome1
        # quantity
        T_ITEM_ALLOC_LINE['PEND'] = pendente
        # sector
        T_ITEM_ALLOC_LINE['SETOR'] = customerGroup
        # KAM group
        T_ITEM_ALLOC_LINE['KAM'] = grupoKAM
        # region
        T_ITEM_ALLOC_LINE['REGIO'] = regional
        # net value of the order item in document curreny
        T_ITEM_ALLOC_LINE['INVO'] = value

        T_ITEM_ALLOC.append(T_ITEM_ALLOC_LINE)
        T_ITEM_ALLOC_LINE = {}

        i += 1

    conn = Connection(ashost=ASHOST, sysnr=SYSNR, client=CLIENT, user=USER, passwd=PASSWD)
    print('Conexão estabelecida com sucesso')

    print(T_ITEM_ALLOC)

    response = conn.call(
        'Z_SD00_ITEM_ALLOC_IN',
        T_ITEM_ALLOC=T_ITEM_ALLOC  # Pass the dictionary directly
    )

    sapResponse = []

    # print('E_SUBRC')
    print(response['E_SUBRC'])
except Exception as e:
    print(e)