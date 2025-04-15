import pandas as pd
from pulp import LpProblem, LpMinimize, LpVariable, lpSum, PULP_CBC_CMD

# Dados de exemplo
centros_distribuicao = ['CD1', 'CD2', 'CD3', 'CD4', 'CD5']
clientes = ['Cliente1', 'Cliente2', 'Cliente3', 'Cliente4', 'Cliente5']
produtos = ['Produto1', 'Produto2', 'Produto3', 'Produto4', 'Produto5']

# Estoque disponível em cada centro de distribuição para cada produto
estoque = {
    'CD1': {'Produto1': 100, 'Produto2': 150, 'Produto3': 200, 'Produto4': 250, 'Produto5': 300},
    'CD2': {'Produto1': 120, 'Produto2': 130, 'Produto3': 140, 'Produto4': 160, 'Produto5': 180},
    'CD3': {'Produto1': 110, 'Produto2': 140, 'Produto3': 170, 'Produto4': 190, 'Produto5': 210},
    'CD4': {'Produto1': 130, 'Produto2': 160, 'Produto3': 180, 'Produto4': 200, 'Produto5': 220},
    'CD5': {'Produto1': 140, 'Produto2': 170, 'Produto3': 190, 'Produto4': 210, 'Produto5': 230},
}

# Demanda dos clientes para cada produto
demanda = {
    'Cliente1': {'Produto1': 80, 'Produto2': 90, 'Produto3': 100, 'Produto4': 110, 'Produto5': 120},
    'Cliente2': {'Produto1': 70, 'Produto2': 80, 'Produto3': 90, 'Produto4': 100, 'Produto5': 110},
    'Cliente3': {'Produto1': 60, 'Produto2': 70, 'Produto3': 80, 'Produto4': 90, 'Produto5': 100},
    'Cliente4': {'Produto1': 50, 'Produto2': 60, 'Produto3': 70, 'Produto4': 80, 'Produto5': 90},
    'Cliente5': {'Produto1': 40, 'Produto2': 50, 'Produto3': 60, 'Produto4': 70, 'Produto5': 80},
}

# Criar o problema de otimização
problema = LpProblem("Problema_de_Transporte", LpMinimize)

# Variáveis de decisão
variaveis = LpVariable.dicts("Alocacao", (centros_distribuicao, clientes, produtos), lowBound=0, cat='Continuous')

# Variáveis binárias para indicar se um centro de distribuição está atendendo um cliente
atendimento = LpVariable.dicts("Atendimento", (centros_distribuicao, clientes), cat='Binary')

# Função objetivo: minimizar o custo de transporte (aqui assumimos custo unitário)
problema += lpSum(variaveis[cd][cl][pr] for cd in centros_distribuicao for cl in clientes for pr in produtos)

# Restrições de oferta (estoque disponível)s
for cd in centros_distribuicao:
    for pr in produtos:
        problema += lpSum(variaveis[cd][cl][pr] for cl in clientes) <= estoque[cd][pr], f"Restricao_Estoque_{cd}_{pr}"

# Restrições de demanda (demanda dos clientes)
for cl in clientes:
    for pr in produtos:
        problema += lpSum(variaveis[cd][cl][pr] for cd in centros_distribuicao) >= demanda[cl][pr] * 0.5, f"Restricao_Demanda_{cl}_{pr}"

# Restrições para garantir que cada cliente seja atendido por apenas um centro de distribuição
for cl in clientes:
    problema += lpSum(atendimento[cd][cl] for cd in centros_distribuicao) == 1, f"Restricao_Atendimento_{cl}"

# Restrições para ligar as variáveis de decisão às variáveis binárias
for cd in centros_distribuicao:
    for cl in clientes:
        for pr in produtos:
            problema += variaveis[cd][cl][pr] <= atendimento[cd][cl] * demanda[cl][pr], f"Restricao_Ligacao_{cd}_{cl}_{pr}"


# Resolver o problema
problema.solve(PULP_CBC_CMD(msg=0))

# Exibir os resultados
for cd in centros_distribuicao:
    for cl in clientes:
        for pr in produtos:
            if variaveis[cd][cl][pr].varValue > 0:
                print(f"Centro de Distribuição {cd} aloca {variaveis[cd][cl][pr].varValue} unidades de {pr} para {cl}")
