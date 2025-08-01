import pandas as pd
import re
from apyori import apriori # Placeholder for potential advanced analysis
from openai import OpenAI # Placeholder for Generative AI integration
import requests
import json
from dotenv import load_dotenv
import os

def analysis_trend_generate_feedback(df: pd.DataFrame):
    
    load_dotenv()

    api_key = os.getenv("API_KEY_GEMINI")

    if not api_key:
        raise ValueError("A chave da API não foi encontrada. Verifique seu arquivo .env")

    sku = df['SKU'].values.tolist()
    grupo_canal = df['Grupo Canal'].values.tolist()
    asp_bbraun = df['ASP B.Braun'].values.tolist()
    asp_abrasp = df['ASP ABRASP'].values.tolist()
    indice_preco = df['Índice de Preço'].values.tolist()
    asp_iqvia = df['ASP de Mercado (IQVIA MAT 25)'].values.tolist()
    share_volume_bbraun = df['Share Volume B Braun %'].values.tolist()
    cagr_valor_mercado = df['CAGR Valor Mercado'].values.tolist()
    cagr_volume_mercado = df['CAGR Volume Mercado'].values.tolist()
    cagr_valor_bbraun = df['CAGR B Braun Valor'].values.tolist()
    cagr_volume_bbraun = df['CAGR B Braun Volume'].values.tolist()
    volume_mercado = df['Volume Mercado 2025'].values.tolist()

    # --- 2. Estruturar os dados para a IA ---
    input_ia = {
        "sku": sku,
        "grupo canal": grupo_canal,
        "average_sales_price_bbraun": asp_bbraun,
        "average_sales_price_market": asp_abrasp,
        "price_index": indice_preco,
        "average_sales_price_iqvia": asp_iqvia,
        "market_share_volume_bbraun": share_volume_bbraun,
        "cagr_value_market": cagr_valor_mercado,
        "cagr_volume_market": cagr_volume_mercado,
        "cagr_value_bbraun": cagr_valor_bbraun,
        "cagr_volume_bbraun": cagr_volume_bbraun,
        "volume_market_2025": volume_mercado,
    }

    # --- 3. Criar o prompt e chamar a API do Gemini ---
    prompt = f"""
    Você é um analista sênior de mercado farmacêutico. Com base nos seguintes dados de um produto da B. Braun, 
    forneça um resumo estratégico conciso com 2 a 3 frases sobre seu desempenho de tendência de mercado.
    Realize uma análise de tendência dos produtos por canal e me dê uma sugestão de alteração no preço caso adotemos um postura focada em volume.
    Levando em consideração que o asp da bbraun é referente ao primeiro semestre de 2025 e o asp do mercado é referente ao segundo semestre de 2024.
    Dados dos Produtos: {str(input_ia)}
    """
    
    feedback = ""
    try:
        # Configuração da chamada para a API do Google Gemini
        apiKey = api_key
        apiUrl = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent?key={apiKey}"
        
        payload = {
            "contents": [{
                "parts": [{
                    "text": prompt
                }]
            }]
        }
        
        headers = {
            'Content-Type': 'application/json'
        }

        # Realiza a chamada de API
        response = requests.post(apiUrl, headers=headers, data=json.dumps(payload))
        response.raise_for_status() # Lança um erro se a resposta não for 200 OK
        
        result = response.json()
        
        # Extrai o texto da resposta
        feedback = result['candidates'][0]['content']['parts'][0]['text']

    except requests.exceptions.RequestException as e:
        feedback = f"Erro de conexão ao chamar a API do Gemini: {e}"
    except (KeyError, IndexError) as e:
        feedback = f"Erro ao processar a resposta da API: {e}. Resposta recebida: {response.text}"
    except Exception as e:
        feedback = f"Um erro inesperado ocorreu: {e}"
        
    return feedback