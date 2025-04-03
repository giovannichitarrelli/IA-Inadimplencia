# V3 - Transformando o código em um dialogo com o usuário (Adicionando função de chat)
from openai import OpenAI
import httpx
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("API_KEY")

client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com", http_client=httpx.Client(verify=False))

user_content = input("Como posso te ajudar hoje? ")

df = pd.read_parquet(r"consolidado.parquet")

def generate_base_insights(df):
    
    insights = ""
    
    state_counts = df['uf'].value_counts()
    top_state = state_counts.idxmax()
    insights += f"O estado com mais inadimplência é {top_state} com {state_counts.max()} casos.\n"
    
    yearly_counts = df['data_base'].value_counts().sort_index()
    insights += "Crescimento da inadimplência ao longo dos anos:\n"
    for year, count in yearly_counts.items():
        insights += f"Ano {year}: {count} casos\n"
    
    
    return insights

# Insights base para iniciar o contexto
base_insights = generate_base_insights(df)

# Lista para manter o histórico de mensagens
messages = [
    {"role": "system", "content": f"Você é um especialista no setor bancário especializado em análise de inadimplência no Brasil. Aqui estão alguns insights iniciais:\n{base_insights}"},
]

# Função principal do chat
def chat_with_model():
    print("Bem-vindo ao Chat de Análise de Inadimplência!")
    print("Digite 'sair' para encerrar a conversa.")
    
    while True:
        # Entrada do usuário
        user_input = input("\nVocê: ")
        
        # Opção de sair
        if user_input.lower() == 'sair':
            print("Encerrando o chat...")
            break
        
        # Adiciona a mensagem do usuário ao histórico
        messages.append({"role": "user", "content": user_input})
        
        try:
            # Envia a conversa para o modelo
            response = client.chat.completions.create(
                model="deepseek-chat",
                messages=messages,
                stream=False
            )
            
            # Obtém a resposta do modelo
            model_response = response.choices[0].message.content
            
            # Imprime a resposta
            print("\nAssistente:", model_response)
            
            # Adiciona a resposta do modelo ao histórico
            messages.append({"role": "assistant", "content": model_response})
        
        except Exception as e:
            print(f"Erro ao processar a solicitação: {e}")

# Iniciar o chat
if __name__ == "__main__":
    chat_with_model()