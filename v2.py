# V2 - Versão 2 do código para geração de insights a partir de um DataFrame e interação com o modelo de IA da Deepseek.

from openai import OpenAI
import httpx
import pandas as pd

client = OpenAI(api_key="sk-461442f8d35c4fc7b6c543e48ac0c431", base_url="https://api.deepseek.com", http_client=httpx.Client(verify=False))

user_content = input("Como posso te ajudar hoje? ")

df = pd.read_parquet("database.parquet" )

# Função para gerar insights a partir do DataFrame
def generate_insights(df):
    print("Gerando insights a partir do DataFrame")
    insights = ""
    
    # Exemplo de análise: Estado com mais inadimplência
    state_counts = df['uf'].value_counts()
    top_state = state_counts.idxmax()
    insights += f"O estado com mais inadimplência é {top_state} com {state_counts.max()} casos.\n"
    
    # Exemplo de análise: Crescimento ao longo dos anos
    yearly_counts = df['data_base'].value_counts().sort_index()
    insights += "Crescimento da inadimplência ao longo dos anos:\n"
    for year, count in yearly_counts.items():
        insights += f"Ano {year}: {count} casos\n"
    
    print("Insights gerados com sucesso")
    return insights

# Gerar insights a partir do DataFrame
print("Chamando a função generate_insights")
content = generate_insights(df)


response = client.chat.completions.create(
    model="deepseek-chat",
    messages=[
        {"role": "system", "content": f"Você é um especialista no setor bancário e precisará gerar insights através da inadimplência no Brasil. {content}"},
        {"role": "user", "content": user_content},
    ],
    
    stream=False
)

print("\nResposta do Modelo de IA:")
print(response.choices[0].message.content)

  