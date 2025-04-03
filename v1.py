# V1 - Conecta com a API Deepseek para obter respostas de um modelo de IA treinado para o setor bancário.

from openai import OpenAI
import httpx
 
client = OpenAI(api_key="sk-461442f8d35c4fc7b6c543e48ac0c431", base_url="https://api.deepseek.com", http_client=httpx.Client(verify=False))

user_content = input("Como posso te ajudar hoje? ")

response = client.chat.completions.create(
    model="deepseek-chat",
    messages=[
        {"role": "system", "content": f"Você é um especialista no setor bancário e precisará gerar insights através da inadimplência no Brasil."},
        {"role": "user", "content": user_content},
    ],
    
    stream=False
)

print(response.choices[0].message.content)

