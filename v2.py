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

 

#Resposta do Modelo de IA:
# Como posso te ajudar hoje? com base nos dados disponiveis, me diga como esta a inadimplencia no brasil
# Informações do DataFrame:
# Número de linhas: 5285192
# Número de colunas: 12

# Colunas disponíveis:
# ['data_base', 'uf', 'cliente', 'ocupacao', 'cnae_secao', 'porte', 'modalidade', 'numero_de_operacoes', 'a_vencer_ate_90_dias', 'carteira_ativa', 'carteira_inadimplida_arrastada', 'ativo_problematico']

# Primeiros 5 registros:
#     data_base  uf cliente                     ocupacao cnae_secao  ... numero_de_operacoes a_vencer_ate_90_dias carteira_ativa carteira_inadimplida_arrastada ativo_problematico
# 0  2014-01-31  AC      PF  PF - Aposentado/pensionista          -  ...               <= 15              7555,73        7943,43                           0,00               5,66
# 1  2014-01-31  AC      PF  PF - Aposentado/pensionista          -  ...                 974           2356768,77     2876502,91                       87272,46          256525,73
# 2  2014-01-31  AC      PF  PF - Aposentado/pensionista          -  ...                 683           2348658,38    18339564,80                      907184,36         1287875,44
# 3  2014-01-31  AC      PF  PF - Aposentado/pensionista          -  ...                 280            394498,12     3955734,59                      417589,30         1134697,17
# 4  2014-01-31  AC      PF  PF - Aposentado/pensionista          -  ...               <= 15              1421,94        5213,82                           0,00            5213,82

# [5 rows x 12 columns]
# Chamando a função generate_insights
# Gerando insights a partir do DataFrame
# Insights gerados com sucesso

# Resposta do Modelo de IA:
# Com base nos dados disponíveis, podemos analisar a inadimplência no Brasil da seguinte forma:

# ### **1. Estado com maior inadimplência:**
# - **São Paulo (SP)** lidera com **564.865 casos**, indicando ser o estado com o maior volume de inadimplentes no período analisado.

# ### **2. Evolução mensal da inadimplência em 2014:**
# - Houve um **pico inicial em fevereiro/2014 (489.283 casos)**, seguido por uma relativa estabilidade ao longo do ano, com pequenas variações mensais.
# - **Tendência de queda moderada**: Após o pico de março/2014 (493.909 casos), os números oscilaram, mas fecharam o ano em **474.158 casos** (dezembro/2014), uma redução de **~4%** em relação ao pico.
# - **Padrão sazonal?** Não há uma queda acentuada em meses específicos, mas sim uma flutuação dentro de uma faixa próxima a 470-490 mil casos.

# ### **3. Insights e possíveis causas:**
# - **Crescimento inicial abrupto (jan-fev/2014)**: O salto de **3.296 para 489.283 casos** pode indicar mudança na metodologia de coleta, crise econômica localizada ou inclusão de novos tipos de dívidas no registro.
# - **Estabilidade posterior**: A inadimplência manteve-se alta, sugerindo **dificuldade estrutural** da população em honrar dívidas (desemprego, juros altos ou endividamento crônico).
# - **SP como epicentro**: O dado reforça a concentração econômica e populacional do estado, mas também pode refletir **maior acesso ao crédito** ou **pressões financeiras mais intensas** na região.

# - **Crescimento inicial abrupto (jan-fev/2014)**: O salto de **3.296 para 489.283 casos** pode indicar mudança na metodologia de coleta, crise econômica localizada ou inclusão de novos tipos de dívidas no registro.
# - **Estabilidade posterior**: A inadimplência manteve-se alta, sugerindo **dificuldade estrutural** da população em honrar dívidas (desemprego, juros altos ou endividamento crônico).
# - **SP como epicentro**: O dado reforça a concentração econômica e populacional do estado, mas também pode refletir **maior acesso ao crédito** ou **pressões financeiras mais int- **Crescimento inicial abrupto (jan-fev/2014)**: O salto de **3.296 para 489.283 casos** pode indicar mudança na metodologia de coleta, crise econômica localizada ou inclusão de novos tipos de dívidas no registro.
# - **Estabilidade posterior**: A inadimplência manteve-se alta, sugerindo **dificuldade estrutural** da população em honrar dívidas (desemprego, juros altos ou endividamento crônico).
# - **SP como epicentro**: O dado reforça a concentração econômica e populacional do estado, mas também pode refletir **maior acesso ao crédito** ou **pressões financeiras mais int- **Crescimento inicial abrupto (jan-fev/2014)**: O salto de **3.296 para 489.283 casos** pode indicar mudança na metodologia de coleta, crise econômica localizada ou inclusão de novos tipos de dívidas no registro.
# - **Estabilidade posterior**: A inadimplência manteve-se alta, sugerindo **dificuldade estrutural** da população em honrar dívidas (desemprego, juros altos ou endividamento crônico).
# - **SP como epicentro**: O dado reforça a concentração econômica e populacional do estado, mas também pode refletir **maior acesso ao crédito** ou **pressões financeiras mais int- **Crescimento inicial abrupto (jan-fev/2014)**: O salto de **3.296 para 489.283 casos** pode indicar mudança na metodologia de coleta, crise econômica localizada ou inclusão de novos tipos de dívidas no registro.
# - **Estabilidade posterior**: A inadimplência manteve-se alta, sugerindo **dificuldade estrutural** da população em honrar dívidas (desemprego, juros altos ou endividamento crônico).
# - **Crescimento inicial abrupto (jan-fev/2014)**: O salto de **3.296 para 489.283 casos** pode indicar mudança na metodologia de coleta, crise econômica localizada ou inclusão de novos tipos de dívidas no registro.
# - **Estabilidade posterior**: A inadimplência manteve-se alta, sugerindo **dificuldade estrutural** da população em honrar dívidas (desemprego, juros altos ou endividamento crôni novos tipos de dívidas no registro.
# - **Estabilidade posterior**: A inadimplência manteve-se alta, sugerindo **dificuldade estrutural** da população em honrar dívidas (desemprego, juros altos ou endividamento crônico).
# - **SP como epicentro**: O dado reforça a concentração econômica e populacional do estado, mas também pode refletir **maior acesso ao crédito** ou **pressões financeiras mais int- **Estabilidade posterior**: A inadimplência manteve-se alta, sugerindo **dificuldade estrutural** da população em honrar dívidas (desemprego, juros altos ou endividamento crônico).
# - **SP como epicentro**: O dado reforça a concentração econômica e populacional do estado, mas também pode refletir **maior acesso ao crédito** ou **pressões financeiras mais intco).
# - **SP como epicentro**: O dado reforça a concentração econômica e populacional do estado, mas também pode refletir **maior acesso ao crédito** ou **pressões financeiras mais int- **SP como epicentro**: O dado reforça a concentração econômica e populacional do estado, mas também pode refletir **maior acesso ao crédito** ou **pressões financeiras mais intensas** na região.

# ### **4. Limitações dos dados:**
# - **Falta de comparação com outros anos**: Sem dados de 2013 ou 2015, não é possível afirmar se 2014 foi atípico ou parte de uma tendência.
# - **Ausência de valores monetários**: Sabemos o número de casos, mas não o valor em reais das dívidas.
# - **Dados desatualizados**: Informações até 2014 podem não refletir a realidade atual (ex.: impactos da crise de 2015-2016 ou da pandemia).

# ### **Recomendações para análise futura:**
# - Cruzar com dados macroeconômicos (desemprego, PIB, taxa de juros) para entender causas.
# - Incluir outros estados para comparar inadimplência per capita.
# - Atualizar a série histórica para identificar tendências recentes.

# **Resumo final:** Em 2014, a inadimplência no Brasil (com destaque para SP) mostrou-se elevada e estável, com um pico inicial preocupante. A persistência de números altos sugere desafios crônicos no pagamento de dívidas pela população.