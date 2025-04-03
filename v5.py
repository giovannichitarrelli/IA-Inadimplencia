# V4 - transformando arquivo parquet para csv
import streamlit as st
from openai import OpenAI
import httpx
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("API_KEY")
 
st.set_page_config(page_title="Análise de Inadimplência", page_icon="💼")

@st.cache_resource
def get_openai_client():
    return OpenAI(
        api_key=api_key,
        base_url="https://api.deepseek.com", 
        http_client=httpx.Client(verify=False)
    )

@st.cache_data
def load_data():
    return pd.read_csv(
        r"C:\Users\AT154GY\OneDrive - EY\Desktop\IA Inadimplencia\df_cons_agg.csv"
    ) 

def generate_base_insights(df):
    # print("Tipos de dados originais:")
    # print(df.dtypes)

    numeric_columns = [
        'carteira_inadimplida_arrastada', 
        'carteira_ativa', 
        'a_vencer_ate_90_dias', 
        'ativo_problematico', 
        'numero_de_operacoes'
    ]

    for col in numeric_columns:
        # Tente múltiplas estratégias de conversão
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(',', '.').str.replace('R$', '').str.replace(' ', ''), 
            errors='coerce'
        ).fillna(0)

    # Adicione verificação de valores após conversão
    # print("\nValores após conversão:")
    # for col in numeric_columns:
    #     print(f"{col} - Soma: {df[col].sum()}, Tipo: {df[col].dtype}")


    # Preparar dados
    df['regiao'] = df['uf'].map({
        'AC': 'Norte', 'AM': 'Norte', 'AP': 'Norte', 'PA': 'Norte', 'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
        'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste', 'PB': 'Nordeste', 'PE': 'Nordeste', 'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
        'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste', 'DF': 'Centro-Oeste',
        'SP': 'Sudeste', 'RJ': 'Sudeste', 'MG': 'Sudeste', 'ES': 'Sudeste',
        'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul'
    })
    
    # Preparar insights detalhados
    insights = "Contexto Abrangente de Inadimplência:\n\n"
    
    # Filtrar por estado e somar a inadimplência
    inadimplencia_por_estado = df.groupby('uf')['carteira_inadimplida_arrastada'].sum()

    # Adicionar ao contexto de insights
    insights += "\nTotal inadimplente por estado:\n"
    for estado, valor in inadimplencia_por_estado.items():
        insights += f"- {estado}: R$ {valor:,.2f}\n"

    # 1. Mapa de Região e Inadimplência
    regiao_inadimplencia = df.groupby('regiao')['carteira_inadimplida_arrastada'].sum().sort_values(ascending=False)
    insights += "1. Panorama Regional de Inadimplência:\n"
    for regiao, valor in regiao_inadimplencia.items():
        percentual = (valor / df['carteira_inadimplida_arrastada'].sum()) * 100
        insights += f"- {regiao}: R$ {valor:,.2f} ({percentual:.2f}% da inadimplência total)\n"
    
    # 2. Inadimplência por CNAE
    cnae_inadimplencia = df.groupby('cnae_secao')['carteira_inadimplida_arrastada'].sum().sort_values(ascending=False)
    insights += "\n2. Setores Econômicos e Inadimplência:\n"
    for cnae, valor in cnae_inadimplencia.head(5).items():
        percentual = (valor / df['carteira_inadimplida_arrastada'].sum()) * 100
        insights += f"- {cnae}: R$ {valor:,.2f} ({percentual:.2f}% da inadimplência)\n"
    
    # 3. Comparativo PF vs PJ
    df['tipo_cliente'] = df['cliente'].apply(lambda x: 'PF' if 'Física' in str(x) else 'PJ')
    cliente_inadimplencia = df.groupby('tipo_cliente')['carteira_inadimplida_arrastada'].agg(['sum', 'mean'])
    insights += "\n3. Comparativo Pessoa Física vs Pessoa Jurídica:\n"
    for tipo, dados in cliente_inadimplencia.iterrows():
        insights += f"- {tipo}: Total R$ {dados['sum']:,.2f}, Média R$ {dados['mean']:,.2f}\n"
    
    # 4. Modalidades por Tipo de Cliente
    modalidade_cliente_inadimplencia = df.groupby(['tipo_cliente', 'modalidade'])['carteira_inadimplida_arrastada'].sum()
    insights += "\n4. Modalidades de Inadimplência:\n"
    for (tipo, modalidade), valor in modalidade_cliente_inadimplencia.sort_values(ascending=False).head(6).items():
        insights += f"- {tipo} - {modalidade}: R$ {valor:,.2f}\n"
    
    # 5. Porte do Cliente
    porte_inadimplencia = df.groupby(['tipo_cliente', 'porte'])['carteira_inadimplida_arrastada'].sum()
    insights += "\n5. Inadimplência por Porte de Empresa:\n"
    for (tipo, porte), valor in porte_inadimplencia.sort_values(ascending=False).head(6).items():
        insights += f"- {tipo} - {porte}: R$ {valor:,.2f}\n"
    
    # 6. Previsão de Inadimplência em 90 dias
    df['previsao_inadimplencia_90d'] = df['a_vencer_ate_90_dias'] * (df['carteira_inadimplida_arrastada'] / df['carteira_ativa'])
    previsao_porte = df.groupby('porte')['previsao_inadimplencia_90d'].mean()
    insights += "\n6. Previsão de Inadimplência em 90 dias por Porte:\n"
    for porte, previsao in previsao_porte.items():
        insights += f"- {porte}: R$ {previsao:,.2f}\n"
    
    # 7. Crescimento de Operações Inadimplentes
    anos_operacoes = df.groupby('data_base').agg({
        'numero_de_operacoes': 'sum',
        'carteira_inadimplida_arrastada': 'sum'
    })
    insights += "\n7. Evolução de Operações Inadimplentes:\n"
    for ano, dados in anos_operacoes.iterrows():
        insights += f"- {ano}: {dados['numero_de_operacoes']} operações, R$ {dados['carteira_inadimplida_arrastada']:,.2f} inadimplidos\n"
    
    # 8. Inadimplência por Ocupação
    ocupacao_inadimplencia = df[df['tipo_cliente'] == 'PF'].groupby('ocupacao')['carteira_inadimplida_arrastada'].sum()
    insights += "\n8. Inadimplência por Ocupação (Pessoa Física):\n"
    for ocupacao, valor in ocupacao_inadimplencia.sort_values(ascending=False).head(5).items():
        insights += f"- {ocupacao}: R$ {valor:,.2f}\n"
    
    # 9. Projeção de Inadimplência Futura
    insights += "\n9. Projeção Estratégica de Inadimplência:\n"
    insights += "- Análise detalhada requer modelagem estatística avançada\n"
    insights += "- Fatores-chave: porte do cliente, setor econômico, comportamento histórico\n"
    
    # 10. Indicador de Reestruturação
    df['indicador_reestruturacao'] = df['ativo_problematico'] - df['carteira_inadimplida_arrastada']
    reestruturacao_analise = df.groupby('porte')['indicador_reestruturacao'].mean()
    insights += "\n10. Análise de Reestruturação:\n"
    for porte, indicador in reestruturacao_analise.items():
        insights += f"- {porte}: Indicador médio R$ {indicador:,.2f}\n"
    
    # Temas Adicionais de um Especialista em Inadimplência
    insights += "\n11. Análises Estratégicas Adicionais:\n"
    insights += "- Correlação entre modalidade de crédito e risco de inadimplência\n"
    insights += "- Impacto de ciclos econômicos no comportamento de pagamento\n"
    insights += "- Segmentação de clientes por perfil de risco\n"
    insights += "- Estratégias de mitigação de inadimplência\n"
    
    # Contextualização Final
    insights += "\nNOTA IMPORTANTE:\n"
    insights += "- Estes insights são baseados em análise estatística descritiva\n"
    insights += "- Recomenda-se análise aprofundada para decisões estratégicas\n"
    insights += "- Variáveis externas podem impactar significativamente as projeções\n"
    
    # Análises Adicionais
    insights += "\n12. Análises Complementares:\n"
    
    # Estado mais Inadimplente
    estado_inadimplencia = df.groupby('uf')['carteira_inadimplida_arrastada'].agg(['sum', 'mean']).sort_values('sum', ascending=False)
    top_estado_inadimplente = estado_inadimplencia.head(3)
    insights += "Estados com Maior Inadimplência:\n"
    for estado, dados in top_estado_inadimplente.iterrows():
        percentual = (dados['sum'] / df['carteira_inadimplida_arrastada'].sum()) * 100
        insights += f"- {estado}: R$ {dados['sum']:,.2f} ({percentual:.2f}% da inadimplência total), Média por registro: R$ {dados['mean']:,.2f}\n"
    
    # Análise de Concentração de Inadimplência
    insights += "\nConcentração de Inadimplência:\n"
    # Top 5 clientes com maior inadimplência
    top_clientes_inadimplentes = df.groupby('cliente')['carteira_inadimplida_arrastada'].sum().nlargest(5)
    insights += "Top 5 Clientes com Maior Inadimplência:\n"
    for cliente, valor in top_clientes_inadimplentes.items():
        percentual = (valor / df['carteira_inadimplida_arrastada'].sum()) * 100
        insights += f"- {cliente}: R$ {valor:,.2f} ({percentual:.2f}% da inadimplência total)\n"
    
    # Análise de Distribuição de Operações
    insights += "\nDistribuição de Operações:\n"
    operacoes_por_cliente = df.groupby('cliente')['numero_de_operacoes'].agg(['mean', 'sum', 'max'])
    insights += f"Média de Operações por Cliente: {operacoes_por_cliente['mean'].mean():.2f}\n"
    insights += f"Total de Operações: {operacoes_por_cliente['sum'].sum():,.0f}\n"
    
    # Análise de Ativos Problemáticos
    insights += "\nAtivos Problemáticos:\n"
    ativos_por_porte = df.groupby('porte')['ativo_problematico'].agg(['sum', 'mean'])
    for porte, dados in ativos_por_porte.iterrows():
        insights += f"- {porte}: Total R$ {dados['sum']:,.2f}, Média R$ {dados['mean']:,.2f}\n"
    
    # Análise de Carteira Ativa vs Inadimplida
    df['percentual_inadimplencia'] = df['carteira_inadimplida_arrastada'] / df['carteira_ativa'] * 100
    percentual_inadimplencia_por_modalidade = df.groupby('modalidade')['percentual_inadimplencia'].mean().sort_values(ascending=False)
    insights += "\nPercentual de Inadimplência por Modalidade:\n"
    for modalidade, percentual in percentual_inadimplencia_por_modalidade.head(5).items():
        insights += f"- {modalidade}: {percentual:.2f}%\n"
    
    # Análise de Ocupações com Maior Risco
    ocupacoes_risco = df[df['tipo_cliente'] == 'PF'].groupby('ocupacao').agg({
        'carteira_inadimplida_arrastada': 'sum',
        'percentual_inadimplencia': 'mean'
    }).sort_values('carteira_inadimplida_arrastada', ascending=False)
    insights += "\nOcupações de Maior Risco (Pessoa Física):\n"
    for ocupacao, dados in ocupacoes_risco.head(5).iterrows():
        insights += f"- {ocupacao}: Inadimplência R$ {dados['carteira_inadimplida_arrastada']:,.2f}, Percentual {dados['percentual_inadimplencia']:.2f}%\n"
    
    # Análise de Sazonalidade
    # More robust date conversion with error handling
    df['ano'] = pd.to_datetime(df['data_base'], format='%d/%m/%Y', errors='coerce').dt.year
    sazonalidade_inadimplencia = df.groupby('ano')['carteira_inadimplida_arrastada'].sum()
    insights += "\nSazonalidade da Inadimplência:\n"
    for ano, valor in sazonalidade_inadimplencia.items():
        insights += f"- {ano}: R$ {valor:,.2f}\n"
    
    # Conclusão Executiva
    insights += "\n🔍 Conclusão Executiva:\n"
    insights += "- Análise multidimensional revela padrões complexos de inadimplência\n"
    insights += "- Recomenda-se estratificação de risco por múltiplos fatores\n"
    insights += "- Monitoramento contínuo e modelagem preditiva são cruciais\n"
    
    return insights

def main():
    st.title("💬 Chatbot Inadimplinha")
    st.caption("🚀 Chatbot Inadimplinha desenvolvido por Grupo de Inadimplência EY")
    
    df = load_data()
    base_insights = generate_base_insights(df)
    if 'messages' not in st.session_state:
        st.session_state.messages = [
            {"role": "system", "content": f"Você é um especialista no setor bancário especializado em análise de inadimplência no Brasil. Aqui estão os dados a serem consultados:\n{base_insights}"},
            {"role": "assistant", "content": "Como posso te ajudar hoje?"}
        ]
    
    for message in st.session_state.messages[1:]:   
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    if prompt := st.chat_input("Faça uma pergunta sobre a inadimplência"):
        client = get_openai_client()
        api_key = client.api_key
        if not api_key:
            st.info("Por favor, adicione sua chave de API para continuar.")
            st.stop()
  
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        with st.chat_message("user"):
            st.markdown(prompt)
         
        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            full_response = ""  
            
            try:
                response = client.chat.completions.create(
                    model="deepseek-chat",
                    messages=st.session_state.messages,
                    stream=True   
                )
        
                for chunk in response:
                    if chunk.choices[0].delta.content is not None:
                        full_response += chunk.choices[0].delta.content
                        message_placeholder.markdown(full_response + "▌")

                message_placeholder.markdown(full_response)

            except Exception as e:
                full_response = f"Erro: {str(e)}"
                message_placeholder.markdown(full_response)
            
            st.session_state.messages.append({"role": "assistant", "content": full_response})
        
    with st.sidebar:
        
        st.sidebar.header("EY Academy | Inadimplência")
        st.sidebar.subheader("🔍 Sugestões de Análise")

        st.sidebar.write("Quais são os principais estados com maior inadimplência?")
        st.sidebar.write("Qual estado com maior inadimplência e quais os valores devidos?")
        st.sidebar.write("Compare a inadimplência entre PFe PJ")
        st.sidebar.write("Qual o perfil de inadimplência em São Paulo?")
        # st.sidebar.header("EY Academy | Inadimplência")
        # st.sidebar.write(f"Número de registros: {len(df)}")
        # st.sidebar.write("Colunas disponíveis:")
        # st.sidebar.write(df.columns.tolist())

if __name__ == "__main__":
    main()
