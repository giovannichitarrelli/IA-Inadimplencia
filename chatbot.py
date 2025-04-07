#V8 Pré definindo insights e acessando dados via .env e azure
import streamlit as st
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain_core.chat_history import InMemoryChatMessageHistory
import httpx
import pandas as pd
import numpy as np
from PIL import Image
import psycopg2
import time
import os
from dotenv import load_dotenv
 
load_dotenv()

api_key = os.getenv("API_KEY")
st.set_page_config(page_title="Análise de Inadimplência", page_icon="")

if "app_initialized" not in st.session_state:
    st.session_state.app_initialized = False
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
    
def get_llm_client():
    return ChatOpenAI(
        api_key=api_key,
        base_url="https://api.deepseek.com",
        model="deepseek-chat",
        http_client=httpx.Client(verify=False)
    )

# def connect_to_db():

#     server = os.getenv('SERVER')
#     database = os.getenv('DATABASE')
#     username = os.getenv('USERNAME')
#     password = os.getenv('PASSWORD')
#     table = os.getenv('TABLE')

#     try:
#         connection_string = (
#             f"DRIVER={{ODBC Driver 18 for SQL Server}};"
#             f"SERVER={server};"
#             f"DATABASE={database};"
#             f"UID={username};"
#             f"PWD={password};"
#             f"Encrypt=yes;"
#             f"TrustServerCertificate=no;"
#             f"Connection Timeout=30;"
#         )
#         conn = pyodbc.connect(connection_string)
#         return conn, table
#     except pyodbc.Error as e:
#         st.error(f"Erro ao conectar ao banco de dados: {str(e)}")
#         return None, None
def connect_to_db():
    try:
        print("Tentando conectar ao banco de dados PostgreSQL no GCP...")
        # Dados de conexão
        host = os.getenv("SERVER")
        database = os.getenv("DATABASE")
        username = os.getenv("USERNAME")
        password = os.getenv("PASSWORD")
        port = os.getenv("PORT")

        # Conexão com o banco de dados
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=username,
            password=password,
            port=port
        )
        print("Conexão com o banco de dados estabelecida com sucesso!")
        return conn

    except Exception as e:
        print("Erro ao conectar ao banco de dados:", e)
        return None
    
def load_data(conn):
    try:
        table = "agg_inad_consolidado"  # Defina a tabela diretamente aqui
        query = f"SELECT * FROM {table}"
        df = pd.read_sql(query, conn)
        return df
    except psycopg2.Error as e:
        st.error(f"Erro ao carregar os dados: {str(e)}")
        return None

import pandas as pd
import numpy as np

def generate_advanced_insights(df):
    """
    Gera insights detalhados sobre inadimplência a partir de dados consolidados de dezembro de 2024
    
    Params:
        df: DataFrame com dados consolidados de inadimplência
    
    Returns:
        String com insights formatados
    """
    # Filtrar apenas dados de dezembro de 2024
    df['data_base'] = pd.to_datetime(df['data_base'], format='%d/%m/%Y', errors='coerce')
    df = df[(df['data_base'].dt.month == 12) & (df['data_base'].dt.year == 2024)].copy()
    
    if df.empty:
        return "Nenhum dado disponível para dezembro de 2024."

    # Preparar dados - mapear regiões
    df['regiao'] = df['uf'].map({
        'AC': 'Norte', 'AM': 'Norte', 'AP': 'Norte', 'PA': 'Norte', 'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
        'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste', 'PB': 'Nordeste', 
        'PE': 'Nordeste', 'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
        'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste', 'DF': 'Centro-Oeste',
        'SP': 'Sudeste', 'RJ': 'Sudeste', 'MG': 'Sudeste', 'ES': 'Sudeste',
        'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul'
    })
    
    # Calcular taxa de inadimplência
    df['taxa_inadimplencia'] = (df['soma_carteira_inadimplida_arrastada'] / df['soma_carteira_ativa'] * 100).fillna(0)
    
    # Calcular índice de ativo problemático
    df['indice_ativo_problematico'] = (df['soma_ativo_problematico'] / df['soma_carteira_ativa'] * 100).fillna(0)
    
    # Calcular projeção de inadimplência em 90 dias
    df['projecao_inadimplencia_90d'] = np.where(
        df['soma_carteira_ativa'] > 0,
        df['soma_a_vencer_ate_90_dias'] * (df['soma_carteira_inadimplida_arrastada'] / df['soma_carteira_ativa']),
        0
    )
    
    # Calcular indicador de reestruturação
    df['indicador_reestruturacao'] = df['soma_ativo_problematico'] - df['soma_carteira_inadimplida_arrastada']
    
    # Determinar tipo de cliente
    df['tipo_cliente'] = df['cliente'].apply(lambda x: 'PF' if 'Física' in str(x) else 'PJ')
    
    # Preparar insights detalhados para dezembro de 2024
    insights = "# ANÁLISE ESTRATÉGICA DE INADIMPLÊNCIA BANCÁRIA - DEZEMBRO 2024\n\n"
    
    # 1. VISÃO GERAL
    insights += "## 1. VISÃO GERAL DO CENÁRIO DE INADIMPLÊNCIA (DEZ/2024)\n\n"
    
    total_inadimplencia = df['soma_carteira_inadimplida_arrastada'].sum()
    total_ativo_problematico = df['soma_ativo_problematico'].sum()
    total_carteira = df['soma_carteira_ativa'].sum()
    taxa_global = (total_inadimplencia / total_carteira * 100) if total_carteira > 0 else 0
    
    insights += f"- **Carteira Total**: R$ {total_carteira:,.2f}\n"
    insights += f"- **Total Inadimplido**: R$ {total_inadimplencia:,.2f} ({taxa_global:.2f}% da carteira total)\n"
    insights += f"- **Ativos Problemáticos**: R$ {total_ativo_problematico:,.2f}\n"
    insights += f"- **Total de Operações**: {df['soma_numero_de_operacoes'].sum():,.0f}\n"
    
    # 2. ANÁLISE REGIONAL
    insights += "\n## 2. PANORAMA REGIONAL DE INADIMPLÊNCIA (DEZ/2024)\n\n"
    
    region_summary = df.groupby('regiao').agg({
        'soma_carteira_inadimplida_arrastada': 'sum',
        'soma_carteira_ativa': 'sum',
        'soma_numero_de_operacoes': 'sum'
    }).reset_index()
    
    region_summary['percentual_inadimplencia'] = region_summary['soma_carteira_inadimplida_arrastada'] / total_inadimplencia * 100
    region_summary['taxa_inadimplencia'] = region_summary['soma_carteira_inadimplida_arrastada'] / region_summary['soma_carteira_ativa'] * 100
    
    for _, row in region_summary.sort_values('soma_carteira_inadimplida_arrastada', ascending=False).iterrows():
        insights += f"### {row['regiao']}:\n"
        insights += f"- **Inadimplência**: R$ {row['soma_carteira_inadimplida_arrastada']:,.2f} "
        insights += f"({row['percentual_inadimplencia']:.2f}% do total inadimplido)\n"
        insights += f"- **Taxa de Inadimplência**: {row['taxa_inadimplencia']:.2f}%\n"
        insights += f"- **Número de Operações**: {row['soma_numero_de_operacoes']:,.0f}\n\n"
    
    # 3. ANÁLISE POR ESTADO
    insights += "\n## 3. ESTADOS COM MAIOR ÍNDICE DE INADIMPLÊNCIA (DEZ/2024)\n\n"
    
    state_summary = df.groupby('uf').agg({
        'soma_carteira_inadimplida_arrastada': 'sum',
        'soma_carteira_ativa': 'sum'
    }).reset_index()
    
    state_summary['percentual_total'] = state_summary['soma_carteira_inadimplida_arrastada'] / total_inadimplencia * 100
    state_summary['taxa_inadimplencia'] = state_summary['soma_carteira_inadimplida_arrastada'] / state_summary['soma_carteira_ativa'] * 100
    
    insights += "### Top 5 Estados em Volume de Inadimplência:\n"
    for _, row in state_summary.sort_values('soma_carteira_inadimplida_arrastada', ascending=False).head(5).iterrows():
        insights += f"- **{row['uf']}**: R$ {row['soma_carteira_inadimplida_arrastada']:,.2f} "
        insights += f"({row['percentual_total']:.2f}% do total, Taxa: {row['taxa_inadimplencia']:.2f}%)\n"
    
    insights += "\n### Top 5 Estados em Taxa de Inadimplência:\n"
    for _, row in state_summary[state_summary['soma_carteira_ativa'] > 1000000].sort_values('taxa_inadimplencia', ascending=False).head(5).iterrows():
        insights += f"- **{row['uf']}**: {row['taxa_inadimplencia']:.2f}% "
        insights += f"(R$ {row['soma_carteira_inadimplida_arrastada']:,.2f})\n"
    
    # 4. ANÁLISE SETORIAL (CNAE)
    insights += "\n## 4. SETORES ECONÔMICOS E INADIMPLÊNCIA (DEZ/2024)\n\n"
    
    cnae_summary = df.groupby('cnae_secao').agg({
        'soma_carteira_inadimplida_arrastada': 'sum',
        'soma_carteira_ativa': 'sum',
        'soma_numero_de_operacoes': 'sum'
    }).reset_index()
    
    cnae_summary['percentual_total'] = cnae_summary['soma_carteira_inadimplida_arrastada'] / total_inadimplencia * 100
    cnae_summary['taxa_inadimplencia'] = cnae_summary['soma_carteira_inadimplida_arrastada'] / cnae_summary['soma_carteira_ativa'] * 100
    
    insights += "### Setores com Maior Volume de Inadimplência:\n"
    for _, row in cnae_summary.sort_values('soma_carteira_inadimplida_arrastada', ascending=False).head(5).iterrows():
        insights += f"- **{row['cnae_secao']}**: R$ {row['soma_carteira_inadimplida_arrastada']:,.2f} "
        insights += f"({row['percentual_total']:.2f}% do total, Taxa: {row['taxa_inadimplencia']:.2f}%)\n"
    
    insights += "\n### Setores com Maior Taxa de Inadimplência:\n"
    for _, row in cnae_summary[cnae_summary['soma_carteira_ativa'] > 1000000].sort_values('taxa_inadimplencia', ascending=False).head(5).iterrows():
        insights += f"- **{row['cnae_secao']}**: {row['taxa_inadimplencia']:.2f}% "
        insights += f"(R$ {row['soma_carteira_inadimplida_arrastada']:,.2f})\n"
    
    # 5. ANÁLISE POR TIPO DE CLIENTE (PF vs PJ)
    insights += "\n## 5. COMPARATIVO PESSOA FÍSICA VS PESSOA JURÍDICA (DEZ/2024)\n\n"
    
    client_type_summary = df.groupby('tipo_cliente').agg({
        'soma_carteira_inadimplida_arrastada': 'sum',
        'soma_carteira_ativa': 'sum',
        'soma_numero_de_operacoes': 'sum',
        'soma_ativo_problematico': 'sum'
    }).reset_index()
    
    client_type_summary['taxa_inadimplencia'] = client_type_summary['soma_carteira_inadimplida_arrastada'] / client_type_summary['soma_carteira_ativa'] * 100
    client_type_summary['media_por_operacao'] = client_type_summary['soma_carteira_inadimplida_arrastada'] / client_type_summary['soma_numero_de_operacoes']
    
    for _, row in client_type_summary.iterrows():
        insights += f"### {row['tipo_cliente']}:\n"
        insights += f"- **Inadimplência Total**: R$ {row['soma_carteira_inadimplida_arrastada']:,.2f}\n"
        insights += f"- **Taxa de Inadimplência**: {row['taxa_inadimplencia']:.2f}%\n"
        insights += f"- **Ativos Problemáticos**: R$ {row['soma_ativo_problematico']:,.2f}\n"
        insights += f"- **Operações**: {row['soma_numero_de_operacoes']:,.0f}\n"
        insights += f"- **Média por Operação**: R$ {row['media_por_operacao']:,.2f}\n\n"
    
    # 6. ANÁLISE POR PORTE
    insights += "\n## 6. INADIMPLÊNCIA POR PORTE DE CLIENTE (DEZ/2024)\n\n"
    
    size_summary = df.groupby(['tipo_cliente', 'porte']).agg({
        'soma_carteira_inadimplida_arrastada': 'sum',
        'soma_carteira_ativa': 'sum',
        'soma_ativo_problematico': 'sum'
    }).reset_index()
    
    size_summary['taxa_inadimplencia'] = size_summary['soma_carteira_inadimplida_arrastada'] / size_summary['soma_carteira_ativa'] * 100
    size_summary['indice_problematico'] = size_summary['soma_ativo_problematico'] / size_summary['soma_carteira_ativa'] * 100
    
    for tipo in ['PF', 'PJ']:
        insights += f"### {tipo}:\n"
        for _, row in size_summary[size_summary['tipo_cliente'] == tipo].sort_values('soma_carteira_inadimplida_arrastada', ascending=False).iterrows():
            insights += f"- **{row['porte']}**: R$ {row['soma_carteira_inadimplida_arrastada']:,.2f} "
            insights += f"(Taxa: {row['taxa_inadimplencia']:.2f}%, Índice Problemático: {row['indice_problematico']:.2f}%)\n"
        insights += "\n"
    
    # 7. ANÁLISE POR MODALIDADE
    insights += "\n## 7. MODALIDADES DE CRÉDITO E INADIMPLÊNCIA (DEZ/2024)\n\n"
    
    modality_summary = df.groupby('modalidade').agg({
        'soma_carteira_inadimplida_arrastada': 'sum',
        'soma_carteira_ativa': 'sum',
        'soma_numero_de_operacoes': 'sum'
    }).reset_index()
    
    modality_summary['taxa_inadimplencia'] = modality_summary['soma_carteira_inadimplida_arrastada'] / modality_summary['soma_carteira_ativa'] * 100
    modality_summary['percentual_total'] = modality_summary['soma_carteira_inadimplida_arrastada'] / total_inadimplencia * 100
    
    insights += "### Top Modalidades por Volume de Inadimplência:\n"
    for _, row in modality_summary.sort_values('soma_carteira_inadimplida_arrastada', ascending=False).head(6).iterrows():
        insights += f"- **{row['modalidade']}**: R$ {row['soma_carteira_inadimplida_arrastada']:,.2f} "
        insights += f"({row['percentual_total']:.2f}% do total, Taxa: {row['taxa_inadimplencia']:.2f}%)\n"
    
    insights += "\n### Top Modalidades por Taxa de Inadimplência:\n"
    for _, row in modality_summary[modality_summary['soma_carteira_ativa'] > 1000000].sort_values('taxa_inadimplencia', ascending=False).head(5).iterrows():
        insights += f"- **{row['modalidade']}**: {row['taxa_inadimplencia']:.2f}% "
        insights += f"(R$ {row['soma_carteira_inadimplida_arrastada']:,.2f})\n"
    
    # 8. ANÁLISE POR OCUPAÇÃO (PF)
    insights += "\n## 8. INADIMPLÊNCIA POR OCUPAÇÃO - PESSOA FÍSICA (DEZ/2024)\n\n"
    
    occupation_summary = df[df['tipo_cliente'] == 'PF'].groupby('ocupacao').agg({
        'soma_carteira_inadimplida_arrastada': 'sum',
        'soma_carteira_ativa': 'sum',
        'soma_numero_de_operacoes': 'sum'
    }).reset_index()
    
    occupation_summary['taxa_inadimplencia'] = occupation_summary['soma_carteira_inadimplida_arrastada'] / occupation_summary['soma_carteira_ativa'] * 100
    occupation_summary['media_por_operacao'] = occupation_summary['soma_carteira_inadimplida_arrastada'] / occupation_summary['soma_numero_de_operacoes']
    
    insights += "### Ocupações com Maior Volume de Inadimplência:\n"
    for _, row in occupation_summary.sort_values('soma_carteira_inadimplida_arrastada', ascending=False).head(5).iterrows():
        insights += f"- **{row['ocupacao']}**: R$ {row['soma_carteira_inadimplida_arrastada']:,.2f} "
        insights += f"(Taxa: {row['taxa_inadimplencia']:.2f}%, Média: R$ {row['media_por_operacao']:,.2f})\n"
    
    insights += "\n### Ocupações com Maior Taxa de Inadimplência:\n"
    valid_occupations = occupation_summary[occupation_summary['soma_carteira_ativa'] > 500000]
    for _, row in valid_occupations.sort_values('taxa_inadimplencia', ascending=False).head(5).iterrows():
        insights += f"- **{row['ocupacao']}**: {row['taxa_inadimplencia']:.2f}% "
        insights += f"(Volume: R$ {row['soma_carteira_inadimplida_arrastada']:,.2f})\n"
    
    # 9. PROJEÇÕES E RISCO FUTURO
    insights += "\n## 9. PROJEÇÃO DE INADIMPLÊNCIA EM 90 DIAS (DEZ/2024)\n\n"
    
    projection_summary = df.groupby(['tipo_cliente', 'porte']).agg({
        'projecao_inadimplencia_90d': 'sum',
        'soma_a_vencer_ate_90_dias': 'sum',
        'soma_carteira_inadimplida_arrastada': 'sum'
    }).reset_index()
    
    projection_summary['risco_percentual'] = projection_summary['projecao_inadimplencia_90d'] / projection_summary['soma_a_vencer_ate_90_dias'] * 100
    projection_summary['aumento_previsto'] = projection_summary['projecao_inadimplencia_90d'] / projection_summary['soma_carteira_inadimplida_arrastada'] * 100
    
    insights += "### Projeção por Tipo e Porte de Cliente:\n"
    for _, row in projection_summary.sort_values('projecao_inadimplencia_90d', ascending=False).head(8).iterrows():
        insights += f"- **{row['tipo_cliente']} - {row['porte']}**: R$ {row['projecao_inadimplencia_90d']:,.2f} "
        insights += f"(Risco: {row['risco_percentual']:.2f}%, Aumento Previsto: {row['aumento_previsto']:.2f}%)\n"
    
    # 10. REESTRUTURAÇÃO DE DÍVIDAS
    insights += "\n## 10. ANÁLISE DE REESTRUTURAÇÃO DE DÍVIDAS (DEZ/2024)\n\n"
    
    restructuring_summary = df.groupby(['tipo_cliente', 'porte']).agg({
        'indicador_reestruturacao': 'sum',
        'soma_ativo_problematico': 'sum',
        'soma_carteira_inadimplida_arrastada': 'sum'
    }).reset_index()
    
    restructuring_summary['percentual_reestruturacao'] = restructuring_summary['indicador_reestruturacao'] / restructuring_summary['soma_ativo_problematico'] * 100
    
    insights += "### Indicadores de Reestruturação por Segmento:\n"
    for _, row in restructuring_summary.sort_values('indicador_reestruturacao', ascending=False).head(6).iterrows():
        if row['soma_ativo_problematico'] > 0:
            insights += f"- **{row['tipo_cliente']} - {row['porte']}**: R$ {row['indicador_reestruturacao']:,.2f} "
            insights += f"({row['percentual_reestruturacao']:.2f}% dos ativos problemáticos)\n"
    
    # 11. RECOMENDAÇÕES ESTRATÉGICAS
    insights += "\n## 11. RECOMENDAÇÕES ESTRATÉGICAS (DEZ/2024)\n\n"
    
    insights += "### Ações Recomendadas por Segmento de Risco:\n"
    
    top_cnae_risk = cnae_summary.sort_values('taxa_inadimplencia', ascending=False).head(3)
    insights += "#### Setores Econômicos de Alto Risco:\n"
    for _, row in top_cnae_risk.iterrows():
        insights += f"- **{row['cnae_secao']}**: Implementar monitoramento especial e revisar políticas de crédito\n"
    
    top_region_risk = region_summary.sort_values('taxa_inadimplencia', ascending=False).head(2)
    insights += "\n#### Regiões Críticas:\n"
    for _, row in top_region_risk.iterrows():
        insights += f"- **{row['regiao']}**: Considerar condições macroeconômicas regionais e ajustar estratégias de cobrança\n"
    
    top_modality_risk = modality_summary.sort_values('taxa_inadimplencia', ascending=False).head(3)
    insights += "\n#### Modalidades de Alto Risco:\n"
    for _, row in top_modality_risk.iterrows():
        insights += f"- **{row['modalidade']}**: Revisar critérios de aprovação e limites de crédito\n"
    
    # Conclusão
    insights += "\n## CONCLUSÃO EXECUTIVA (DEZ/2024)\n\n"
    insights += f"- A taxa global de inadimplência em dezembro de 2024 está em **{taxa_global:.2f}%** da carteira total\n"
    insights += "- Aproximadamente **{:.2f}%** do volume inadimplido está concentrado na região {}\n".format(
        region_summary.iloc[0]['percentual_inadimplencia'], 
        region_summary.iloc[0]['regiao']
    )
    insights += "- O setor **{}** apresenta a maior concentração de inadimplência ({:.2f}%)\n".format(
        cnae_summary.iloc[0]['cnae_secao'],
        cnae_summary.iloc[0]['percentual_total']
    )
    insights += "- A modalidade **{}** apresenta a maior taxa de inadimplência ({:.2f}%)\n".format(
        modality_summary.sort_values('taxa_inadimplencia', ascending=False).iloc[0]['modalidade'],
        modality_summary.sort_values('taxa_inadimplencia', ascending=False).iloc[0]['taxa_inadimplencia']
    )
    insights += "- Projeção de inadimplência para os próximos 90 dias indica potencial aumento de até **{:.2f}%**\n".format(
        projection_summary['aumento_previsto'].mean()
    )
    
    insights += "\n### Próximos Passos Recomendados:\n"
    insights += "1. Revisar políticas de crédito para os setores e modalidades de maior risco\n"
    insights += "2. Monitorar de perto as regiões com altas taxas de inadimplência\n"
    insights += "3. Avaliar estratégias de reestruturação para os segmentos com ativos problemáticos elevados\n"
    insights += "4. Implementar alertas precoces baseados nas projeções de 90 dias\n"
    
    return insights

def main():
    st.title(" Chatbot Inadimplinha")
    st.caption(" Chatbot Inadimplinha desenvolvido por Grupo de Inadimplência EY")

    conn = connect_to_db()
    if conn is None:
        st.stop()


    if "insights" not in st.session_state:
        df = load_data(conn)
        if df is None:
            conn.close()
            st.stop()
        st.session_state.insights = generate_advanced_insights(df)

    llm = get_llm_client()
    prompt_template = ChatPromptTemplate.from_messages([
        ("system", (
            "Você é um especialista em análise de inadimplência no Brasil. "
            "Use os insights pré-calculados abaixo para responder às perguntas do usuário de forma clara e objetiva. "
            "Não gere consultas SQL ou acesse dados diretamente; baseie-se apenas nos insights fornecidos. "
            "Se a pergunta não puder ser respondida com os insights disponíveis, informe isso ao usuário. "
            "Insights disponíveis:\n\n{insights}"
        )),
        ("human", "{input}")
    ])

    # Criar a cadeia de execução
    chain = prompt_template | llm

    # Inicializar o histórico de mensagens
    if "chat_history_store" not in st.session_state:
        st.session_state.chat_history_store = InMemoryChatMessageHistory()

    # Envolver a cadeia com histórico de mensagens
    conversation = RunnableWithMessageHistory(
        runnable=chain,
        get_session_history=lambda: st.session_state.chat_history_store,
        input_messages_key="input",
        history_messages_key="chat_history"
    )

    # Adicionar mensagem inicial apenas uma vez
    if not st.session_state.app_initialized and not st.session_state.chat_history:
        initial_message = "Como posso te ajudar hoje?"
        st.session_state.chat_history.append({"role": "assistant", "content": initial_message})
        st.session_state.chat_history_store.add_ai_message(initial_message)
        st.session_state.app_initialized = True

    # Exibir histórico de chat para o usuário
    for message in st.session_state.chat_history:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    if prompt := st.chat_input("Faça uma pergunta sobre a inadimplência"):
        # Adicionar a pergunta do usuário à interface de chat
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Adicionar à exibição do histórico
        st.session_state.chat_history.append({"role": "user", "content": prompt})
        
        # Processar a resposta
        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            
            try:
                with st.spinner(""):
                    # Executar a consulta com os insights pré-definidos
                    response = conversation.invoke(
                        {"input": prompt, "insights": st.session_state.insights},
                        config={"configurable": {"session_id": "default"}}
                    )
                    response_stream = response.content
                    
                    # Simulando streaming para melhor UX
                    full_response = ""
                    for i in range(len(response_stream)):
                        full_response = response_stream[:i+1]
                        message_placeholder.markdown(full_response + "▌")
                        time.sleep(0.01)
                    message_placeholder.markdown(full_response)
                    
                    # Adicionar à exibição do histórico
                    st.session_state.chat_history.append({"role": "assistant", "content": full_response})
                
            except Exception as e:
                error_message = f"Erro no processamento: {str(e)}"
                message_placeholder.markdown(error_message)
                st.session_state.chat_history.append({"role": "assistant", "content": error_message})
                st.session_state.chat_history_store.add_ai_message(error_message)

        # Sidebar
  
  
    with st.sidebar:
        ey_logo = Image.open(r"EY_Logo.png")
        ey_logo_resized = ey_logo.resize((100, 100))   
        st.sidebar.image(ey_logo_resized)
        st.sidebar.header("EY Academy | Inadimplência")

        st.sidebar.subheader("🔍 Sugestões de Análise")
        st.sidebar.write("➡️ Qual estado com maior inadimplência e quais os valores devidos?")
        st.sidebar.write("➡️ Qual tipo de cliente apresenta o maior número de operações?")
        st.sidebar.write("➡️ Em qual modalidade existe maior inadimplência?")
        st.sidebar.write("➡️ Compare a inadimplência entre PF e PJ")
        st.sidebar.write("➡️ Qual ocupação entre PF possui maior inadimplência?")
        st.sidebar.write("➡️ Qual o principal porte de cliente com inadimplência entre PF?")
        # st.sidebar.write("➡️ Qual seção CNAE possui a maior inadimplência?")
        # st.sidebar.write("➡️ Qual estado tem o maior valor médio de operações a vencer em até 90 dias?")
        
        # Botão para limpar histórico de conversa
        if st.button("Limpar Conversa"):
            st.session_state.chat_history_store = InMemoryChatMessageHistory()
            st.session_state.chat_history = []
            st.session_state.app_initialized = False
            if "insights" in st.session_state:
                del st.session_state.insights  # Recarregar insights na próxima execução
            st.rerun()

    conn.close()

if __name__ == "__main__":
    main()
