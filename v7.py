import streamlit as st
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
import httpx
import pandas as pd
import plotly.express as px  # Added missing import for visualizations
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import time

# Load environment variables
load_dotenv()

# Get API key from environment
api_key = os.getenv("API_KEY")

# Streamlit page configuration
st.set_page_config(page_title="Análise de Inadimplência", page_icon="💼")

# Initialize LLM client
def get_llm_client():
    return ChatOpenAI(
        api_key=api_key,
        base_url="https://api.deepseek.com",
        model="deepseek-chat",
        http_client=httpx.Client(verify=False)
    )

# Database connection
def connect_to_db():
    try:
        host = os.getenv("SERVER")
        database = os.getenv("DATABASE")
        username = os.getenv("USERNAME")
        password = os.getenv("PASSWORD")
        port = os.getenv("PORT")

        if not all([host, database, username, password, port]):
            raise ValueError("One or more environment variables are missing in .env")

        encoded_password = quote_plus(password)
        connection_string = f"postgresql://{username}:{encoded_password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)

        with engine.connect() as connection:
            print("Database connection established successfully!")
        return engine

    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

# Generate advanced insights
def generate_advanced_insights(df):
    df_atual = df[df['data_base'] == '2024-12-01']
    insights = []

    # Insight 1: Top UFs by default
    uf_inad = df_atual.groupby('uf')['soma_carteira_inadimplida_arrastada'].sum().sort_values(ascending=False)
    top_ufs = uf_inad.head(3)
    insights.append(f"Os estados com maior inadimplência são: {', '.join(top_ufs.index)}, "
                    f"com valores de R$ {top_ufs.iloc[0]:,.2f}, R$ {top_ufs.iloc[1]:,.2f} e R$ {top_ufs.iloc[2]:,.2f}.")

    # Insight 2: Top modalities by default
    mod_inad = df_atual.groupby('modalidade')['soma_carteira_inadimplida_arrastada'].sum().sort_values(ascending=False)
    top_mod = mod_inad.head(3)
    insights.append(f"As modalidades com maior inadimplência são: {', '.join(top_mod.index)}, "
                    f"com valores de R$ {top_mod.iloc[0]:,.2f}, R$ {top_mod.iloc[1]:,.2f} e R$ {top_mod.iloc[2]:,.2f}.")

    # Add more insights as in your original code...

    return "\n\n".join(insights)

# Dynamic SQL query based on user intent
def create_dynamic_query(user_intent):
    base_query = "SELECT * FROM table_agg_inad_consolidado WHERE data_base = '2024-12-01'"
    queries = {
        "UF_INAD": f"{base_query} ORDER BY soma_carteira_inadimplida_arrastada DESC",
        "MODALIDADE_INAD": f"{base_query} ORDER BY soma_carteira_inadimplida_arrastada DESC",
        "PF_PJ_COMP": base_query,
        "OCUPACAO_PF": f"{base_query} AND cliente = 'PF' ORDER BY soma_carteira_inadimplida_arrastada DESC",
        "CNAE_PJ": f"{base_query} AND cliente = 'PJ' ORDER BY soma_carteira_inadimplida_arrastada DESC",
        "PORTE_CLIENTE": f"{base_query} ORDER BY soma_carteira_inadimplida_arrastada DESC",
        "OPERACOES_VENCER": f"{base_query} ORDER BY media_a_vencer_ate_90_dias DESC"
    }
    return queries.get(user_intent, base_query)

# Classify user intent
def classify_user_intent(user_query, llm):
    prompt = f"""
    Analise a pergunta: "{user_query}"
    Classifique em uma categoria:
    - UF_INAD
    - MODALIDADE_INAD
    - PF_PJ_COMP
    - OCUPACAO_PF
    - CNAE_PJ
    - PORTE_CLIENTE
    - OPERACOES_VENCER
    - CONCENTRACAO
    - GERAL
    Retorne apenas a categoria.
    """
    response = llm.invoke(prompt)
    return response.content.strip()

# Process user question
def process_question_with_insights(user_query, df, llm):
    intent = classify_user_intent(user_query, llm)
    
    if intent == "UF_INAD":
        result_df = df.groupby('uf')['soma_carteira_inadimplida_arrastada'].sum().sort_values(ascending=False)
        insight_type = "distribuição estadual de inadimplência"
    elif intent == "MODALIDADE_INAD":
        result_df = df.groupby('modalidade')['soma_carteira_inadimplida_arrastada'].sum().sort_values(ascending=False)
        insight_type = "inadimplência por modalidade de crédito"
    else:
        result_df = df
        insight_type = "análise geral"

    prompt = f"""
    Pergunta: "{user_query}"
    Dados: {result_df.head(5).to_string()}
    Tipo de insight: {insight_type}
    Responda com:
    1. Resposta direta
    2. Valores em R$
    3. Insight adicional
    """
    response = llm.invoke(prompt)
    return response.content

# System prompt
system_prompt = """
Você é um especialista em análise de inadimplência no Brasil.
Responda com base nos dados de dezembro de 2024.
Formate valores como R$ XX.XXX,XX e contextualize os números.
"""

# Generate visualizations
def generate_visual_insight(intent, df):
    if intent == "UF_INAD":
        top_ufs = df.groupby('uf')['soma_carteira_inadimplida_arrastada'].sum().sort_values(ascending=False).head(10)
        return px.bar(x=top_ufs.index, y=top_ufs.values, title="Top 10 Estados por Inadimplência",
                      labels={"x": "UF", "y": "Valor Inadimplente (R$)"})
    return None

# Main function
def main():
    st.title("💬 Chatbot Inadimplinha")
    st.caption("🚀 Desenvolvido por Grupo de Inadimplência EY")

    conn = connect_to_db()
    if conn is None:
        st.error("Falha na conexão com o banco de dados.")
        st.stop()

    llm = get_llm_client()
    
    @st.cache_data(ttl=3600)
    def load_data():
        query = "SELECT * FROM table_agg_inad_consolidado WHERE data_base = '2024-12-01'"
        return pd.read_sql(query, conn)
    
    df = load_data()
    insights = generate_advanced_insights(df)

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    if prompt := st.chat_input("Faça uma pergunta sobre a inadimplência"):
        with st.chat_message("user"):
            st.markdown(prompt)
        st.session_state.chat_history.append({"role": "user", "content": prompt})

        with st.chat_message("assistant"):
            intent = classify_user_intent(prompt, llm)
            response = process_question_with_insights(prompt, df, llm)
            st.markdown(response)
            fig = generate_visual_insight(intent, df)
            if fig:
                st.plotly_chart(fig)
            st.session_state.chat_history.append({"role": "assistant", "content": response})

if __name__ == "__main__":
    main()