 # V7 - Eliminando base insights e passando contexto via endpoints e palavras-chave 
import streamlit as st
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain_core.chat_history import InMemoryChatMessageHistory
import httpx
import pandas as pd
from PIL import Image
import pyodbc
import re
import time
from dotenv import load_dotenv
import os

load_dotenv()

api_key = os.getenv("API_KEY")
 
st.set_page_config(page_title="Análise de Inadimplência", page_icon="💼")

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

# Conexão com o banco de dados
def connect_to_db():
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    username = os.getenv('USERNAME')
    password = os.getenv('PASSWORD')
    table = os.getenv('TABLE')
    try:
        connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )
        conn = pyodbc.connect(connection_string)
        return conn, table
    except pyodbc.Error as e:
        st.error(f"Erro ao conectar ao banco de dados: {str(e)}")
        return None, None

# Função para obter colunas da tabela
def get_table_columns(conn, table):
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT TOP 1 * FROM {table}")
        columns = [desc[0] for desc in cursor.description]
        cursor.close()
        return columns
    except pyodbc.Error as e:
        st.error(f"Erro ao obter colunas da tabela: {str(e)}")
        return []

def main():
    st.title("💬 Chatbot Inadimplinha")
    st.caption("🚀 Chatbot Inadimplinha desenvolvido por Grupo de Inadimplência EY")
    
    conn, table = connect_to_db()
    if conn is None:
        st.stop()
    
    available_columns = get_table_columns(conn, table)
    if not available_columns:
        st.error("Não foi possível recuperar as colunas da tabela 'inad_consolidado'.")
        conn.close()
        st.stop()
    
    llm = get_llm_client()

    # Definir o template de prompt
    prompt_template = ChatPromptTemplate.from_messages([
        ("system", (
            "Você é um especialista em análise de inadimplência no Brasil. "
            "Responda a pergunta do usuário com base nos dados da tabela '{table}' "
            "sem passar detalhes técnicos e informações sobre as tabelas que estão sendo usadas. "
            "Sempre forneça valores totais reais (em reais, R$) calculados a partir dos dados, "
            "Não forneça detalhes como nome das colunas ou como deveria ser feita a consulta sql, "
            "Insira informações adicionais relevantes sobre o tema quando apropriado. "
            "As colunas disponíveis são: {available_columns}. "
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
                    # Executar a consulta com contexto
                    response = conversation.invoke(
                        {"input": prompt, "table": table, "available_columns": available_columns},
                        config={"configurable": {"session_id": "default"}}
                    )
                    response_stream = response.content
                    
                    # Simulando streaming para melhor UX
                    full_response = ""
                    for i in range(len(response_stream)):
                        full_response = response_stream[:i+1]
                        message_placeholder.markdown(full_response + "▌")
                        time.sleep(0.01)
                    
                    # Verificar se há uma consulta SQL na resposta
                    sql_match = re.search(r"```sql\n(.*?)\n```", full_response, re.DOTALL)
                    if sql_match:
                        query = sql_match.group(1)
                        # Corrigir a posição do TOP se necessário
                        if "TOP" in query and "ORDER BY" in query and query.index("TOP") > query.index("ORDER BY"):
                            top_match = re.search(r"TOP\s+(\d+)", query)
                            top_number = top_match.group(1) if top_match else "1"
                            query = re.sub(r"TOP\s+\d+\s*;", "", query)
                            query = query.replace("SELECT", f"SELECT TOP {top_number}", 1)
                        
                        # Executar a consulta no banco
                        df = pd.read_sql(query, conn)
                        
                        # Substituir SQL com os resultados formatados
                        formatted_response = full_response.replace(sql_match.group(0), df.to_string(index=False))
                        message_placeholder.markdown(formatted_response)
                        final_response = formatted_response
                    else:
                        # Se não houver SQL, usar a resposta direta
                        message_placeholder.markdown(full_response)
                        final_response = full_response
                    
                    # Adicionar à exibição do histórico após a resposta estar completa
                    st.session_state.chat_history.append({"role": "assistant", "content": final_response})
                
            except pyodbc.Error as e:
                error_message = f"Erro ao executar a consulta no banco: {str(e)}"
                message_placeholder.markdown(error_message)
                st.session_state.chat_history.append({"role": "assistant", "content": error_message})
                st.session_state.chat_history_store.add_ai_message(error_message)
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
        st.sidebar.write("➡️ Qual cliente apresenta o maior número de operações?")
        st.sidebar.write("➡️ Em qual modalidade existe maior inadimplência?")
        st.sidebar.write("➡️ Compare a inadimplência entre PF e PJ")
        st.sidebar.write("➡️ Qual ocupação entre PF possui maior inadimplência?")
        st.sidebar.write("➡️ Qual o principal porte de cliente com inadimplência entre PF?")
        st.sidebar.write("➡️ Qual seção CNAE possui a maior inadimplencia?")
        st.sidebar.write("➡️ Qual estado tem o maior valor médio de operações a vencer em até 90 dias?")
        
        # Botão para limpar histórico de conversa
        if st.button("Limpar Conversa"):
            st.session_state.chat_history_store = InMemoryChatMessageHistory()
            st.session_state.chat_history = []
            st.session_state.app_initialized = False
            st.rerun()
   
    conn.close()

if __name__ == "__main__":
    main()
    
 