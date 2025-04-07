
import streamlit as st
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain_core.chat_history import InMemoryChatMessageHistory
import httpx
import pandas as pd
from PIL import Image
import time
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from insights import generate_advanced_insights
from urllib.parse import quote_plus
 
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

def connect_to_db():
    try:
        # print("Tentando conectar ao banco de dados PostgreSQL no GCP...")
        # Dados de conexão
        host = os.getenv("SERVER")
        database = os.getenv("DATABASE")
        username = os.getenv("USERNAME")
        password = os.getenv("PASSWORD")
        port = os.getenv("PORT")

        # Validar os valores
        if not all([host, database, username, password, port]):
            raise ValueError("Uma ou mais variáveis de ambiente não estão definidas no .env")

        # Codificar a senha para lidar com caracteres especiais
        encoded_password = quote_plus(password)

        # String de conexão com senha codificada
        connection_string = f"postgresql://{username}:{encoded_password}@{host}:{port}/{database}"
        # print(f"String de conexão: {connection_string}")  # Para depuração, remova em produção

        # Criar engine do SQLAlchemy
        engine = create_engine(connection_string)

        # Testar a conexão
        with engine.connect() as connection:
            print("Conexão com o banco de dados estabelecida com sucesso!")
        
        return engine

    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None  

def load_data(engine):
    try:
        table = "table_agg_inad_consolidado"
        query = f"SELECT * FROM {table}"
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar os dados: {str(e)}")
        return None

def main():
    st.title("Chatbot Inadimplinha")
    st.caption("Chatbot Inadimplinha desenvolvido por Grupo de Inadimplência EY")

    engine = connect_to_db()
    if engine is None:
        st.stop()

    if "insights" not in st.session_state:
        df = load_data(engine)
        if df is None:
            engine.dispose()  # Fechar o engine em caso de erro
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
        
        # Botão para limpar histórico de conversa
        if st.button("Limpar Conversa"):
            st.session_state.chat_history_store = InMemoryChatMessageHistory()
            st.session_state.chat_history = []
            st.session_state.app_initialized = False
            if "insights" in st.session_state:
                del st.session_state.insights  # Recarregar insights na próxima execução
            st.rerun()

    engine.dispose()  # Fechar o engine ao final da execução

if __name__ == "__main__":
    main()