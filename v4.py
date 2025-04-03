# V4 - Adicionado streamlit para exibir o chatbot sem usar insights como base. apenas carregando os dados e exibindo o chatbot

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
        api_key= api_key, 
        base_url="https://api.deepseek.com", 
        http_client=httpx.Client(verify=False)
    )

@st.cache_data
def load_data():
    return pd.read_parquet(r"consolidado_amostra.parquet" ) 

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
                full_response = f"Erro: {str(e)}"  # Garantir que full_response tenha um valor
                message_placeholder.markdown(full_response)
            
             
            st.session_state.messages.append({"role": "assistant", "content": full_response})
        
  
    with st.sidebar:
        st.sidebar.header("EY Academy | Inadimplência")
        st.sidebar.write(f"Número de registros: {len(df)}")
        st.sidebar.write("Colunas disponíveis:")
        st.sidebar.write(df.columns.tolist())

if __name__ == "__main__":
    main()
  