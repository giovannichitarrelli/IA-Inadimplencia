import pyodbc
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()


def connect_to_sql_server():
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    username = os.getenv('USERNAME')
    password = os.getenv('PASSWORD')

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

        print("Tentando estabelecer conexão com o banco de dados...")
        conn = pyodbc.connect(connection_string)
        print("Conexão bem-sucedida!")
        return conn

    except pyodbc.Error as e:
        print("Erro ao conectar ao SQL Server:", e)
        return None

def get_table_insights(connection, table_name):
    try:
        # Query to fetch data from the table
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, connection)

        # Insights
        print(f"Insights da tabela '{table_name}':")
        print(f"- Número de linhas: {len(df)}")
        print(f"- Número de colunas: {len(df.columns)}")
        print(f"- Colunas disponíveis: {list(df.columns)}")
        print(f"- Tipos de colunas:\n{df.dtypes}")
        print(f"- Primeiras 5 linhas:\n{df.head()}")

    except Exception as e:
        print(f"Erro ao obter insights da tabela '{table_name}':", e)

# Example usage
if __name__ == "__main__":
    connection = connect_to_sql_server()
    if connection:
        table_name = os.getenv("TABLE")  # Substitua pelo nome da tabela que deseja analisar
        get_table_insights(connection, table_name)
        connection.close()
        print("Conexão encerrada com sucesso.")
    else:
        print("Não foi possível estabelecer a conexão com o banco de dados.")
