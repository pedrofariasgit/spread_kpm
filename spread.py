import os
import streamlit as st
import psycopg2
from psycopg2 import sql
import pandas as pd
import json
import base64

# Função para converter a imagem em base64
def get_base64_image(image_path):
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode("utf-8")

# Função para adicionar a logo
def add_logo():
    logo_path = os.path.join(os.path.dirname(__file__), 'logo.png')
    logo_base64 = get_base64_image(logo_path)
    st.markdown(
        f"""
        <style>
        .logo {{
            position: fixed;
            top: 50px;
            left: 10px;
            padding: 10px;
        }}
        </style>
        <div class="logo">
            <img src="data:image/png;base64,{logo_base64}" width="150" height="auto">
        </div>
        """,
        unsafe_allow_html=True
    )

# Função para carregar usuários e senhas
def load_users():
    file_path = os.path.join(os.path.dirname(__file__), 'users.json')
    with open(file_path, 'r') as f:
        return json.load(f)

# Função de login
def login_page():
    add_logo()
    st.title("Login")
    users = load_users()
    usernames = [user['username'] for user in users]

    username = st.text_input("Usuário")
    password = st.text_input("Senha", type="password")

    if st.button("Entrar"):
        if username in usernames:
            user = next(user for user in users if user['username'] == username)
            if user['password'] == password:
                st.session_state['logged_in'] = True
                st.session_state['page'] = 'main'
                st.query_params.from_dict({"logged_in": "True"})
            else:
                st.error("Senha incorreta")
        else:
            st.error("Usuário não encontrado")

# Função para conectar ao banco de dados PostgreSQL
def conectar_bd():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "89.117.17.6"),
            port=os.getenv("DB_PORT", "5432"),
            database=os.getenv("DB_NAME", "kpm_spread"),
            user=os.getenv("DB_USER", "kpm"),
            password=os.getenv("DB_PASSWORD", "@Kpm<102030>")
        )
        return conn
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return None

# Função para inserir os dados da planilha Excel no banco de dados
def inserir_dados_excel(conn, df):
    try:
        cursor = conn.cursor()
        for index, row in df.iterrows():
            insert_query = sql.SQL("""
                INSERT INTO kpm_spread_table (ref_kpm, data, agente, moeda, valor, abs_valor, conversao, taxa_rec_cliente, taxa_pgto_banco, fator_conversao, ganho)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """)
            cursor.execute(insert_query, (
                row['ref_kpm'],
                row['data'],
                row['agente'],
                row['moeda'],
                round(row['valor'], 2),  # Salva com 2 casas decimais
                row['abs_valor'],
                row['conversao'],
                round(row['taxa_rec_cliente'], 4),  # Salva com 4 casas decimais
                round(row['taxa_pgto_banco'], 4),  # Salva com 4 casas decimais
                round(row['fator_conversao'], 4),  # Salva com 4 casas decimais
                round(row['ganho'], 2)  # Salva com 2 casas decimais
            ))
        conn.commit()
        st.success("Dados da planilha inseridos com sucesso!")
    except Exception as e:
        conn.rollback()
        st.error(f"Erro ao inserir dados: {e}")
    finally:
        cursor.close()

# Função para processar o Excel e corrigir os nomes das colunas
def processar_excel(file):
    df = pd.read_excel(file)

    # Padroniza as colunas para garantir que todos os nomes sejam tratados corretamente
    df.columns = df.columns.str.strip()

    # Mapeia os nomes das colunas do Excel para os nomes esperados pelo banco de dados
    colunas_mapeamento = {
        'REF.': 'ref_kpm',
        'DATA': 'data',
        'AGENTE': 'agente',
        'MOEDA': 'moeda',
        'VALOR': 'valor',
        'ABS': 'abs_valor',
        'Conversão': 'conversao',
        'TAXA REC CLIENTE': 'taxa_rec_cliente',
        'TAXA PAGA AO BANCO': 'taxa_pgto_banco',
        'FATOR CONVERSÃO': 'fator_conversao',
        'GANHO R$': 'ganho'
    }

    # Renomeia as colunas do DataFrame com base no mapeamento
    df.rename(columns=colunas_mapeamento, inplace=True)

    # Verifica se as colunas necessárias estão presentes
    colunas_necessarias = ['ref_kpm', 'data', 'agente', 'moeda', 'valor']
    if not all(coluna in df.columns for coluna in colunas_necessarias):
        st.error(f"Erro: As colunas necessárias não foram encontradas no arquivo Excel.")
        st.write("Colunas encontradas:", df.columns)
        return None
 
    return df

# Função principal para a página principal
def main_page():
    st.title("Controle de Spread - KPM Logistics")
    uploaded_file = st.file_uploader("Escolha um arquivo Excel", type="xlsx")
    if uploaded_file:
        df = processar_excel(uploaded_file)
        if df is not None:
            st.write("Pré-visualização dos dados carregados:")
            st.dataframe(df)

            # Exibe o botão para salvar os dados no banco de dados
            if st.button('Salvar dados'):
                conn = conectar_bd()
                if conn:
                    inserir_dados_excel(conn, df)
                    conn.close()

# Verifica o login via URL
def check_login_from_url():
    if st.query_params.get("logged_in") == ["True"]:
        st.session_state['logged_in'] = True

# Gerencia as páginas do aplicativo
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False

check_login_from_url()

if st.session_state['logged_in']:
    add_logo()
    main_page()
else:
    login_page()
