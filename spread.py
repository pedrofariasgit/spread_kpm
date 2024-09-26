import os
import streamlit as st
import psycopg2
from psycopg2 import sql
import pandas as pd
import json
import base64
import numpy as np

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

# Função para tratar valores NaN e NaT
def tratar_valores_invalidos(df):
    df = df.replace({np.nan: None, pd.NaT: None})
    return df

# Função para verificar e tratar valores inválidos em campos numéricos
def tratar_valores_numericos(df, colunas_numericas):
    for coluna in colunas_numericas:
        df[coluna] = pd.to_numeric(df[coluna], errors='coerce')  # Converte para número, substitui texto por NaN
    df = df.replace({np.nan: None})  # Substitui NaN por None
    return df

# Função para inserir os dados da planilha Excel no banco de dados
def inserir_dados_excel(conn, df, id_inicial):
    try:
        cursor = conn.cursor()
        current_id = id_inicial

        for index, row in df.iterrows():
            # Arredonda os campos numéricos, verifica se são numéricos antes de arredondar
            valor = round(row['valor'], 2) if isinstance(row['valor'], (int, float)) else row['valor']
            taxa_rec_cliente = round(row['taxa_rec_cliente'], 4) if isinstance(row['taxa_rec_cliente'], (int, float)) else row['taxa_rec_cliente']
            taxa_pgto_banco = round(row['taxa_pgto_banco'], 4) if isinstance(row['taxa_pgto_banco'], (int, float)) else row['taxa_pgto_banco']
            fator_conversao = round(row['fator_conversao'], 4) if isinstance(row['fator_conversao'], (int, float)) else row['fator_conversao']
            ganho = round(row['ganho'], 2) if isinstance(row['ganho'], (int, float)) else row['ganho']

            # Insere o dado com o ID sequencial gerado
            insert_query = sql.SQL("""
                INSERT INTO kpm_spread_table (id, ref_kpm, data, agente, moeda, valor, abs_valor, conversao, taxa_rec_cliente, taxa_pgto_banco, fator_conversao, ganho)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """)
            cursor.execute(insert_query, (
                current_id,
                row['ref_kpm'],
                row['data'],
                row['agente'],
                row['moeda'],
                valor,
                row['abs_valor'],
                row['conversao'],
                taxa_rec_cliente,
                taxa_pgto_banco,
                fator_conversao,
                ganho
            ))
            current_id += 1  # Incrementa o ID para a próxima linha

        conn.commit()
        st.success(f"Dados inseridos com sucesso! Último ID: {current_id - 1}")
    except Exception as e:
        conn.rollback()
        st.error(f"Erro ao inserir dados: {e}")
    finally:
        cursor.close()

# Função para processar o Excel e corrigir os nomes das colunas
def processar_excel(file):
    # Lê todas as abas do Excel
    xls = pd.ExcelFile(file)
    
    # Verifica se há pelo menos duas abas
    if len(xls.sheet_names) < 2:
        st.error("O arquivo Excel deve conter pelo menos duas abas.")
        return None, None

    # Carrega a primeira e segunda abas
    df1 = pd.read_excel(xls, sheet_name=xls.sheet_names[0])
    df2 = pd.read_excel(xls, sheet_name=xls.sheet_names[1])

    # Padroniza as colunas da primeira aba
    df1.columns = df1.columns.str.strip()
    df2.columns = df2.columns.str.strip()

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

    # Renomeia as colunas de ambas as abas com base no mapeamento
    df1.rename(columns=colunas_mapeamento, inplace=True)
    df2.rename(columns=colunas_mapeamento, inplace=True)

    # Tratar valores numéricos inválidos
    colunas_numericas = ['valor', 'abs_valor', 'conversao', 'taxa_rec_cliente', 'taxa_pgto_banco', 'fator_conversao', 'ganho']
    df1 = tratar_valores_numericos(df1, colunas_numericas)
    df2 = tratar_valores_numericos(df2, colunas_numericas)

    # Substitui valores NaN e NaT
    df1 = tratar_valores_invalidos(df1)
    df2 = tratar_valores_invalidos(df2)

    # Verifica se as colunas necessárias estão presentes
    colunas_necessarias = ['ref_kpm', 'data', 'agente', 'moeda', 'valor']
    if not all(coluna in df1.columns for coluna in colunas_necessarias):
        st.error(f"Erro: As colunas necessárias não foram encontradas na primeira aba.")
        return None, None

    if not all(coluna in df2.columns for coluna in colunas_necessarias):
        st.error(f"Erro: As colunas necessárias não foram encontradas na segunda aba.")
        return None, None

    return df1, df2

# Função principal para a página principal
def main_page():
    st.title("Controle de Spread - KPM Logistics")
    uploaded_file = st.file_uploader("Escolha um arquivo Excel", type="xlsx")
    if uploaded_file:
        df1, df2 = processar_excel(uploaded_file)
        if df1 is not None and df2 is not None:
            # Exibe as duas abas em tabelas diferentes
            st.write("Pré-visualização dos dados da primeira aba:")
            st.dataframe(df1)
            
            st.write("Pré-visualização dos dados da segunda aba:")
            st.dataframe(df2)

            # Exibe o botão para salvar os dados no banco de dados (ambas as abas)
            if st.button('Salvar dados'):
                conn = conectar_bd()
                if conn:
                    # Consulta o maior ID já existente no banco
                    cursor = conn.cursor()
                    cursor.execute("SELECT COALESCE(MAX(id), 0) FROM kpm_spread_table")
                    max_id = cursor.fetchone()[0]  # Obter o maior ID
                    cursor.close()

                    # Inicia a inserção das duas abas
                    inserir_dados_excel(conn, df1, max_id + 1)  # Inserir primeira aba
                    inserir_dados_excel(conn, df2, max_id + len(df1) + 1)  # Inserir segunda aba, após a primeira

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
