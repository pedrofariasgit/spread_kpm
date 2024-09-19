import os
import streamlit as st
import psycopg2
from psycopg2 import sql
import pandas as pd
from datetime import datetime
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

# Função para calcular os campos necessários
def calcular_campos(valor, taxa_rec_cliente, taxa_pgto_banco):
    abs_valor = abs(valor)
    fator_conversao = taxa_rec_cliente - taxa_pgto_banco
    conversao = abs_valor * taxa_rec_cliente
    ganho = fator_conversao * abs_valor
    return abs_valor, conversao, fator_conversao, ganho

# Função para inserir os dados da planilha Excel no banco de dados
def inserir_dados_excel(conn, df):
    try:
        cursor = conn.cursor()
        for index, row in df.iterrows():
            abs_valor, conversao, fator_conversao, ganho = calcular_campos(row['valor'], row['taxa_rec_cliente'], row['taxa_pgto_banco'])
            insert_query = sql.SQL("""
                INSERT INTO kpm_spread_table (ref_kpm, data, agente, moeda, valor, abs_valor, conversao, taxa_rec_cliente, taxa_pgto_banco, fator_conversao, ganho)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """)
            cursor.execute(insert_query, (
                row['ref_kpm'],
                row['data'],
                row['agente'],
                row['moeda'],
                row['valor'],
                abs_valor,
                conversao,
                row['taxa_rec_cliente'],
                row['taxa_pgto_banco'],
                fator_conversao,
                ganho
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

    # Gera um ID para cada linha
    df['id'] = df.index + 1

    # Reorganiza a coluna 'id' para ser a primeira
    colunas = ['id'] + [col for col in df.columns if col != 'id']
    df = df[colunas]

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
    colunas_necessarias = ['id', 'ref_kpm', 'data', 'agente', 'moeda', 'valor']
    if not all(coluna in df.columns for coluna in colunas_necessarias):
        st.error(f"Erro: As colunas necessárias não foram encontradas no arquivo Excel.")
        st.write("Colunas encontradas:", df.columns)
        return

    # Salva o DataFrame no session_state para preservar as edições
    if 'df' not in st.session_state or st.session_state['df'].empty:
        st.session_state['df'] = df.copy()  # Armazena o dataframe no session_state

    # Exibe a planilha para o usuário revisar com o campo "id"
    st.write("Pré-visualização dos dados carregados:")
    st.dataframe(st.session_state['df'])  # Exibe o dataframe do session_state

    # Permite ao usuário selecionar um intervalo de linhas ou uma linha única
    linhas_selecionadas = st.text_input("Informe as linhas que deseja editar (ex: 2 ou 3;4):")
    if linhas_selecionadas:
        try:
            if ";" in linhas_selecionadas:
                inicio, fim = map(int, linhas_selecionadas.split(";"))
                linhas_validas = list(range(inicio, fim + 1))
            else:
                linhas_validas = [int(linhas_selecionadas.strip())]

            if not linhas_validas:
                st.warning("Nenhuma linha válida selecionada.")
                return
        except ValueError:
            st.error("Por favor, insira apenas números válidos.")
            return

        # Exibe as linhas selecionadas
        st.write(f"Linhas selecionadas para edição: {linhas_validas}")
        st.dataframe(st.session_state['df'][st.session_state['df']['id'].isin(linhas_validas)])

        # Permite a edição das taxas
        taxa_rec_cliente = st.number_input("Informe a TAXA REC CLIENTE para as linhas selecionadas", min_value=0.0, format="%.4f")
        taxa_pgto_banco = st.number_input("Informe a TAXA PAGA AO BANCO para as linhas selecionadas", min_value=0.0, format="%.4f")

        if st.button("Aplicar taxas e calcular"):
            for linha in linhas_validas:
                st.session_state['df'].loc[st.session_state['df']['id'] == linha, 'taxa_rec_cliente'] = taxa_rec_cliente
                st.session_state['df'].loc[st.session_state['df']['id'] == linha, 'taxa_pgto_banco'] = taxa_pgto_banco

            # Calcula os campos antes do insert
            for index, row in st.session_state['df'].iterrows():
                abs_valor, conversao, fator_conversao, ganho = calcular_campos(row['valor'], row['taxa_rec_cliente'], row['taxa_pgto_banco'])
                st.session_state['df'].at[index, 'abs_valor'] = abs_valor
                st.session_state['df'].at[index, 'conversao'] = conversao
                st.session_state['df'].at[index, 'fator_conversao'] = fator_conversao
                st.session_state['df'].at[index, 'ganho'] = ganho

            st.success(f"Taxas aplicadas e cálculos realizados para as linhas {', '.join(map(str, linhas_validas))}")
            st.write("Dados atualizados com cálculos:")
            st.dataframe(st.session_state['df'])  # Mostra os dados com cálculos atualizados

    # Exibe o botão para salvar os dados no banco de dados
    if st.button('Salvar dados'):
        conn = conectar_bd()
        if conn:
            inserir_dados_excel(conn, st.session_state['df'])
            conn.close()

# Função principal para a página principal
def main_page():
    st.title("Controle de Spread - KPM Logistics")
    uploaded_file = st.file_uploader("Escolha um arquivo Excel", type="xlsx")
    if uploaded_file:
        processar_excel(uploaded_file)

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
