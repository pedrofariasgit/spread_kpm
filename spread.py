import os
import streamlit as st
import psycopg2
from psycopg2 import sql
import pandas as pd
from datetime import datetime, date
import json
import base64

# Função para converter a imagem em base64
def get_base64_image(image_path):
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode("utf-8")

# Função para adicionar a logo
def add_logo():
    # Usar caminho relativo, igual ao users.json
    logo_path = os.path.join(os.path.dirname(__file__), 'logo.png')

    logo_base64 = get_base64_image(logo_path)  # Usa o caminho relativo
    st.markdown(
        f"""
        <style>
        .logo {{
            position: fixed;
            top: 50px;
            left: 0px;
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
    st.title("Login")

    # Carrega os dados de usuários
    users = load_users()
    usernames = [user['username'] for user in users]

    username = st.text_input("Usuário")
    password = st.text_input("Senha", type="password")

    if st.button("Entrar"):
        if username in usernames:
            # Verifica a senha
            user = next(user for user in users if user['username'] == username)
            if user['password'] == password:
                st.session_state['logged_in'] = True
                st.session_state['page'] = 'main'
                # Força a atualização dos parâmetros para simular uma recarga
                st.query_params.from_dict({"logged_in": "True"})
            else:
                st.error("Senha incorreta")
        else:
            st.error("Usuário não encontrado")


# Função para converter a data inserida no formato brasileiro
def converter_data(data_str):
    if data_str:  # Apenas tenta converter se houver uma data informada
        try:
            return datetime.strptime(data_str, "%d/%m/%Y")
        except ValueError:
            st.error("Data inválida! Use o formato dd/mm/aaaa.")
            return None
    return None

# Função para formatar a data ao carregar para o formulário de edição
def formatar_data_para_formulario(data_valor):
    # Se for um objeto datetime.date ou datetime, converta para string
    if isinstance(data_valor, (datetime, date)):
        return data_valor.strftime('%d/%m/%Y')
    return data_valor

# Função para conectar ao banco de dados PostgreSQL
def conectar_bd():
    try:
        conn = psycopg2.connect(
            host="89.117.17.6",
            port="5432",
            database="kpm_spread",
            user="kpm",
            password="@Kpm<102030>"
        )
        return conn
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return None

# Função para inserir os dados na tabela kpm_spread_table
def inserir_dados(conn, data, agente, moeda, valor, abs_valor, conversao, taxa_rec_cliente, taxa_pgto_banco, fator_conversao, ganho):
    try:
        cursor = conn.cursor()
        insert_query = sql.SQL("""
            INSERT INTO kpm_spread_table (data, agente, moeda, valor, abs_valor, conversao, taxa_rec_cliente, taxa_pgto_banco, fator_conversao, ganho)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """)
        cursor.execute(insert_query, (data, agente, moeda, valor, abs_valor, conversao, taxa_rec_cliente, taxa_pgto_banco, fator_conversao, ganho))
        conn.commit()
        st.success("Dados inseridos com sucesso!")
    except Exception as e:
        conn.rollback()
        st.error(f"Erro ao inserir dados: {e}")
    finally:
        cursor.close()

# Função para atualizar os dados na tabela kpm_spread_table
def atualizar_dados(conn, id_registro, data, agente, moeda, valor, abs_valor, conversao, taxa_rec_cliente, taxa_pgto_banco, fator_conversao, ganho):
    try:
        cursor = conn.cursor()
        update_query = sql.SQL("""
            UPDATE kpm_spread_table
            SET data = %s, agente = %s, moeda = %s, valor = %s, abs_valor = %s, conversao = %s, taxa_rec_cliente = %s, taxa_pgto_banco = %s, fator_conversao = %s, ganho = %s
            WHERE id = %s
        """)
        cursor.execute(update_query, (data, agente, moeda, float(valor), abs_valor, conversao, taxa_rec_cliente, taxa_pgto_banco, fator_conversao, ganho, int(id_registro)))
        conn.commit()
        st.success("Dados atualizados com sucesso!")
    except Exception as e:
        conn.rollback()
        st.error(f"Erro ao atualizar dados: {e}")
    finally:
        cursor.close()

# Função para excluir um registro
def excluir_dados(conn, id_registro):
    try:
        cursor = conn.cursor()
        delete_query = sql.SQL("DELETE FROM kpm_spread_table WHERE id = %s")
        cursor.execute(delete_query, (int(id_registro),))  # Converter para int para evitar erro
        conn.commit()
        st.success("Registro excluído com sucesso!")
    except Exception as e:
        conn.rollback()
        st.error(f"Erro ao excluir dados: {e}")
    finally:
        cursor.close()

# Função para realizar o SELECT e mostrar os dados
def exibir_dados(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM kpm_spread_table")
        rows = cursor.fetchall()
        colunas = [desc[0] for desc in cursor.description]  # Nome das colunas
        df = pd.DataFrame(rows, columns=colunas)
        return df
    except Exception as e:
        st.error(f"Erro ao buscar dados: {e}")
    finally:
        cursor.close()

# Função para calcular os campos necessários
def calcular_campos(valor, taxa_rec_cliente, taxa_pgto_banco):
    abs_valor = abs(valor)
    fator_conversao = taxa_rec_cliente - taxa_pgto_banco
    conversao = abs_valor * taxa_rec_cliente
    ganho = fator_conversao * abs_valor
    return abs_valor, conversao, fator_conversao, ganho

# Função principal para exibir o formulário e os dados
def main_page():
    st.title("Formulário SPREAD - KPM")

    # Conectando ao banco de dados
    conn = conectar_bd()

    if conn:
        # Formulário de inserção e edição
        with st.form(key='form'):
            data_str = st.text_input("Informe a DATA (dd/mm/aaaa)", placeholder="dd/mm/aaaa")
            agente = st.text_input("Informe o AGENTE")
            moeda = st.text_input("Informe a MOEDA")
            valor = st.number_input("Informe o VALOR", min_value=0.0, format="%.2f")
            taxa_rec_cliente = st.number_input("Informe a TAXA REC. CLIENTE", value=0.0000, format="%.4f")
            taxa_pgto_banco = st.number_input("Informe a TAXA PGTO BANCO", value=0.0000, format="%.4f")
            
            submit_button = st.form_submit_button(label="Salvar")
            
            # Converter a data fornecida
            data = converter_data(data_str)

            if submit_button:
                if data and agente and moeda:
                    abs_valor, conversao, fator_conversao, ganho = calcular_campos(valor, taxa_rec_cliente, taxa_pgto_banco)
                    inserir_dados(conn, data.strftime('%Y-%m-%d'), agente, moeda, valor, abs_valor, conversao, taxa_rec_cliente, taxa_pgto_banco, fator_conversao, ganho)
                    # Força a atualização da página
                    st.query_params.from_dict({"updated": "True"})
    
        # Campo de filtro de ID abaixo do formulário
        filtro_id = st.text_input("Filtrar pelo ID", placeholder="Informe o ID para editar ou excluir")
        
        # Exibindo os dados salvos e capturando o DataFrame
        df = exibir_dados(conn)
        registro_para_editar = None
        
        if filtro_id:
            filtro_id = int(filtro_id)
            registro_para_editar = df[df['id'] == filtro_id]
            if registro_para_editar.empty:
                st.error(f"Nenhum registro encontrado para o ID {filtro_id}")
                registro_para_editar = None
            else:
                registro_para_editar = registro_para_editar.iloc[0]

        if registro_para_editar is not None:
            with st.form(key='edit_form'):
                data_str = st.text_input("Informe a DATA (dd/mm/aaaa)", value=formatar_data_para_formulario(registro_para_editar['data']), placeholder="dd/mm/aaaa")
                agente = st.text_input("Informe o AGENTE", value=registro_para_editar['agente'])
                moeda = st.text_input("Informe a MOEDA", value=registro_para_editar['moeda'])
                valor = st.number_input("Informe o VALOR", min_value=0.0, format="%.2f", value=float(registro_para_editar['valor']))
                taxa_rec_cliente = st.number_input("Informe a TAXA REC. CLIENTE", value=float(registro_para_editar['taxa_rec_cliente']), format="%.4f")
                taxa_pgto_banco = st.number_input("Informe a TAXA PGTO BANCO", value=float(registro_para_editar['taxa_pgto_banco']), format="%.4f")
                
                editar_button = st.form_submit_button(label="Editar")
                excluir_button = st.form_submit_button(label="Excluir")

                data = converter_data(data_str)

                if editar_button:
                    if data and agente and moeda:
                        abs_valor, conversao, fator_conversao, ganho = calcular_campos(valor, taxa_rec_cliente, taxa_pgto_banco)
                        atualizar_dados(conn, registro_para_editar['id'], data.strftime('%Y-%m-%d'), agente, moeda, valor, abs_valor, conversao, taxa_rec_cliente, taxa_pgto_banco, fator_conversao, ganho)
                        st.query_params.from_dict({"updated": "True"})
                    
                if excluir_button:
                    excluir_dados(conn, registro_para_editar['id'])
                    st.query_params.from_dict({"updated": "True"})

        st.write("Tabela de registros")
        st.dataframe(df)

        conn.close()

# Gerencia as páginas do aplicativo
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False

if 'page' not in st.session_state:
    st.session_state['page'] = 'login'

if st.session_state['logged_in']:
    add_logo()  # Adiciona a logo ao topo da página
    main_page()
else:
    login_page()
