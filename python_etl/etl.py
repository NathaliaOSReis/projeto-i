#defina o código de ETL

import pandas as pd
import pymongo
from pymongo import MongoClient
import psycopg2
from psycopg2 import sql
from datetime import datetime
import os
import time
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

# Função para aguardar a disponibilidade dos serviços
def wait_for_services():
    print("Aguardando serviços ficarem disponíveis...")
    time.sleep(10)  # Aguarda 10 segundos para garantir que os serviços estejam prontos

# Variáveis de conexão PostgreSQL
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "pb_dw")

# Variáveis de conexão MongoDB
MONGO_HOST = os.getenv("MONGO_HOST", "mongodb")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_DB = os.getenv("MONGO_DB", "ecommerce")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "order_reviews")
MONGO_USER = os.getenv("MONGO_USER", "mongo")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "mongo")

# Caminhos dos arquivos CSV
INPUT_DIR = "/app/input"
CUSTOMERS_FILE = os.path.join(INPUT_DIR, "olist_customers_dataset.csv")
ORDER_ITEMS_FILE = os.path.join(INPUT_DIR, "olist_order_items_dataset.csv")
ORDER_PAYMENTS_FILE = os.path.join(INPUT_DIR, "olist_order_payments_dataset.csv")
ORDERS_FILE = os.path.join(INPUT_DIR, "olist_orders_dataset.csv")
PRODUCTS_FILE = os.path.join(INPUT_DIR, "olist_products_dataset.csv")

# Verificar existência dos arquivos CSV
def check_files_exist():
    files = [CUSTOMERS_FILE, ORDER_ITEMS_FILE, ORDER_PAYMENTS_FILE, ORDERS_FILE, PRODUCTS_FILE]
    missing_files = []
    
    for file in files:
        if not os.path.exists(file):
            missing_files.append(file)
    
    if missing_files:
        print(f"ERRO: Os seguintes arquivos não foram encontrados:")
        for file in missing_files:
            print(f"  - {file}")
        print("Verificando arquivos disponíveis no diretório:")
        try:
            available_files = os.listdir(INPUT_DIR)
            print(f"Arquivos disponíveis em {INPUT_DIR}:")
            for file in available_files:
                print(f"  - {file}")
        except Exception as e:
            print(f"Erro ao listar arquivos no diretório {INPUT_DIR}: {e}")
        raise FileNotFoundError(f"Arquivos necessários não encontrados.")
    else:
        print("Todos os arquivos CSV foram encontrados.")

# Função para criar conexão com o PostgreSQL
def create_postgres_connection():
    try:
        # Tentativa de conexão com o PostgreSQL
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            connect_timeout=10
        )
        
        # Verificar se a conexão está ativa
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        
        return conn
    except psycopg2.OperationalError as e:
        print(f"Erro operacional ao conectar ao PostgreSQL: {e}")
        print(f"Verifique se o servidor PostgreSQL está em execução em {POSTGRES_HOST}:{POSTGRES_PORT}")
        print(f"Verifique também se o banco de dados '{POSTGRES_DB}' existe")
        raise
    except psycopg2.DatabaseError as e:
        print(f"Erro de banco de dados ao conectar ao PostgreSQL: {e}")
        print("Verifique as credenciais de usuário e senha do PostgreSQL")
        raise
    except Exception as e:
        print(f"Erro inesperado ao conectar ao PostgreSQL: {e}")
        raise

# Função para criar conexão com o MongoDB
def create_mongo_connection():
    try:
        # Construir a string de conexão
        connection_string = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
        
        # Tentar conectar ao MongoDB
        client = pymongo.MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        
        # Verificar se a conexão foi bem-sucedida
        client.server_info()  # Isso vai lançar uma exceção se não conseguir conectar
        
        # Verificar se o banco de dados e a coleção existem
        db = client[MONGO_DB]
        collections = db.list_collection_names()
        if MONGO_COLLECTION not in collections:
            print(f"AVISO: A coleção '{MONGO_COLLECTION}' não existe no banco de dados '{MONGO_DB}'.")
            print(f"Coleções disponíveis: {collections}")
        
        return client
    except pymongo.errors.ServerSelectionTimeoutError as e:
        print(f"Erro de timeout ao conectar ao MongoDB: {e}")
        print(f"Verifique se o servidor MongoDB está em execução em {MONGO_HOST}:{MONGO_PORT}")
        raise
    except pymongo.errors.OperationFailure as e:
        print(f"Erro de autenticação no MongoDB: {e}")
        print("Verifique as credenciais de usuário e senha do MongoDB")
        raise
    except Exception as e:
        print(f"Erro inesperado ao conectar ao MongoDB: {e}")
        raise

# Função para criar as tabelas do data warehouse
def create_dw_tables(conn):
    print("Criando tabelas do Data Warehouse...")
    cursor = conn.cursor()
    
    # Criação das tabelas de dimensão e fato
    cursor.execute("""
        -- Dimensão Cliente
        CREATE TABLE IF NOT EXISTS dim_cliente (
            cliente_id SERIAL PRIMARY KEY,
            cliente_key VARCHAR(50) UNIQUE NOT NULL,
            cliente_cidade VARCHAR(100),
            cliente_estado VARCHAR(2),
            cliente_zip_code VARCHAR(10),
            data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    cursor.execute("""
        -- Dimensão Produto
        CREATE TABLE IF NOT EXISTS dim_produto (
            produto_id SERIAL PRIMARY KEY,
            produto_key VARCHAR(50) UNIQUE NOT NULL,
            produto_categoria_id INTEGER,
            produto_nome_comprimento INTEGER,
            produto_descricao_comprimento INTEGER,
            produto_fotos_qtd INTEGER,
            produto_peso_g FLOAT,
            produto_comprimento_cm FLOAT,
            produto_altura_cm FLOAT,
            produto_largura_cm FLOAT,
            data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    cursor.execute("""
        -- Dimensão Categoria de Produto
        CREATE TABLE IF NOT EXISTS dim_categoria_produto (
            categoria_id SERIAL PRIMARY KEY,
            categoria_nome VARCHAR(100) UNIQUE NOT NULL,
            data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    cursor.execute("""
        -- Dimensão Estado
        CREATE TABLE IF NOT EXISTS dim_estado (
            estado_id SERIAL PRIMARY KEY,
            estado_sigla VARCHAR(2) UNIQUE NOT NULL,
            estado_nome VARCHAR(50),
            data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    cursor.execute("""
        -- Dimensão Data
        CREATE TABLE IF NOT EXISTS dim_data (
            data_id SERIAL PRIMARY KEY,
            data_completa DATE UNIQUE NOT NULL,
            dia INTEGER,
            mes INTEGER,
            ano INTEGER,
            dia_semana INTEGER,
            nome_dia_semana VARCHAR(20),
            mes_nome VARCHAR(20),
            trimestre INTEGER,
            data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    cursor.execute("""
        -- Dimensão Hora
        CREATE TABLE IF NOT EXISTS dim_hora (
            hora_id SERIAL PRIMARY KEY,
            hora_completa TIME UNIQUE NOT NULL,
            hora INTEGER,
            minuto INTEGER,
            segundo INTEGER,
            periodo VARCHAR(10),
            data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    cursor.execute("""
        -- Dimensão Tipo de Pagamento
        CREATE TABLE IF NOT EXISTS dim_tipo_pagamento (
            tipo_pagamento_id SERIAL PRIMARY KEY,
            tipo_pagamento VARCHAR(50) UNIQUE NOT NULL,
            data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    cursor.execute("""
        -- Tabela Fato Vendas
        CREATE TABLE IF NOT EXISTS fato_vendas (
            venda_id SERIAL PRIMARY KEY,
            order_id VARCHAR(50) NOT NULL,
            cliente_id INTEGER REFERENCES dim_cliente(cliente_id),
            produto_id INTEGER REFERENCES dim_produto(produto_id),
            data_id INTEGER REFERENCES dim_data(data_id),
            hora_id INTEGER REFERENCES dim_hora(hora_id),
            estado_id INTEGER REFERENCES dim_estado(estado_id),
            tipo_pagamento_id INTEGER REFERENCES dim_tipo_pagamento(tipo_pagamento_id),
            review_score INTEGER,
            valor_pago NUMERIC(10,2),
            numero_parcelas INTEGER,
            preco_produto NUMERIC(10,2),
            custo_frete NUMERIC(10,2),
            data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    conn.commit()
    print("Tabelas criadas com sucesso!")

# Função para carregar dados nas dimensões
def load_dimension_data(conn):
    print("Carregando dados nas tabelas de dimensão...")
    
    # Carregar Dimensão Cliente
    def load_dim_cliente():
        print("Carregando dimensão Cliente...")
        try:
            customers_df = pd.read_csv(CUSTOMERS_FILE)
            cursor = conn.cursor()
            
            # Processar cada cliente e inserir na dimensão
            for _, row in customers_df.iterrows():
                cursor.execute(
                    """
                    INSERT INTO dim_cliente (cliente_key, cliente_cidade, cliente_estado, cliente_zip_code)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (cliente_key) DO UPDATE SET
                        cliente_cidade = EXCLUDED.cliente_cidade,
                        cliente_estado = EXCLUDED.cliente_estado,
                        cliente_zip_code = EXCLUDED.cliente_zip_code
                    """,
                    (
                        row['customer_id'],
                        row['customer_city'],
                        row['customer_state'],
                        row['customer_zip_code_prefix']
                    )
                )
            
            conn.commit()
            print("Dimensão Cliente carregada com sucesso!")
        except Exception as e:
            print(f"Erro ao carregar dimensão Cliente: {e}")
            conn.rollback()
    
    # Carregar Dimensão Estado
    def load_dim_estado():
        print("Carregando dimensão Estado...")
        try:
            customers_df = pd.read_csv(CUSTOMERS_FILE)
            estados_unicos = customers_df['customer_state'].unique()
            
            # Mapeamento de siglas para nomes completos dos estados brasileiros
            estados_map = {
                'AC': 'Acre', 'AL': 'Alagoas', 'AP': 'Amapá', 'AM': 'Amazonas',
                'BA': 'Bahia', 'CE': 'Ceará', 'DF': 'Distrito Federal', 'ES': 'Espírito Santo',
                'GO': 'Goiás', 'MA': 'Maranhão', 'MT': 'Mato Grosso', 'MS': 'Mato Grosso do Sul',
                'MG': 'Minas Gerais', 'PA': 'Pará', 'PB': 'Paraíba', 'PR': 'Paraná',
                'PE': 'Pernambuco', 'PI': 'Piauí', 'RJ': 'Rio de Janeiro', 'RN': 'Rio Grande do Norte',
                'RS': 'Rio Grande do Sul', 'RO': 'Rondônia', 'RR': 'Roraima', 'SC': 'Santa Catarina',
                'SP': 'São Paulo', 'SE': 'Sergipe', 'TO': 'Tocantins'
            }
            
            cursor = conn.cursor()
            
            # Inserir estados únicos na dimensão
            for estado_sigla in estados_unicos:
                estado_nome = estados_map.get(estado_sigla, 'Desconhecido')
                cursor.execute(
                    """
                    INSERT INTO dim_estado (estado_sigla, estado_nome)
                    VALUES (%s, %s)
                    ON CONFLICT (estado_sigla) DO UPDATE SET
                        estado_nome = EXCLUDED.estado_nome
                    """,
                    (estado_sigla, estado_nome)
                )
            
            conn.commit()
            print("Dimensão Estado carregada com sucesso!")
        except Exception as e:
            print(f"Erro ao carregar dimensão Estado: {e}")
            conn.rollback()
    
    # Carregar Dimensão Produto e Categoria
    def load_dim_produto_categoria():
        print("Carregando dimensões Produto e Categoria...")
        try:
            products_df = pd.read_csv(PRODUCTS_FILE)
            cursor = conn.cursor()
            
            # Primeiro, carregar categorias únicas
            categorias_unicas = products_df['product_category_name'].dropna().unique()
            for categoria in categorias_unicas:
                cursor.execute(
                    """
                    INSERT INTO dim_categoria_produto (categoria_nome)
                    VALUES (%s)
                    ON CONFLICT (categoria_nome) DO NOTHING
                    """,
                    (categoria,)
                )
            
            conn.commit()
            
            # Agora, carregar produtos com referência às categorias
            for _, row in products_df.iterrows():
                # Obter o ID da categoria se existir
                categoria_id = None
                if pd.notna(row['product_category_name']):
                    cursor.execute(
                        "SELECT categoria_id FROM dim_categoria_produto WHERE categoria_nome = %s",
                        (row['product_category_name'],)
                    )
                    result = cursor.fetchone()
                    if result:
                        categoria_id = result[0]
                
                cursor.execute(
                    """
                    INSERT INTO dim_produto (
                        produto_key, produto_categoria_id, produto_nome_comprimento,
                        produto_descricao_comprimento, produto_fotos_qtd,
                        produto_peso_g, produto_comprimento_cm, produto_altura_cm, produto_largura_cm
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (produto_key) DO UPDATE SET
                        produto_categoria_id = EXCLUDED.produto_categoria_id,
                        produto_nome_comprimento = EXCLUDED.produto_nome_comprimento,
                        produto_descricao_comprimento = EXCLUDED.produto_descricao_comprimento,
                        produto_fotos_qtd = EXCLUDED.produto_fotos_qtd,
                        produto_peso_g = EXCLUDED.produto_peso_g,
                        produto_comprimento_cm = EXCLUDED.produto_comprimento_cm,
                        produto_altura_cm = EXCLUDED.produto_altura_cm,
                        produto_largura_cm = EXCLUDED.produto_largura_cm
                    """,
                    (
                        row['product_id'],
                        categoria_id,
                        row['product_name_lenght'] if pd.notna(row['product_name_lenght']) else None,
                        row['product_description_lenght'] if pd.notna(row['product_description_lenght']) else None,
                        row['product_photos_qty'] if pd.notna(row['product_photos_qty']) else None,
                        row['product_weight_g'] if pd.notna(row['product_weight_g']) else None,
                        row['product_length_cm'] if pd.notna(row['product_length_cm']) else None,
                        row['product_height_cm'] if pd.notna(row['product_height_cm']) else None,
                        row['product_width_cm'] if pd.notna(row['product_width_cm']) else None
                    )
                )
            
            conn.commit()
            print("Dimensões Produto e Categoria carregadas com sucesso!")
        except Exception as e:
            print(f"Erro ao carregar dimensões Produto e Categoria: {e}")
            conn.rollback()
    
    # Carregar Dimensão Tipo de Pagamento
    def load_dim_tipo_pagamento():
        print("Carregando dimensão Tipo de Pagamento...")
        try:
            payments_df = pd.read_csv(ORDER_PAYMENTS_FILE)
            tipos_pagamento = payments_df['payment_type'].unique()
            
            cursor = conn.cursor()
            
            for tipo in tipos_pagamento:
                cursor.execute(
                    """
                    INSERT INTO dim_tipo_pagamento (tipo_pagamento)
                    VALUES (%s)
                    ON CONFLICT (tipo_pagamento) DO NOTHING
                    """,
                    (tipo,)
                )
            
            conn.commit()
            print("Dimensão Tipo de Pagamento carregada com sucesso!")
        except Exception as e:
            print(f"Erro ao carregar dimensão Tipo de Pagamento: {e}")
            conn.rollback()
    
    # Carregar Dimensões Data e Hora
    def load_dim_data_hora():
        print("Carregando dimensões Data e Hora...")
        try:
            orders_df = pd.read_csv(ORDERS_FILE)
            
            # Converter colunas de data para datetime
            date_columns = ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']
            for col in date_columns:
                orders_df[col] = pd.to_datetime(orders_df[col], errors='coerce')
            
            # Extrair datas e horas únicas de todas as colunas de data
            all_dates = set()
            all_times = set()
            
            for col in date_columns:
                # Extrair datas únicas
                valid_dates = orders_df[col].dropna().dt.date.unique()
                all_dates.update(valid_dates)
                
                # Extrair horas únicas
                valid_times = orders_df[col].dropna().dt.time.unique()
                all_times.update(valid_times)
            
            cursor = conn.cursor()
            
            # Carregar dimensão Data
            for date_val in all_dates:
                if pd.notna(date_val):
                    day = date_val.day
                    month = date_val.month
                    year = date_val.year
                    weekday = date_val.weekday()
                    weekday_name = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo'][weekday]
                    month_name = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 
                                 'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'][month-1]
                    quarter = (month - 1) // 3 + 1
                    
                    cursor.execute(
                        """
                        INSERT INTO dim_data (
                            data_completa, dia, mes, ano, dia_semana,
                            nome_dia_semana, mes_nome, trimestre
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (data_completa) DO NOTHING
                        """,
                        (date_val, day, month, year, weekday, weekday_name, month_name, quarter)
                    )
            
            # Carregar dimensão Hora
            for time_val in all_times:
                if pd.notna(time_val):
                    hour = time_val.hour
                    minute = time_val.minute
                    second = time_val.second
                    period = 'AM' if hour < 12 else 'PM'
                    
                    cursor.execute(
                        """
                        INSERT INTO dim_hora (
                            hora_completa, hora, minuto, segundo, periodo
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (hora_completa) DO NOTHING
                        """,
                        (time_val, hour, minute, second, period)
                    )
            
            conn.commit()
            print("Dimensões Data e Hora carregadas com sucesso!")
        except Exception as e:
            print(f"Erro ao carregar dimensões Data e Hora: {e}")
            conn.rollback()
    
    # Executar carregamento de todas as dimensões
    load_dim_cliente()
    load_dim_estado()
    load_dim_produto_categoria()
    load_dim_tipo_pagamento()
    load_dim_data_hora()

# Função para carregar dados na tabela fato
def load_fact_data(conn, mongo_client):
    print("Carregando dados na tabela fato...")
    
    try:
        # Carregar dados dos arquivos CSV
        orders_df = pd.read_csv(ORDERS_FILE)
        order_items_df = pd.read_csv(ORDER_ITEMS_FILE)
        order_payments_df = pd.read_csv(ORDER_PAYMENTS_FILE)
        
        # Converter colunas de data para datetime
        date_columns = ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']
        for col in date_columns:
            orders_df[col] = pd.to_datetime(orders_df[col], errors='coerce')
        
        # Obter dados de revisões do MongoDB
        db = mongo_client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        reviews_cursor = collection.find({})
        reviews_list = list(reviews_cursor)
        reviews_df = pd.DataFrame(reviews_list)
        
        # Converter colunas de data para datetime no DataFrame de revisões
        if 'review_creation_date' in reviews_df.columns:
            reviews_df['review_creation_date'] = pd.to_datetime(reviews_df['review_creation_date'], errors='coerce')
        if 'review_answer_timestamp' in reviews_df.columns:
            reviews_df['review_answer_timestamp'] = pd.to_datetime(reviews_df['review_answer_timestamp'], errors='coerce')
        
        # Juntar os DataFrames
        # 1. Juntar orders com order_items
        merged_df = pd.merge(order_items_df, orders_df, on='order_id', how='inner')
        
        # 2. Juntar com order_payments
        # Agrupar pagamentos por order_id para obter valor total e número de parcelas
        payment_agg = order_payments_df.groupby('order_id').agg({
            'payment_value': 'sum',
            'payment_installments': 'max',
            'payment_type': lambda x: x.iloc[0] if len(x) > 0 else None
        }).reset_index()
        
        merged_df = pd.merge(merged_df, payment_agg, on='order_id', how='left')
        
        # 3. Juntar com reviews do MongoDB
        if not reviews_df.empty and 'order_id' in reviews_df.columns:
            merged_df = pd.merge(merged_df, reviews_df[['order_id', 'review_score']], on='order_id', how='left')
        
        # Preparar para inserção na tabela fato
        cursor = conn.cursor()
        
        for _, row in merged_df.iterrows():
            # Obter IDs das dimensões
            
            # Cliente ID
            cursor.execute(
                "SELECT cliente_id FROM dim_cliente WHERE cliente_key = %s",
                (row['customer_id'],)
            )
            cliente_id_result = cursor.fetchone()
            cliente_id = cliente_id_result[0] if cliente_id_result else None
            
            # Produto ID
            cursor.execute(
                "SELECT produto_id FROM dim_produto WHERE produto_key = %s",
                (row['product_id'],)
            )
            produto_id_result = cursor.fetchone()
            produto_id = produto_id_result[0] if produto_id_result else None
            
            # Estado ID
            cursor.execute(
                "SELECT c.cliente_estado, e.estado_id FROM dim_cliente c JOIN dim_estado e ON c.cliente_estado = e.estado_sigla WHERE c.cliente_key = %s",
                (row['customer_id'],)
            )
            estado_result = cursor.fetchone()
            estado_id = estado_result[1] if estado_result else None
            
            # Data ID (usando order_purchase_timestamp)
            data_id = None
            hora_id = None
            if pd.notna(row['order_purchase_timestamp']):
                purchase_date = row['order_purchase_timestamp'].date()
                cursor.execute(
                    "SELECT data_id FROM dim_data WHERE data_completa = %s",
                    (purchase_date,)
                )
                data_id_result = cursor.fetchone()
                data_id = data_id_result[0] if data_id_result else None
                
                # Hora ID (usando order_purchase_timestamp)
                purchase_time = row['order_purchase_timestamp'].time()
                cursor.execute(
                    "SELECT hora_id FROM dim_hora WHERE hora_completa = %s",
                    (purchase_time,)
                )
                hora_id_result = cursor.fetchone()
                hora_id = hora_id_result[0] if hora_id_result else None
            
            # Tipo de Pagamento ID
            tipo_pagamento_id = None
            if pd.notna(row['payment_type']):
                cursor.execute(
                    "SELECT tipo_pagamento_id FROM dim_tipo_pagamento WHERE tipo_pagamento = %s",
                    (row['payment_type'],)
                )
                tipo_pagamento_result = cursor.fetchone()
                tipo_pagamento_id = tipo_pagamento_result[0] if tipo_pagamento_result else None
            
            # Inserir na tabela fato
            cursor.execute(
                """
                INSERT INTO fato_vendas (
                    order_id, cliente_id, produto_id, data_id, hora_id,
                    estado_id, tipo_pagamento_id, review_score,
                    valor_pago, numero_parcelas, preco_produto, custo_frete
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row['order_id'],
                    cliente_id,
                    produto_id,
                    data_id,
                    hora_id,
                    estado_id,
                    tipo_pagamento_id,
                    row['review_score'] if 'review_score' in row and pd.notna(row['review_score']) else None,
                    row['payment_value'] if pd.notna(row['payment_value']) else None,
                    row['payment_installments'] if pd.notna(row['payment_installments']) else None,
                    row['price'] if pd.notna(row['price']) else None,
                    row['freight_value'] if pd.notna(row['freight_value']) else None
                )
            )
        
        conn.commit()
        print("Dados carregados na tabela fato com sucesso!")
    except Exception as e:
        print(f"Erro ao carregar dados na tabela fato: {e}")
        conn.rollback()
        raise

# Função principal
def main():
    print("Iniciando processo ETL...")
    
    # Aguardar serviços
    wait_for_services()
    
    try:
        # Verificar a existência dos arquivos CSV necessários
        check_files_exist()
        
        # Conectar ao PostgreSQL
        print("Conectando ao PostgreSQL...")
        pg_conn = create_postgres_connection()
        print("Conexão com PostgreSQL estabelecida com sucesso!")
        
        # Conectar ao MongoDB
        print("Conectando ao MongoDB...")
        mongo_client = create_mongo_connection()
        print("Conexão com MongoDB estabelecida com sucesso!")
        
        # Criar tabelas do data warehouse
        create_dw_tables(pg_conn)
        
        # Carregar dados nas dimensões
        load_dimension_data(pg_conn)
        
        # Carregar dados na tabela fato
        load_fact_data(pg_conn, mongo_client)
        
        print("Processo ETL concluído com sucesso!")
    except Exception as e:
        print(f"Erro no processo ETL: {e}")
    finally:
        # Fechar conexões
        if 'pg_conn' in locals() and pg_conn:
            pg_conn.close()
        if 'mongo_client' in locals() and mongo_client:
            mongo_client.close()

if __name__ == "__main__":
    main()



