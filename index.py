import streamlit as st
import snowflake.connector as sc

# Connexion à Snowflake
def connect_to_snowflake():
    return sc.connect(
        account="skmydlt-dx40258",
        user="OUAFID",
        password="Wafid12340"
    )

# Création de l'entrepôt (warehouse)
def create_warehouse(conn, warehouse_name):
    cursor = conn.cursor()
    cursor.execute(f"CREATE WAREHOUSE IF NOT EXISTS {warehouse_name}")
    st.success(f"Warehouse '{warehouse_name}' created successfully.")

# Création du schéma
def create_schema(conn, database_name, schema_name):
    cursor = conn.cursor()
    cursor.execute(f"USE DATABASE {database_name}")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    st.success(f"Schema '{schema_name}' created successfully.")

# Création de la table
def create_table(conn, database_name, schema_name, table_name, columns):
    cursor = conn.cursor()
    cursor.execute(f"USE DATABASE {database_name}")
    cursor.execute(f"USE SCHEMA {schema_name}")
    columns_str = ', '.join([f"{col_name} {col_type}" for col_name, col_type in columns.items()])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str})")
    st.success(f"Table '{table_name}' created successfully.")

# Insertion de données dans la table
def insert_data(conn, database_name, schema_name, table_name, data):
    cursor = conn.cursor()
    cursor.execute(f"USE DATABASE {database_name}")
    cursor.execute(f"USE SCHEMA {schema_name}")
    columns = ', '.join(data.keys())
    values = ', '.join([f"'{value}'" if isinstance(value, str) else str(value) for value in data.values()])
    cursor.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({values})")
    st.success("Data inserted successfully.")

def inserer():
    st.write("### Welcome to Snowflake Demo")
    
    # Connexion à Snowflake
    conn = connect_to_snowflake()
    
    # Formulaire pour créer l'entrepôt (warehouse)
    st.write("## Create Warehouse")
    warehouse_name = st.text_input("Enter warehouse name")
    
    # Formulaire pour créer le schéma
    st.write("## Create Schema")
    database_name = st.text_input("Enter database name")
    schema_name = st.text_input("Enter schema name")
    
    # Formulaire pour créer la table
    st.write("## Create Table")
    table_name = st.text_input("Enter table name")
    num_columns = st.number_input("Enter the number of columns", min_value=1, value=1)
    columns = {}
    for i in range(num_columns):
        col_name = st.text_input(f"Enter column {i+1} name")
        col_type = st.text_input(f"Enter column {i+1} type")
        columns[col_name] = col_type
    
    # Formulaire pour insérer des données dans la table
    st.write("## Insert Data into Table")
    data = {}
    for col_name, col_type in columns.items():
        value = st.text_input(f"Enter value for column '{col_name}'")
        if col_type == 'INT':
            value = int(value)
        elif col_type == 'FLOAT':
            value = float(value)
        data[col_name] = value
    
    # Bouton pour exécuter toutes les actions
    if st.button("Execute All"):
        create_warehouse(conn, warehouse_name)
        create_schema(conn, database_name, schema_name)
        create_table(conn, database_name, schema_name, table_name, columns)
        insert_data(conn, database_name, schema_name, table_name, data)
        st.success("All actions executed successfully.")

