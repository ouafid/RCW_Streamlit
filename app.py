import streamlit as st
import snowflake.connector as sc
import pandas as pd
from index import inserer

def afficher():
    st.write("### Welcome to Snowflake Demo")
    
    con = sc.connect(
        account="skmydlt-dx40258",
        user="OUAFID",
        password="Wafid12340"
    )
    cursor = con.cursor()
    
    def get_databases():
        # Snowflake SQL to get the list of databases
        sql = "SHOW DATABASES"
        cursor.execute(sql)
        data = cursor.fetchall()
        return [row[1] for row in data]
    
    def get_schemas(database):
        # Snowflake SQL to get the list of schemas for the selected database
        sql = f"SHOW SCHEMAS IN DATABASE {database}"
        cursor.execute(sql)
        data = cursor.fetchall()
        return [row[1] for row in data]
    
    def get_tables(database, schema):
        # Snowflake SQL to specify the database and get the list of tables for the selected schema
        sql_use = f"USE DATABASE {database}"
        cursor.execute(sql_use)
        
        sql_show_tables = f"SHOW TABLES IN SCHEMA {schema}"
        cursor.execute(sql_show_tables)
        
        data = cursor.fetchall()
        return [row[1] for row in data]
    
    selected_database = st.selectbox("Select a database", get_databases())
    
    if selected_database:
        selected_schema = st.selectbox("Select a schema", get_schemas(selected_database))
        
        if selected_schema:
            st.write(f"### Tables in schema '{selected_schema}'")
            tables = get_tables(selected_database, selected_schema)
            selected_table = st.selectbox("Select a table", tables)
            
            if selected_table:
                st.write(f"### Data in table '{selected_table}'")
                # Snowflake SQL to get the data from the selected table
                sql = f"SELECT * FROM {selected_schema}.{selected_table}"
                cursor.execute(sql)
                data = cursor.fetchall()
                df = pd.DataFrame(data, columns=[desc[0] for desc in cursor.description])
                st.write(df)
    

# Bouton pour la fonction index
if st.sidebar.button("CREATE "):
          inserer()
                
if __name__ == "__main__":
        afficher()

        


