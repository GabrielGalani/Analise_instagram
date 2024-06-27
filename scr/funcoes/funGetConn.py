# importando bibliotecas utilizadas
from sqlalchemy import create_engine, text
import sys


# Função para se conectar ao banco de dados
def connect_to_database(database):
    try:
        connection_string = f'mssql+pyodbc://localhost/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'
        engine = create_engine(connection_string)
        return engine 
    except Exception as e:
        print("Erro ao conectar ao banco de dados:", e)
        return None


# Função para carregar as tabelas com os dados ainda não existentes
def send_to_db(engine, table, pk_column, dataset): 
    if not engine: 
        print('conexão fail')
        sys.exit()

    with engine.connect() as connection:
        for i, row in dataset.iterrows():
            query = text(f"select count(*) from {table} WHERE {pk_column} = :value")
            result = connection.execute(query, parameters=dict(value=row[pk_column]))
            count = result.scalar()

            if count > 0:
                print(f'Registro com {pk_column} = {row[pk_column]} já existe.')
            else: 
                row.to_frame().T.to_sql(table, con=connection, if_exists='append', index=False)
                print(f"Registro com {pk_column} = {row[pk_column]} inserido com sucesso.")
        connection.commit()
        connection.close()

# Função para carregar as tabelas com os dados ainda não existentes
def send_to_db_insights(engine, table_name, key_column, dataset):
        if not engine: 
            print('conexão fail')
            sys.exit
        with engine.connect() as connection:
            for i, row in dataset.iterrows():
                query = text(f"select count(*) from {table_name} WHERE {key_column} = :value")
                result = connection.execute(query, parameters=dict(value=row[key_column]))
                count = result.scalar()

                try:
                    if count > 0:
                        print(f'Registro com {key_column} = {row[key_column]} já existe.')
                    else: 
                        row.to_frame().T.to_sql(table_name, con=connection, if_exists='append', index=False)
                        print(f"Registro com {key_column} = {row[key_column]} inserido com sucesso.")
                except:
                    pass
            connection.commit()
            connection.close()