import os
import sys
sys.path.append(os.getcwd())
import pandas as pd
from pathlib import Path
from scr.funcoes.funGetConn import connect_to_database, send_to_db, send_to_db_insights
from scr.funcoes.funGetEnv import get_creds



class TratamentoGoldSearchIg(): 
    def __init__(self, folder, output_path): 
        self.folder = folder
        self.output_path = output_path

    def mont_path(self, file):
        for i in os.listdir(self.folder): 
            if i.endswith('.csv') and i == file:
                file_path = os.path.join(self.folder, i)
                return file_path
            

    def create_parent_folder(self):
        Path(self.output_path).mkdir(parents=True, exist_ok=True)


    def tratamento_tb_account(self): 
        
        file_path = self.mont_path('TbCabecalho.csv')
        dataset = pd.read_csv(file_path, sep='\t')

        select_columns = [
            'id_account',
            'account_username',
            'account_biography',
            'profile_picture_url',
            'account_name',
            'followers_count', 
            'follows_count', 
            'media_count'
        ]
        dataset= dataset[select_columns]

        d_type = {
            'id_account': object
        }
        dataset = dataset.astype(d_type)
        dataset['definicao'] = 'SEARCH'

        creds = get_creds('DW')
        engine = connect_to_database(creds)
        table = 'TbAccount'
        pk_column = 'id_account'
        send_to_db(engine, table, pk_column, dataset)

        self.create_parent_folder()

        output = os.path.join(self.output_path, os.path.basename(file_path))
        dataset.to_csv(output, sep='\t')
        print('----- TbAccount carregada -----')


    def tratamento_dim_midias(self):
            file_path = self.mont_path('TbMedias.csv')
            dataset = pd.read_csv(file_path, sep='\t')

            select_columns = [
                'id_midia',
                'id_account',
                'media_type',
                'media_url',
                'caption',
                'timestamp',
                'permalink',
                'media_product_type',
                'thumbnail_url'
            ]
            DTbMidias = dataset[select_columns]

            # Primeiro, vamos converter as colunas de data para o tipo de dados datetime64
            DTbMidias['timestamp'] = pd.to_datetime(DTbMidias['timestamp'])
            # # Agora, vamos extrair apenas a data (sem a hora) das colunas de data
            # DTbMidias['timestamp'] = DTbMidias['timestamp'].dt.date
            type_columns = {
                'id_midia':  'object',
                'id_account': 'object',
            }
            DTbMidias = DTbMidias.astype(type_columns)

            creds = get_creds('DW')
            engine = connect_to_database(creds)
            table = 'DTbMidias'
            pk_column = 'id_midia'
            send_to_db(engine, table, pk_column, DTbMidias)


            self.create_parent_folder()

            
            output = os.path.join(self.output_path, 'TbMedias.csv')
            DTbMidias.to_csv(output, sep='\t')
            print('----- TbMedias carregada com feed -----')


    def tratamento_fato_midias(self):
            FTbMidias = pd.read_csv(self.mont_path('TbMedias.csv'), sep='\t')

            select_columns = [
                'id_midia',
                'id_account',
                'comments_count',
                'like_count',
                'extract_date',
                'period',
                'year',
                'day'
            ]

            df_1 = FTbMidias[select_columns]
            FTbMidias__ = FTbMidias[select_columns]
            FTbMidias__['like_count'] = FTbMidias__['like_count'].fillna(0)


            FTbMidias__['id'] = FTbMidias__['id_midia'].astype(str) + \
                FTbMidias__['id_account'].astype(str) + FTbMidias__['year'].astype(str) \
                    + FTbMidias__['period'].astype(str) + FTbMidias__['day'].astype(str)
            FTbMidias__ = FTbMidias__[['id','id_midia', 'id_account', 'comments_count', 'like_count', 'extract_date', 'period', 'year', 'day']]

            FTbMidias__['extract_date'] = pd.to_datetime(FTbMidias__['extract_date'])
            FTbMidias__['extract_date'] = FTbMidias__['extract_date'].dt.date

            type_columns = {
                'id_midia': 'object',
                'id_account': 'object',
                'comments_count': 'int',
                'like_count': 'int',
                'period': 'object',
                'year': 'object',
                'day': 'object'
            }

            FTbMidias__ = FTbMidias__.astype(type_columns)

            creds = get_creds('DW')
            engine = connect_to_database(creds)
            table = 'FTbMidias'
            pk_column = 'id'
            send_to_db(engine, table, pk_column, FTbMidias__)


            self.create_parent_folder()


            output = os.path.join(self.output_path, 'FTbMidias.csv')
            FTbMidias__.to_csv(output, sep='\t')
            print('----- FTbMidias carregada com feed -----')


    def tratamento_dim_carrossel(self): 
            TbCarrossel = self.mont_path('TbCarrossel.csv')
            dataset = pd.read_csv(TbCarrossel, sep='\t', dtype={'id_midia': str})
            rename_columns = {
                'media_type.1': 'type'
            }
            dataset.rename(columns=rename_columns, inplace=True)
            dataset.drop(columns=('Unnamed: 0'),inplace=True)
            dataset['id'] = dataset['id'].astype(str) + dataset['id_midia'].astype(str)

            dataset.dropna(subset=('id_midia'), inplace=True)

            creds = get_creds('DW')
            table = 'DTbcarrossel'
            pk_column = 'id'
            engine = connect_to_database(creds)
            send_to_db(engine, table, pk_column, dataset)

            self.create_parent_folder()


            output = os.path.join(self.output_path, 'DTbcarrossel.csv')
            dataset.to_csv(output, sep='\t')
            print('----- DTbcarrossel carregada  -----')




if __name__ == "__main__": 

    ## Carregamento tabela cabecalho
    # file_path = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\silver\TbCabecalho.csv'
    folder = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\concorrentes\silver'
    output = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\concorrentes\gold'

    #A Classe vai receber a pasta onde estão os arquivos que irão subir para o banco e a pasta onde os arquivos que subiram vão sair
    tratamento_gold = TratamentoGoldSearchIg(folder, output)

    #Todos os metodos da classe
    tratamento_gold.tratamento_tb_account()
    tratamento_gold.tratamento_dim_midias()
    tratamento_gold.tratamento_fato_midias()
    tratamento_gold.tratamento_dim_carrossel()