import os
import sys
sys.path.append(os.getcwd())
import pandas as pd
from pathlib import Path
from datetime import datetime
from scr.funcoes.funGetConn import connect_to_database, send_to_db, send_to_db_insights
from scr.funcoes.funGetEnv import get_creds



class TratamentoGoldIg(): 
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
        dataset['definicao'] = 'CLIENTE'

        creds = get_creds('DW')
        engine = connect_to_database(creds)
        table = 'TbAccount'
        pk_column = 'id_account'
        send_to_db(engine, table, pk_column, dataset)

        self.create_parent_folder()

        output = os.path.join(self.output_path, os.path.basename(file_path))
        dataset.to_csv(output, sep='\t')
        print('----- TbAccount carregada -----')

    

    def tratamento_ftb_account(self): 
        
        file_path = self.mont_path('TbCabecalho.csv')
        dataset = pd.read_csv(file_path, sep='\t')
        today = datetime.now()
        extract_date = today.strftime('%Y-%m-%d')
        period = today.strftime('%m')
        year = today.strftime('%Y')
        day = today.strftime('%d')


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



        dataset['extract_date'] = extract_date
        dataset['period'] = period
        dataset['year'] = year
        dataset['day'] = day
        dataset['id'] = dataset['id_account'].astype(str) + dataset['year'] + dataset['period'] + dataset['day']

        dataset['extract_date'] = pd.to_datetime(dataset['extract_date'])
        d_type = {
            'id': 'object',
            'id_account': 'object',
            'year': 'object',
            'period': 'object',
            'day': 'object',

        }
        dataset = dataset.astype(d_type)


        creds = get_creds('DW')
        engine = connect_to_database(creds)
        table = 'FTbAccount'
        pk_column = 'id'
        send_to_db(engine, table, pk_column, dataset)

        self.create_parent_folder()

        output = os.path.join(self.output_path, os.path.basename(file_path))
        dataset.to_csv(output, sep='\t')
        print('----- FbAccount carregada -----')



    def tratamento_dim_descricao_insights(self): 
        TbAccontDayInsights_file = self.mont_path('TbAccontDayInsights.csv')
        cidade_seguidores = self.mont_path('TbAccountLifeCidadeDosSeguidores.csv')
        Faixa_genero = self.mont_path('TbAccountLifeFaixaGenero.csv')
        local_pais = self.mont_path('TbAccountLifeLocaPais.csv')
        pais_seguidores = self.mont_path('TbAccountLifePaisDosSeguidores.csv')
        seguidores_on = self.mont_path('TbAccountLifeSeguidoresOnline.csv')
        stories = self.mont_path('TbStoriesInsights.csv')
        midias = self.mont_path('TbMidiasInsights.csv')
        
        ## Tratamentos TbAccontDayInsights_file
        dataset = pd.read_csv(TbAccontDayInsights_file, sep='\t')
        select_columns = [
            'name',
            'period',
            'title',
            'description'
        ]
        dataset_day_dimension = dataset[select_columns]


        # Arquivos TbAccountLifeCidadeDosSeguidores, TbAccountLifeFaixaGenero, TbAccountLifeLocaPais, TbAccountLifePaisDosSeguidores, TbAccountLifeSeguidoresOnline
        list_df = [cidade_seguidores, Faixa_genero, local_pais, pais_seguidores, seguidores_on]
        dfs = [] 
        for df in list_df: 
            data = pd.read_csv(df, sep='\t')
            dfs.append(data)

        dataset = pd.concat(dfs)
        select_columns = [
            'name',
            'period',
            'title',
            'description'
        ]
        dataset_lifetime_dimension = dataset[select_columns]


        ## TbStoriesInsights
        dataset_1 = pd.read_csv(stories, sep='\t')
        select_columns = [
            'name',
            'period',
            'title',
            'description'
        ]
        stories_descricao_dimensao = dataset_1[select_columns]


        #TbMidiasInsights
        dataset_2 = pd.read_csv(midias, sep='\t')
        select_columns = [
            'name',
            'period',
            'title',
            'description'
        ]
        midias_descricao_dimensao = dataset_2[select_columns]


        ## Transformando em dados únicos
        dataset_day_dimension.drop_duplicates(inplace=True)
        dataset_lifetime_dimension.drop_duplicates(inplace=True)
        stories_descricao_dimensao.drop_duplicates(subset=('name'), inplace=True)
        midias_descricao_dimensao.drop_duplicates(subset=('name'),inplace=True)


        ## Unindo todas as dimensões 
        DTbAccountInsights = pd.concat([dataset_day_dimension,dataset_lifetime_dimension, stories_descricao_dimensao, midias_descricao_dimensao])
        DTbAccountInsights.sort_values('name', inplace=True)
        DTbAccountInsights.drop_duplicates(subset=('name'),inplace=True)
        DTbAccountInsights.reset_index(inplace=True)
        DTbAccountInsights['id_insight'] = DTbAccountInsights.index + 1
        DTbAccountInsights = DTbAccountInsights[['id_insight'] + [col for col in DTbAccountInsights.columns if col != 'id_insight']]
        DTbAccountInsights.drop(columns='index', inplace=True)
        DTbAccountInsights.rename(columns={'period': 'frequencia'}, inplace=True)

        creds = get_creds('DW')
        engine = connect_to_database(creds)
        table = 'DTbDescricaoInsights'
        pk_column = 'id_insight'

        send_to_db(engine, table, pk_column, DTbAccountInsights)

            
        self.create_parent_folder()

        output = os.path.join(self.output_path, 'DTbDescricaoInsights.csv')
        DTbAccountInsights.to_csv(output, sep='\t')
        print('----- DTbDescricaoInsights carregada -----')

    
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
            'thumbnail_url',
            'shortcode'
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


    def tratamento_stories_dim_midias(self):
        file_path = self.mont_path('TbStories.csv')
        dataset = pd.read_csv(file_path, sep='\t')
        stories = dataset

        select_columns = [
            'id_midia',
            'id_account',
            'media_type',
            'media_url',
            'caption',
            'timestamp',
            'permalink',
            'media_product_type',
            'thumbnail_url',
            'shortcode'
        ]

        if 'thumbnail_url' not in dataset.columns:
            dataset['thumbnail_url'] = pd.Series(dtype=object)

        if "caption" not in dataset.columns: 
            dataset['caption'] = pd.Series(dtype=object)

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


        output = os.path.join(self.output_path, 'TbMediasStories.csv')
        DTbMidias.to_csv(output, sep='\t')
        print('----- TbMedias carregada com stoires -----')



    def tratamento_fato_midias(self):
        stories = pd.read_csv(self.mont_path('TbStories.csv'), sep='\t')
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
        df_2 = stories[select_columns]
        FTbMidias__ = pd.concat([df_1, df_2], ignore_index=True)


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
        print('----- FTbMidias carregada com feed e stories -----')
    

    def tratamento_fato_account_day_insights(self): 
        TbAccontDayInsights_file = self.mont_path('TbAccontDayInsights.csv')
        FTbAccountDayInsights = pd.read_csv(TbAccontDayInsights_file, sep='\t')
        

        # Unindo com a tb de dimensão para verificar
        creds = get_creds('DW')
        engine = connect_to_database(creds)
        query = "select * from DTbDescricaoInsights"
        DTbDescricaoInsights = pd.read_sql(query, engine)

        FTbAccountDayInsights = pd.merge(FTbAccountDayInsights, DTbDescricaoInsights, on='name', how= 'inner', suffixes=('_left', '_right'))

        select_columns = [
        'id_tb_midia',
        'id_insight',
        'id_account',
        'last_day',
        'last_end_time',
        'actual_day',
        'actual_end_time',
        'extract_date',
        'period_extraction',
        'year',
        'day'
        ]
        FTbAccountDayInsights = FTbAccountDayInsights[select_columns]


        columns_rename = {
            'id_tb_midia': 'id',
            'last_day': 'value_last_day',
            'actual_day': 'value_actual_day'
        }
        FTbAccountDayInsights.rename(columns = columns_rename, inplace=True)


        # Primeiro, vamos converter as colunas de data para o tipo de dados datetime64
        FTbAccountDayInsights['last_end_time'] = pd.to_datetime(FTbAccountDayInsights['last_end_time'])
        FTbAccountDayInsights['actual_end_time'] = pd.to_datetime(FTbAccountDayInsights['actual_end_time'])
        FTbAccountDayInsights['extract_date'] = pd.to_datetime(FTbAccountDayInsights['extract_date'])

        # Agora, vamos extrair apenas a data (sem a hora) das colunas de data
        FTbAccountDayInsights['last_end_time'] = FTbAccountDayInsights['last_end_time'].dt.date
        FTbAccountDayInsights['actual_end_time'] = FTbAccountDayInsights['actual_end_time'].dt.date
        FTbAccountDayInsights['extract_date'] = FTbAccountDayInsights['extract_date'].dt.date

        type_columns = {
            'period_extraction': 'object',
            'year': 'object',
            'day': 'object'                
        }

        FTbAccountDayInsights = FTbAccountDayInsights.astype(type_columns)


        creds = get_creds('DW')
        engine = connect_to_database(creds)
        table = 'FTbAccountDayInsights'
        pk_column = 'id'
        send_to_db(engine, table, pk_column, FTbAccountDayInsights)

        
        self.create_parent_folder()


        output = os.path.join(self.output_path, 'TbAccontDayInsights.csv')
        FTbAccountDayInsights.to_csv(output, sep='\t')
        print('----- TbAccontDayInsights carregada  -----')
    

    def tratamento_fato_midias_insights(self): 
        TbStoriesInsights_path = self.mont_path('TbStoriesInsights.csv')
        TbMidiasInsights = self.mont_path('TbMidiasInsights.csv')


        TbStoriesInsights = pd.read_csv(TbStoriesInsights_path, sep='\t')
        TbMidiasInsights = pd.read_csv(TbMidiasInsights, sep='\t')
        # TbMidiasInsights.rename(columns={'id': 'id_midia'}, inplace=True)


        creds = get_creds('DW')
        engine = connect_to_database(creds)
        query = "select * from DTbDescricaoInsights"
        DTbDescricaoInsights = pd.read_sql(query, engine)


        TbStoriesInsights = pd.merge(TbStoriesInsights, DTbDescricaoInsights, on='name', how= 'inner')
        TbStoriesInsights.rename(columns={'id_insight_x': 'id_insight'}, inplace=True)
        TbMidiasInsights = pd.merge(TbMidiasInsights, DTbDescricaoInsights, on='name', how= 'inner')

        select_columns = [
            'id_midia',
            'id_insight',
            'extract_date',
            'period_extraction',
            'year',
            'day',
            'name',  # Renomear
            'value'
        ]

        TbStoriesInsights = TbStoriesInsights[select_columns]
        TbMidiasInsights = TbMidiasInsights[select_columns]

        FTbMidiasInsights = pd.concat([TbStoriesInsights, TbMidiasInsights], ignore_index=True)
        FTbMidiasInsights['id'] = FTbMidiasInsights['id_midia'].astype(str) + FTbMidiasInsights['id_insight'].astype(str) + \
                                    FTbMidiasInsights['year'].astype(str) + FTbMidiasInsights['period_extraction'].astype(str) + \
                                        FTbMidiasInsights['day'].astype(str)

        FTbMidiasInsights = FTbMidiasInsights[['id','id_midia', 'id_insight', 'extract_date', 'period_extraction', 'year', 'day', 'name', 'value']]
        FTbMidiasInsights.rename(columns={'name': 'definicao'}, inplace=True)

        FTbMidiasInsights['extract_date'] = pd.to_datetime(FTbMidiasInsights['extract_date'])
        FTbMidiasInsights['extract_date'] = FTbMidiasInsights['extract_date'].dt.date

        type_columns = {
            'id_midia': 'object',
            'id_insight': 'object',
            'period_extraction': 'object',
            'year': 'object',
            'day': 'object',
            'value': 'int'
        }

        FTbMidiasInsights = FTbMidiasInsights.astype(type_columns)

        creds = get_creds('DW')
        engine = connect_to_database(creds)
        table = 'FTbMidiasInsights'
        pk_column = 'id'
        send_to_db(engine, table, pk_column, FTbMidiasInsights)


        self.create_parent_folder()


        output = os.path.join(self.output_path, 'FTbMidiasInsights.csv')
        FTbMidiasInsights.to_csv(output, sep='\t')
        print('----- FTbMidiasInsights carregada com feeds e stories  -----')



    def tratamento_fato_account_lifetime_insights(self):
        creds = get_creds('DW')
        engine = connect_to_database(creds)
        query = "select * from DTbDescricaoInsights"
        DTbDescricaoInsights = pd.read_sql(query, engine)

        cidade_seguidores = self.mont_path('TbAccountLifeCidadeDosSeguidores.csv')
        Faixa_genero = self.mont_path('TbAccountLifeFaixaGenero.csv')
        local_pais = self.mont_path('TbAccountLifeLocaPais.csv')
        pais_seguidores = self.mont_path('TbAccountLifePaisDosSeguidores.csv')
        seguidores_on = self.mont_path('TbAccountLifeSeguidoresOnline.csv')


        list_df = [cidade_seguidores, Faixa_genero, local_pais, pais_seguidores, seguidores_on]
        dfs = [] 

        for df in list_df: 
            data = pd.read_csv(df, sep='\t')
            dfs.append(data)

        dataset = pd.concat(dfs)


        FTbAccountLifetimeInsights = dataset

        FTbAccountLifetimeInsights = pd.merge(FTbAccountLifetimeInsights, DTbDescricaoInsights, on='name', how= 'inner')

        select_columns = [
            'id_tb_account',
            'id_account',
            'id_insight',
            'extract_date',
            'period_extraction',
            'year',
            'day',
            'name',
            'age_gender',
            'value'
        ]

        FTbAccountLifetimeInsights = FTbAccountLifetimeInsights[select_columns]

        rename_columns = {
            'id_tb_account': 'id',
            'name': 'definicao',
            'age_gender': 'value_descricao'
        }

        FTbAccountLifetimeInsights.rename(columns=rename_columns, inplace=True)
        FTbAccountLifetimeInsights['value_descricao'] = FTbAccountLifetimeInsights['value_descricao'].str.replace('value.', '')


        def replace_non_numeric(value):
            try:
                float(value)  # Tenta converter o valor para float
                return value  # Se for numérico, mantenha o valor
            except ValueError:
                return 0  # Se não for numérico, substitua por 0

        # Aplique a função à coluna 'value'
        FTbAccountLifetimeInsights['value'] = FTbAccountLifetimeInsights['value'].apply(lambda x: replace_non_numeric(x))
        FTbAccountLifetimeInsights['value'] = FTbAccountLifetimeInsights['value'].fillna(0)


        FTbAccountLifetimeInsights['extract_date'] = pd.to_datetime(FTbAccountLifetimeInsights['extract_date'])

        type_columns = {
            'id_account': 'object',
            'period_extraction': 'object',
            'year': 'object',
            'day': 'object'
        }

        FTbAccountLifetimeInsights = FTbAccountLifetimeInsights.astype(type_columns)
        FTbAccountLifetimeInsights.drop(columns='id', inplace=True)
        FTbAccountLifetimeInsights['id'] = FTbAccountLifetimeInsights['id_account'].astype(str) + FTbAccountLifetimeInsights['id_insight'].astype(str) +  FTbAccountLifetimeInsights['extract_date'].astype(str) + FTbAccountLifetimeInsights['value_descricao'] 

        colunas = ['id'] + [coluna for coluna in FTbAccountLifetimeInsights if coluna != 'id']
        FTbAccountLifetimeInsights = FTbAccountLifetimeInsights[colunas]


        FTbAccountLifetimeInsights = FTbAccountLifetimeInsights[FTbAccountLifetimeInsights['value'] != 0]
        FTbAccountLifetimeInsights.drop_duplicates(inplace=True)


        creds = get_creds('DW')
        engine = connect_to_database(creds)
        table_name = 'FTbAccountLifetimeInsights'
        key_column = 'id'


        send_to_db_insights(engine, table_name, key_column, FTbAccountLifetimeInsights)



        self.create_parent_folder()


        output = os.path.join(self.output_path, 'TbAccontDayInsights.csv')
        FTbAccountLifetimeInsights.to_csv(output, sep='\t')
        print('----- FTbAccountLifetimeInsights carregada  -----')


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
    folder = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\silver'
    output = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\gold'

    #A Classe vai receber a pasta onde estão os arquivos que irão subir para o banco e a pasta onde os arquivos que subiram vão sair
    tratamento_gold = TratamentoGoldIg(folder, output)

    #Todos os metodos da classe
    # tratamento_gold.tratamento_tb_account()
    # tratamento_gold.tratamento_ftb_account()
    # tratamento_gold.tratamento_dim_descricao_insights()
    # tratamento_gold.tratamento_dim_midias()
    # tratamento_gold.tratamento_stories_dim_midias()
    # tratamento_gold.tratamento_fato_midias()
    # tratamento_gold.tratamento_dim_carrossel()
    # tratamento_gold.tratamento_fato_account_day_insights()
    # tratamento_gold.tratamento_fato_midias_insights()
    # tratamento_gold.tratamento_fato_account_lifetime_insights()
