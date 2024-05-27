from pathlib import Path
from datetime import datetime
import pandas as pd

class TratamentoSilverSearchApiIg(): 
    def __init__(self, file_json): 
        self.file_json = file_json
    
    def read_json(self, json):
        dataset = pd.read_json(json)

        return dataset
    

    def output_file(self, output_path, dataset, file):
        try:
            dataset.to_csv(output_path, sep='\t')
            print(f'Arquivo - {file} - Salvo com sucesso')
        except: 
            print(f'Erro ao salvar arquivo {file}')
    

    def tb_cabecalho(self): 
        data_cabecalho = self.read_json(self.file_json)
        rename_column_cabecalho = {
            'id': 'id_account',
            'username': 'account_username',
            'biography': 'account_biography',
            'profile_picture_url': 'profile_picture_url',
            'name': 'account_name'
        }
        data_cabecalho.rename(columns=rename_column_cabecalho, inplace=True)

        select_column_cabecalho = ['id_account', 'account_username', 'account_biography', 'profile_picture_url', 'account_name', 'followers_count', 'follows_count', 'media_count']
        data_cabecalho = data_cabecalho[select_column_cabecalho]

        return data_cabecalho
    
    def tb_midias_cabecalho(self, extract_date, period, year, day, json): 
        # data_midias_sem_insights = self.read_json(self.file_json)
        data_midias_sem_insights = pd.read_json(json, orient='records', dtype={'id': str})
        data_midias_sem_insights
        dataframes = []
        owner_df = pd.json_normalize(data_midias_sem_insights['owner']).rename(columns={'id': 'id_account'})
        df_final_midia = pd.concat([data_midias_sem_insights, owner_df], axis=1)
        df_final_midia.drop(columns=['owner'], inplace=True)
        df_final_midia.rename(columns={'id': 'id_midia'}, inplace=True)

        select_tb_midia = ['id_account', 'username', 'id_midia', 'comments_count', 'like_count',
                    'media_type', 'media_url', 'caption', 'timestamp', 'permalink',
                    'media_product_type', 'thumbnail_url']
        df_tb_midia = df_final_midia[select_tb_midia]
        df_tb_midia['extract_date'] = extract_date
        df_tb_midia['period'] = period
        df_tb_midia['year'] = year
        df_tb_midia['day'] = day
        df_tb_midia['id_tb_midia'] = df_tb_midia['id_midia'].astype(str) + df_tb_midia['year'] + df_tb_midia['period'] + df_tb_midia['day']

        # filtro = df_tb_midia[df_tb_midia['id_midia'] == '18002752616172667']
        df_tb_midia.head()

        #######
        # Tb Carrocel
        #######
        select_carrocel = ['id_midia', 'media_type', 'children']
        df_carroceu = df_final_midia[select_carrocel]
        df_carroceu = df_carroceu[df_carroceu['media_type'] == "CAROUSEL_ALBUM"]
        normalized_df = pd.json_normalize(df_carroceu['children'])
        normalized_df = normalized_df.explode('data')
        expanded_df = normalized_df.drop(columns='data').join(pd.json_normalize(normalized_df['data']))

        dataframe_carrocel = pd.concat([df_carroceu.reset_index(drop=True), expanded_df.reset_index(drop=True)], axis=1)
        dataframe_carrocel.drop(columns='children', inplace=True)

        return df_tb_midia, dataframe_carrocel


if __name__ == "__main__": 

    #####
    # Variaveis Globais
    #####
    today = datetime.now()
    extract_date = today.strftime('%Y-%m-%d')
    period = today.strftime('%m')
    year = today.strftime('%Y')
    day = today.strftime('%d')

    print(f'Hoje: {extract_date}, period: {period}, Year: {year}, day: {day}')

        
    ########
    ## Tb Cabeçalho
    ########
    file_json = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\concorrentes\bronze\TbAccount.json'
    file_output = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\concorrentes\silver\TbCabecalho.csv'
    input_file = 'TbAccount.json'

    silver = TratamentoSilverSearchApiIg(file_json)
    dataset = silver.tb_cabecalho()
    silver.output_file(file_output, dataset, input_file)


    ########
    ## Tb Midia
    ########
    file_json = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\concorrentes\bronze\TbMedias.json'
    file_output_midias = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\concorrentes\silver\TbMedias.csv'
    carrocel_output = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\concorrentes\silver\TbCarrocel.csv'
    input_file_midias = 'TbMedias.json'
    input_file_carrocel = 'TbCarrocel.json'

    silver = TratamentoSilverSearchApiIg(file_json)
    dataset_midia, dataset_carrocel = silver.tb_midias_cabecalho(extract_date, period, year, day, file_json)
    
    #Salvando dataset midia
    silver.output_file(file_output_midias, dataset_midia, input_file)

    #Salvando dataset carrocel
    silver.output_file(carrocel_output, dataset_carrocel, input_file_carrocel)