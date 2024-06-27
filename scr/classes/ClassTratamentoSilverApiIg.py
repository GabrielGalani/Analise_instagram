# Importando bibliotecas
from pathlib import Path
from datetime import datetime
import pandas as pd


# Classe de tratamento de dados
class TratamentoSilverApiIg(): 
    def __init__(self, file_json): 
        self.file_json = file_json
    
    # Função de leitura de json
    def read_json(self, json):
        dataset = pd.read_json(json)

        return dataset
    
    # Função de salvamento de arquivo
    def output_file(self, output_path, dataset, file):
        try:
            dataset.to_csv(output_path, sep='\t')
            print(f'Arquivo - {file} - Salvo com sucesso')
        except: 
            print(f'Erro ao salvar arquivo {file}')
    

    # Tratamentos da tabela de cabeçalho
    def tb_cabecalho(self): 
        data_cabecalho = pd.read_json(self.file_json, orient='records', dtype={'id': str})
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
    
    
    # Função de tratamento de Mídias cabeçalho
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
                    'media_product_type', 'thumbnail_url', 'shortcode']
        df_tb_midia = df_final_midia[select_tb_midia]
        df_tb_midia['extract_date'] = extract_date
        df_tb_midia['period'] = period
        df_tb_midia['year'] = year
        df_tb_midia['day'] = day
        df_tb_midia['id_tb_midia'] = df_tb_midia['id_midia'].astype(str) + df_tb_midia['year'] + df_tb_midia['period'] + df_tb_midia['day']


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
    

    # Tratamento da tabela de stories
    def tb_stories(self, extract_date, period, year, day, json):
        # data_stories_sem_insights = self.read_json(self.file_json)
        data_stories_sem_insights = pd.read_json(json, orient='records', dtype={'id': str})
        data_stories_sem_insights.dropna(subset=['owner'], inplace=True)
        owner_df = pd.json_normalize(data_stories_sem_insights['owner']).rename(columns={'id': 'id_account'})
        df_final_midia = pd.concat([data_stories_sem_insights.reset_index(drop=True), owner_df.reset_index(drop=True)], axis=1)
        df_final_midia.drop(columns=['owner'], inplace=True)
        df_final_midia.rename(columns={'id': 'id_midia'}, inplace=True)

        df_final_midia['extract_date'] = extract_date
        df_final_midia['period'] = period
        df_final_midia['year'] = year
        df_final_midia['day'] = day
        df_final_midia['id_tb_midia'] = df_final_midia['id_midia'].astype(str) + df_final_midia['year'] + df_final_midia['period'] + df_final_midia['day']


        return df_final_midia
    
    
    # Tratamento da tabela de account por dia
    def tb_account_day(self, extract_date, period, year, day): 
        data_day_account = self.read_json(self.file_json)

        columns = pd.json_normalize(data_day_account['values'])
        column_0 = pd.json_normalize(columns[0])
        column_0.rename(columns={'value': 'last_day', 'end_time': 'last_end_time'}, inplace=True)
        column_1 = pd.json_normalize(columns[1])
        column_1.rename(columns={'value': 'actual_day', 'end_time': 'actual_end_time'}, inplace=True)
        resultado = pd.concat([data_day_account, column_0, column_1], axis=1)

        resultado['id_account'] = resultado['id'].str.split('/').str[0]
        resultado['chave'] = resultado['id_account'].astype(str) + resultado['name'] 

        resultado['extract_date'] = extract_date
        resultado['period_extraction'] = period
        resultado['year'] = year
        resultado['day'] = day
        resultado['id_tb_midia'] = (
            resultado['id_account'].astype(str) + 
            resultado['year'].astype(str) + 
            resultado['period_extraction'].astype(str) + 
            resultado['day'].astype(str) + 
            resultado['name'].astype(str) + 
            resultado['period'].astype(str)
        )
        resultado.drop(columns='values',inplace=True)

        
        return resultado


    # Tratamento da tabela de account por lifetime
    def tb_account_lifetime(self, extract_date, period, year, day): 
        data_lifetime_account = self.read_json(self.file_json)

        data_lifetime_account['id_account'] = data_lifetime_account['id'].str.split('/').str[0]

        data_lifetime_account['extract_date'] = extract_date
        data_lifetime_account['period_extraction'] = period
        data_lifetime_account['year'] = year
        data_lifetime_account['day'] = day
        data_lifetime_account['id_tb_account'] = (
            data_lifetime_account['id_account'].astype(str) + 
            data_lifetime_account['year'].astype(str) + 
            data_lifetime_account['period_extraction'].astype(str) + 
            data_lifetime_account['day'].astype(str) + 
            data_lifetime_account['name'].astype(str) + 
            data_lifetime_account['period'].astype(str)
        )


        ######
        # Tb Faixa Etaria e Genero
        ######
        audience_gender_age = data_lifetime_account[data_lifetime_account['name'] == 'audience_gender_age']
        values_0 = pd.json_normalize(audience_gender_age['values'])
        values_0 = pd.json_normalize(values_0[0])

        merged_df = pd.concat([audience_gender_age.reset_index(drop=True), values_0.reset_index(drop=True)], axis=1)
        merged_df.drop(columns = ['values', 'id'],inplace=True)
        df_faixa_etaria_e_genero = merged_df.melt(id_vars=['name', 'period', 'title', 'description', 'username', 'id_account', 'extract_date', 'period_extraction', 'year', 'day', 'id_tb_account'], var_name='age_gender', value_name='value')


        #####
        # Tb Localizacao Pais
        #####
        audience_locale = data_lifetime_account[data_lifetime_account['name'] == 'audience_locale']
        values_0 = pd.json_normalize(audience_locale['values'])
        values_0 = pd.json_normalize(values_0[0])

        merged_df = pd.concat([audience_locale.reset_index(drop=True), values_0.reset_index(drop=True)], axis=1)
        merged_df.drop(columns = ['values', 'id'],inplace=True)
        df_localizacao_pais = merged_df.melt(id_vars=['name', 'period', 'title', 'description', 'username', 'id_account', 'extract_date', 'period_extraction', 'year', 'day', 'id_tb_account'], var_name='age_gender', value_name='value')


        #####
        # Tb Pais do Publico
        #####
        audience_country = data_lifetime_account[data_lifetime_account['name'] == 'audience_country']
        values_0 = pd.json_normalize(audience_country['values'])
        values_0 = pd.json_normalize(values_0[0])

        merged_df = pd.concat([audience_country.reset_index(drop=True), values_0.reset_index(drop=True)], axis=1)
        merged_df.drop(columns = ['values', 'id'],inplace=True)
        df_pais_do_publico = merged_df.melt(id_vars=['name', 'period', 'title', 'description', 'username', 'id_account', 'extract_date', 'period_extraction', 'year', 'day', 'id_tb_account'], var_name='age_gender', value_name='value')


        #####
        # Tb Cidade dos Seguidores
        #####
        audience_city = data_lifetime_account[data_lifetime_account['name'] == 'audience_city']
        values_0 = pd.json_normalize(audience_city['values'])
        values_0 = pd.json_normalize(values_0[0])

        merged_df = pd.concat([audience_city.reset_index(drop=True), values_0.reset_index(drop=True)], axis=1)
        merged_df.drop(columns = ['values', 'id'],inplace=True)
        df_cidade_seguidores = merged_df.melt(id_vars=['name', 'period', 'title', 'description', 'username', 'id_account', 'extract_date', 'period_extraction', 'year', 'day', 'id_tb_account'], var_name='age_gender', value_name='value')


        ##### 
        # Tb Seguidores Online
        #####
        online_followers = data_lifetime_account[data_lifetime_account['name'] == 'online_followers']
        values_0 = pd.json_normalize(online_followers['values'])
        values_0 = pd.json_normalize(values_0[0])

        merged_df = pd.concat([online_followers.reset_index(drop=True), values_0.reset_index(drop=True)], axis=1)
        merged_df.drop(columns = ['values', 'id'],inplace=True)
        df_seguidores_online = merged_df.melt(id_vars=['name', 'period', 'title', 'description', 'username', 'id_account', 'extract_date', 'period_extraction', 'year', 'day', 'id_tb_account'], var_name='age_gender', value_name='value')


        return df_faixa_etaria_e_genero, df_localizacao_pais, df_pais_do_publico, df_cidade_seguidores, df_seguidores_online


    # Tratamento da tabela de stroies insights
    def tb_stories_insights(self, extract_date, period, year, day, json): 
        data_lifetime_story = pd.read_json(json, orient='records', dtype={'id': str})
        data_lifetime_story.dropna(subset=['insights'], inplace=True)
        insights = pd.json_normalize(data_lifetime_story['insights'])
        data = pd.concat([data_lifetime_story.reset_index(drop=True), insights], axis=1).drop(columns='insights')
        data = pd.json_normalize(insights['data'])
        data.head()

        dfs = []

        for index, row in data.iterrows():
            try:
                column_0 = pd.json_normalize(row)
                values = pd.json_normalize(pd.json_normalize(column_0['values'])[0])
                column_0 = pd.concat([column_0, values], axis=1).drop(columns='values')
                dfs.append(column_0)
            except:
                pass      
                      
        df_final = pd.concat(dfs).drop_duplicates().dropna()
        df_final['id_midia'] = df_final['id'].str.split('/').str[0]
        df_final = df_final[['id_midia', 'name', 'period', 'title','description', 'id', 'value']]

        df_final['extract_date'] = extract_date
        df_final['period_extraction'] = period
        df_final['year'] = year
        df_final['day'] = day

        df_final['id_tb_midias'] = (
            df_final['id_midia'].astype(str) + 
            df_final['year'].astype(str) + 
            df_final['period_extraction'].astype(str) + 
            df_final['day'].astype(str) + 
            df_final['name'].astype(str) + 
            df_final['period'].astype(str)
        )

        return df_final


    # Tabela de mídia insights
    def tb_midia_insights(self, extract_date, period, year, day):
        data_lifetime_midia = self.read_json(self.file_json)
        insights = pd.json_normalize(data_lifetime_midia['insights'])
        insights = pd.concat([data_lifetime_midia, insights], axis=1).drop(columns='insights')
        data = pd.json_normalize(insights['data'])
        dfs = []


        for index, row in data.iterrows():
            try:
                column_0 = pd.json_normalize(row)
                values = pd.json_normalize(pd.json_normalize(column_0['values'])[0])
                column_0 = pd.concat([column_0, values], axis=1).drop(columns='values')
                dfs.append(column_0)
            except:
                pass
                
        df_final = pd.concat(dfs).drop_duplicates().dropna()
        df_final.head(50)

        df_final['id_midia'] = df_final['id'].str.split('/').str[0]
        df_final = df_final[['id_midia', 'name', 'period', 'title','description', 'id', 'value']]

        df_final['extract_date'] = extract_date
        df_final['period_extraction'] = period
        df_final['year'] = year
        df_final['day'] = day

        df_final['id_tb_midias'] = (
            df_final['id_midia'].astype(str) + 
            df_final['year'].astype(str) + 
            df_final['period_extraction'].astype(str) + 
            df_final['day'].astype(str) + 
            df_final['name'].astype(str) + 
            df_final['period'].astype(str)
        )

        return df_final
if __name__ == "__main__": 

    ## Testando a classe
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
    file_json = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\bronze\tbAccount_ig.json'
    file_output = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\silver\TbCabecalho.csv'
    input_file = 'tbAccount_ig.json'

    silver = TratamentoSilverApiIg(file_json)
    dataset = silver.tb_cabecalho()
    silver.output_file(file_output, dataset, input_file)


    ########
    ## Tb Midia
    ########
    file_json = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\bronze\TbMedias.json'
    file_output_midias = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\silver\TbMedias.csv'
    carrocel_output = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\silver\TbCarrocel.csv'
    input_file_midias = 'TbMedias.json'
    input_file_carrocel = 'TbCarrocel.json'

    silver = TratamentoSilverApiIg(file_json)
    dataset_midia, dataset_carrocel = silver.tb_midias_cabecalho(extract_date, period, year, day)
    
    #Salvando dataset midia
    silver.output_file(file_output_midias, dataset_midia, input_file)

    #Salvando dataset carrocel
    silver.output_file(carrocel_output, dataset_carrocel, input_file_carrocel)

    

    ########
    ## Tb Stories
    ########
    file_json = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\bronze\TbStories.json'
    stories_output = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\silver\TbStories.csv'
    input_file_stories = 'TbStories.json'

    silver = TratamentoSilverApiIg(file_json)
    dataset_stories = silver.tb_stories(extract_date, period, year, day)
    
    #Salvando dataset midia
    silver.output_file(stories_output, dataset_stories, input_file_stories)


    ########
    ## Tb Account Day Insights
    ########
    file_json = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\bronze\TbDayAccounIgMetrics.json'
    account_day_insights_output = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\silver\TbAccontDayInsights.csv'
    input_file_stories = 'TbDayAccounIgMetrics.json'

    silver = TratamentoSilverApiIg(file_json)
    dataset_account_day = silver.tb_account_day(extract_date, period, year, day)
    
    #Salvando dataset midia
    silver.output_file(account_day_insights_output, dataset_account_day, input_file_stories)



    #######
    ## Tb Account lifetime Insights
    ########
    file_json = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\bronze\TbLifetimeAccounIgMetrics.json'

    account_lifetime_faixa_genero = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\silver\TbAccountLifeFaixaGenero.csv'
    file_faixa_genero = 'TbAccountLifeFaixaGenero.json'

    account_lifetime_pais = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\silver\TbAccountLifeLocaPais.csv'
    file_lifetime_pais = 'TbAccountLifeLocaPais.json'

    account_lifetime_pais_do_pulico = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\silver\TbAccountLifePaisDosSeguidores.csv'
    file_pais_do_publico = 'TbAccountLifePaisDosSeguidores.json'

    account_lifetime_cidade_do_pulico = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\silver\TbAccountLifeCidadeDosSeguidores.csv'
    file_cidade_publico = 'TbAccountLifeCidadeDosSeguidores.json'

    account_lifetime_segidores_on = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\silver\TbAccountLifeSeguidoresOnline.csv'
    file_seguidores_on = 'TbAccountLifeSeguidoresOnline.json'
    

    silver = TratamentoSilverApiIg(file_json)
    df_faixa_etaria_e_genero, df_localizacao_pais, df_pais_do_publico, df_cidade_seguidores, df_seguidores_online = silver.tb_account_lifetime(extract_date, period, year, day)
    
    #Salvando dataset FAIXA GENERO
    silver.output_file(account_lifetime_faixa_genero, df_faixa_etaria_e_genero, file_faixa_genero)

    #Salvando dataset PAIS
    silver.output_file(account_lifetime_pais, df_localizacao_pais, file_lifetime_pais)

    #Salvando dataset PAIS DO PUBLICO
    silver.output_file(account_lifetime_pais_do_pulico, df_pais_do_publico, file_pais_do_publico)

    #Salvando dataset CIDADE DO PUBLICO
    silver.output_file(account_lifetime_cidade_do_pulico, df_cidade_seguidores, file_cidade_publico)

    #Salvando dataset SEGUIDORES ON
    silver.output_file(account_lifetime_segidores_on, df_seguidores_online, file_seguidores_on)


    ##### 
    # Tb Stories Insights 
    #####

    file_json = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\bronze\TbStoriesInsightsLifetime.json'
    stories_insights_output = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\silver\TbStoriesInsights.csv'
    input_file_stories_insight = 'TbStoriesInsightsLifetime.json'

    silver = TratamentoSilverApiIg(file_json)
    dataset_stories_insight = silver.tb_stories_insights(extract_date, period, year, day)
    
    #Salvando dataset midia
    silver.output_file(stories_insights_output, dataset_stories_insight, input_file_stories_insight)


    ##### 
    # Tb Midia Insights 
    #####

    file_json = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\bronze\TbMediaInsightsLifetime.json'
    midias_insights_output = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\silver\TbMidiasInsights.csv'
    input_file_midia_insight = 'TbMediaInsightsLifetime.json'

    silver = TratamentoSilverApiIg(file_json)
    dataset_midia_insight = silver.tb_midia_insights(extract_date, period, year, day)
    
    #Salvando dataset midia
    silver.output_file(midias_insights_output, dataset_midia_insight, input_file_midia_insight)
