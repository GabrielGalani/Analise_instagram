import sys
import os
sys.path.append(os.getcwd())
from datetime import datetime
from scr.classes.ClassTratamentoSilverApiIg import TratamentoSilverApiIg


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
TbCarrossel_output = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\silver\TbCarrossel.csv'
input_file_midias = 'TbMedias.json'
input_file_TbCarrossel = 'TbCarrossel.json'

silver = TratamentoSilverApiIg(file_json)
dataset_midia, dataset_TbCarrossel = silver.tb_midias_cabecalho(extract_date, period, year, day, file_json)

#Salvando dataset midia
silver.output_file(file_output_midias, dataset_midia, input_file)

#Salvando dataset TbCarrossel
silver.output_file(TbCarrossel_output, dataset_TbCarrossel, input_file_TbCarrossel)



########
## Tb Stories
########
try:
    file_json = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\bronze\TbStories.json'
    stories_output = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\silver\TbStories.csv'
    input_file_stories = 'TbStories.json'

    silver = TratamentoSilverApiIg(file_json)
    dataset_stories = silver.tb_stories(extract_date, period, year, day, file_json)

    #Salvando dataset midia
    silver.output_file(stories_output, dataset_stories, input_file_stories)
except Exception as e: 
    print(f'Não tratei silver stories porque {e}')


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
try:
    file_json = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\bronze\TbStoriesInsightsLifetime.json'
    stories_insights_output = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\silver\TbStoriesInsights.csv'
    input_file_stories_insight = 'TbStoriesInsightsLifetime.json'

    silver = TratamentoSilverApiIg(file_json)
    dataset_stories_insight = silver.tb_stories_insights(extract_date, period, year, day, file_json)

    #Salvando dataset midia
    silver.output_file(stories_insights_output, dataset_stories_insight, input_file_stories_insight)
except Exception as e: 
    print(f'Não tratei silver stories Insights porque {e}')

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
