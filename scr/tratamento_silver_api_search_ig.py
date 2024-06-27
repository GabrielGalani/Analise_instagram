import sys
import os
sys.path.append(os.getcwd())
from datetime import datetime
from pathlib import Path
from scr.classes.ClassTratamentoSilverSearchApi import TratamentoSilverSearchApiIg


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
file_json = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\concorrentes\bronze\tbAccount.json'
file_output = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\concorrentes\silver\TbCabecalho.csv'
input_file = 'tbAccount_ig.json'


silver = TratamentoSilverSearchApiIg(file_json)
dataset = silver.tb_cabecalho()
silver.output_file(file_output, dataset, input_file)


########
## Tb Midia
########
file_json = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\concorrentes\bronze\TbMedias.json'
file_output_midias = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\concorrentes\silver\TbMedias.csv'
TbCarrossel_output = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\concorrentes\silver\TbCarrossel.csv'
input_file_midias = 'TbMedias.json'
input_file_TbCarrossel = 'TbCarrossel.json'

silver = TratamentoSilverSearchApiIg(file_json)
dataset_midia, dataset_TbCarrossel = silver.tb_midias_cabecalho(extract_date, period, year, day, file_json)

#Salvando dataset midia
silver.output_file(file_output_midias, dataset_midia, input_file)

#Salvando dataset TbCarrossel
silver.output_file(TbCarrossel_output, dataset_TbCarrossel, input_file_TbCarrossel)