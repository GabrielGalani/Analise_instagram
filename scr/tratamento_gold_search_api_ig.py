import sys
import os
sys.path.append(os.getcwd())
from scr.classes.ClassTratamentoGoldSearchApi import TratamentoGoldSearchIg 


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