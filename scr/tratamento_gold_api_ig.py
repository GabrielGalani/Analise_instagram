import sys
import os
sys.path.append(os.getcwd())
from scr.classes.ClassTratamentoGoldIg import TratamentoGoldIg 


## Carregamento tabela cabecalho
folder = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\silver'
output = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\gold'

#A Classe vai receber a pasta onde estão os arquivos que irão subir para o banco e a pasta onde os arquivos que subiram vão sair
tratamento_gold = TratamentoGoldIg(folder, output)

#Todos os metodos da classe
tratamento_gold.tratamento_tb_account()
tratamento_gold.tratamento_dim_descricao_insights()
tratamento_gold.tratamento_dim_midias()
tratamento_gold.tratamento_stories_dim_midias()
tratamento_gold.tratamento_fato_midias()
tratamento_gold.tratamento_dim_carrossel()
tratamento_gold.tratamento_fato_account_day_insights()
tratamento_gold.tratamento_fato_midias_insights()
tratamento_gold.tratamento_fato_account_lifetime_insights()