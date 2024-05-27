import sys
import os
sys.path.append(os.getcwd())
from scr.funcoes.funApiExtraction import get_id, get_others_accounts_info
from scr.funcoes.funGetEnv import get_creds
from pathlib import Path


access_token = get_creds('ACCESS_TOKEN')
id_ig = get_id(access_token, ['gabgalani',]) 
BASEFOLDER = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\concorrentes\{stage}'
OUTPUT_PATH = BASEFOLDER.format(stage='bronze')
Path(OUTPUT_PATH).mkdir(parents=True, exist_ok=True)


## TbMidias (publicações)
file_name = 'TbMedias.json'
output_tb_midias = os.path.join(OUTPUT_PATH, file_name)
instagram_name = 'raentretenimento'
fields_no_insights = [
  '{media{comments_count,like_count, media_type, media_url, caption, timestamp, permalink, media_product_type,thumbnail_url, children{media_type, media_url}, owner{id, username}}}'
]
get_others_accounts_info(access_token, id_ig, fields_no_insights, output_tb_midias, None, instagram_name)


# TbAccount (cabeçalho conta)
# {id,username,name,biography, followers_count,follows_count, profile_picture_url, media_count}
file_name = 'TbAccount.json'
output_tb_midias = os.path.join(OUTPUT_PATH, file_name)
instagram_name = 'raentretenimento'
fields_no_insights = [
  '{id,username,name,biography, followers_count,follows_count, profile_picture_url, media_count}'
]
get_others_accounts_info(access_token, id_ig, fields_no_insights, output_tb_midias, None, instagram_name)
