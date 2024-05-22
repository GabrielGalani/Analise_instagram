import requests
import json
from funGetEnv import get_creds
import csv
import pandas as pd
from io import StringIO


## Função para coletar as contas de instagram
def get_facebook_pages(access_token):
    url = "https://graph.facebook.com/v19.0/me/accounts"
    params = {
        "access_token": access_token
    }
    response = requests.get(url, params=params)
    return response.json()

def get_id(access_token, usernames_required):
    pages_data = get_facebook_pages(access_token)
    ids = []
    usernames = []
    for page in pages_data['data']:
        page_id = page['id']
        url = f"https://graph.facebook.com/v19.0/{page_id}"
        params = {
            "fields": "instagram_business_account",
            "access_token": access_token
        }
        response = requests.get(url, params=params)
        if "instagram_business_account" in response.json(): 
            ig_business = response.json()
            id_ig = ig_business['instagram_business_account']['id']

            url = f"https://graph.facebook.com/v19.0/{id_ig}"
            params = {
                "fields": "username",
                "access_token": access_token
            }
            response = requests.get(url, params=params)

            username = response.json()['username']
            if username in usernames_required:
                ids.append((username, id_ig))
    
    return ids


def get_info(access_token, ids, fields, output_path, period):
    all_data = []

    for username, id_ig in ids:
        url = f"https://graph.facebook.com/v19.0/{id_ig}"
        params = {
            "fields": ",".join(fields),
            "access_token": access_token
        }
        
        if period:
            params["period"] = period
        
        base_url = url
        page = 0
        print(f'iniciei nesse: {base_url}')
        while base_url:
            print(f'in: {base_url}')
            page = page + 1
            response = requests.get(base_url, params=params)
            data = response.json()

            # Verifique se a resposta contém erros
            if 'error' in data:
                print(f"Error fetching data for {username}: {data['error']['message']}")
                break
            
            dic = ''
            # Verifique e extraia os dados de acordo com a estrutura esperada
            if 'data' in data:
                # Caso onde 'data' está na raiz do JSON
                extracted_data = data['data']
                aux = False

            elif 'media' in data: #and 'data' in data['media']:
                extracted_data = data['media']['data']
                dic = 'media'
                aux = False
            
            elif 'insights' in data:
                extracted_data = data['insights']['data']
                dic = 'insights'
                aux = False

            elif 'stories' in data:
                extracted_data = data['stories']['data']
                dic = 'stories'
                aux = False

            else:                
                print(f"--------------- Unexpected data format for {username}  ------------------")
                extracted_data = (data)
                aux = True
                print(output_path)
                pass
            
            # Adicione o username e os dados extraídos à lista de todos os dados
            if aux == False:
                for item in extracted_data:
                    item['username'] = username
                    all_data.append(item)
            else:
                all_data.append(extracted_data)

            try:
                base_url = data[dic]['paging']['next']
            except: 
                base_url = data.get('paging', {}).get('next')
                print(f'Pelo except')


            print(base_url)
            print(f'next: {base_url}')
            # try:
                # base_url = data.get('media', {}).get('paging', {}).get('next') if 'media' in data else None
            # except:
            #     pass
            
            # try:
            #     base_url = data.get('insights', {}).get('paging', {}).get('next') if 'insights' in data else None
            # except:
            #     pass

            # try:
            #     base_url = data.get('stories', {}).get('paging', {}).get('next') if 'stories' in data else None
            # except:
            #     pass



            print(f'passei a {page} vez')

            # Após a primeira solicitação, remova os parâmetros para evitar duplicidade
            params = {}

    # Escreva todos os dados em um arquivo
    with open(output_path, 'w') as json_file:
        json.dump(all_data, json_file, indent=4)



# # Função para extrair dados da api para json
# def get_info(access_token, ids, fields, output_path, period):
#     all_data = []

#     for username, id_ig in ids:
#         url = f"https://graph.facebook.com/v19.0/{id_ig}"

#         if period == None:
#             params = {
#                 "fields": ",".join(fields),
#                 "access_token": access_token
#             }
#         else: 
#             params = {
#                 "fields": ",".join(fields),
#                 "period": period,
#                 "access_token": access_token
#             }
#         base_url = url
#         page = 0
#         while base_url:
#             page += 1
#             response = requests.get(base_url, params=params)
#             data = response.json()

#             # Verifique se a resposta contém erros
#             if 'error' in data:
#                 print(f"Error fetching data for {username}: {data['error']['message']}")
#                 break

#             # Adicione os dados da página atual à lista de todos os dados
#             if 'data' in data:
#                 for item in data['data']:
#                     item['username'] = username  # Adicione o username aos dados
#                     all_data.append(item)
#             else:
#                 data['username'] = username  # Adicione o username aos dados
#                 all_data.append(data)

#             # Verifique se há uma próxima página
#             base_url = data.get('media', {}) .get('paging', {}).get('next', None)
#             print(f'passei a {page} vez')

#             # Após a primeira solicitação, remova os parâmetros para evitar duplicidade
#             params = {}

    
#     # Escreva todos os dados em um arquivo
#     with open(output_path, 'w') as json_file:
#         json.dump(all_data, json_file, indent=4)


if __name__ == "__main__":

    access_token = get_creds('ACCESS_TOKEN')
    id_ig = get_id(access_token, ['gabgalani', 'bodemeier.digital'])
    print(id_ig)

    # Roda de 3 em 3 dias

    fields = ['id','username','name','biography', 'followers_count','follows_count', 'profile_picture_url', 'media_count']
    output_path = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\tbAccount_ig.json'
    get_info(access_token, id_ig, fields, output_path, 'lifetime')