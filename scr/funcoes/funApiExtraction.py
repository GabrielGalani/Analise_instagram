import requests
import json
from scr.funcoes.funGetEnv import get_creds


## Função para coletar as contas de instagram
def get_facebook_pages(access_token):
    url = "https://graph.facebook.com/v19.0/me/accounts"
    params = {
        "access_token": access_token
    }
    response = requests.get(url, params=params)

    # Retornando as contas
    return response.json()

# Função para coletar as contas de instagram proficionais baseadas nos usernames passadsos no segundo parâmetro
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
    
    # Retorna os Ids das contas do instagram
    return ids

# Função para coletar os dados de mídias, ou contas passadas através do field e retornar json base
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
        while base_url:
            page = page + 1
            response = requests.get(base_url, params=params)
            data = response.json()

            # Verificando se a resposta contém erros
            if 'error' in data:
                print(f"Error fetching data for {username}: {data['error']['message']}")
                break
            
            dic = ''
            # Verificando e extraindo os dados de acordo com a estrutura esperada
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
                print(f"--------------- Formato inexperado de dados {username}  ------------------")
                extracted_data = (data)
                aux = True
                print(output_path)
                pass
            
            # Adicionando o username e os dados extraídos à lista de todos os dados
            if aux == False:
                for item in extracted_data:
                    item['username'] = username
                    all_data.append(item)
            else:
                all_data.append(extracted_data)

            # Paginando a api
            try:
                base_url = data[dic]['paging']['next']
            except: 
                base_url = data.get('paging', {}).get('next')
                print(f'Pelo except')

            params = {}

    # Escrevendo todos os dados em um arquivo
    with open(output_path, 'w') as json_file:
        json.dump(all_data, json_file, indent=4)


# Função para coletar os dados das contas
def get_others_accounts_info(access_token, ids, fields, output_path, period, instagram_name):
    all_data = []

    for username, id_ig in ids:
        url = f"https://graph.facebook.com/v19.0/{id_ig}"
        params = {
            "fields": f"business_discovery.username({instagram_name}){','.join(fields)}",
            "access_token": access_token
        }
        
        if period:
            params["period"] = period
        
        base_url = url
        page = 0
        while base_url:
            page = page + 1
            response = requests.get(base_url, params=params)
            data = response.json()

            # Verificando se a resposta contém erros
            if 'error' in data:
                print(f"Error fetching data for {username}: {data['error']['message']}")
                break
            
            dic = ''
            if 'business_discovery' in data:
                if 'data' in data:
                    # Caso onde 'data' está na raiz do JSON
                    extracted_data = data['data']
                    aux = False

                elif 'media' in data['business_discovery']: #and 'data' in data['media']:
                    extracted_data = data['business_discovery']['media']['data']
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
                    print(f"--------------- Formato inexperado de dados {username}  ------------------")
                    extracted_data = data['business_discovery']
                    aux = True
                    print(output_path)
                    pass
            
            # Adicionando o username e os dados extraídos à lista de todos os dados
            if aux == False:
                for item in extracted_data:
                    item['username'] = username
                    all_data.append(item)
            else:
                all_data.append(extracted_data)

            after = data['business_discovery'].get(dic, {}).get('paging', {}).get('cursors', {}).get('after')

            base_url = None

    # Escreva todos os dados em um arquivo
    with open(output_path, 'w') as json_file:
        json.dump(all_data, json_file, indent=4)


if __name__ == "__main__":

    # Exemplo de rodagem
    access_token = get_creds('ACCESS_TOKEN')
    id_ig = get_id(access_token, ['gabgalani',])
    print(id_ig)

    # Roda de 3 em 3 dias

    fields = ['id','username','name','biography', 'followers_count','follows_count', 'profile_picture_url', 'media_count']
    output_path = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\tbAccount_ig.json'
    get_info(access_token, id_ig, fields, output_path, 'lifetime')