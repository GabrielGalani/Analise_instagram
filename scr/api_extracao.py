import requests

def get_facebook_pages(access_token):
    url = "https://graph.facebook.com/v19.0/me/accounts"
    params = {
        "access_token": access_token
    }
    response = requests.get(url, params=params)
    return response.json()

def get_instagram_business_account(page_id, access_token):
    url = f"https://graph.facebook.com/v19.0/{page_id}"
    params = {
        "fields": "instagram_business_account",
        "access_token": access_token
    }
    response = requests.get(url, params=params)
    return response.json()

def get_instagram_account_details(instagram_account_id, access_token):
    url = f"https://graph.facebook.com/v19.0/{instagram_account_id}"
    params = {
        "fields": "username",
        "access_token": access_token
    }
    response = requests.get(url, params=params)
    return response.json()

def find_instagram_account_by_username(access_token, target_username):
    pages_data = get_facebook_pages(access_token)
    
    for page in pages_data['data']:
        page_id = page['id']
        business_account_data = get_instagram_business_account(page_id, access_token)
        
        if 'instagram_business_account' in business_account_data:
            instagram_account_id = business_account_data['instagram_business_account']['id']

            instagram_details = get_instagram_account_details(instagram_account_id, access_token)
            
            if instagram_details['username'] == target_username:
                return instagram_details
    
    return None



# Exemplo de uso:
access_token = "EAAExXGRZCoZAcBOzbVwpPxOCrLj87JPURk6YDq5slAquRwYZBj1jglaamiNHCnoIaU3AxANzTqoyY4INAJdIK6aFBxw1Biyf7LDD0bvilhzNZBKjuvr6MUPzxAHBhQ5mF80KdRxyJb2YMJ984u4eVoSpmkJvzmDaY9iQn2YNm1E8aGYLafRvdItDVQwSAbRHaRgHxDyHLqosAD0zMgZDZD"
target_username = "gabgalani"

instagram_account = find_instagram_account_by_username(access_token, target_username)
if instagram_account:
    print(f"Username: {instagram_account['username']}, ID: {instagram_account['id']}")
else:
    print("Conta do Instagram não encontrada.")
