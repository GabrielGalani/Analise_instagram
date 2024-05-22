from dotenv import load_dotenv
import os

load_dotenv() 


def get_creds(creds):
    client_id = os.getenv(creds)    
    return client_id

if __name__ == "__main__":
    # Exemplo de uso da função
    creds = get_creds('TOKEN_CLIENTE')
    print(creds)