import pandas as pd
from pathlib import Path
import os



class TratamentoSlver():
    def __init__(self, folder, output): 
        self.folder = folder
        self.output = output
        pd.set_option('display.max_rows', 500)
        pd.set_option('display.max_columns', 500)
        pd.set_option('display.width', 1000)

    def tratamento_cabecalho_ig(self, file): #file = 'cabecalho_ig.csv'
        file_path = os.path.join(self.folder, file)
        dataset = pd.read_csv(file_path, sep='\t')

        rename = {
        'Account ID': 'id_account',
        'Account Name': 'account_name',
        'Account Photo URL': 'profile_img',
        'Username': 'username',
        'Age & Gender': 'age_gender',
        'Page ID': 'id_page',
        'Page Name': 'page_name',
        'Data': 'extract_date',
        'Mês': 'month',
        'Ano': 'year'
    }
        dataset.rename(columns=rename, inplace=True)

        return dataset, file
    

    def tratamento_audicencia_city(self, file): #Auciencia_city_country.csv
        file_path = os.path.join(self.folder, file)
        dataset = pd.read_csv(file_path, sep='\t')

        rename = {
            'Account ID': 'id_account',
            'Account Name': 'account_name',
            'City': 'city',
            'Region': 'region',
            'Audience Size (Followers)': 'audience_size_followers',
            'Data': 'extract_date',
            'Mês': 'month',
            'Ano': 'year'
        }
        dataset.rename(columns=rename, inplace=True)

        return dataset, file
    

    def tratamento_audicencia_age_genero(self, file): #Audiencia_age_genero.csv
        file_path = os.path.join(self.folder, file)
        dataset = pd.read_csv(file_path, sep='\t')

        rename = {
            'Account ID': 'id_account',
            'Account Name': 'account_name',
            'Age & Gender': 'age_gender',
            'Gender': 'gender',
            'age': 'age',
            'Data': 'extract_date',
            'Mês': 'month',
            'Ano': 'year'
        }
        dataset.rename(columns=rename, inplace=True)

        return dataset, file


    def tratamento_audicencia_alcanca_pais(self, file): #Audiencia_alcancada_country.csv
        file_path = os.path.join(self.folder, file)
        dataset = pd.read_csv(file_path, sep='\t')

        rename = {
            'Account ID': 'id_account',
            'Account Name': 'account_name',
            'City': 'city',
            'Region': 'region',
            'Audience Size (Reached)': 'audience_size_reached',
            'Data': 'extract_date',
            'Mês': 'month',
            'Ano': 'year'
        }
        dataset.rename(columns=rename, inplace=True)

        return dataset, file


    def tratamento_audicencia_cidade(self, file): #Audiencia_city.csv
        file_path = os.path.join(self.folder, file)
        dataset = pd.read_csv(file_path, sep='\t')

        rename = {
            'Account ID': 'id_account',
            'Account Name': 'account_name',
            'Country Code': 'country_code',
            'Country': 'country',
            'Audience Size (Reached)': 'audience_size_reached',
            'Data': 'extract_date',
            'Mês': 'month',
            'Ano': 'year'
        }
        dataset.rename(columns=rename, inplace=True)

        return dataset, file

    def tratamento_audicencia_pais(self, file): #Audiencia_country.csv
        file_path = os.path.join(self.folder, file)
        dataset = pd.read_csv(file_path, sep='\t')

        rename = {
            'Account ID': 'id_account',
            'Account Name': 'account_name',
            'Country Code': 'country_code',
            'Country': 'country',
            'Audience Size (Followers)': 'audience_size_followers',
            'Mês': 'month',
            'Ano': 'year'
        }
        dataset.rename(columns=rename, inplace=True)

        return dataset, file


    def tratamento_day_report(self, file): #day_report.csv
        file_path = os.path.join(self.folder, file)
        dataset = pd.read_csv(file_path, sep='\t')

        rename = {
            'Account ID': 'id_account',
            'Account Name': 'account_name',
            'Day': 'posting_date',
            'Impressions': 'impressions',
            'Reach': 'reach',
            'Profile Views': 'profile_views',
            'New Followers': 'new_followers', 
            'Get Directions': 'get_directions',
            'Calls': 'calls', 
            'Texts': 'texts',
            'Website Clicks': 'websites_clicks',
            'Emails': 'emails',
            'Data': 'extract_date',
            'Mês': 'month',
            'Ano': 'year'
        }
        dataset.rename(columns=rename, inplace=True)

        return dataset, file
    

    def tratamento_medias_report(self, file): #medias_report.csv
        file_path = os.path.join(self.folder, file)
        dataset = pd.read_csv(file_path, sep='\t')

        rename = {
            'Account ID': 'id_account',
            'Account Media Count': 'account_media_count',
            'Media ID': 'id_media',
            'Follows': 'follows',
            'Impressions': 'impressions',
            'Media Caption': 'media_caption',
            'Media Comments': 'media_coments',
            'Media Likes': 'media_likes',
            'Media Permalink': 'media_permalink', 
            'Avg Reel Watch Time': 'avarage_reel_watch_time',
            'Media Plays': 'media_plays',
            'Media Post Time': 'posting_date',
            'Media Product Type': 'media_product_type',
            'Media Shares': 'media_shares',
            'Media Short Code': 'media_short_code',
            'Media Total Interactions': 'media_total_interactions',
            'Media Type': 'media_type',
            'Media URL': 'media_url',
            'Reach': 'reach',
            'Saves': 'saves',
            'Video Views': 'video_saves',
            'Total Watch Time': 'total_watch_time',
            'Profile Visits': 'profile_visits',
            'Profile Activity': 'profile_activity',
            'Page ID': 'page_id',
            'Page Name': 'page_name', 
            'Data': 'extract_date',
            'Mês': 'month',
            'Ano': 'year'
        }
        dataset.rename(columns=rename, inplace=True)

        return dataset, file


    def tratamento_story_report(self, file): #story_report.csv
        file_path = os.path.join(self.folder, file)
        dataset = pd.read_csv(file_path, sep='\t')

        rename = {
            'Account ID': 'id_account',
            'Account Name': 'account_name',
            'Media Type': 'media_type',
            'Media URL': 'media_url',
            'Media Permalink': 'media_permalink',
            'Media Caption': 'media_caption',
            'Media Comments': 'media_coments',
            'Media Likes': 'media_likes',
            'Impressions': 'impressions',
            'Reach': 'reach',
            'Exits': 'exits',
            'Replies': 'replies',
            'Taps Forward': 'taps_forward',
            'Taps Back': 'taps_back',
            'Navigations': 'navigations',
            'Swipes Forward': 'swipes_forward',
            'Media Short Code': 'media_short_code',
            'Media ID': 'media_id',
            'Media Post Time': 'posting_date',
            'Media Product Type': 'media_product_type',
            'Data': 'extract_date',
            'Mês': 'month',
            'Ano': 'year'
        }
        dataset.rename(columns=rename, inplace=True)

        return dataset, file


    def tratamento_weekly_report(self, file): #weekly_report.csv
        file_path = os.path.join(self.folder, file)
        dataset = pd.read_csv(file_path, sep='\t')

        rename = {
            'Account ID': 'id_account',
            'Account Name': 'account_name',
            'Week': 'week',
            'Impressions': 'impresssions',
            'Reach': 'reach',
            'Data': 'extract_date',
            'Mês': 'month',
            'Ano': 'year'
        }
        dataset.rename(columns=rename, inplace=True)

        return dataset, file



    def tratamento_ig_total_accounts(self, file): #IG_total_accounts.csv
        file_path = os.path.join(self.folder, file)
        dataset = pd.read_csv(file_path, sep='\t')

        rename = {
            'Account ID': 'id_account',
            'Account Name': 'account_name',
            'Username': 'username',
            'Total Followers': 'total_followers',
            'Total Following': 'total_following',
            'Website': 'website',
            'Account Media Count': 'account_media_count',
            'Data': 'extract_date',
            'Mês': 'month',
            'Ano': 'year'
        }
        dataset.rename(columns=rename, inplace=True)

        return dataset, file

    def execute(self):

        for file in os.listdir(self.folder):
            if file == 'cabecalho_ig.csv':
                dataset, file_output = self.tratamento_cabecalho_ig(file)
                Path(self.output).mkdir(parents=True, exist_ok=True)
                output_path = os.path.join(self.output, file_output)

                dataset.to_csv(output_path, sep='\t')

            if file == 'Auciencia_city_country.csv':
                dataset, file_output = self.tratamento_audicencia_city(file)
                Path(self.output).mkdir(parents=True, exist_ok=True)
                output_path = os.path.join(self.output, file_output)

                dataset.to_csv(output_path, sep='\t')

            if file == 'Audiencia_age_genero.csv':
                dataset, file_output = self.tratamento_audicencia_age_genero(file)
                Path(self.output).mkdir(parents=True, exist_ok=True)
                output_path = os.path.join(self.output, file_output)

                dataset.to_csv(output_path, sep='\t')
            

            if file == 'Audiencia_alcancada_country.csv':
                dataset, file_output = self.tratamento_audicencia_alcanca_pais(file)
                Path(self.output).mkdir(parents=True, exist_ok=True)
                output_path = os.path.join(self.output, file_output)

                dataset.to_csv(output_path, sep='\t')
            

            if file == 'Audiencia_city.csv':
                dataset, file_output = self.tratamento_audicencia_cidade(file)
                Path(self.output).mkdir(parents=True, exist_ok=True)
                output_path = os.path.join(self.output, file_output)

                dataset.to_csv(output_path, sep='\t')

            if file == 'day_report.csv':
                dataset, file_output = self.tratamento_day_report(file)
                Path(self.output).mkdir(parents=True, exist_ok=True)
                output_path = os.path.join(self.output, file_output)

                dataset.to_csv(output_path, sep='\t')
            
            if file == 'medias_report.csv':
                dataset, file_output = self.tratamento_medias_report(file)
                Path(self.output).mkdir(parents=True, exist_ok=True)
                output_path = os.path.join(self.output, file_output)

                dataset.to_csv(output_path, sep='\t')
            

            if file == 'story_report.csv':
                dataset, file_output = self.tratamento_story_report(file)
                Path(self.output).mkdir(parents=True, exist_ok=True)
                output_path = os.path.join(self.output, file_output)

                dataset.to_csv(output_path, sep='\t')
            

            if file == 'weekly_report.csv':
                dataset, file_output = self.tratamento_weekly_report(file)
                Path(self.output).mkdir(parents=True, exist_ok=True)
                output_path = os.path.join(self.output, file_output)

                dataset.to_csv(output_path, sep='\t')
            

            if file == 'Audiencia_country.csv':
                dataset, file_output = self.tratamento_audicencia_pais(file)
                Path(self.output).mkdir(parents=True, exist_ok=True)
                output_path = os.path.join(self.output, file_output)

                dataset.to_csv(output_path, sep='\t')
            

            if file == 'IG_total_accounts.csv':
                dataset, file_output = self.tratamento_ig_total_accounts(file)
                Path(self.output).mkdir(parents=True, exist_ok=True)
                output_path = os.path.join(self.output, file_output)

                dataset.to_csv(output_path, sep='\t')
            
            else:
                pass

if __name__ == '__main__':
    file_path = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\data'
    output = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\data\SILVER'

    tratamento = TratamentoSlver(file_path, output)
    ti = tratamento.execute()