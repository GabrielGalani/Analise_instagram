import sys
import os
sys.path.append(os.getcwd())
from scr.funcoes.funApiExtraction import get_id, get_info
from scr.funcoes.funGetEnv import get_creds
from pathlib import Path


access_token = get_creds('ACCESS_TOKEN')
id_ig = get_id(access_token, ['gabgalani',]) 
BASEFOLDER = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Instagram_data\api\{stage}'
OUTPUT_PATH = BASEFOLDER.format(stage='bronze')
Path(OUTPUT_PATH).mkdir(parents=True, exist_ok=True)


##TbAccount 
fields = ['id','username','name','biography', 'followers_count','follows_count', 'profile_picture_url', 'media_count']
file_name = 'tbAccount_ig.json'
output_tb_account = os.path.join(OUTPUT_PATH, file_name)
get_info(access_token, id_ig, fields, output_tb_account, 'lifetime')

## TbMidias (publicações)
file_name = 'TbMedias.json'
output_tb_midias = os.path.join(OUTPUT_PATH, file_name)
fields_no_insights = [
    'id','media{id, ad_id, comments_count,like_count, media_type, media_url, caption, timestamp, permalink, media_product_type, thumbnail_url, shortcode, children{media_type, media_url}, owner{id, username}}'
]
get_info(access_token, id_ig, fields_no_insights, output_tb_midias, None)


#TbStories (Publicações)
fields_story_no_in = [
    'id, stories{comments_count,like_count, media_type, media_url, caption, timestamp, permalink, media_product_type, thumbnail_url, shortcode, children{media_type, media_url, id}, owner{id, username}}'
]
file_name = 'TbStories.json'
output_path_tb_stories = os.path.join(OUTPUT_PATH, file_name)
get_info(access_token, id_ig, fields_story_no_in, output_path_tb_stories, 'day')


##TbAccountInsights
# Day
day_level_metrics = [
  'id, insights.metric(impressions, reach, follower_count,email_contacts, phone_call_clicks, text_message_clicks,get_directions_clicks,website_clicks,profile_views).period(day)'
]
file_name = 'TbDayAccounIgMetrics.json'
output_path_accounts_day = os.path.join(OUTPUT_PATH, file_name)
get_info(access_token, id_ig, day_level_metrics, output_path_accounts_day, 'day')


# Lifetime
lifetime_level_metrics = [
'id, insights.metric(audience_gender_age, audience_locale, audience_country, audience_city, online_followers).period(lifetime)'
]
file_name = 'TbLifetimeAccounIgMetrics.json'
output_path_account_lifetime = os.path.join(OUTPUT_PATH, file_name)
get_info(access_token, id_ig, lifetime_level_metrics, output_path_account_lifetime, 'lifetime')



## TbMidiasInsights
lifetime_level_metrics = [
'id, media{id, insights.metric(impressions, reach, taps_forward, taps_back, exits, replies, engagement, saved, video_views)}']
file_name = 'TbMediaInsightsLifetime.json'
output_path_midias_insights = os.path.join(OUTPUT_PATH, file_name)
get_info(access_token, id_ig, lifetime_level_metrics, output_path_midias_insights, None)


## TbStoiresInsights
lifetime_level_metrics_stories = [
'id, stories{id, insights.metric(exits, impressions, reach, replies, taps_forward, taps_back)}'
]
file_name = 'TbStoriesInsightsLifetime.json'
output_path_stories_insights = os.path.join(OUTPUT_PATH, file_name)
get_info(access_token, id_ig, lifetime_level_metrics_stories, output_path_stories_insights, None)