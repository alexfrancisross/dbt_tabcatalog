from datetime import datetime
import json
import yaml
from yaml.loader import SafeLoader
from operator import itemgetter
from collections import defaultdict
import requests
import dbt_tabcatalog
from itertools import groupby
CONFIG='settings.yml'
TABLEAU_API_VERSION='3.17'

#read project yaml file
class app_settings:
    try:
        with open(CONFIG) as f:
            data = yaml.load(f, Loader=SafeLoader)

            dbt_token = data['DBT']['DBT_PAT']
            dbt_cloud_api = data['DBT']['DBT_CLOUD_API']
            dbt_metadata_api = data['DBT']['DBT_METADATA_API']
            dbt_meta_certification_flag = data['DBT']['DBT_META_CERTIFICATION_FLAG']
            dbt_project_filter = data['DBT']['DBT_PROJECT_FILTER']
            dbt_generate_exposures = data['DBT']['DBT_GENERATE_EXPOSURES']
            dbt_exposures_environment_filter = data['DBT']['DBT_EXPOSURES_ENVIRONMENT_FILTER']

            tableau_token = data['TABLEAU']['TABLEAU_TOKEN']
            tableau_token_name = data['TABLEAU']['TABLEAU_TOKEN_NAME']
            tableau_token = data['TABLEAU']['TABLEAU_TOKEN']
            tableau_site = data['TABLEAU']['TABLEAU_SITE']
            tableau_server = data['TABLEAU']['TABLEAU_SERVER']
            tableau_certification_note = data['TABLEAU']['TABLEAU_CERTIFICATION_NOTE']
            tableau_dq_warning_isSevere = data['TABLEAU']['TABLEAU_DQ_WARNING_IS_SEVERE']

            database_type_filter = data['DATABASE']['DATABASE_TYPE_FILTER']
            database_name_filter = data['DATABASE']['DATABASE_NAME_FILTER']
            database_account_filter = data['DATABASE']['DATABASE_ACCOUNT_FILTER']

    except Exception as e:
        print("failed to read yaml file " + str(e))

def dbt_develop(dbt_cloud_api, dbt_token):

    url = dbt_cloud_api + '/60964/environments/132956/develop/start/?reclone=false'
    payload = {}
    headers = {
        'Content-Type': 'appication/json',
        'Authorization': 'Token ' + dbt_token
    }
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        response_json = json.loads(response.text)
        if 'errors' in response_json.keys():
            raise Exception(response_json['errors'][0]['message'])
    except Exception as e:
        print('Error creting dbt Cloud development session ' + str(e))
    #print('updated table certification for tableau table: ' + full_table_name)
    return

#MAIN PROGRAM
settings = app_settings()
dbt_develop(settings.dbt_cloud_api, settings.dbt_token)