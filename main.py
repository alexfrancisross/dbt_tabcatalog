import requests
import yaml
import json
import pandas as pd
from yaml.loader import SafeLoader
from operator import itemgetter
from collections import defaultdict
CONFIG='settings.yml'
TABLEAU_API_VERSION='3.17'

def dbt_get_account_id(dbt_cloud_api, dbt_token):
    url = dbt_cloud_api
    payload = {}
    headers = {
        'Content-Type': 'appication/json',
        'Authorization': 'Token ' + dbt_token
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    response_json = json.loads(response.text)
    dbt_account_id = response_json['data'][0]['id']
    return (dbt_account_id)

def dbt_get_jobs(dbt_account_id, dbt_cloud_api, dbt_token):
    url = dbt_cloud_api + str(dbt_account_id) +"/jobs"
    payload={}
    headers = {
      'Content-Type': 'appication/json',
      'Authorization': 'Token '+ dbt_token
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    response_json = json.loads(response.text)
    dbt_jobs = response_json['data']
    return(dbt_jobs)

def dbt_get_models_for_job(dbt_metadata_api, dbt_token, job_id):

    url = dbt_metadata_api
    #job_id = 146628
    payload = '{\"query\":\"{\\n  models(jobId: ' + str(job_id) + ') {\\n    uniqueId\\n    runId\\n    accountId\\n    projectId\\n    environmentId\\n    jobId\\n    executionTime\\n    status\\n    executeCompletedAt\\n    database\\n    schema\\n\\n   name\\n\\n    description\\n\\n  columns {\\n        name\\n        description\\n    }\\n\\n  }\\n}\",\"variables\":{}}'
    headers = {
      'Authorization': 'Token ' + dbt_token,
      'Content-Type': 'application/json'
    }
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        response_json = json.loads(response.text)
        print(response_json)
        dbt_models = response_json['data']['models']
        return(dbt_models)
    except Exception as e:
        print(e)
        return([])


def authenticate_tableau(tableau_server, tableau_site_name, tableau_token_name, tableau_token):
    url = tableau_server + "/api/" + TABLEAU_API_VERSION + "/auth/signin"

    payload = json.dumps({
        "credentials": {
            "personalAccessTokenName": tableau_token_name,
            "personalAccessTokenSecret": tableau_token,
            "site": {
                "contentUrl": tableau_site_name
            }
        }
    })
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    response_json = json.loads(response.text)
    token = response_json['credentials']['token']
    tableau_creds = response_json['credentials']
    return (tableau_creds)

def tableau_get_databases(tableau_server, database_type, database_name, tableau_creds):
    mdapi_query = '''query get_datasources {
          databases(filter: {connectionType: "''' + database_type + '''", name: "''' + database_name + '''"}) {
            name
            id
            tables {
              name
              id
              luid
            }
          }
        }'''

    auth_headers = {'accept': 'application/json', 'content-type': 'application/json',
                                   'x-tableau-auth': tableau_creds['token']}
    metadata_query = requests.post(tableau_server + '/api/metadata/graphql', headers=auth_headers, verify=True,
                                   json={"query": mdapi_query})
    tableau_databases = json.loads(metadata_query.text)['data']['databases']

    return (tableau_databases)


def get_tableau_columns(tableau_server, table_luid, tableau_creds):
    site_id=tableau_creds['site']['id']
    get_columns_url = tableau_server + "/api/" + TABLEAU_API_VERSION + "/sites/" + site_id + "/tables/" + table_luid + '/columns'

    payload = ""
    headers = {
        'X-Tableau-Auth': tableau_creds['token'],
        'Content-Type': 'appication/json',
        'Accept': 'application/json'
    }

    response = requests.request("GET", get_columns_url, headers=headers, data=payload)
    tableau_columns = json.loads(response.text)['columns']['column']
    return (tableau_columns)

def add_comments_to_tab_table(tableau_columns, dbt_columns):
    join_result = tableau_columns.merge(dbt_columns, how='inner',left_on="column_name",right_on='name')

    return(join_result)

def publish_tableau_column_descriptions(tableau_server, dbt_columns, tableau_columns, tableau_creds):
    d = defaultdict(dict)
    for l in (tableau_columns, dbt_columns):
        for elem in l:
            d[elem['name']].update(elem)
    merged_columns = sorted(d.values(), key=itemgetter("name"))
    print(merged_columns)

    for column in merged_columns:
        url = tableau_server + "/api/" + TABLEAU_API_VERSION + "/sites/" + tableau_creds['site']['id'] + "/tables/" + column['parentTableId'] + "/columns/" + column['id']
        payload = "<tsRequest>\n  <column description=\"" + column['description'] + " \">\n  </column>\n</tsRequest>"
        headers = {
            'X-Tableau-Auth': tableau_creds['token'],
            'Content-Type': 'text/plain'
        }
        column_description_response = requests.request("PUT", url, headers=headers, data=payload)
        column_description_response_code = column_description_response.text
        print (column_description_response_code)
        return (column_description_response_code)

def publish_tableau_column_tags(tableau_server, tableau_columns, tag, tableau_creds):
    headers = {
        'X-Tableau-Auth': tableau_creds['token'],
        'Content-Type': 'text/plain'
    }
    for tableau_column in tableau_columns:
        url = tableau_server + "/api/" + TABLEAU_API_VERSION + "/sites/" + tableau_creds['site']['id'] + "/columns/" + tableau_column['id'] + "/tags"
        payload = "<tsRequest>\n  <tags>\n <tag label=\"" + tag + "\"/>\n  </tags>\n</tsRequest>"

        column_tags_response = requests.request("PUT", url, headers=headers, data=payload)
        column_tags_response_code = column_tags_response.text
        print(column_tags_response_code)

    return (column_tags_response_code)

def publish_tableau_table_tags(tableau_server, tableau_table, tag, tableau_creds):
    headers = {
        'X-Tableau-Auth': tableau_creds['token'],
        'Content-Type': 'text/plain'
    }
    url = tableau_server + "/api/" + TABLEAU_API_VERSION + "/sites/" + tableau_creds['site']['id'] + "/tables/" + tableau_table['luid'] + "/tags"
    payload = "<tsRequest>\n  <tags>\n <tag label=\"" + tag + "\"/>\n  </tags>\n</tsRequest>"

    table_tags_response = requests.request("PUT", url, headers=headers, data=payload)
    table_tags_response_code = table_tags_response.text
    print(table_tags_response_code)

    return (table_tags_response_code)


def publish_tableau_table_description(tableau_server,table_id, description_text,tableau_creds):
    url = tableau_server+"/api/" + TABLEAU_API_VERSION + "/sites/"+ tableau_creds['site']['id']+"/tables/" + table_id
    payload = '<tsRequest>\n  <table description=\"' + description_text +'\">\n  </table>\n</tsRequest>'
    headers = {
        'X-Tableau-Auth': tableau_creds['token'],
      'Content-Type': 'text/plain'
    }

    table_description_response = requests.request("PUT", url, headers=headers, data=payload)

    table_description_response_code = table_description_response.text
    print(table_description_response_code)
    return(table_description_response_code)

def make_table_description(dbt_model):

    ###Example description
    #This table has basic information about orders, as well as some derived facts based on payments
    #"dbt lineage":https://cloud.getdbt.com/accounts/60964/jobs/146628/docs/#!/model/model.jaffle_shop_metrics.orders?g_v=1&g_i=%2Borders%2B
    #"dbt docs":https://cloud.getdbt.com/accounts/60964/jobs/146628/docs/#!/model/model.jaffle_shop_metrics.orders#details

    dbt_cloud_base_url = 'https://cloud.getdbt.com/accounts/'+ str(dbt_model['accountId']) +'/jobs/' + str(dbt_model['jobId']) + '/docs/#!/model/' + dbt_model['uniqueId']
    line1 = dbt_model['description']
    line2 = '\"dbt lineage\":https://prod-uk-a.online.tableau.com'
            #+dbt_cloud_base_url + '?g_v=1&g_i=%2Borders%2B'
    line3 = '"dbt docs":' + dbt_cloud_base_url + '#details'

    table_description=line1 + " " + line2

    return(table_description)


def read_yaml():
    try:
        with open(CONFIG) as f:
            data = yaml.load(f, Loader=SafeLoader)
        return data
    except Exception as e:
        print("failed to read yaml file " + str(e))

#MAIN PROGRAM
settings = read_yaml()
dbt_token = settings['DBT']['DBT_PAT']
dbt_cloud_api = settings['DBT']['DBT_CLOUD_API']
dbt_metadata_api = settings['DBT']['DBT_METADATA_API']

tableau_token = settings['TABLEAU']['TABLEAU_TOKEN']
tableau_token_name = settings['TABLEAU']['TABLEAU_TOKEN_NAME']
tableau_token = settings['TABLEAU']['TABLEAU_TOKEN']
tableau_site =  settings['TABLEAU']['TABLEAU_SITE']
tableau_server =  settings['TABLEAU']['TABLEAU_SERVER']
database_type =  settings['TABLEAU']['DATABASE_TYPE']
database_name =  settings['TABLEAU']['DATABASE_NAME']

dbt_account_id = dbt_get_account_id(dbt_cloud_api, dbt_token)
print(dbt_account_id)

dbt_jobs = dbt_get_jobs(dbt_account_id,dbt_cloud_api,dbt_token)
print (dbt_jobs)

tableau_creds = authenticate_tableau(tableau_server, tableau_site, tableau_token_name, tableau_token)
print(tableau_creds)

tableau_databases = tableau_get_databases(tableau_server, database_type, database_name, tableau_creds)
print(tableau_databases)

for dbt_job in dbt_jobs:
    dbt_models = dbt_get_models_for_job(dbt_metadata_api, dbt_token, dbt_job['id'])
    #print(dbt_models)

    for dbt_model in dbt_models:
        if dbt_model['database']==database_name:

            for tableau_database in tableau_databases:
                for tableau_table in tableau_database['tables']:
                    #print(tableau_table)

                    if dbt_model['name'].upper() == tableau_table['name'].upper(): #if dbt table name in model matched table in Tableau Data Source
                        #print('update tableau catalog')
                        #print(dbt_model['uniqueId'])
                        #print(dbt_job['id'])
                        tableau_columns = get_tableau_columns(tableau_server,tableau_table['luid'], tableau_creds)
                        #print(tableau_columns)
                        table_description=make_table_description(dbt_model)
                        publish_tableau_table_description(tableau_server,tableau_table['luid'],table_description,tableau_creds)
                        publish_tableau_table_tags(tableau_server, tableau_table, dbt_model['uniqueId'], tableau_creds)
                        publish_tableau_column_descriptions(tableau_server, dbt_model['columns'], tableau_columns, tableau_creds)
                        publish_tableau_column_tags(tableau_server, tableau_columns, dbt_model['uniqueId'], tableau_creds)


