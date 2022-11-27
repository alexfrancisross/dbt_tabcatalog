from datetime import datetime
import json
import yaml
from yaml.loader import SafeLoader
from operator import itemgetter
from collections import defaultdict
import requests
CONFIG='settings.yml'
TABLEAU_API_VERSION='3.17'

def xmlesc(txt):
    txt = txt.replace("&", "&amp;")
    txt = txt.replace("<", "&lt;")
    txt = txt.replace(">", "&gt;")
    txt = txt.replace('"', "&quot;")
    txt = txt.replace("'", "&apos;")
    txt = txt.replace("\n", "&#xA;")
    return txt

def get_full_table_name(merged_table):
    full_table_name = '[' + merged_table['database'] + '].[' + merged_table['schema'] + '].[' + merged_table[
        'name'] + ']'
    return full_table_name

def dbt_get_account_id(dbt_cloud_api, dbt_token):
    print('getting dbt Cloud account id...')
    url = dbt_cloud_api
    payload = {}
    headers = {
        'Content-Type': 'appication/json',
        'Authorization': 'Token ' + dbt_token
    }
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        response_json = json.loads(response.text)
        dbt_account_id = response_json['data'][0]['id']
        print('dbt Cloud account Id: ' + str(dbt_account_id))
    except Exception as e:
        print('Error getting account id from dbt Cloud: ' + str(e))
    return dbt_account_id

def dbt_get_jobs(dbt_account_id, dbt_cloud_api, dbt_token):
    print('getting dbt jobs for account id ' + str(dbt_account_id) + '...')
    url = dbt_cloud_api + str(dbt_account_id) +"/jobs"
    payload={}
    headers = {
      'Content-Type': 'appication/json',
      'Authorization': 'Token '+ dbt_token
    }
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        response_json = json.loads(response.text)
        dbt_jobs = response_json['data']
        print('retrieved: ' + str(len(dbt_jobs)) + ' dbt jobs')
    except Exception as e:
        print('Error getting account id from dbt Cloud: ' + str(e))
    return dbt_jobs

def dbt_get_models_for_job(dbt_metadata_api, dbt_token, job_id):
    print('getting dbt models for jobId: ' + str(job_id) + '...')
    url = dbt_metadata_api
    dbt_models=[]
    payload = '{\"query\":\"{\\n  models(jobId: ' + str(job_id) + ') {\\n    uniqueId\\n    packageName\\n    runId\\n    accountId\\n    projectId\\n    environmentId\\n    jobId\\n    executionTime\\n    status\\n    executeCompletedAt\\n    database\\n    schema\\n\\n   name\\n\\n  description\\n\\n meta\\n\\n  stats {\\n        id\\n        value\\n    }\\n\\n   columns {\\n        name\\n        description\\n    }\\n\\n  }\\n}\",\"variables\":{}}'
    headers = {
      'Authorization': 'Token ' + dbt_token,
      'Content-Type': 'application/json'
    }
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        response_json = json.loads(response.text)
        dbt_models = response_json['data']['models']
        print('retreived ' + str(len(dbt_models)) + ' dbt models for jobId: ' + str(job_id))
    except Exception as e:
        print('Error getting dbt models for job id: ' + str(job_id) + ' ' + str(e))
    return dbt_models

def authenticate_tableau(tableau_server, tableau_site_name, tableau_token_name, tableau_token):
    url = tableau_server + "/api/" + TABLEAU_API_VERSION + "/auth/signin"
    print('authenticating with tableau server url: ' + url + '...')
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
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        response_json = json.loads(response.text)
        tableau_creds = response_json['credentials']
        print('tableau user id: ' + str(tableau_creds['user']['id']))
    except Exception as e:
        print('Error authenticating with Tableau. Servername : ' + tableau_server + ' Site : ' + tableau_site_name + ' ' +  str(e))
    return tableau_creds

def tableau_get_databases(tableau_server, database_type, tableau_creds):
    print('getting tableau databases with database type: ' + database_type + '...')
    mdapi_query = '''query get_databases {
          databases(filter: {connectionType: "''' + database_type + '''"}) {
            name
            id
            tables {
              name
              schema
              id
              luid
            }
          }
        }'''
    auth_headers = {'accept': 'application/json', 'content-type': 'application/json',
                                   'x-tableau-auth': tableau_creds['token']}
    try:
        metadata_query = requests.post(tableau_server + '/api/metadata/graphql', headers=auth_headers, verify=True,
                                       json={"query": mdapi_query})
        tableau_databases = json.loads(metadata_query.text)['data']['databases']
    except Exception as e:
        print('Error getting databases from Tableau metadata API ' + str(e))
    print('retrieved ' + str(len(tableau_databases)) + ' tableau databases')
    return tableau_databases

def tableau_get_databaseServers(tableau_server, database_type, tableau_creds):
    print('getting database server list from Tableau metadata API with database type: ' + database_type + '...')
    mdapi_query = '''query get_databaseServers {
          databaseServers(filter: {connectionType: "''' + database_type + '''"}) {
            name
            id
            hostName
          }
        }'''
    auth_headers = {'accept': 'application/json', 'content-type': 'application/json',
                                   'x-tableau-auth': tableau_creds['token']}
    try:
        metadata_query = requests.post(tableau_server + '/api/metadata/graphql', headers=auth_headers, verify=True,
                                       json={"query": mdapi_query})
        tableau_databaseServers = json.loads(metadata_query.text)['data']['databaseServers']
        print('retrieved ' + str(len(tableau_databaseServers)) + ' Tableau database servers')
    except Exception as e:
        print('Error getting databases from Tableau metadata API ' + str(e))
    return tableau_databaseServers

def get_tableau_columns(tableau_server, merged_table, tableau_creds):
    full_table_name = get_full_table_name(merged_table)
    print('getting columns for tableau table ' + full_table_name + '...')
    site_id=tableau_creds['site']['id']
    get_columns_url = tableau_server + "/api/" + TABLEAU_API_VERSION + "/sites/" + site_id + "/tables/" + merged_table['luid'] + '/columns'

    payload = ""
    headers = {
        'X-Tableau-Auth': tableau_creds['token'],
        'Content-Type': 'appication/json',
        'Accept': 'application/json'
    }
    try:
        response = requests.request("GET", get_columns_url, headers=headers, data=payload)
        tableau_columns = json.loads(response.text)['columns']['column']
    except Exception as e:
        print('Error getting columns from Tableau metadata API ' + str(e))
    print('retrieved ' + str(len(tableau_columns)) + ' columns for tableau table: ' + full_table_name)
    return tableau_columns

def publish_tableau_column_descriptions(tableau_server, merged_table, tableau_columns, tableau_creds):
    full_table_name = get_full_table_name(merged_table)
    print('publishing tableau column descriptions for table [' + full_table_name + '...')
    d = defaultdict(dict)
    for l in (tableau_columns, merged_table['columns']):
        for elem in l:
            d[elem['name']].update(elem)
    merged_columns = sorted(d.values(), key=itemgetter("name"))

    for column in merged_columns:
        url = tableau_server + "/api/" + TABLEAU_API_VERSION + "/sites/" + tableau_creds['site']['id'] + "/tables/" + column['parentTableId'] + "/columns/" + column['id']
        payload = "<tsRequest>\n  <column description=\"" + column['description'] + " \">\n  </column>\n</tsRequest>"
        headers = {
            'X-Tableau-Auth': tableau_creds['token'],
            'Content-Type': 'text/plain'
        }
        try:
            column_description_response = requests.request("PUT", url, headers=headers, data=payload).text
        except Exception as e:
            print('Error publishing Tableau column descriptions ' + str(e))
    print('published tableau column descriptions for table ' + full_table_name)
    return column_description_response

def publish_tableau_column_tags(tableau_server, tableau_columns, merged_table, tableau_creds):
    tag = merged_table['packageName']
    full_table_name = get_full_table_name(merged_table)
    print('publishing tableau column tags: ' + tag + ' for table: ' + full_table_name + '...')
    headers = {
        'X-Tableau-Auth': tableau_creds['token'],
        'Content-Type': 'text/plain'
    }
    for tableau_column in tableau_columns:
        url = tableau_server + "/api/" + TABLEAU_API_VERSION + "/sites/" + tableau_creds['site']['id'] + "/columns/" + tableau_column['id'] + "/tags"
        payload = "<tsRequest>\n  <tags>\n <tag label=\"" + tag + "\"/>\n  </tags>\n</tsRequest>"

        try:
            column_tags_response = requests.request("PUT", url, headers=headers, data=payload).text
        except Exception as e:
            print('Error publishing Tableau column tags ' + str(e))
    print('published tableau column tags: ' + tag + ' for table: ' + full_table_name)
    return column_tags_response

def merge_dbt_tableau_tables(tableau_database,dbtmodels):
    print('merging dbt models with tableau tables for tableau database: ' + tableau_database['name'] + '...')
    d = defaultdict(dict)
    m = defaultdict(dict)
    for table in tableau_database['tables']:
        d[table['name'].upper()].update(table)
        for model in dbtmodels:
            if model['name'].upper() == table['name'].upper() and model['schema'].upper() == table['schema'].upper() and model['database'].upper() == tableau_database['name'].upper(): #if table/schema/database match
                m[model['name'].upper()].update(model)
                m[table['name'].upper()].update(table)
    merged_tables = sorted(m.values(), key=itemgetter("name"))
    print('merged ' + str(len(merged_tables)) + ' dbt models and tableau tables in tableau database: ' + tableau_database['name'])
    return merged_tables

def publish_tableau_table_tags(tableau_server, merged_table, tag, tableau_creds):
    full_table_name = get_full_table_name(merged_table)
    print('publishing tag: ' + tag + ' for tableau table ' + full_table_name + '...')
    headers = {
        'X-Tableau-Auth': tableau_creds['token'],
        'Content-Type': 'text/plain'
    }
    url = tableau_server + "/api/" + TABLEAU_API_VERSION + "/sites/" + tableau_creds['site']['id'] + "/tables/" + merged_table['luid'] + "/tags"
    payload = "<tsRequest>\n  <tags>\n <tag label=\"" + tag + "\"/>\n  </tags>\n</tsRequest>"
    try:
        table_tags_response = requests.request("PUT", url, headers=headers, data=payload).text
    except Exception as e:
        print('Error publishing Tableau table tag ' + str(e))
    print('published table tag: ' + tag + ' for tableau table ' + full_table_name)
    return table_tags_response


def publish_tableau_table_description(tableau_server, merged_table, description_text,tableau_creds):
    full_table_name = get_full_table_name(merged_table)
    print('publishing description for tableau table ' + full_table_name + '...')
    url = tableau_server+"/api/" + TABLEAU_API_VERSION + "/sites/"+ tableau_creds['site']['id']+"/tables/" + merged_table['luid']
    payload = '<tsRequest>\n  <table description=\'' + description_text +'\'>\n  </table>\n</tsRequest>'
    headers = {
        'X-Tableau-Auth': tableau_creds['token'],
        'Content-Type': 'text/plain'
    }
    try:
        table_description_response = requests.request("PUT", url, headers=headers, data=payload).text
    except Exception as e:
        print('Error publishing Tableau table description ' + str(e))
    print('published tableau table description for table ' + full_table_name )
    return table_description_response

def set_tableau_table_quality_warning(tableau_server, merged_table, dbt_model, isSevere, tableau_creds):
    full_table_name = get_full_table_name(merged_table)
    print('updating table data quality warning for tableau table: ' + full_table_name + '...')

    url = tableau_server+"/api/" + TABLEAU_API_VERSION + "/sites/"+ tableau_creds['site']['id']+"/dataQualityWarnings/table/" + merged_table['luid']
    dbt_model_status = dbt_model['status']
    dbt_model_status='failed' #TEST FLAG

    dbt_cloud_base_url = 'https://cloud.getdbt.com/next/deploy/' + str(dbt_model['accountId']) + '/projects/' + str(dbt_model['projectId'])
    message = 'dbt model status: *' + dbt_model_status + "*&#xA;" \
              + xmlesc('"dbt job":' + dbt_cloud_base_url + "/jobs/" + str(dbt_model['jobId'])) \
              + xmlesc(' | "dbt run":' + dbt_cloud_base_url + "/runs/" + str(dbt_model['runId']))
            #+ 'dbt model uniqueId: *' + str(dbt_model['uniqueId']) \
            #+ "*&#xA;" + 'dbt runId: *' + str(dbt_model['runId']) + "*&#xA;" + 'dbt jobId: *' + str(dbt_model['jobId']) \
            #+ "*&#xA;" + 'data quality warning last updated: *'+ str(datetime.utcnow().strftime("%Y-%m-%d %H:%MUTC")) +  "*&#xA;" \

    json_headers = {
        'X-Tableau-Auth': tableau_creds['token'],
        'Content-Type': 'appication/json',
        'Accept': 'application/json'
    }
    plain_headers = {
        'X-Tableau-Auth': tableau_creds['token'],
        'Content-Type': 'plain/text'
    }
    existing_dq_warning=requests.request('get', url, headers=json_headers).text
    existing_dq_warning_object = json.loads(existing_dq_warning)

    payload = '<tsRequest>\n  <dataQualityWarning type="WARNING" isActive="true" message="'+ message + '" isSevere="'+ str(isSevere) + '"/>\n   </tsRequest>'

    try:
        if existing_dq_warning_object['dataQualityWarningList']=={} and dbt_model_status!='success': #create new dq warning
            response = requests.request("POST", url, headers=plain_headers, data=payload).text
        else:
            existing_dq_warning_object_id = existing_dq_warning_object['dataQualityWarningList']['dataQualityWarning'][0]['id']
            dq_warning_url = tableau_server + "/api/" + TABLEAU_API_VERSION + "/sites/" + tableau_creds['site']['id'] + "/dataQualityWarnings/" + existing_dq_warning_object_id
            if dbt_model_status != 'success': #update existing dq warning
                response = requests.request("PUT", dq_warning_url, headers=plain_headers, data=payload).text
            else: #delete existing dq warning
                response = requests.request("DELETE", dq_warning_url, headers=plain_headers).text
    except Exception as e:
        print('Error setting data quality warning table description ' + str(e))
    print('updated table data quality warning for tableau table: ' + full_table_name)
    return response

def set_tableau_table_certification(tableau_server, merged_table, isCertified, certification_note, tableau_creds):
    full_table_name = get_full_table_name(merged_table)
    print('updating table certification for tableau table: ' + full_table_name + '...')

    url = tableau_server+"/api/" + TABLEAU_API_VERSION + "/sites/"+ tableau_creds['site']['id']+"/tables/" + merged_table['luid']

    if isCertified:
        certification_note = xmlesc(certification_note)
    else:
        certification_note=""

    payload = '<tsRequest>\n  <table isCertified="'+ str(isCertified).lower() + '"\n   certificationNote="'+ certification_note + '"  >\n  </table>\n</tsRequest>'
    headers = {
        'X-Tableau-Auth': tableau_creds['token'],
        'Content-Type': 'text/plain'
    }
    try:
        response = requests.request("PUT", url, headers=headers, data=payload).text
    except Exception as e:
        print('Error certifying Tableau table ' + str(e))
    print('updated table certification for tableau table: ' + full_table_name)
    return response

def make_table_description(dbt_model):
    dbt_cloud_base_url = 'https://cloud.getdbt.com/accounts/'+ str(dbt_model['accountId']) +'/jobs/' + str(dbt_model['jobId']) + '/docs/#!/model/' + dbt_model['uniqueId']
    has_stats=False
    for stat in dbt_model['stats']:
        if stat['id'] == "has_stats":
            has_stats=stat['value']
        if stat['id'] == "row_count":
            row_count=stat['value']
        if stat['id'] == "last_modified":
            last_modified = stat['value']
    line1 = xmlesc(dbt_model['description'])
    line3 = 'table description last updated: *' + str(datetime.utcnow().strftime("%Y-%m-%d %H:%MUTC")) + '*'
    line4 = '"dbt lineage":' + dbt_cloud_base_url + "?g_v=1" + ' | "dbt docs":' + dbt_cloud_base_url + '#details'
    if has_stats:
        line2 = "dbt approx row count: *" + str(row_count) + "* |  dbt model last modified date: *" + str(last_modified) + "*"
        table_description = line1 + "&#xA;" + line2 + "&#xA;" + line3 + "&#xA;" + line4
    else:
        table_description = line1 + "&#xA;" + line3 + "&#xA;" + line4
    return table_description

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
dbt_meta_certification_flag = settings['DBT']['DBT_META_CERTIFICATION_FLAG']

tableau_token = settings['TABLEAU']['TABLEAU_TOKEN']
tableau_token_name = settings['TABLEAU']['TABLEAU_TOKEN_NAME']
tableau_token = settings['TABLEAU']['TABLEAU_TOKEN']
tableau_site = settings['TABLEAU']['TABLEAU_SITE']
tableau_server = settings['TABLEAU']['TABLEAU_SERVER']
database_type = settings['TABLEAU']['DATABASE_TYPE']
database_name = settings['TABLEAU']['DATABASE_NAME']
certification_note = settings['TABLEAU']['CERTIFICATION_NOTE']
isSevere = settings['TABLEAU']['DQ_WARNING_IS_SEVERE']

dbt_account_id = dbt_get_account_id(dbt_cloud_api, dbt_token)
dbt_jobs = dbt_get_jobs(dbt_account_id,dbt_cloud_api,dbt_token)
tableau_creds = authenticate_tableau(tableau_server, tableau_site, tableau_token_name, tableau_token)
tableau_databases = tableau_get_databases(tableau_server, database_type, tableau_creds)
#tableau_databaseServers = tableau_get_databaseServers(tableau_server, database_type, tableau_creds)

for dbt_job in dbt_jobs:
    dbt_models = dbt_get_models_for_job(dbt_metadata_api, dbt_token, dbt_job['id'])

    if len(dbt_models)>0:
        for tableau_database in tableau_databases:
            merged_tables = merge_dbt_tableau_tables(tableau_database, dbt_models)
            for merged_table in merged_tables:
                if dbt_meta_certification_flag in merged_table['meta']:
                    tableau_certified = merged_table['meta'][dbt_meta_certification_flag]
                else:
                    tableau_certified = False
                tableau_columns = get_tableau_columns(tableau_server,merged_table, tableau_creds)
                table_description=make_table_description(merged_table)
                publish_tableau_table_description(tableau_server,merged_table, table_description, tableau_creds)
                set_tableau_table_quality_warning(tableau_server, merged_table, merged_table, isSevere, tableau_creds)
                set_tableau_table_certification(tableau_server, merged_table, tableau_certified, certification_note,tableau_creds)
                publish_tableau_table_tags(tableau_server, merged_table, merged_table['packageName'], tableau_creds)
                publish_tableau_column_descriptions(tableau_server, merged_table, tableau_columns, tableau_creds)
                publish_tableau_column_tags(tableau_server, tableau_columns, merged_table, tableau_creds)