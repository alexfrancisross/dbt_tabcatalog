from datetime import datetime
import json
import yaml
from yaml.loader import SafeLoader
from operator import itemgetter
from collections import defaultdict
import requests
from itertools import groupby
import base64
CONFIG='settings.yml'
tableau_API_VERSION='3.17'

#helper function to create xml formatted strings
def xmlesc(txt):
    txt = txt.replace("&", "&amp;")
    txt = txt.replace("<", "&lt;")
    txt = txt.replace(">", "&gt;")
    txt = txt.replace('"', "&quot;")
    txt = txt.replace("'", "&apos;")
    txt = txt.replace("\n", "&#xA;")
    return txt

#helper function to get full table name in the format [DATABASE].[SCHEMA].[TABLE]
def get_full_table_name(merged_table):
    full_table_name = '[' + merged_table['database'].upper() + '].[' + merged_table['schema'].upper() + '].[' + merged_table['name'].upper() + ']'
    return full_table_name

#returns dbt Cloud account id
def dbt_get_account_id(dbt_cloud_api, dbt_token):
    print('getting dbt Cloud account id from dbt Cloud API: ' + dbt_cloud_api + '...')
    url = dbt_cloud_api
    payload = {}
    headers = {
        'Content-Type': 'appication/json',
        'Authorization': 'Token ' + dbt_token
    }
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        response_json = json.loads(response.text)
        if 'errors' in response_json.keys():
            raise Exception(response_json['errors'][0]['message'])

        dbt_account_id = response_json['data'][0]['id']
        print('dbt Cloud account Id: ' + str(dbt_account_id))
    except Exception as e:
        print('Error getting account id from dbt Cloud: ' + str(e))
    return dbt_account_id

#returns list of dbt projects for a given dbt Cloud account
def dbt_get_projects(dbt_account_id, dbt_cloud_api, dbt_project_filter, database_account_filter, dbt_token):
    print('getting dbt projects for account id ' + str(dbt_account_id) + '...')
    url = dbt_cloud_api + str(dbt_account_id) +"/projects"
    payload={}
    headers = {
      'Content-Type': 'appication/json',
      'Authorization': 'Token '+ dbt_token
    }
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        response_json = json.loads(response.text)
        if 'errors' in response_json.keys():
            raise Exception(response_json['errors'][0]['message'])

        dbt_projects = response_json['data']

        if len(dbt_project_filter) > 0:
            filtered_dbt_projects = []
            for dbt_project in dbt_projects:
                if dbt_project['name'] in dbt_project_filter and dbt_project['connection']['details']['account'] in database_account_filter:
                    filtered_dbt_projects.append(dbt_project)
            dbt_projects=filtered_dbt_projects
        print('retrieved: ' + str(len(dbt_projects)) + ' dbt projects')
    except Exception as e:
        print('Error getting projects from dbt Cloud: ' + str(e))
    return dbt_projects

#returns list of dbt jobs for a given dbt Cloud account (filtered by dbt_project_filter list)
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
        if 'errors' in response_json.keys():
            raise Exception(response_json['errors'][0]['message'])

        dbt_jobs = response_json['data']
        print('retrieved: ' + str(len(dbt_jobs)) + ' dbt jobs')
    except Exception as e:
        print('Error getting jobs from dbt Cloud: ' + str(e))
    return dbt_jobs

#returns filtered list of dbt jobs for a given dbt Cloud account (filtered by dbt_project_filter and database_account_filter lists)
def filter_dbt_jobs(dbt_jobs, dbt_projects):
    print('filtering dbt jobs matching projects...')
    try:
        filtered_dbt_jobs = []
        dbt_project_ids = list(map(lambda d: d['id'],dbt_projects))

        for dbt_job in dbt_jobs:
            if dbt_job['project_id'] in dbt_project_ids:
                filtered_dbt_jobs.append(dbt_job)
        print('retrieved: ' + str(len(dbt_jobs)) + ' dbt jobs')

    except Exception as e:
        print('Error filtering dbt jobs matching projects' + str(e))
    return filtered_dbt_jobs

#returns list of dbt models for a given job
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
        if 'errors' in response_json.keys():
            raise Exception(response_json['errors'][0]['message'])
        dbt_models = response_json['data']['models']
        print('retreived ' + str(len(dbt_models)) + ' dbt models for jobId: ' + str(job_id))
    except Exception as e:
        print('Error getting dbt models for job id: ' + str(job_id) + ' ' + str(e))
    return dbt_models

#authenticates with tableau server/cloud and returns credentials object
def authenticate_tableau(tableau_server, tableau_site_name, tableau_token_name, tableau_token):
    url = tableau_server + "/api/" + tableau_API_VERSION + "/auth/signin"
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
        if 'error' in response_json.keys():
            raise Exception(response_json['error'])

        tableau_creds = response_json['credentials']
        print('tableau user id: ' + str(tableau_creds['user']['id']))
    except Exception as e:
        print('Error authenticating with tableau. Servername: ' + tableau_server + ' Site: ' + tableau_site_name + ' ' +  str(e))
    return tableau_creds

#returns a list of tableau databases (filter using database_type_filter and database_name_filter)
def tableau_get_databases(tableau_server, database_type_filter, database_name_filter, tableau_creds):
    print('getting tableau databases with database type: ' + database_type_filter + '...')
    filter = 'connectionType: "' + database_type_filter + '"'
    if len(database_name_filter)>0:
        filter = filter + ', nameWithin: ' + json.dumps(database_name_filter)

    mdapi_query = '''query get_databases {
          databases(filter: {''' + filter + '''}) {
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
        print('Error getting databases from tableau metadata API ' + str(e))
    print('retrieved ' + str(len(tableau_databases)) + ' tableau databases')
    return tableau_databases

#returns a list of downstream workbooks (filter using database_type_filter and database_name_filter)
def tableau_get_downstream_workbooks(tableau_server, merged_table, tableau_creds):
    full_table_name = get_full_table_name(merged_table)
    print('getting downstream workbooks for table: ' + full_table_name + '...')
    filter = 'luid: "' + merged_table['luid'] + '"'

    mdapi_query = '''query get_downstream_workbooks {
  databaseTables(filter: {''' + filter + '''}) {
    name
    id
    luid
    downstreamWorkbooks {
      id
      luid
      name
      description
      projectName
      vizportalUrlId
      tags {
        id
        name
      }
      owner {
        id
        name
        username
      }
      upstreamTables
      {
        id
        luid
        name
      }
    }
  }
}'''
    auth_headers = {'accept': 'application/json', 'content-type': 'application/json',
                                   'x-tableau-auth': tableau_creds['token']}
    try:
        metadata_query = requests.post(tableau_server + '/api/metadata/graphql', headers=auth_headers, verify=True,
                                       json={"query": mdapi_query})
        downstream_workbooks = json.loads(metadata_query.text)['data']['databaseTables'][0]['downstreamWorkbooks']
    except Exception as e:
        print('Error getting downstream workbooks from tableau metadata API ' + str(e))
    print('retrieved ' + str(len(downstream_workbooks)) + ' downstream tableau workbooks')
    return downstream_workbooks



#returns a list of tableau databases (including database hostnames and tables)
def tableau_get_databaseServers(tableau_server, database_type_filter, database_name_filter, tableau_creds):
    print('getting database server list from tableau metadata API with database type: ' + database_type_filter + '...')

    filter = 'connectionType: "' + database_type_filter + '"'
    if len(database_name_filter)>0:
        filter = filter + ', nameWithin: ' + json.dumps(database_name_filter)

    mdapi_query = '''query get_databaseServers {
          databaseServers(filter: {''' + filter + '''}) {
            name
            id
            hostName
            tables
    	    {
              id
              luid
              name
              schema
            }
          }
        }'''
    auth_headers = {'accept': 'application/json', 'content-type': 'application/json',
                                   'x-tableau-auth': tableau_creds['token']}
    try:
        metadata_query = requests.post(tableau_server + '/api/metadata/graphql', headers=auth_headers, verify=True,
                                       json={"query": mdapi_query})

        response_json = json.loads(metadata_query.text)
        tableau_databaseServers = response_json['data']['databaseServers']
        print('retrieved ' + str(len(tableau_databaseServers)) + ' tableau database servers')
        if 'errors' in response_json.keys():
            raise Exception(response_json['errors'][0]['message'])
    except Exception as e:
        print('Error getting databases from tableau metadata API: ' + str(e))
    return tableau_databaseServers

#returns a list of tableau columns for a given table
def get_tableau_columns(tableau_server, merged_table, tableau_creds):
    full_table_name = get_full_table_name(merged_table)
    print('getting columns for tableau table ' + full_table_name + '...')
    site_id=tableau_creds['site']['id']
    get_columns_url = tableau_server + "/api/" + tableau_API_VERSION + "/sites/" + site_id + "/tables/" + merged_table['luid'] + '/columns'
    payload = ""
    headers = {
        'X-tableau-Auth': tableau_creds['token'],
        'Content-Type': 'appication/json',
        'Accept': 'application/json'
    }
    try:
        response = requests.request("GET", get_columns_url, headers=headers, data=payload)
        tableau_columns = json.loads(response.text)['columns']['column']
    except Exception as e:
        print('Error getting columns from tableau metadata API ' + str(e))
    print('retrieved ' + str(len(tableau_columns)) + ' columns for tableau table: ' + full_table_name)
    return tableau_columns

#publishes tableau column descriptions for a given table and list of columns
def publish_tableau_column_descriptions(tableau_server, merged_table, tableau_columns, tableau_creds):
    full_table_name = get_full_table_name(merged_table)

    print('publishing tableau column descriptions for table: ' + full_table_name + '...')
    d = defaultdict(dict)
    for l in (tableau_columns, merged_table['columns']):
        for elem in l:
            d[elem['name']].update(elem)
    merged_columns = sorted(d.values(), key=itemgetter("name"))
    for column in merged_columns:
        if 'description' in column.keys():
            url = tableau_server + "/api/" + tableau_API_VERSION + "/sites/" + tableau_creds['site']['id'] + "/tables/" + column['parentTableId'] + "/columns/" + column['id']
            payload = "<tsRequest>\n  <column description=\"" + column['description'] + " \">\n  </column>\n</tsRequest>"
            headers = {
                'X-tableau-Auth': tableau_creds['token'],
                'Content-Type': 'text/plain'
            }
            try:
                response = requests.request("PUT", url, headers=headers, data=payload).text
            except Exception as e:
                print('Error publishing tableau column descriptions ' + str(e))
    #print('published tableau column descriptions for table ' + full_table_name)
    return

#publishes tableau column tags for a given table and list of columns
def publish_tableau_column_tags(tableau_server, tableau_columns, merged_table, tableau_creds):
    tag = merged_table['packageName']
    full_table_name = get_full_table_name(merged_table)
    print('publishing tableau column tags: ' + tag + ' for table: ' + full_table_name + '...')
    headers = {
        'X-tableau-Auth': tableau_creds['token'],
        'Content-Type': 'text/plain'
    }
    for tableau_column in tableau_columns:
        url = tableau_server + "/api/" + tableau_API_VERSION + "/sites/" + tableau_creds['site']['id'] + "/columns/" + tableau_column['id'] + "/tags"
        payload = "<tsRequest>\n  <tags>\n <tag label=\"" + tag + "\"/>\n  </tags>\n</tsRequest>"

        try:
            column_tags_response = requests.request("PUT", url, headers=headers, data=payload).text
        except Exception as e:
            print('Error publishing tableau column tags ' + str(e))
    #print('published tableau column tags: ' + tag + ' for table: ' + full_table_name)
    return column_tags_response

#returns a list of merged (i.e. matched database/schema/table name) tableau database tables and dbt models
def merge_dbt_tableau_tables(tableau_database, dbt_models):
    print('merging dbt models with tableau tables for tableau database: ' + tableau_database['name'] + '...')
    d = defaultdict(dict)
    m = defaultdict(dict)
    for table in tableau_database['tables']:
        d[table['name'].lower()].update(table)
        for model in dbt_models:
            model_database_account = dbt_projects[0]['connection']['details']['account']
            if model['name'].lower() == table['name'].lower() and model['schema'].lower() == table['schema'].lower() and model['database'].lower() == tableau_database['name'].lower() and tableau_database['hostName'].lower().startswith(model_database_account.lower()): #if table/schema/database/hostname match
                m[model['name'].lower()].update(table)
                m[table['name'].lower()].update(model)
    merged_tables = sorted(m.values(), key=itemgetter("name"))
    print('merged ' + str(len(merged_tables)) + ' dbt models and tableau tables in tableau database: ' + tableau_database['name'])
    return merged_tables

#publishes tableau table tags for a given table
def publish_tableau_table_tags(tableau_server, merged_table, tableau_creds):
    full_table_name = get_full_table_name(merged_table)
    tag = merged_table['packageName']
    print('publishing tag ' + tag + ' for tableau table: ' + full_table_name + '...')
    headers = {
        'X-tableau-Auth': tableau_creds['token'],
        'Content-Type': 'text/plain'
    }
    url = tableau_server + "/api/" + tableau_API_VERSION + "/sites/" + tableau_creds['site']['id'] + "/tables/" + merged_table['luid'] + "/tags"
    payload = "<tsRequest>\n  <tags>\n <tag label=\"" + tag + "\"/>\n  </tags>\n</tsRequest>"
    try:
        table_tags_response = requests.request("PUT", url, headers=headers, data=payload).text
    except Exception as e:
        print('Error publishing tableau table tag ' + str(e))
    #print('published table tag ' + tag + ' for tableau table: ' + full_table_name)
    return table_tags_response

#publishes tableau table description for a given table
def publish_tableau_table_description(tableau_server, merged_table, description_text, tableau_creds):
    full_table_name = get_full_table_name(merged_table)
    print('publishing description for tableau table ' + full_table_name + '...')
    url = tableau_server+"/api/" + tableau_API_VERSION + "/sites/"+ tableau_creds['site']['id']+"/tables/" + merged_table['luid']
    payload = '<tsRequest>\n  <table description=\'' + description_text +'\'>\n  </table>\n</tsRequest>'
    headers = {
        'X-tableau-Auth': tableau_creds['token'],
        'Content-Type': 'text/plain'
    }
    try:
        table_description_response = requests.request("PUT", url, headers=headers, data=payload).text
    except Exception as e:
        print('Error publishing tableau table description ' + str(e))
    #print('published tableau table description for table ' + full_table_name )
    return table_description_response

#sets tableau table quality warning for a given table if dbt model status was not a success
def set_tableau_table_quality_warning(tableau_server, merged_table, isSevere, tableau_creds):
    full_table_name = get_full_table_name(merged_table)
    print('updating table data quality warning for tableau table: ' + full_table_name + '...')
    url = tableau_server+"/api/" + tableau_API_VERSION + "/sites/"+ tableau_creds['site']['id']+"/dataQualityWarnings/table/" + merged_table['luid']
    dbt_model_status = merged_table['status']
    dbt_model_status='failed' #TEST FLAG
    dbt_cloud_base_url = 'https://cloud.getdbt.com/next/deploy/' + str(merged_table['accountId']) + '/projects/' + str(merged_table['projectId'])
    message = 'dbt model status: *' + dbt_model_status + "*&#xA;" \
              + xmlesc('"dbt job":' + dbt_cloud_base_url + "/jobs/" + str(merged_table['jobId'])) \
              + xmlesc(' | "dbt run":' + dbt_cloud_base_url + "/runs/" + str(merged_table['runId']))
            #+ 'dbt model uniqueId: *' + str(dbt_model['uniqueId']) \
            #+ "*&#xA;" + 'dbt runId: *' + str(dbt_model['runId']) + "*&#xA;" + 'dbt jobId: *' + str(dbt_model['jobId']) \
            #+ "*&#xA;" + 'data quality warning last updated: *'+ str(datetime.utcnow().strftime("%Y-%m-%d %H:%MUTC")) +  "*&#xA;" \
    json_headers = {
        'X-tableau-Auth': tableau_creds['token'],
        'Content-Type': 'appication/json',
        'Accept': 'application/json'
    }
    plain_headers = {
        'X-tableau-Auth': tableau_creds['token'],
        'Content-Type': 'plain/text'
    }
    existing_dq_warning=requests.request('get', url, headers=json_headers).text
    existing_dq_warning_object = json.loads(existing_dq_warning)
    payload = '<tsRequest>\n  <dataQualityWarning type="WARNING" isActive="true" message="'+ message + '" isSevere="'+ str(isSevere).lower() + '"/>\n   </tsRequest>'
    try:
        if existing_dq_warning_object['dataQualityWarningList']=={} and dbt_model_status!='success': #create new dq warning
            response = requests.request("POST", url, headers=plain_headers, data=payload).text
        else:
            existing_dq_warning_object_id = existing_dq_warning_object['dataQualityWarningList']['dataQualityWarning'][0]['id']
            dq_warning_url = tableau_server + "/api/" + tableau_API_VERSION + "/sites/" + tableau_creds['site']['id'] + "/dataQualityWarnings/" + existing_dq_warning_object_id
            if dbt_model_status != 'success': #update existing dq warning
                response = requests.request("PUT", dq_warning_url, headers=plain_headers, data=payload).text
            else: #delete existing dq warning
                response = requests.request("DELETE", dq_warning_url, headers=plain_headers).text
    except Exception as e:
        print('Error setting data quality warning on tableau table '  + full_table_name + str(e))
    #print('updated table data quality warning for tableau table: ' + full_table_name)
    return

#sets tableau table certification for a given table based on dbt meta config for the dbt model
def set_tableau_table_certification(tableau_server, merged_table, dbt_meta_certification_flag, certification_note, tableau_creds):
    full_table_name = get_full_table_name(merged_table)
    print('updating table certification for tableau table: ' + full_table_name + '...')

    if dbt_meta_certification_flag=='':
        isCertified = True
        certification_note = xmlesc(certification_note)
    elif dbt_meta_certification_flag in merged_table['meta']:
        isCertified = merged_table['meta'][dbt_meta_certification_flag]
        certification_note = xmlesc(certification_note)
        #+ '\nmodel: *' + merged_table['uniqueId'] + '*' )
    else:
        isCertified = False
        certification_note = ""

    url = tableau_server+"/api/" + tableau_API_VERSION + "/sites/"+ tableau_creds['site']['id']+"/tables/" + merged_table['luid']
    payload = '<tsRequest>\n  <table isCertified="'+ str(isCertified).lower() + '"\n   certificationNote="'+ certification_note + '"  >\n  </table>\n</tsRequest>'
    headers = {
        'X-tableau-Auth': tableau_creds['token'],
        'Content-Type': 'text/plain'
    }
    try:
        response = requests.request("PUT", url, headers=headers, data=payload).text
    except Exception as e:
        print('Error certifying tableau table ' + str(e))
    #print('updated table certification for tableau table: ' + full_table_name)
    return

#helper function makes tableau table description
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

def generate_dbt_exposures(dbt_account_id, dbt_cloud_api, dbt_token, github_token, downstream_workbooks, tableau_server, tableau_site, dbt_exposure_maturity):
    print('generating dbt exposures for downstream workbooks...')
    temp = groupby(downstream_workbooks, lambda x: x['dbt_projectId'])
    workbooks_grouped_by_project = [list(group) for key, group in temp]

    for project in workbooks_grouped_by_project:
        exposures_list = []
        for workbook in project:
            url=tableau_server + '/#/site/' + tableau_site + '/workbooks/'+ workbook['vizportalUrlId']
            workbook_name = workbook['name']
            description = workbook['description']
            owner = workbook['owner']['name']
            owner_username = workbook['owner']['username']
            depends_on = []
            for upstreamTable in workbook['upstreamTables']:
                depends_on.append("ref('"+upstreamTable['name'].lower()+"')")

            exposures_list.append({'name': workbook_name,'type':'dashboard','maturity': dbt_exposure_maturity,'url': url,'description': description,'depends_on': depends_on,'owner':{'name':owner,'email':owner_username}})

        dict_file = {'version': 2,
                     'exposures': exposures_list}
        write_dbt_project_exposures_file(dict_file, str(project[0]['dbt_projectId']))
        write_github_exposures_file(dbt_account_id, dbt_cloud_api, dbt_token, github_token, dict_file, str(project[0]['dbt_projectId']))
    return


def write_github_exposures_file(dbt_account_id, dbt_cloud_api, dbt_token, github_token, dict_file, project_id):
    headers = {
        'Content-Type': 'appication/json',
        'Authorization': 'Token ' + dbt_token
    }
    url = 'https://cloud.getdbt.com/api/v3/accounts/' + str(dbt_account_id) + '/projects/' + str(project_id)

    try:
        response = requests.request("get", url, headers=headers)
        response_json=json.loads(response.text)
        repository = response_json['data']['repository']

        #update github repo
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + github_token
        }
        filename = 'models/tab_exposures.yml'
        git_url = 'https://api.github.com/repos/' + repository['full_name'] + '/contents/' + filename
        message_bytes = yaml.dump(dict_file).encode('utf-8')
        base64_bytes = base64.b64encode(message_bytes)

        print('uploading dbt exposures file to github repo ' + git_url)
        payload = json.loads('{"message": "auto generated by tableau dbt integration", "content":"' + base64_bytes.decode('utf-8') + '"}')

        #check if exposures file already exists
        response = requests.request("get", git_url, headers=headers)

        #if exposures file exists in github then update headers to includ sha
        if response.status_code == 200:
            response_json = json.loads(response.text)
            sha = response_json['sha']
            payload['sha'] = sha

        response = requests.put(git_url, headers=headers, data=json.dumps(payload))
        print(response.text)

    except Exception as e:
        print(e)
    return

def write_dbt_project_exposures_file(dict_file, project_name):
    print('writing dbt exposures to file for project: ' + project_name + '...')
    try:
        filename = '.\\exposures\\' + project_name + '_tab_exposures.yml'
        with open(filename, 'w') as file:
            documents = yaml.dump(dict_file, file)
    except Exception as e:
        print('Error writing dbt exposures ' + str(e))
    return

def remove_duplicate_workbooks(workbooks):
    print('removing duplicate workbooks: ...')
    res_list = [i for n, i in enumerate(workbooks)
                if i not in workbooks[n + 1:]]
    print('done')
    return res_list

#read project yaml file
class app_settings:
    try:
        with open(CONFIG) as f:
            data = yaml.load(f, Loader=SafeLoader)

            dbt_token = data['DBT']['DBT_TOKEN']
            dbt_cloud_api = data['DBT']['DBT_CLOUD_API']
            dbt_metadata_api = data['DBT']['DBT_METADATA_API']
            dbt_meta_certification_flag = data['DBT']['DBT_META_CERTIFICATION_FLAG']
            dbt_project_filter = data['DBT']['DBT_PROJECT_FILTER']
            dbt_generate_exposures = data['DBT']['DBT_GENERATE_EXPOSURES']
            dbt_exposures_maturity = data['DBT']['DBT_EXPOSURES_MATURITY']

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

            github_token = data['GITHUB']['GITHUB_TOKEN']

    except Exception as e:
        print("failed to read yaml file " + str(e))

#MAIN PROGRAM
settings = app_settings()

dbt_account_id = dbt_get_account_id(settings.dbt_cloud_api, settings.dbt_token)
dbt_projects = dbt_get_projects(dbt_account_id, settings.dbt_cloud_api, settings.dbt_project_filter, settings.database_account_filter, settings.dbt_token)
dbt_jobs = dbt_get_jobs(dbt_account_id, settings.dbt_cloud_api, settings.dbt_token)
tableau_creds = authenticate_tableau(settings.tableau_server, settings.tableau_site, settings.tableau_token_name, settings.tableau_token)
tableau_databases = tableau_get_databaseServers(settings.tableau_server, settings.database_type_filter, settings.database_name_filter, tableau_creds)
all_downstream_workbooks=[]

for dbt_job in dbt_jobs:
    dbt_models = dbt_get_models_for_job(settings.dbt_metadata_api,settings. dbt_token, dbt_job['id'])

    if len(dbt_models)>0:
        for tableau_database in tableau_databases:
            merged_tables = merge_dbt_tableau_tables(tableau_database, dbt_models)

            for merged_table in merged_tables:
                tableau_columns = get_tableau_columns(settings.tableau_server, merged_table, tableau_creds)
                table_description=make_table_description(merged_table)
                publish_tableau_table_description(settings.tableau_server, merged_table, table_description, tableau_creds)
                set_tableau_table_quality_warning(settings.tableau_server, merged_table, settings.tableau_dq_warning_isSevere, tableau_creds)
                set_tableau_table_certification(settings.tableau_server, merged_table, settings.dbt_meta_certification_flag, settings.tableau_certification_note, tableau_creds)
                publish_tableau_table_tags(settings.tableau_server, merged_table, tableau_creds)
                publish_tableau_column_descriptions(settings.tableau_server, merged_table, tableau_columns, tableau_creds)
                publish_tableau_column_tags(settings.tableau_server, tableau_columns, merged_table, tableau_creds)

                if settings.dbt_generate_exposures:
                    downstream_workbooks = tableau_get_downstream_workbooks(settings.tableau_server, merged_table, tableau_creds)
                    for workbook in downstream_workbooks:
                        workbook['dbt_projectId'] = merged_table['projectId']
                        workbook['dbt_environmentId'] = merged_table['environmentId']
                        all_downstream_workbooks.append(workbook)

if len(all_downstream_workbooks)>0:
    all_downstream_workbooks = remove_duplicate_workbooks(all_downstream_workbooks)
    generate_dbt_exposures(dbt_account_id, settings.dbt_cloud_api, settings.dbt_token, settings.github_token, all_downstream_workbooks, settings.tableau_server, settings.tableau_site, settings.dbt_exposures_maturity)
