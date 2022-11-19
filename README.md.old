# Automate dbt Metadata to Tableau Catalog

Note: This method employs REST API commands in Python. While REST API commands are a functionality supported by Tableau, note that the use of python or other 3rd party applications and functions may not be supported by Tableau.

WHY: When evaluating Tableau Catalog, many customers have asked how they can move field descriptions freely through their database to Tableau and have that populate downstream. This is extremely important for security and governance. dbt can also store rich column descriptions specific to models run during a job.

Right now, a description can exist in a dbt .yaml file. However when an end user connects to a table in Tableau that was written using dbt, the descriptions do not carry over. The Tableau user must manually add them through the server UI at the table level or each time at the datasource level in Tableau Desktop. This can lead to error, duplicative work, and no single source of truth due to lag time for updating the descriptions.

This code seeks to improve the process by using python to automate the movement of dbt descriptions to field descriptions in Tableau Catalog. As changes are made and new models are created, the script can be run at whatever cadence makes sense for your business, daily, weekly, or you could make it event-based. 

BASIC WORKFLOW:

By querying dbt by using the administrative and Metadata APIs, you can grab a table of metadata and store it in a pandas dataframe.

By querying the metadata API for a list of tables connected to data sources in Tableau, you can get a list of tables names and table LUIDs. Note: you cannot get table LUID from MDAPI by querying tables object alone, but you can get published data source LUID and database LUID. You can get table LUID if you query database and query downstream tables OR by using the databaseTables object.

Then, using Tableau Metadata Methods REST API, you can easily publish descriptions to columns in a table. These cascade down to all published datasources using these columns.


* * *
dbt data is organized by job. In order to run the API call to get a list of jobs run,  we'll first need our account ID.

### Get Account ID
Input: PAT   
Output: Account ID (string)

```py
def get_account_id(token):
    
    url = "https://cloud.getdbt.com/api/v2/accounts/"

    payload={}
    headers = {
      'Content-Type': 'appication/json',
      'Authorization': 'Token '+ token
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    #print(response.text)
    response_json = json.loads(response.text)
    account_id = response_json['data'][0]['id']
    return(account_id)
```
* * *
Next, we'll get the list of jobs and Job IDs so we can query information about them using the dbt Metadata API.
* * *

### Get list of jobs and IDs
Input: account ID, PAT Token  
Output: pandas dataframe with job name, job ID

```py
def get_jobs(account_id, token)
    url = "https://cloud.getdbt.com/api/v2/accounts/"+account_id+"/jobs"

    payload={}
    headers = {
      'Content-Type': 'appication/json',
      'Authorization': 'Token '+ token
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    response_json = json.loads(response.text)
    job_dataframe = pd.json_normalize(response_json['data'])
    name_plus_job_id = job_dataframe[['id','name']]
    return(name_plus_job_id)
```

* * *
Now that we have a list of names and job IDs, we can isolate one of the IDs to get information about them.
* * *

### Create table of models/executecompletedat/uniqueId
Input: job ID, PAT Token  
Output: Table with all models run during the job and their executedCompletedAt, model name (uniqueId), and database written to.


```py
def get_models_information(token, job_id):

    url = "https://metadata.cloud.getdbt.com/graphql"
    job_id = '102781'
    payload="{\"query\":\"{\\n  models(jobId: " + job_id + ") {\\n    uniqueId\\n    executionTime\\n    status\\n    executeCompletedAt\\n    database\\n    schema\\n\\n  }\\n}\",\"variables\":{}}"
    headers = {
      'Authorization': 'Token ' + token,
      'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    response_json = json.loads(response.text)
    models_table = pd.json_normalize(response_json['data']['models'])
    models_table = models_table[['uniqueId', 'executeCompletedAt', 'database']]
    return(models_table)
```

* * *
The above query only grabs the necessary information about a job to publish a table description to Tableau that informs the user that the table is from a dbt model, but if you want to grab other metadata, there are many more fields you can add to your query. see more here:

* * *

### create table of column names and descriptions with unique Id
Input: job ID, PAT Token  
Output: Table with all models run during the job and their executedCompletedAt, model name (uniqueId), and database written to.
Table with all column names, descriptions, and model it belongs to.

```py
def get_all_column_descriptions(token, job_id):
    #for each model, get column descriptions in a table with table name, column name, description
    #union all together
    url = "https://metadata.cloud.getdbt.com/graphql"

    payload="{\"query\":\"{\\n  models(jobId: " + job_id + ") {\\n    uniqueId\\n    executionTime\\n    status\\n    executeCompletedAt\\n    database\\n    columns {\\n        name\\n        description\\n    }\\n\\n  }\\n}\",\"variables\":{}}"
    headers = {
      'Authorization': 'Token '+ token
      'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    response_json = json.loads(response.text)
    models_table = pd.json_normalize(response_json['data']['models'])
    model_to_text_description = models_table[['uniqueId', 'executeCompletedAt', 'database']]
    column_info = pd.DataFrame(columns = ['name','description','uniqueId'])
    
    for index,row in models_table.iterrows():
        

        column_descriptions = pd.json_normalize(row['columns'])
        column_descriptions['uniqueId'] = row['uniqueId'].split('.')[-1]
        column_info = pd.concat([column_info, column_descriptions], ignore_index=True)
    
    to_return = [model_to_text_description, column_info]    
    return(to_return)

```

* * *
The above call returns model information along with column names and descriptions.
* * *

### Authenticate to Tableau
Input: url name (e.g. bi.xyz.com), PAT, site_name (e.g. 'superstore'), token_name  
Output: temp token, site ID

```py
 def authenticate_tableau(url_name,PAT,site_name, token_name):
    url = "https://" + url_name + " /api/3.13/auth/signin"

    payload = json.dumps({
      "credentials": {
        "personalAccessTokenName": token_name,
        "personalAccessTokenSecret": PAT,
        "site": {
          "contentUrl": site_name
        }
      }
    })
    headers = {
      'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    response = response.text
    token_string = response.split('token="',2)
    token_string_split1 = token_string[1].split('"',1)
    token = token_string_split1[0]
    
    site_id = token_string[2].split('site id="',1)
    site_id = site_id.split('"')
    site_id = site_id[0]
    
    req_strings=[token,site_id]
    return(req_strings)
```

* * *
Now that we have authenticated and gotten our temp token, we can filter through content on the server to only surface tables related to the database we wrote into from dbt.
* * *

### Get list of tables from a database
Input: URL name, database type (e.g. 'databricks'), database name, temp token  
Output: pandas dataframe with table name and table LUID

```py
def get_table_luids(url_name, database_type, database_name, token)
mdapi_query = '''
query get_databases {
  databases (filter: {connectionType: " +database_type+ ", name:"''' + database_name + '''"}){
 name
 id
 tables{
     name
     id
     luid
 
 }

 
}
}
'''
auth_headers = auth_headers = {'accept': 'application/json','content-type': 'application/json','x-tableau-auth': token}
metadata_query = requests.post('https://'+ts_url + '/api/metadata/graphql', headers = auth_headers, verify=True, json = {"query": mdapi_query})
mdapi_result = json.loads(metadata_query.text)

k=0
table_luid_list = []
while k < len(mdapi_result['data']['databases'][0]['tables']):
    print(mdapi_result['data']['databases'][0]['tables'][k]['luid'])
    table_luid_list.append(mdapi_result['data']['databases'][0]['tables'][k]['luid'])
    k = k+1
    
table_dictionary = {"table_name":table_name_list, "table_luid":table_luid_list}
tableau_tables_info = pd.DataFrame(table_dictionary, columns=['table_name','table_luid'])

return(tableau_tables_info)
```

* * *
We will need the table LUID to make changes to it using the REST API.
* * *

### Get table ID
```py
def get_table_id(url_name, table_name, site_id, token):
    get_tables_url = "https://"+url_name+"/api/3.13/sites/"+site_id+"/tables"

    payload = ""
    headers = {
      'X-Tableau-Auth': token
    }

    table_response = requests.request("GET", get_tables_url, headers=headers, data=payload)
    table_id_string = table_response.text.split('name="'+table_name+'"',1)
    table_id_string_split1 = table_id_string[0].split('<table id="')
    table_id_string_split2 = table_id_string_split1[-1]
    table_id_string_split3 = table_id_string_split2[0:-1]
    table_id_string_split4 = table_id_string_split2[0:-2]
    return(table_id_string_split4)
```

* * *
Now that we have the table LUID, we can grab all columns in the table and their LUIDs to publish descriptions.
* * *

### Get column names and column IDs
Input: URL Name, table LUID, site ID, temp token  
Output: Pandas dataframe with column name, column ID
```py
def get_list_of_columns(url_name,table_id,site_id,token):
    get_columns_url = "https://"+url_name+"/api/3.13/sites/"+site_id+"/tables/"+table_id+'/columns'

    payload = ""
    headers = {
      'X-Tableau-Auth': token
    }

    columns_response = requests.request("GET", get_columns_url, headers=headers, data=payload)
    columns_response_split = columns_response.text.split('" name="')
    i = 0
    column_names_list = []
    column_ids_list = []
    while i < len(columns_response_split):
        if i%2 == 0: 
            #even number, grab the column ID
            column_id = columns_response_split[i].split('column id="', 1)
            column_id = column_id[1][0:-2]
            column_ids_list.append(column_id)
        if i%2 == 1:
            #odd number, this is to grab the name
            column_name = columns_response_split[i].split('"',1)
            column_name = column_name[0]
            column_names_list.append(column_name)
        i = i+1
    
    column_dictionary = {"column_name":column_names_list, "column_id":column_ids_list}
    tableau_column_info = pd.DataFrame(column_dictionary, columns=['column_name','column_id'])
    return(tableau_column_info)
 ```
 
* * *
Now that we have a dataframe that has all column names and descriptions for a table, we can merge this information with the dbt column descriptions.
* * *

### Merge column descriptions and IDs
Input: tableau column names/LUIDs, dbt column names/description  
Output: joined table that has column name, LUID and description
```py
def add_comments_to_tab_table(tableau_columns, dbt_columns):
    join_result = tableau_columns.merge(dbt_columns, how='inner',left_on="column_name",right_on='name')

    return(join_result)

joined_tables = add_comments_to_tab_table(fct_order_items, columns_table)
print(joined_tables)
```

* * *
Now that we have a mapping from dbt descriptiosn to tableau column LUIDs, Let's publish the descriptions to Tableau.
* * *

### Publish descriptions to columns
Input: URL Name, Site ID, table LUID, column LUID, description (string), temp token)  
Output: response code for the API call.

```py
def publish_description_to_column(url_name,site_id,table_id,column_id, description_text,token):
    column_description_url = "https://"+url_name+"/api/3.13/sites/"+site_id+"/tables/" + table_id + "/columns/" + column_id

    payload = "<tsRequest>\n  <column description=\"" + description_text +" \">\n  </column>\n</tsRequest>"
    headers = {
        'X-Tableau-Auth': token,
      'Content-Type': 'text/plain'
    }

    column_description_response = requests.request("PUT", column_description_url, headers=headers, data=payload)

    column_description_response_code = column_description_response.text
    return(column_description_response_code)
 ```
 
* * *
We can also populate the table description with relevant information from DBT.
* * *

### Publish description to table
Input: URL Name, site ID, table LUID, description (string), temp token  
Output: response text

```py
def publish_description_to_table(url_name,site_id,table_id, description_text,token):
    column_description_url = "https://"+url_name+"/api/3.13/sites/"+site_id+"/tables/" + table_id

    payload = "<tsRequest>\n  <description=\"" + description_text +" \">\n  </table>\n</tsRequest>"
    headers = {
        'X-Tableau-Auth': token,
      'Content-Type': 'text/plain'
    }

    table_description_response = requests.request("PUT", column_description_url, headers=headers, data=payload)

    table_description_response_code = table_description_response.text
    return(table_description_response_code
```
