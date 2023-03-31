# dbt Cloud and Tableau Catalog Metadata Integration
 This repo contains a sample project that implements a bi-directional integration between the dbt Cloud Metadata API and Tableau Catalog. The solution is implemented as a standlone Python application which creates a link between the dbt models/tables and downstream Tableau Data Sources in order to populate the table/field descriptions, data quality warnings, certifications, tags etc. in the Tableau Catalog. The solution also generates dbt dashboard exposures for the downstream Tableau Workbooks which can be automatically commited to your dbt github repo. Note that the solution has only been tested using Snowflake, although other cloud database platforms *should work.
 
 Please watch [this video](https://cloudydata.substack.com/p/dbt-and-tableau-metadata-integration) for details on how the integration works and read [this blog post](https://cloudydata.substack.com/p/tableau-and-dbt-governed-self-service) explaining why you should consider using dbt & Tabelau together. If you have any questions/comments/feedback then please join the [#tools-tableau](https://getdbt.slack.com/archives/C03PYMUCB8Q) dbt slack channel where you can get in touch with me.
 
 **Step 1.** Configure the settings.yml file with values that represent your environment. I would recommend starting by filtering on a single dbt project and or Snowflake database. 
 
 ![image](https://user-images.githubusercontent.com/11485060/229070580-1d88825f-cc9c-4b56-8566-042a63c17c77.png)
 
  **Step 2.** Install Python and ensure you have installed the following libraries
  
  **Step 3.** Run the dbt_tabcatalog.py and check the output console for any errors/warnings.
  
  **Step 4.** If the integration ran successfully you go into Tableau Server/Cloud -> `External Assets` and select a table which is linked to one of your dbt models. You should see the following information populated:
  ![image](https://user-images.githubusercontent.com/11485060/229073350-8cbeccb8-f437-485f-aa6a-5b20ce05298a.png)
  
   **Step 5.** Check whether the dbt exposures were added to your project by either opening the github repo directly or opening dbt Cloud and selecting `Develop`. You should see the downstream Tableau workbooks have been added as dashboard exposures to your dbt DAG:
   ![image](https://user-images.githubusercontent.com/11485060/229073302-afd83dc1-f768-47c3-930b-6932b6eb8494.png)

  
  
  
  
  
  
  
