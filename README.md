%% Web Data Ingestion and Processing Automation %%

Overview
This project automates the process of extracting data from a website using the Selenium WebDriver. It involves downloading zip files, storing them in a Download_path , extracting all the files to the landing folder of the project directory, and storing the Processed data in clean folder and sql database.

Prerequisites[Run In Order]
--virtual -env - python -m venv .venv
--activate virtual env = .venv\Scripts\activate.ps1
--run requirements.txt = pip install -r /content/requirements.txt
--Python (version)-3.10.11
--Selenium WebDriver-4.18.1
--Requirement txt are needed for all the dependencies.
--need Chrome Browser(For Selenium webdriver)

[Git Link]
Clone the repository: https://github.com/kumargautam02/Batch_pipeline_DataEngineering_task.git


Project Folder Structure : 
Zip_file_download_path: "C:\Users\Admin\Downloads"
Zip_file_path_inside_project_directory - "C:\Users\Admin\Downloads\Batch_pipeline_DataEngineering_task/DOWNLOAD_PATH"
landing: "C:\Users\Admin\Downloads\Batch_pipeline_DataEngineering_task/LANDING"
clean: "C:\Users\Admin\Downloads\Batch_pipeline_DataEngineering_task\CLEAN"
database_path : "C:\Users\Admin\Downloads\Batch_pipeline_DataEngineering_task\DATABASE"
database : cricket.db
table_name: ODI_CRICKET_RESULT


Scripts Name:
--__main__.py - main function to execute all the scripts
--dataprocessing_scripts.py- This Script hold all the necessary components from Ingestion to load of data in the clean file.
--data_ingestion_script.py - This script is used to Download the data from Cricksheet.org site, using selenium webdriver. 
--database_script.py - This Script will load the data to a database cricket.db, inside table - ODI_CRICKET_RESULT