# %%
import os
import shutil
import json
import zipfile
import pyspark
import logging
import pandas as pd
from database_script import get_connection
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from data_ingestion_script import *
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger('Data_Processing')
logger.info('Data_Processing Script started')

spark = SparkSession.builder.getOrCreate()

# %%
def start_ingesting_data(origin, target_directory):
    try:
        # Data Ingestion  to correct path
        origin = origin
        target_directory = target_directory
        download_required_files(logger)
        logger.info("Ingestion of Data Completed successfully")

        file_in_origin = os.listdir(origin)
        
        while ("odis_female_json.zip" not in file_in_origin) and ("odis_male_json.zip" not in file_in_origin):
            file_in_origin = os.listdir(origin)
            if ("odis_female_json.zip" in file_in_origin) and ("odis_male_json.zip" in file_in_origin):
                logger.info("waiting for file to get downloaded")
                break
        if ('LANDING' in os.listdir(f'{target_directory}')) and ('DOWNLOAD_PATH' in os.listdir(f'{target_directory}')) and ('DATABASE' in os.listdir(f'{target_directory}')):
            logger.info("LANDING PATH IS THERE")
            logger.info("DOWNLOAD PATH IS THERE")
            logger.info("DATABASE PATH IS THERE")
        else :
            os.makedirs(f'{target_directory}LANDING')
            logger.info("LANDING path created successfully")
            os.makedirs(f'{target_directory}DOWNLOAD_PATH')
            logger.info("DOWNLOAD_PATH created created successfully")
            os.makedirs(f'{target_directory}DATABASE')
            logger.info("DATABASE created created successfully")

        files_in_target_directory = os.listdir(target_directory+'DOWNLOAD_PATH')
        needed_files = ['odis_female_json.zip', "odis_male_json.zip"]
        file_in_origin = os.listdir(origin)
        for file in file_in_origin:
            if (file.startswith("odis_female_json") or file.startswith("odis_male_json")):
                shutil.copy(origin+file, target_directory+'DOWNLOAD_PATH')
        logger.info("Data Loaded to Download path Successfully")

        #extracting all the files in landing folder
        for file_name in needed_files:
            if (file_name == "odis_female_json.zip") or (file_name == "odis_male_json.zip"):
              logger.info(f"Started Extraction of file {file_name}")
              with zipfile.ZipFile(f'{target_directory}/DOWNLOAD_PATH/{file_name}') as f:
                      # f.extractall()/
                      f.extractall(f'{target_directory}/LANDING/')
        logger.info("Data Extraction Completed Successfully")
        
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)

origin = 'C:/Users/Admin/Downloads/'
target_directory = os.getcwd()+'/'
logger.info(f"Your Working Directory is: {target_directory}")
start_ingesting_data(origin, target_directory)

# %%
#Necessay Function
def get_batter_bowler_striker(column, column_need):
  try:
    if column_need.strip() == 'batter':
      return column['batter']
 
    elif column_need.strip() == 'bowler':
      return column['bowler']

    elif column_need.strip() == 'non_striker':
      return column['non_striker']
  except Exception as e:
    logging.error("Exception occurred", exc_info=True)



#Necessay Function
def get_run_ball_by_ball(column):
  try:

    runs_list = [-1]*3
    for scores in column.keys():
      if scores == 'runs':
        return column[scores]
      
  except Exception as e:
    logging.error("Exception occurred", exc_info=True)


def get_info_and_meta_data(all_information):
  try:
      for data in all_information.keys():
        if data == 'meta':
          meta_information = all_information['meta']
        elif data == 'info':
          info_information = all_information["info"]
      return meta_information, info_information
  except Exception as e:
    logging.error("Exception occurred", exc_info=True)

def get_necessary_information(data):
  try:
      date = data['dates'][0]

      if "city" in data.keys():
        city = data['city']
      else:
        city = "NULL"
      if 'event' in data['event'].keys():
        event_name = data['event']['name']
        if 'match_number' in data['event'].keys():
          match_number = data['event']['match_number']
        else:
          match_number = "NULL"
      else:
        event_name = "NULL"
        match_number = "NULL"

      gender = data['gender']

      if "winner" in data['outcome'].keys():

        winner_team = data['outcome']['winner']
      elif 'result' in data['outcome'].keys():

        winner_team = data['outcome']['result']

      if "winner" in data['outcome'].keys():
        if 'wickets' in data['outcome']['by'].keys():
          winned_by = f"{data['outcome']['by']['wickets']} wickets"
        elif 'runs' in data['outcome']['by'].keys():
          winned_by = f"{data['outcome']['by']['runs']} runs"
      else:
        winned_by = "NULL"
      team_1 = data['teams'][0]
      team_2 = data['teams'][1]
      return date, city, event_name, match_number, gender, winner_team, winned_by, team_1, team_2
  except Exception as e:
    logging.error("Exception occurred", exc_info=True)

#UDF created to get the run scored ball-by-ball
get_run_ball_by_ball_udf =  udf(lambda column: get_run_ball_by_ball(column), StringType())
#UDF created to get the batter name, striker name, bowler name
get_batter_bowler_striker_udf =  udf(lambda column, column_need: get_batter_bowler_striker(column, column_need), StringType())

# %%
try:
  path = target_directory+'LANDING/'
  all_the_files = os.listdir(path)
  # print(all_the_files)
  necessary_columns = []
  for i in all_the_files:
    # print(f"/female_dataset/{i}")
    print(i)
    if i.endswith(".json"):
      with open(f"{path}/{i}","r") as file_obj:
        file_content = file_obj.read()
        # print(file_content)
        details = json.loads(file_content)
        meta_information, info_information = get_info_and_meta_data(details)
        date, city, event_name, match_number, gender, winner_team, winned_by, team_1, team_2 = get_necessary_information(info_information)
        necessary_columns = []
        temp_dict = {}
        for i in range(len(details['innings'][0]['overs'])):
          temp_dict['overs'] = i
          temp_dict['balls_per_over'] = details['innings'][0]['overs'][i]['deliveries']
          # print(student_details['innings'][0]['overs'][i]['deliveries'])
          necessary_columns.append(temp_dict)
          temp_dict = {}
        dataframe = spark.createDataFrame(necessary_columns)
        # dataframe.show(1000, False)
        dataframe = dataframe.select("overs", posexplode_outer(dataframe.balls_per_over))

        dataframe_new = dataframe.withColumn("BATTER", get_batter_bowler_striker_udf(col("col"), lit("batter")))
        dataframe_new = dataframe_new.withColumn("BOWLER", get_batter_bowler_striker_udf(col("col"), lit("bowler")))
        dataframe_new = dataframe_new.withColumn("NON_STRIKER", get_batter_bowler_striker_udf(col("col"), lit("non_striker")))
        dataframe_new = dataframe_new.withColumn("runs_scored_per_ball", get_run_ball_by_ball_udf(col("col")))
        dataframe_new = dataframe_new.select('*', lit(date).alias("MATCH_DATE"), lit(city).alias("MATCH_CITY"),\
                                            lit(event_name).alias("EVENT_NAME"),lit(match_number).alias("MATCH_NUMBER"),lit(gender).alias("GENDER"),\
                                            lit(winner_team).alias("WINNER_TEAM"),lit(winned_by).alias("WINNED_BY"),lit(team_1).alias("FIRST_TEAM"),lit(team_2).alias("SECOND_TEAM"))

        dataframe_new = dataframe_new.withColumn("runs_scored_per_ball", regexp_replace(col("runs_scored_per_ball"), "(\{extras=)|(total=)|(batter=)|(\})", "")).withColumn("EXTRAS_EARNED_PER_BALL", trim(split(col("runs_scored_per_ball"), ',').getItem(0))).withColumn("TOTAL_RUNS_PER_BALL", trim(split(col("runs_scored_per_ball"), ',').getItem(1))).withColumn("BATTER_SCORED_RUNS_PER_BALL", trim(split(col("runs_scored_per_ball"), ',').getItem(2)))
        dataframe_new = dataframe_new.select('overs','FIRST_TEAM', 'SECOND_TEAM','EVENT_NAME','MATCH_DATE','MATCH_CITY','MATCH_NUMBER','GENDER','WINNER_TEAM','WINNED_BY','BATTER','BOWLER','NON_STRIKER','BATTER_SCORED_RUNS_PER_BALL','TOTAL_RUNS_PER_BALL', 'EXTRAS_EARNED_PER_BALL')
        conn = get_connection(logger, target_directory)

        # dataframe_new.write.mode("append").format("parquet").save(f"{target_directory}CLEAN/")
        # dataframe_new = dataframe_new.toPandas()
        # dataframe_new.to_csv(f"{target_directory}/CLEAN/")
        # print(dataframe_new.columns)
        # dataframe_new.show(10, False)
        dataframe_new = dataframe_new.toPandas()
        dataframe_new.to_sql(con = conn, name = 'ODI_CRICKET_RESULT', if_exists='replace')
        # dataframe_new.show(10, Fals
        # result = my_conn.execute(text("select * from male where WINNED_BY NOT like '%runs%'"))
        # print(result.all())
        print(f"{path}/{i}")
except Exception as e:
  logging.error("Exception occurred", exc_info=True)
   
