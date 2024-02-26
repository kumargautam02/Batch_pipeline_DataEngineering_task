from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError


def get_connection(logger, path_of_database):
    """
        This Python-function is used to create cricket.db database inside the DATABASE path of project directory, 
        if  ODI_CRICKET_RESULT is not present in the cricket.db database then, it will create ODI_CRICKET_RESULT table, and will return the connection.
        arguments :
        logger - This is the logger object, used to log info.
        path_of_database[Data-Type - python-string] - This contains the path of the project_directory to created a "cricket.db". 
        return : 
            my_conn - This contains the connection obj of the sqlite databse. 
    """
    path = (f"sqlite:///{path_of_database}DATABASE/cricket.db")
    print(path)
    my_conn = create_engine(path)
    my_conn = my_conn.connect()
    result = my_conn.execute(text("select name from sqlite_master"))
    for tables in result.all():
        if tables[0] != "ODI_CRICKET_RESULT":
            create_table = "CREATE TABLE IF NOT EXISTS ODI_CRICKET_RESULT(overs INTEGER NOT NULL, \
            FIRST_TEAM VARCHAR(30), SECOND_TEAM VARCHAR(30), EVENT_NAME VARCHAR(60), MATCH_DATE DATE , \
            MATCH_CITY VARCHAR(50), MATCH_NUMBER INTEGER, GENDER VARCHAR(50), WINNER_TEAM VARCHAR(50), WINNED_BY VARCHAR(50),\
            BATTER VARCHAR(50), BOWLER VARCHAR(50), NON_STRIKER VARCHAR(50), BATTER_SCORED_RUNS_PER_BALL VARCHAR(50), TOTAL_RUNS_PER_BALL VARCHAR(50), EXTRAS_EARNED_PER_BALL VARCHAR(50))"
            my_conn.execute(text(create_table))
            logger.info("ODI_CRICKET_RESULT table created successfully")
    return my_conn