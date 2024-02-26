from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError


def get_connection(logger, path_of_database):
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







# try:
    
#     result = my_conn.execute(text("select name from sqlite_master"))
#     print(result.all())
# except SQLAlchemyError as e:
#     # error = str(e.__dict__['org'])
#     print(e)
