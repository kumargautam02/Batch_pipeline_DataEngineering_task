from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

path = ("sqlite:///D:/DataSlush_project/database/cricket.db")

my_conn = create_engine(path)
my_conn = my_conn.connect()



try:
    

    result = my_conn.execute(text("select name from sqlite_master"))
    print(result.all())
except SQLAlchemyError as e:
    # error = str(e.__dict__['org'])
    print(e)
