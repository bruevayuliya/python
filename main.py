import psycopg2
import pandas as pd
from sqlalchemy import create_engine


class Data:
    def __init__(self, file_name):
        self.file = pd.read_json(file_name)


class Connection:

    def __init__(self, username, password, db):
        self.cursor = None
        self.engine = None
        self.username = username
        self.password = password
        self.db = db
        self.conn_string = f"postgresql://{username}:{password}@localhost/{db}"

    def create_engine_connection(self):
        self.engine = create_engine(self.conn_string).connect()
        return self.engine

    def establish_connection(self):
        connection_to_psql = psycopg2.connect(self.conn_string)
        connection_to_psql.autocommit = True
        self.cursor = connection_to_psql.cursor()

    def execute_query(self, query):
        self.establish_connection()
        return self.cursor.execute(query)

    def get_query_results(self):
        return self.cursor.fetchall()


# rooms = Data("rooms.json")
# students = Data("students.json")

connection = Connection('postgres', '25102020', 'task_1')
engine = connection.create_engine_connection()

# rooms.file.to_sql('rooms', con=engine, if_exists='replace', index=False)
# students.file.to_sql('students', con=engine, if_exists='replace', index=False)

query_string_1 = """select room, count(students.name)
from rooms
         join students on rooms.id = students.id
group by room;
"""

query_string_2 = """select room, avg(date_part('year', age(current_date, birthday::date))) as age
from students
         join rooms r on students.id = r.id
group by 1
order by 2 asc
limit 5;
"""

query_string_3 = """select max(date_part('year', age(current_date, birthday::date))), min(date_part('year', age(current_date, birthday::date))) as age, room
from students
         join rooms r on students.id = r.id
group by room
order by 1 desc
limit 5;
"""

query_string_4 = """with sex as
         (select students.sex as ss, rooms.name as rn
          from rooms
                   join students on rooms.id = students.room
          group by ss, rn)
select f.rn
from (select rn from sex where ss = 'F') as f
         join (select rn from sex where ss = 'M') as m
              ON f.rn = m.rn;
"""
list_of_queries = [query_string_1, query_string_2, query_string_3, query_string_4]

for query in list_of_queries:
    query_string = connection.execute_query(query)
    results = pd.DataFrame(connection.get_query_results(), index=None)
    results.to_json(f'/home/stamix/PycharmProjects/pythonProject3/results/query_{list_of_queries.index(query)}.json', index=False, orient='table')
