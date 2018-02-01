import sqlite3
import atexit
import sys
_conn = sqlite3.connect("world.db")


def _close_db():
    _conn.commit()
    _conn.close()

atexit.register(_close_db)

def create_tables():
   _conn.executescript("""
    CREATE TABLE tasks (
       id      INTEGER         PRIMARY KEY,
       task_name    TEXT        NOT NULL,
       worker_id INTEGER REFERENCES workers(id),
       time_to_make INTEGER NOT NULL,
       resource_name TEXT NOT NULL,
       resource_amount INTEGER NOT NULL
        );
    CREATE TABLE workers (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        status TEXT NOT NULL
         );
    CREATE TABLE resources(
        name TEXT PRIMARY KEY,
        amount INTEGER NOT NULL
        );
    
    """)
def addWorker(line):
    Words = line.split(',')
    _conn.execute("""
        INSERT INTO workers(id,name,status) VALUES (?,?,?)
    """,[Words[1],Words[2],'idle'])

def addResource(Words):
    _conn.execute("""
        INSERT INTO resources(name,amount) VALUES(?,?) 
    """,[Words[0],Words[1]])

def addTask(Words,task_counts):
    _conn.execute("""
        INSERT INTO tasks(id,worker_id,task_name,time_to_make,resource_name,resource_amount) VALUES(?,?,?,?,?,?)
    """,[task_counts,Words[1],Words[0],Words[4],Words[2],Words[3]])

def main(path):
   table_exist = "SELECT name FROM sqlite_master WHERE type='table' AND name='tasks'"
   if not _conn.execute(table_exist).fetchone():
        create_tables()
        tasks_count = 1
        file = open(path)
        lines = file.readlines()
        file.close()
        for line in lines:
            line = line.strip()
            if line.find('worker') != -1:
                addWorker(line)
            else:
                Words = line.split(',')
                if Words.__len__() == 2:##this is a resource
                    addResource(Words)
                else:
                   addTask(Words,tasks_count)
                   tasks_count = tasks_count +1



main(sys.argv[1])
