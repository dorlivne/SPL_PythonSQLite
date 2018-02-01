import sqlite3

_conn = sqlite3.connect("world.db")
print_list = {}
busy_workers = {}
def assign_task(worker_id,task_name):
    c = _conn.cursor()

    c.execute("""
            SELECT resource_name,resource_amount FROM tasks WHERE task_name = ?
            """,[task_name])
    task_details = c.fetchone()
    ##deacreasing resources
    resource_name = task_details[0]
    resource_amount = task_details[1]
    _conn.execute("""
    UPDATE resources
    SET amount = amount -?
    WHERE name = ?
    """,[resource_amount,resource_name])
    ##updating busy status
    _conn.execute("""
    UPDATE workers
    SET status = "busy"
    WHERE id = ?
    """,[worker_id])
    c.execute("""
    SELECT name FROM workers WHERE id = ?
    """,[worker_id])
    print(c.fetchone()[0] + " says: work work")
    _conn.commit()
def execute_iteration(worker_id,task_name):
    c = _conn.cursor()
    if busy_workers[worker_id] != task_name:
        return
    c.execute("""
    SELECT workers.name
    FROM workers
    WHERE id = ?                           
    """,[worker_id])
    name = c .fetchone()[0]
    c.execute("""
    SELECT time_to_make
    FROM tasks
    WHERE task_name = ?
    """,[task_name])
    tuple = c.fetchone()
    time = tuple[0]
    print(name + " is busy " + task_name +"...")
    _conn.execute("""
    UPDATE tasks
    SET time_to_make = time_to_make -1
    WHERE task_name = ?
    """,[task_name])
    time = time -1
    if time == 0 :##final iteration
        print_list[worker_id] = name + " says: All Done!"

def main():
    c = _conn.cursor()
    test_dict = {}
    stop = False
    while not stop:
     c.execute("""         
     SELECT * FROM tasks
     ORDER BY id
     """)
     list_of_tasks = c.fetchall()
     stop = True
     c.execute("""
     SELECT time_to_make FROM tasks
     """)
     time_to_make =c.fetchall()
     for timeleft in time_to_make:
         if timeleft[0] > 0 :
            stop = False
            break
     for list in list_of_tasks:
            if list[3] > 0:
                     c.execute("""
                          SELECT status FROM workers WHERE id = ?
                           """,[list[2]])
                     if c.fetchone()[0].__eq__("idle"):
                          busy_workers[list[2]] = list[1]
                          assign_task(list[2],list[1])
                     else:
                         execute_iteration(list[2],list[1])

     while print_list.__len__() > 0 :
         keys = print_list.keys()
         for key in keys:
             _conn.execute("""
                    UPDATE workers
                    SET status = "idle"
                    WHERE id = ?
                    """, [key])
             _conn.commit()
             print(print_list[key])
             del busy_workers[key]
         print_list.clear()





main()
