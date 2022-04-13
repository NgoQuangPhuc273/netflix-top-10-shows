import psycopg2
import pandas as pd

def connect():
    try: 
        conn = psycopg2.connect("host=127.0.0.1 dbname=myfirstdb user=postgres password=Neverland1")
        # print("Successfully connected!")
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get cursor to the Postgres database")
        print(e)

    # conn.set_session(autocommit=True)
    
    return conn, cur

# Note 1: Create a database
# try:
#     cur.execute('Create database myfirstdb')
# except psycopg2.Error as e:
#     print(e)

def create_table():
    conn, cur = connect()
    try:
        cur.execute('CREATE TABLE IF NOT EXISTS students (student_id INT PRIMARY KEY, name VARCHAR(30), age INT, gender VARCHAR(30), subject VARCHAR(30), marks INT);')
    except psycopg2.Error as e:
        print("Error: Issue in creating table")
        print(e)

    conn.commit()


#Note 2:  Inserting values
def insert():
    conn, cur = connect() 
    try:
        cur.execute('INSERT INTO students (student_id, name, age, gender, subject, marks) VALUES (%s,%s,%s,%s,%s,%s)', (1, "Tran Phuong Nga", 20, "Female", "Math", 100))
        cur.execute('INSERT INTO students (student_id, name, age, gender, subject, marks) VALUES (%s,%s,%s,%s,%s,%s)', (2, "Nguyen Canh Minh", 20, "Male", "CS", 99))
        cur.execute('INSERT INTO students (student_id, name, age, gender, subject, marks) VALUES (%s,%s,%s,%s,%s,%s)', (3, "Duong Quang Minh", 20, "Male", "EE", 98))
        cur.execute('INSERT INTO students (student_id, name, age, gender, subject, marks) VALUES (%s,%s,%s,%s,%s,%s)', (4, "Vu Nhat Anh", 20, "Male", "CS", 97))
        cur.execute('INSERT INTO students (student_id, name, age, gender, subject, marks) VALUES (%s,%s,%s,%s,%s,%s)', (5, "Tran Tuan Hiep", 20, "Male", "CS", 96))
        cur.execute('INSERT INTO students (student_id, name, age, gender, subject, marks) VALUES (%s,%s,%s,%s,%s,%s)', (6, "Ngo Tuan Quang", 15, "Male", "Basketball", 95))

    except psycopg2.Error as e:
        print("Error: Issue in inserting row 1-6.")
        print(e)
    
    conn.commit() 

def fetch():
    conn, cur = connect()
    try: 
        cur.execute('SELECT * FROM students')
    except psycopg2.Error as e:
        print("Error: Issue in selecting.")
        print(e)

    # data = cur.fetchone()
    data = cur.fetchall()
    return data

def print(data):
    conn, cur = connect()
    # data = cur.fetchall()
    # print(data)

    # while data:
    #     print(data)
    #     data = cur.fetchone()

    print('Query result: ') 
    print() 

    for row in data: 
  
        # printing the columns 
        print('id: ', row[0]) 
        print('name: ', row[1]) 
        print('age: ', row[2]) 
        print('gender: ', row[3]) 
        print('subject: ', row[4]) 
        print('marks: ', row[5]) 
        print('----------------------------------') 
    cur.close()
    conn.close()

if __name__ == '__main__': 
    # connect()
    create_table()

    insert()

    data = fetch()

    print(data)

