import pymysql
from pymysql import DatabaseError
import json
import collections

class DatabaseNotExistsError(Exception):
    """Custom Exception when Database not Exist"""
    pass

class MySQLConnectionError(Exception):
    """Custom Exception when MySQL server Not Connected"""
    pass

# Set database connection
db_name = ""
connection = pymysql.connect(host='',
                             user='',
                             password='',
                             db=db_name,
                             cursorclass=pymysql.cursors.DictCursor)

try:
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    # Query database table
    cursor.execute("""
                SELECT ID, FirstName, LastName, Street, City, ST, Zip
                FROM Students
                """)
    rows = cursor.fetchall()
except DatabaseError  as e:
    if e.args[1] == '#42000Unknown database \'{0}\''.format(db_name):
        raise DatabaseNotExistsError
    else:
        raise MySQLConnectionError

# Convert query to objects of key-value pairs
objects_list = []
for row in rows:
    d = collections.OrderedDict()
    d['id'] = row.ID
    d['FirstName'] = row.FirstName
    d['LastName'] = row.LastName
    d['Street'] = row.Street
    d['City'] = row.City
    d['ST'] = row.ST
    d['Zip'] = row.Zip
    objects_list.append(d)

json_object = json.dumps(objects_list)
objects_file = 'student_objects.json'
with open(objects_file, 'w') as file:
    file.write(json_object)


connection.close()
