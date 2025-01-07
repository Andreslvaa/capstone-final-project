import mysql.connector
import psycopg2

connection = mysql.connector.connect(user='root', password='YOYR2dnY12YfL2VkP6b2PjTI',host='172.21.211.35',database='sales')



dsn_hostname = '172.21.178.218'
dsn_user='postgres'        # e.g. "abc12345"
dsn_pwd ='KcJyhwyqrPLEBOB2GK5EQWJv'      # e.g. "7dBZ3wWt9XN6$o0J"
dsn_port ="5432"                # e.g. "50000" 
dsn_database ="postgres"           # i.e. "BLUDB"


# create connection

conn = psycopg2.connect(
   database=dsn_database, 
   user=dsn_user,
   password=dsn_pwd,
   host=dsn_hostname, 
   port= dsn_port
)

cursor = conn.cursor()
cursorSQL = connection.cursor()

# Find out the last rowid from DB2 data warehouse or PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database or PostgreSql.

def get_last_rowid():
    cursor.execute("SELECT MAX(rowid) FROM sales_data;")
    result = cursor.fetchone() 
	
     # Extract the last rowid from the query result
    last_row_id = result[0] if result and result[0] is not None else None

    return last_row_id



# Usage
if __name__ == "__main__":
    last_row_id = get_last_rowid()
    print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.
def get_latest_records(rowid):
    cursor = connection.cursor()
	
    query = "SELECT * FROM sales_data WHERE rowid > %s;"
    cursor.execute(query, (last_row_id,))

    records = cursor.fetchall()

  

    return records

if __name__ == "__main__":
    last_row_id = 12289  

    new_records = get_latest_records(last_row_id)

    print("New rows on staging data warehouse =", len(new_records))


# Insert the additional records from MySQL into DB2 or PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database or PostgreSql.

def insert_records(records):
    cursor = connection.cursor()
    query = "INSERT INTO sales_data (rowid, product_id, customer_id, price, quantity, timeestamp) VALUES (%s, %s, %s, %s, %s, %s)"
        
    cursor.executemany(query, records)
        
    conn.commit()
  

if __name__ == "__main__":

    new_records = [
        (1, 1254, 1254, 1284, 1, '2024-12-10 10:00:00'),
        (2, 4301, 4301, 4381, 1, '2024-12-10 10:15:00'),
    ]

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))


cursor.close()
conn.close()
# disconnect from mysql warehouse

# disconnect from DB2 or PostgreSql data warehouse 

# End of program