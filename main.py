import os
import mysql.connector
from dotenv import load_dotenv
load_dotenv()

# Connect to database

mydb = mysql.connector.connect (
    host=os.getenv('DB_HOST'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)

mycursor = mydb.cursor()

mycursor.execute("SHOW DATABASES")

for x in mycursor:
    print(x)

# VARIABLE = os.getenv('DB_HOST')

# print(VARIABLE)