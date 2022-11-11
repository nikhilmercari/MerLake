import mysql.connector as mysql
from mysql.connector import Error
import pandas as pd
df = pd.read_csv("customers.csv", delimiter=",", index_col=False)
try:
    conn = mysql.connect(host='localhost', database='customers_db', user='root', password='password')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)
        # cursor.execute('DROP TABLE IF EXISTS customers_data;')
        print('Creating table....')
# in the below line please pass the create table statement which you want #to create
        # cursor.execute("CREATE TABLE customers_data(identifierHash bigint, type varchar(255), country varchar(255), language varchar(255), socialNbFollowers int, socialNbFollows int, socialProductsLiked int, productsListed int, productsSold int, productsPassRate double, productsWished int, productsBought int, gender varchar(255), civilityGenderId int, civilityTitle varchar(255), hasAnyApp varchar(255), hasAndroidApp varchar(255), hasIosApp varchar(255), hasProfilePicture varchar(255), daysSinceLastLogin int, seniority int, seniorityAsMonths double, seniorityAsYears double, countryCode varchar(255))")
        print("Table is created....")
        #loop through the data frame
        for i,row in df.iterrows():
            #here %S means string values
            sql = "INSERT INTO customers_db.customers_data VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")
            # the connection is not auto committed by default, so we must commit to save our changes
            conn.commit()
except Error as e:
            print("Error while connecting to MySQL", e)