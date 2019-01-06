import pyodbc
from connect_db import connect_db
from datetime import datetime

def loadRentalPlan(filename, conn):
    """
        Input:
            $filename: "RentalPlan.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "RentalPlan" in the "VideoStore" database on Azure
            2. Read data from "RentalPlan.txt" and insert them into "RentalPlan"
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    # WRITE YOUR CODE HERE

    conn.execute("create table RentalPlan (pid INT, pname VARCHAR(50), monthly_fee FLOAT, max_movies INT, primary key(pid))")

    file = open(filename, "r")

    lines =  file.readlines()
    for line in lines:
    	val = line.split("|")
    	#print (val[1])
    	pid = int(val[0])
    	pname = val[1]
    	monthly_fee = float(val[2])
    	max_movies = int(val[3])

    	conn.execute("insert into RentalPlan values(?, ?, ?, ?)", pid, pname, monthly_fee, max_movies)


    file.close()


def loadCustomer(filename, conn):
    """
        Input:
            $filename: "Customer.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "Customer" in the "VideoStore" database on Azure
            2. Read data from "Customer.txt" and insert them into "Customer".
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    # WRITE YOUR CODE HERE

    conn.execute("create table Customer (cid INT, pid INT, username VARCHAR(50), password VARCHAR(50), primary key(cid), foreign key (pid) references RentalPlan(pid) on delete cascade)")

    file = open(filename, "r")
    
    lines =  file.readlines()
    for line in lines:
    	val = line.split("|")

    	cid = int(val[0])
    	pid = int(val[1])
    	username = val[2]
    	password = val[3].strip()

    	#print (password)

    	conn.execute("insert into Customer values(?, ?, ?, ?)", cid, pid, username, password)


    file.close()


def loadMovie(filename, conn):
    """
        Input:
            $filename: "Movie.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "Movie" in the "VideoStore" database on Azure
            2. Read data from "Movie.txt" and insert them into "Movie".
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    # WRITE YOUR CODE HERE

    conn.execute("create table Movie (mid INT, mname VARCHAR(50), year INT, primary key(mid))")

    file = open(filename, "r")
    
    lines =  file.readlines()
    for line in lines:
    	val = line.split("|")

    	mid = int(val[0])
    	mname = val[1]
    	year = int(val[2])
    	#print(year)

    	conn.execute("insert into Movie values(?, ?, ?)", mid, mname, year)


    file.close()

def loadRental(filename, conn):
    """
        Input:
            $filename: "Rental.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "Rental" in the VideoStore database on Azure
            2. Read data from "Rental.txt" and insert them into "Rental".
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    # WRITE YOUR CODE HERE


    conn.execute("create table Rental (cid INT, mid INT, date_and_time DATETIME, status VARCHAR(10), foreign key (cid) references Customer(cid), foreign key (mid) references Movie(mid) on delete cascade)")

    file = open(filename, "r")
    
    lines =  file.readlines()
    for line in lines:
    	val = line.split("|")

    	cid = int(val[0])
    	mid = int(val[1])
    	date_and_time = datetime.strptime(val[2], '%Y-%m-%d %H:%M:%S') #2018-02-28 15:28:39
    	status = val[3].strip()
    	#print(status)

    	conn.execute("insert into Rental values(?, ?, ?, ?)", cid, mid, date_and_time, status)


    file.close()


def dropTables(conn):
    conn.execute("DROP TABLE IF EXISTS Rental")
    conn.execute("DROP TABLE IF EXISTS Customer")
    conn.execute("DROP TABLE IF EXISTS RentalPlan")
    conn.execute("DROP TABLE IF EXISTS Movie")



if __name__ == "__main__":
    conn = connect_db()

    dropTables(conn)

    loadRentalPlan("RentalPlan.txt", conn)
    loadCustomer("Customer.txt", conn)
    loadMovie("Movie.txt", conn)
    loadRental("Rental.txt", conn)


    conn.commit()
    conn.close()






