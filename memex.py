#! /usr/bin/python3.4


# Noel Carrascal
# September 9, 20155
# Python program to benchmark database transaction.
import sys, getopt
import random
import threading
import time
import logging

from memsql.common import database

def get_connection():
    return database.connect(host='127.0.0.1', port='3306', user='root', password='', database='MemEx')

def create_table():
    try:
        with get_connection() as conn:
            conn.execute("DROP TABLE IF EXISTS SCANS")
            conn.execute("CREATE TABLE SCANS( \
                          SCAN_ID BIGINT AUTO_INCREMENT NOT NULL, \
                          SCAN_HASH VARCHAR(11) NOT NULL, \
                          SCAN_TYPE VARCHAR(3), \
                          SCAN_COUNT INT, \
                          MACHINE_TYPE VARCHAR(10), \
                          SEQUENCE_CODE VARCHAR(5), \
                          LOAD_DATE TIMESTAMP, \
                          PRIMARY KEY (SCAN_ID))")
            #conn.execute("SHOW TABLES")
            #fields = conn.get("SHOW TABLES")
            #for field in fields:
            #    print("Test",field[:],"Type:",type(fields),len(fields))
    except database.MySQLError:
        print("Exception: problem dropping or creating the table.")

def custom_sql_query(loops, query):
    try:
        for i in range(loops):
            with get_connection() as conn:
                conn.execute(query)
                #fields = cur.fetchall()
                #for field in fields:
                #    print(field[:])
    except database.MySQLError:
        print("Exception: Custom sql query failed.")

def single_db_entry(p,r,tp,i,of):
    if p[r] < 7:
        try:
            with get_connection() as conn:
                conn.execute("""INSERT INTO SCANS (SCAN_HASH, SCAN_TYPE, SCAN_COUNT, MACHINE_TYPE, SEQUENCE_CODE) VALUES \
                               ("%s", "%s", %d, "%s", "%s")""" % (str((r+i*tp)+of),'ttt',p[r],'abcdefghij','vwxyz'))
                p[r] = p[r] + 1
        except database.MySQLError:
            print("Exception: Insert.")
    else:
        if r == (tp-1):
            single_db_entry(p,0,tp,i,of)
        else:
            single_db_entry(p,r+1,tp,i,of)

def write_only_db(parcels,loops,i,of):
    tot_parcels = parcels * loops
    p = [0] * tot_parcels          # Init list of parcels and number of scans as value initialized to zero.
    num_scans = tot_parcels * 7
    time = 0
    for j in range(num_scans):
        r = random.randrange(0,tot_parcels,1)
        single_db_entry(p,r,tot_parcels,i,of)

def single_db_read_SCAN_HASH(loops,shash):
    try:
        for i in range(loops):
            with get_connection() as conn:
                conn.execute("""SELECT * FROM SCANS WHERE SCAN_HASH = ("%s")""" % (shash))
                #fields = conn.fetchall() # Just fetch results. No need to display results, I/O delay
                #for field in fields:
                #    print(field[:])
    except database.MySQLError:
        print("Exception: Read table by SCAN_HASH failed.")

def single_db_read_SCAN_ID(loops,sid):
    try:
        for i in range(loops):
            with get_connection() as conn:
                conn.execute("""SELECT * FROM SCANS WHERE SCAN_ID = ("%s")""" % (int(sid)))
                #fields = cur.fetchall() # Just fetch the results. No need to display results, I/O delay
    except database.MySQLError:
        print("Exception: Read table by SCAN_ID failed.")

if __name__ == "__main__":
    if sys.argv[1] == "-c":
        create_table()
    elif sys.argv[1] == "-wo":
        a = list()
        parcels = int(sys.argv[2])  # number of parcels
        ll = int(sys.argv[3])       # Number of loops for each thread.
        th = int(sys.argv[4])       # Number of threads
        of = int(sys.argv[5])       # When runing it sequentially create SCAN_HASH starting from this number
        for i in range(th):
            a.append(threading.Thread(target=write_only_db, args=(parcels,ll,i,of)))
            a[i].setDaemon(True)
            a[i].start()

        for k in range(th):
            a[k].join()   # enter time (s) that join will wait for exiting threads

    elif sys.argv[1] == "-ro":
        a = list()
        parcels = int(sys.argv[2])  # number of reads
        ll = int(sys.argv[3])       # number of loops
        th = int(sys.argv[4])       # number of threads
        rt = sys.argv[5]            # read type
        n = int(sys.argv[6])                 # Know before hand SCAN_ID, SCAN_HASH numbers
        if rt == "SCAN_ID":
            for i in range(th):
                a.append(threading.Thread(target=single_db_read_SCAN_ID, args=(ll,random.randrange(0,n,1))))
                a[i].setDaemon(True)
                a[i].start()

            for k in range(th):
                a[k].join()    # enter time (s) that join will wait for exiting threads
        if rt == "SCAN_HASH":
            for i in range(th):
                a.append(threading.Thread(target=single_db_read_SCAN_HASH, args=(ll,random.randrange(0,n,1))))
                a[i].setDaemon(True)
                a[i].start()

            for k in range(th):
                a[k].join()    # enter time (s) that join will wait for exiting threads

    elif sys.argv[1] == "-rw":
        a = list()
        parcels = int(sys.argv[2])  # number of reads
        ll = int(sys.argv[3])       # number of loops
        th = int(sys.argv[4])       # number of threads
        rt = sys.argv[5]            # read type
        n = argv[6]                 # Know before hand SCAN_ID, SCAN_HASH numbers
        for i in range(0,th,2):
            a.append(threading.Thread(target=write_only_db, args=(parcels,ll,(i/2))))
            a[i].setDaemon(True)
            a[i].start()
            a.append(threading.Thread(target=single_db_read_SCAN_HASH, args=(ll,random.randrange(0,n,1))))
            a[i+1].setDaemon(True)
            a[i+1].start()

        for k in range(0,th,2):
            a[k].join()    # enter time (s) that join will wait for exiting threads
            a[k+1].join()    # enter time (s) that join will wait for exiting threads
    elif sys.argv[1] == "-cu":
        a = list()
        parcels = int(sys.argv[2])  # number of custom sql queries per loop
        ll = int(sys.argv[3])       # number of loops
        th = int(sys.argv[4])       # number of threads
        rt = sys.argv[5]            # read type
        for i in range(th):
            a.append(threading.Thread(target=custom_sql_query, args=(ll,sys.argv[7])))
            a[i].setDaemon(True)
            a[i].start()

        for k in range(th):
            a[k].join()    # enter time (s) that join will wait for exiting threads
