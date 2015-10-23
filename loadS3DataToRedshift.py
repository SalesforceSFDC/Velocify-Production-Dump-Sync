##!/usr/bin/python
import psycopg2
import pprint
import requests
import xmltodict, json
import urllib
import datetime
import csv
import threading
from threading import Thread
import sys

reload(sys);
sys.setdefaultencoding("utf-8");

conn_string = "dbname='sunedison' port='5439' user='XXXX' password='XXXX' host='sunedisondatawarehouse.cgnr3c8sn1sz.us-west-2.redshift.amazonaws.com'";
print "Connecting to database";
conn = psycopg2.connect(conn_string);
print "Connected to DB";

def deleteBackupTables():
	cursor = conn.cursor();
	cursor.execute("delete from actionlog_qa");
	cursor.execute("delete from assignmentlog_qa");
	cursor.execute("delete from creationlog_qa");
	cursor.execute("delete from distributionlog_qa");
	cursor.execute("delete from exportlog_qa");
	cursor.execute("delete from emaillog_qa");
	cursor.execute("delete from leads_qa");
	cursor.execute("delete from statuslog_qa");
	conn.commit();
	print "Backup tables deleted!!";

def backupProdTables():
	cursor = conn.cursor();
	cursor.execute("insert into actionlog_qa select * from actionlog");
	print "actionlog_qa copied";
	cursor.execute("insert into assignmentlog_qa select * from assignmentlog");
	print "assignmentlog copied";
	cursor.execute("insert into creationlog_qa select * from creationlog");
	print "creationlog copied";
	cursor.execute("insert into distributionlog_qa select * from distributionlog");
	print "distributionlog copied";
	cursor.execute("insert into exportlog_qa select * from exportlog");
	print "exportlog copied";
	cursor.execute("insert into emaillog_qa select * from emaillog");
	print "emaillog copied";
	cursor.execute("insert into leads_qa select * from leads");
	print "leads copied";
	cursor.execute("insert into statuslog_qa select * from statuslog");
	print "statuslog copied";
	print "Production tables copied.";
	conn.commit();

def deleteProdTables():
	cursor = conn.cursor();	
	cursor.execute("delete from actionlog");
	cursor.execute("delete from assignmentlog");
	cursor.execute("delete from creationlog");
	cursor.execute("delete from distributionlog");
	cursor.execute("delete from exportlog");
	cursor.execute("delete from emaillog");
	ursor.execute("delete from leads");
	cursor.execute("delete from statuslog");
	print "Production tables deleted!!";
	conn.commit();

def copyProdTablesFromS3():
	cursor = conn.cursor();
	print "statuslog copied to redshift";
	print "Production tables ready for use!!"

deleteBackupTables();
backupProdTables();
deleteProdTables();
copyProdTablesFromS3();
