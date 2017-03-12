DROP KEYSPACE IF EXISTS tiger;

CREATE KEYSPACE tiger WITH replication= {'class':'SimpleStrategy', 'replication_factor':1};

USE tiger;

CREATE TABLE IF NOT EXISTS iotdata (datatime text, dataid text, datavalue double, datatype text, datarank int, PRIMARY KEY(datatime));
CREATE TABLE IF NOT EXISTS iotdatakey (datatype text, datavalue double, PRIMARY KEY(datatype));

