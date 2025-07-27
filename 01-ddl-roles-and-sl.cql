--CREATE KEYSPACE kstest1 WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy','datacenter1' : 3};
CREATE KEYSPACE kstest1 WITH replication = { 'class' : 'NetworkTopologyStrategy','AWS_US_EAST_1': '3'} AND durable_writes = true AND tablets = {'enabled': false};
use kstest1; 

CREATE TABLE tbtest1 (
   partitionkey1 text,
   clusterkey1 text,
   data1 text,
   data2 text,
   PRIMARY KEY(partitionkey1, clusterkey1)
   );

--CREATE ROLE IF NOT EXISTS app1_user1 WITH PASSWORD = 'YOUR_PASSWORD_HERE' AND LOGIN = true;
--CREATE SERVICE_LEVEL IF NOT EXISTS SL_APP1 WITH SHARES = 1000;
--ATTACH SERVICE_LEVEL SL_APP1 TO app1_user1;
--GRANT SELECT on KEYSPACE kstest1 TO app1_user1;
--GRANT MODIFY on KEYSPACE kstest1 TO app1_user1;
