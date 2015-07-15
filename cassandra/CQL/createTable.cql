CREATE KEYSPACE demo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

CREATE TABLE load_averages(
m1 float,
m5 float,
m10 float,
ts timeuuid,
host text,
PRIMARY KEY (ts, host)
);


