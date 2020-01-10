# Key Features

ChubaoDB is a cloud native distributed database with intelligent storage tiering. 

Our motivation is to build a scalable and fast enough database system which makes the frontend caching cluster not needed, but itself does not use too many RAM nodes. 


## multiple APIs

key-value interface: redis protocol compatible

SQL interface: MySQL protocol 

RESTful


## highly scalable and reliable

sharding by table ranges, multi-raft replication, logical split, dynamic rebalancing


## smart scheduling of storage tiers

according to the access temperature, intelligent transition of hot/warm/cold ranges among RAM (masstree), SSD (rocksdb) and disks (rocksdb on ChubaoFS).  

## others

other important features include distributed transactions, change data capture, online schema change, backup & recovery, auto incremental primary keys, et al. please read the docs. 








