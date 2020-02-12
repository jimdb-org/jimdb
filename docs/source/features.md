# Key Features

JIMDB is a cloud-native key-value and SQL database with intelligent storage tiering

## Highly Scalable and Reliable

sharding by table ranges, multi-raft replication, logical split, dynamic rebalancing


## Multiple APIs

key-value interface: redis protocol compatible

SQL interface: MySQL protocol


## Smart Scheduling of Storage Tiers

according to the access temperature and QoS(Quality of Service), intelligent transition of hot/warm/cold ranges among RAM (masstree), SSD (rocksdb) and disks (rocksdb on CFS) 


## Distributed Transactions

read committed, currently implemented

txn record, intent, version, 2PC


## Cloud Native

Orchestrated by Kubernetes


## Esay to Use

change data capture

online schema change

backup & recovery

management system, alarm system, rich monitoring report





