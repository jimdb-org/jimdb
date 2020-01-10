Change Data Capture 
=========================

overview
-----------------

the feature change data capture (cdc for short) that capturing data change and performed to the third-party application. For user, cdc supported function of table subscribe, throw the table’s data to a system such as MQ, KAFKA, mysql-server, or search, analyce, backup, or any specified system.

arch
-----------------
 
The main idea of data capturing is a pull model, the most benefit is simple to understand and easy to realize. Design consider point:
* lone process: do not depend on any others components of chubaodb, decrease risk for the whole system and reduce affected to data service core function.
* no status: do not maintain cdc progress and meta, esay logic for starting, stopping, and deploying.

design
-----------------
All cdc-server reserve theirs own subscribes pullers’subscribe info and progress in mysql-server tables. These meta is loaded by a cdc-server at server start, and is updated at subscribe/unsubscribe request is executed or by data capturing puller.

subscribe table scheme:
----------------------------------
::

    field               type             primary key     comment
    cluster_id          int              yes             point to a cluster
    db_name             varchar          yes             point to a db
    table_name          varchar          yes             point to a table
    subscribe_name      varchar          yes             a subscriber
    db_id               int              
    table id            int              
    master_addrs        varchar                          master address list of cluster
    sink                varchar                          thirdpart app config info
    cancel              int                              subscribe cancel flag
    owner               varchar                          task belong to cdc-server


progress table sheme:
---------------------------------------------------
::

    field                                type        primary key    comment
    db_name                              varchar     yes            point to a db
    table_name                           varchar     yes            point to a table
    subscribe_name                       varchar     yes            a subscriber
    range_id                             int         yes            point to a range
    progress_stage                       int                        puller stage(register/snapshot/increament_log/failed)
    progress_init_snapshot_version       int                        first checkpoint version
    progress_current_snapshot_version    int                        in using checkpoint version
    progress_last_snapshot_key           varchar                    snapshot iterator last key
    progress_last_log_index              int                        incremental log last index

When a cdc-server is start, it loads itself subscribe task by condition owner in meta table subscribe. And then load table range progress in meta table progress, up to now, range pullers are recoverd, they will work by progress_stage transform continuously.

Every range puller is running only in one thread at one time, that say there’s no concurrent problem neither capturing data nor transferring data out. The stage is a simple status machine, 
which from ‘register’to ‘snapshot’to ‘incremental_log’, each puller run only go forward, it can retry at one stage but never fallback. At each stage, a puller send a correspond request:

In register stage:
---------------------------------------------------
::

    message CdcRegisterRequest {
        uint64 range_id = 1;
        basepb.RangeEpoch range_epoch = 2; // current epoch for verification
        string subscriber_name = 3;
    }

    message CdcRegisterResponse {
        uint64 init_snapshot_version = 1;
    }

    In snapshot stage:
    message CdcChangeData {
        enum ChangeType {
            WRITE = 0;
            DELETE = 1;
        }

        ChangeType type = 1;
        bytes key = 2;
        bytes value = 3;
    }

    message CdcPullSnapshotRequest {
        uint64 range_id = 1;
        basepb.RangeEpoch range_epoch = 2;
        string subscriber_name = 3;

        uint64 current_snapshot_version = 4;
        string last_key         = 5;
    }

    message CdcPullSnapshotResponse {
        CDCError error = 1;
        uint64 current_snapshot_version = 2;
        repeated CdcChangeData snapshot_data = 3;
        bool end = 4;
    }

    In increamental_log stage:
    message CdcPullLogRequest {
        uint64 range_id = 1;
        basepb.RangeEpoch range_epoch = 2;
        string subscriber_name = 3;

        uint64 last_log_index = 4;
    }

    message CdcPullLogResponse {
        CDCError error = 1;
        repeated CdcChangeData log_data = 2;
        uint64 current_log_index = 3;
    }



these request is dealed by data-server provided cdc handles: 

* CdcRegister: Generate a snapshot version as progress_init_snapshot_version. The index is using data-server raft committed index.

* CdcPullSnapshot: Iterate snapshot data reply to cdc-server, if iterator is disabled, data-server

* CdcPullLog: Pull the raft log from progress_init_snapshot_version

limitation

* Each data change is pulled and pushed to external system as a single row change, any range data change has no relate to other, althrough chubaodb provides transaction semantic.

* A same row data may pulled repeated. At snapshot stage, checkpoint maybe execute serval times, and that lead to external system will comsume repeated changes.

* Change data has no before value.

* If a transaction intent is not committed or aborted, this may lead range puller thread blocked.
