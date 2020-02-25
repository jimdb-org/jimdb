# Schedule Design

## 1.1 General Structure

Schedule is the main part of JIMDB’s Master with scheduled job of Failover, Balance, Monitor etc. Schedule runs at 1-minute interval, within a cluster of masters, only one master which takes the lock can start Schedule.

Schedule jobs run serially, a job can only start after its previous job finished successfully, the image below shows the detail order of pipeline.

![design-schedule](http://img11.360buyimg.com/da/s800x800_jfs/t1/101978/7/13215/39352/5e54d435Edf4487b5/c437711d16d857c8.png)


## 1.2 NodeStateJob

The state of a data-server(ds) node will be marked as `update` or `offline` when it’s ready to Update or Offline, so NodeStateJob handles the changes of ds state.

If the state of a ds node is marked as Update, NodeStateJob will transfer all of the leaders(leader of range) on it to other nodes, state == Offline, all of the ranges(leaders & followers) will be transferred. 

## 1.3 RangeTypeJob

We dived data into two types, hot data on memory, and warm data which is on disk. Both of the ds node and range have a data-type property and they two must match. As a result, a range with a hot data type must be stored on a hot ds node, vice versa. This explains how RangeTypeJob works, it checks and transfers ranges whose data-type incompatible with ds node.

## 1.4 GCJob

GCJob, as its name suggests, is responsible for garbage collection and recycle.

When GCJob starts,

1)	Firstly, it checks NodeRange relation, a range is about to be deleted if the node which a range is stored on is offline state already.

2)	Secondly, comes to the RangeTable relation check, a range is going to be recycled if its table has been deleted.

3)	Lastly, TableDatabase relation is checked, a table will be recycled if its database has been dropped.


## 1.5 FailOverJob

FailOverJob handles health inspection for ranges. Ranges need to be repaired include:

1)	Range that with less replicas than table replicas, new replicas of range will be created.

2)	Range that has more replicas than table’s, the replica of range on busiest node will be deleted.

3)	Range that exists any dead replica (lost heartbeat over 5 mins), this range then becomes invalid and will be removed further.


## 1.6 BalanceJob

Leader balance and range balance are included in BalanceJob, to make sure the count of leader and range is kind of average on each node. Leader balance processes first because of a lower price, a leader transfer is enough. Then it checks the ranges count of each ds node and transfer range from the ds node with  max count of ranges to the least one.

## 1.7 MonitorJob

Just a job for monitor info, data like bytes read and write for range, or range count of a table, is collected and pushed to Prometheus and viewed via Grafana.


## A Brief Introduction To Split-Range

### General Plan

Usually the administrator will set a default max limitation for the count of ranges on a ds node, when the max limitation number is reached, the ds node will send a request to master and ask for range split.

### Details about Split

* Master gets the specific range that wants to split after receiving a ‘askSplit’ request

* Return error of ‘ErrorType_NotAllowSplit’ if property of ‘auto_split’ == false

* Check data validity, include the table which the range belongs to exists or not, table running or not

* Check the version of range, return error if version in request less than version in cache of Master

* Create new peers with number of table replicas

* Return new range-id and peer-ids to ds node
