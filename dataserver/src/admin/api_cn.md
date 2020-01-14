## SET_CONFIG
配置项的值都为string，不同的数据类型都转为string。     
支持的配置项（section.name)：            

- log.level      
级别：DEBUG, INFO, WARN, ERR, CRIT         
大小写均可   

- range.max_size         
整型，单位为字节
- range.split_size       
整型，单位为字节    
- range.check_size      
整型，单位为字节


以下为可在运行期修改的rocksdb参数   
设置时具体传值请参考rocksdb头文件。

- rocksdb.disable_auto_compactions    
- rocksdb.write_buffer_size       
- rocksdb.max_write_buffer_number        
- rocksdb.soft_pending_compaction_bytes_limit          
- rocksdb.hard_pending_compaction_bytes_limit       
- rocksdb.level0_file_num_compaction_trigger    
- rocksdb.level0_slowdown_writes_trigger        
- rocksdb.level0_stop_writes_trigger        
- rocksdb.target_file_size_base     
- rocksdb.target_file_size_multiplier
- rocksdb.max_bytes_for_level_base
- rocksdb.max_bytes_for_level_multiplier
- rocksdb.report_bg_io_stats
- rocksdb.paranoid_file_checks
- rocksdb.compression
- rocksdb.max_background_jobs
- rocksdb.base_background_compactions
- rocksdb.max_background_compactions
- rocksdb.avoid_flush_during_shutdown
- rocksdb.delayed_write_rate
- rocksdb.max_total_wal_size
- rocksdb.delete_obsolete_files_period_micros
- rocksdb.stats_dump_period_sec
- rocksdb.max_open_files
- rocksdb.bytes_per_sync
- rocksdb.wal_bytes_per_sync
- rocksdb.compaction_readahead_size




## GET_CONFIG
与配置文件ds.conf一致，如 log.level

## GET_INFO
参数为string类型的path，多级路径使用`.`分隔        
目前支持的path路径：  
   
- sever 或者 path传空       
返回server的运行状态总结

- rocksdb       
返回rocksdb相关的信息

- range     
后面可以跟range id， 如`range.123`表示获取range id=123的range信息。      
不跟range id（path=range）返回range整体信息，如range个数等

- raft      
后面可以跟raft id(range id)，如`raft.123`表示获取 id=123 的raft信息。   
不加id (path=raft)返回raft整体信息，如raft总个数、快照计数等。

## ForceSplit
强制分裂某个range     
// TODO: 暂不支持保留第一主键在同一个range的分裂
##  Compaction
手动compaction，可选指定compaction某个range范围内的数据。      
不指定compaction整个db。
##  ClearQueue
清除指定类型的worker队列，并返回清除了多少个。
## GetPendings
待实现
##  FlushDB
rocksdb flushdb操作，wait为true表示同步等待操作完成。



