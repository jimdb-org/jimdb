/*
 * Copyright 2019 The JIMDB Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package io.jimdb.common.exception;

/**
 * Definition of error code that is compatible with MySQL 's error code definition
 *
 */
public enum ErrorCode {
  // Mysql errorcode
  ER_HASHCHK(1000, "HY000", "hashchk"),
  ER_NISAMCHK(1001, "HY000", "isamchk"),
  ER_NO(1002, "HY000", "NO"),
  ER_YES(1003, "HY000", "YES"),
  ER_CANT_CREATE_FILE(1004, "HY000", "Can't create file '%s' (errno"),
  ER_CANT_CREATE_TABLE(1005, "HY000", "Can't create table '%s' (errno"),
  ER_CANT_CREATE_DB(1006, "HY000", "Can't create database '%s' (errno"),
  ER_DB_CREATE_EXISTS(1007, "HY000", "Can't create database '%s'; database exists"),
  ER_DB_DROP_EXISTS(1008, "HY000", "Can't drop database '%s'; database doesn't exist"),
  ER_DB_DROP_DELETE(1009, "HY000", "Error dropping database (can't delete '%s', errno"),
  ER_DB_DROP_RMDIR(1010, "HY000", "Error dropping database (can't rmdir '%s', errno"),
  ER_CANT_DELETE_FILE(1011, "HY000", "Error on delete of '%s' (errno"),
  ER_CANT_FIND_SYSTEM_REC(1012, "HY000", "Can't read record in system table"),
  ER_CANT_GET_STAT(1013, "HY000", "Can't get status of '%s' (errno"),
  ER_CANT_GET_WD(1014, "HY000", "Can't get working directory (errno"),
  ER_CANT_LOCK(1015, "HY000", "Can't lock file (errno"),
  ER_CANT_OPEN_FILE(1016, "HY000", "Can't open file"),
  ER_FILE_NOT_FOUND(1017, "HY000", "Can't find file"),
  ER_CANT_READ_DIR(1018, "HY000", "Can't read dir of '%s' (errno"),
  ER_CANT_SET_WD(1019, "HY000", "Can't change dir to '%s' (errno"),
  ER_CHECKREAD(1020, "HY000", "Record has changed since last read in table '%s'"),
  ER_DISK_FULL(1021, "HY000", "Disk full (%s); waiting for someone to free some space... (errno"),
  ER_DUP_KEY(1022, "23000", "Can't write; duplicate key in table '%s'"),
  ER_ERROR_ON_CLOSE(1023, "HY000", "Error on close of '%s' (errno"),
  ER_ERROR_ON_READ(1024, "HY000", "Error reading file '%s' (errno"),
  ER_ERROR_ON_RENAME(1025, "HY000", "Error on rename of '%s' to '%s' (errno"),
  ER_ERROR_ON_WRITE(1026, "HY000", "Error writing file '%s' (errno"),
  ER_FILE_USED(1027, "HY000", "'%s' is locked against change"),
  ER_FILSORT_ABORT(1028, "HY000", "Sort aborted"),
  ER_FORM_NOT_FOUND(1029, "HY000", "View '%s' doesn't exist for '%s'"),
  ER_GET_ERRNO(1030, "HY000", "Got error %s from storage engine"),
  ER_ILLEGAL_HA(1031, "HY000", "Table storage engine for '%s' doesn't have this option"),
  ER_KEY_NOT_FOUND(1032, "HY000", "Can't find record in '%s'"),
  ER_NOT_FORM_FILE(1033, "HY000", "Incorrect information in file"),
  ER_NOT_KEYFILE(1034, "HY000", "Incorrect key file for table '%s'; try to repair it"),
  ER_OLD_KEYFILE(1035, "HY000", "Old key file for table '%s'; repair it!"),
  ER_OPEN_AS_READONLY(1036, "HY000", "Table '%s' is read only"),
  ER_OUTOFMEMORY(1037, "HY001", "Out of memory; restart server and try again (needed %s bytes)"),
  ER_OUT_OF_SORTMEMORY(1038, "HY001", "Out of sort memory, consider increasing server sort buffer size"),
  ER_UNEXPECTED_EOF(1039, "HY000", "Unexpected EOF found when reading file '%s' (errno"),
  ER_CON_COUNT_ERROR(1040, "08004", "Too many connections"),
  ER_OUT_OF_RESOURCES(1041, "HY000", "Out of memory; check if mysqld or some other process uses all available "
          + "memory; if not, you may have to use 'ulimit' to allow mysqld to use more memory or you can add more swap space"),
  ER_BAD_HOST_ERROR(1042, "08S01", "Can't get hostname for your address"),
  ER_HANDSHAKE_ERROR(1043, "08S01", "Bad handshake"),
  ER_DBACCESS_DENIED_ERROR(1044, "42000", "Access denied for user '%s'@'%s' to database '%s'"),
  ER_ACCESS_DENIED_ERROR(1045, "28000", "Access denied for user '%s'@'%s' (using password"),
  ER_NO_DB_ERROR(1046, "3D000", "No database selected"),
  ER_UNKNOWN_COM_ERROR(1047, "08S01", "Unknown command"),
  ER_BAD_NULL_ERROR(1048, "23000", "Column '%s' cannot be null"),
  ER_BAD_DB_ERROR(1049, "42000", "Unknown database '%s'"),
  ER_TABLE_EXISTS_ERROR(1050, "42S01", "Table '%s' already exists"),
  ER_BAD_TABLE_ERROR(1051, "42S02", "Unknown table '%s'"),
  ER_NON_UNIQ_ERROR(1052, "23000", "Column '%s' in %s is ambiguous"),
  ER_SERVER_SHUTDOWN(1053, "08S01", "Server shutdown in progress"),
  ER_BAD_FIELD_ERROR(1054, "42S22", "Unknown column '%s' in '%s'"),
  ER_WRONG_FIELD_WITH_GROUP(1055, "42000", "'%s' isn't in GROUP BY"),
  ER_WRONG_GROUP_FIELD(1056, "42000", "Can't group on '%s'"),
  ER_WRONG_SUM_SELECT(1057, "42000", "Statement has sum functions and columns in same statement"),
  ER_WRONG_VALUE_COUNT(1058, "21S01", "Column count doesn't match value count"),
  ER_TOO_LONG_IDENT(1059, "42000", "Identifier name '%s' is too long"),
  ER_DUP_FIELDNAME(1060, "42S21", "Duplicate column name '%s'"),
  ER_DUP_KEYNAME(1061, "42000", "Duplicate key name '%s'"),
  ER_DUP_ENTRY(1062, "23000", "Duplicate entry '%s' for key %s"),
  ER_WRONG_FIELD_SPEC(1063, "42000", "Incorrect column specifier for column '%s'"),
  ER_PARSE_ERROR(1064, "42000", "%s near '%s' at line %s"),
  ER_PARSE_NO_TYPE_ERROR(1064, "42000", "no specified data type, %s"),
  ER_PARSE_TYPE_ONE_ERROR(1064, "42000", "can only have one param, but %s"),
  ER_PARSE_TYPE_TWO_ERROR(1064, "42000", "can have up to two param, but %s"),
  ER_PARSE_TYPE_ERROR(1064, "42000", "data type param error, %s"),
  ER_EMPTY_QUERY(1065, "42000", "Query was empty"),
  ER_NONUNIQ_TABLE(1066, "42000", "Not unique table/alias"),
  ER_INVALID_DEFAULT(1067, "42000", "Invalid default value for '%s'"),
  ER_MULTIPLE_PRI_KEY(1068, "42000", "Multiple primary key defined"),
  ER_TOO_MANY_KEYS(1069, "42000", "Too many keys specified; max %s keys allowed"),
  ER_TOO_MANY_KEY_PARTS(1070, "42000", "Too many key parts specified; max %s parts allowed"),
  ER_TOO_LONG_KEY(1071, "42000", "Specified key was too long; max key length is %s bytes"),
  ER_KEY_COLUMN_DOES_NOT_EXISTS(1072, "42000", "Key column '%s' doesn't exist in table"),
  ER_BLOB_USED_AS_KEY(1073, "42000", "BLOB column '%s' can't be used in key specification with the used table type"),
  ER_TOO_BIG_FIELDLENGTH(1074, "42000", "Column length too big for column '%s' (max = %s); use BLOB or TEXT instead"),
  ER_WRONG_AUTO_KEY(1075, "42000", "Incorrect table definition; there can be only one auto column and it must be "
          + "defined as a key"),
  ER_READY(1076, "HY000", "%s"),
  ER_NORMAL_SHUTDOWN(1077, "HY000", "%s"),
  ER_GOT_SIGNAL(1078, "HY000", "%s"),
  ER_SHUTDOWN_COMPLETE(1079, "HY000", "%s"),
  ER_FORCING_CLOSE(1080, "08S01", "%s"),
  ER_IPSOCK_ERROR(1081, "08S01", "Can't create IP socket"),
  ER_NO_SUCH_INDEX(1082, "42S12", "Table '%s' has no index like the one used in CREATE INDEX; recreate the table"),
  ER_WRONG_FIELD_TERMINATORS(1083, "42000", "Field separator argument is not what is expected; check the manual"),
  ER_BLOBS_AND_NO_TERMINATED(1084, "42000", "You can't use fixed rowlength with BLOBs; please use 'fields terminated by'"),
  ER_TEXTFILE_NOT_READABLE(1085, "HY000", "The file '%s' must be in the database directory or be readable by all"),
  ER_FILE_EXISTS_ERROR(1086, "HY000", "File '%s' already exists"),
  ER_LOAD_INFO(1087, "HY000", "Records"),
  ER_ALTER_INFO(1088, "HY000", "Records"),
  ER_WRONG_SUB_KEY(1089, "HY000", "Incorrect prefix key; the used key part isn't a string, the used length is longer"
          + "than the key part, or the storage engine doesn't support unique prefix keys"),
  ER_CANT_REMOVE_ALL_FIELDS(1090, "42000", "You can't delete all columns with ALTER TABLE; use DROP TABLE instead"),
  ER_CANT_DROP_FIELD_OR_KEY(1091, "42000", "Can't DROP '%s'; check that column/key exists"),
  ER_INSERT_INFO(1092, "HY000", "Records"),
  ER_UPDATE_TABLE_USED(1093, "HY000", "You can't specify target table '%s' for update in FROM clause"),
  ER_NO_SUCH_THREAD(1094, "HY000", "Unknown thread id"),
  ER_KILL_DENIED_ERROR(1095, "HY000", "You are not owner of thread %lu"),
  ER_NO_TABLES_USED(1096, "HY000", "No tables used"),
  ER_TOO_BIG_SET(1097, "HY000", "Too many strings for column %s and SET"),
  ER_NO_UNIQUE_LOGFILE(1098, "HY000", "Can't generate a unique log-filename %s.(1-999)"),
  ER_TABLE_NOT_LOCKED_FOR_WRITE(1099, "HY000", "Table '%s' was locked with a READ lock and can't be updated"),
  ER_TABLE_NOT_LOCKED(1100, "HY000", "Table '%s' was not locked with LOCK TABLES"),
  ER_BLOB_CANT_HAVE_DEFAULT(1101, "42000", "BLOB, TEXT, GEOMETRY or JSON column '%s' can't have a default value"),
  ER_WRONG_DB_NAME(1102, "42000", "Incorrect database name '%s'"),
  ER_WRONG_TABLE_NAME(1103, "42000", "Incorrect table name '%s'"),
  ER_TOO_BIG_SELECT(1104, "42000", "The SELECT would examine more than MAX_JOIN_SIZE rows; check your WHERE and use "
          + "SET SQL_BIG_SELECTS=1 or SET MAX_JOIN_SIZE=# if the SELECT is okay"),
  ER_UNKNOWN_ERROR(1105, "HY000", "Unknown error"),
  ER_UNKNOWN_PROCEDURE(1106, "42000", "Unknown procedure '%s'"),
  ER_WRONG_PARAMCOUNT_TO_PROCEDURE(1107, "42000", "Incorrect parameter count to procedure '%s'"),
  ER_WRONG_PARAMETERS_TO_PROCEDURE(1108, "HY000", "Incorrect parameters to procedure '%s'"),
  ER_UNKNOWN_TABLE(1109, "42S02", "Unknown table '%s' in %s"),
  ER_FIELD_SPECIFIED_TWICE(1110, "42000", "Column '%s' specified twice"),
  ER_INVALID_GROUP_FUNC_USE(1111, "HY000", "Invalid use of group function"),
  ER_UNSUPPORTED_EXTENSION(1112, "42000", "Table '%s' uses an extension that doesn't exist in this MySQL version"),
  ER_TABLE_MUST_HAVE_COLUMNS(1113, "42000", "A table must have at least 1 column"),
  ER_RECORD_FILE_FULL(1114, "HY000", "The table '%s' is full"),
  ER_UNKNOWN_CHARACTER_SET(1115, "42000", "Unknown character set"),
  ER_TOO_MANY_TABLES(1116, "HY000", "Too many tables; MySQL can only use %s tables in a join"),
  ER_TOO_MANY_FIELDS(1117, "HY000", "Too many columns"),
  ER_TOO_BIG_ROWSIZE(1118, "42000", "Row size too large. The maximum row size for the used table type, not counting "
          + "BLOBs, is %ld. This includes storage overhead, check the manual. You have to change some columns to TEXT or BLOBs"),
  ER_STACK_OVERRUN(1119, "HY000", "Thread stack overrun"),
  ER_WRONG_OUTER_JOIN(1120, "42000", "Cross dependency found in OUTER JOIN; examine your ON conditions"),
  ER_NULL_COLUMN_IN_INDEX(1121, "42000", "Table handler doesn't support NULL in given index. Please change column "
          + "'%s' to be NOT NULL or use another handler"),
  ER_CANT_FIND_UDF(1122, "HY000", "Can't load function '%s'"),
  ER_CANT_INITIALIZE_UDF(1123, "HY000", "Can't initialize function '%s'; %s"),
  ER_UDF_NO_PATHS(1124, "HY000", "No paths allowed for shared library"),
  ER_UDF_EXISTS(1125, "HY000", "Function '%s' already exists"),
  ER_CANT_OPEN_LIBRARY(1126, "HY000", "Can't open shared library '%s' (errno"),
  ER_CANT_FIND_DL_ENTRY(1127, "HY000", "Can't find symbol '%s' in library"),
  ER_FUNCTION_NOT_DEFINED(1128, "HY000", "Function '%s' is not defined"),
  ER_HOST_IS_BLOCKED(1129, "HY000", "Host '%s' is blocked because of many connection errors; unblock with "
          + "'mysqladmin flush-hosts'"),
  ER_HOST_NOT_PRIVILEGED(1130, "HY000", "Host '%s' is not allowed to connect to this MySQL server"),
  ER_PASSWORD_ANONYMOUS_USER(1131, "42000", "You are using MySQL as an anonymous user and anonymous users are not "
          + "allowed to change passwords"),
  ER_PASSWORD_NOT_ALLOWED(1132, "42000", "You must have privileges to update tables in the mysql database to be able"
          + "to change passwords for others"),
  ER_PASSWORD_NO_MATCH(1133, "42000", "Can't find any matching row in the user table"),
  ER_UPDATE_INFO(1134, "HY000", "Rows matched"),
  ER_CANT_CREATE_THREAD(1135, "HY000", "Can't create a new thread (errno %s); if you are not out of available "
          + "memory, you can consult the manual for a possible OS-dependent bug"),
  ER_WRONG_VALUE_COUNT_ON_ROW(1136, "21S01", "Column count doesn't match value count at row %s"),
  ER_CANT_REOPEN_TABLE(1137, "HY000", "Can't reopen table"),
  ER_INVALID_USE_OF_NULL(1138, "22004", "Invalid use of NULL value"),
  ER_REGEXP_ERROR(1139, "42000", "Got error '%s' from regexp"),
  ER_MIX_OF_GROUP_FUNC_AND_FIELDS(1140, "42000", "Mixing of GROUP columns (MIN(),MAX(),COUNT(),...) with no GROUP "
          + "columns is illegal if there is no GROUP BY clause"),
  ER_NONEXISTING_GRANT(1141, "42000", "There is no such grant defined for user '%s' on host '%s'"),
  ER_TABLEACCESS_DENIED_ERROR(1142, "42000", "%s command denied to user '%s'@'%s' for table '%s'"),
  ER_COLUMNACCESS_DENIED_ERROR(1143, "42000", "%s command denied to user '%s'@'%s' for column '%s' in table '%s'"),
  ER_ILLEGAL_GRANT_FOR_TABLE(1144, "42000", "Illegal GRANT/REVOKE command; please consult the manual to see which "
          + "privileges can be used"),
  ER_GRANT_WRONG_HOST_OR_USER(1145, "42000", "The host or user argument to GRANT is too long"),
  ER_NO_SUCH_TABLE(1146, "42S02", "Table '%s.%s' doesn't exist"),
  ER_NONEXISTING_TABLE_GRANT(1147, "42000", "There is no such grant defined for user '%s' on host '%s' on table '%s'"),
  ER_NOT_ALLOWED_COMMAND(1148, "42000", "The used command is not allowed with this MySQL version"),
  ER_SYNTAX_ERROR(1149, "42000", "You have an error in your SQL syntax; check the manual that corresponds to your "
          + "MySQL server version for the right syntax to use"),
  ER_UNUSED1(1150, "HY000", "Delayed insert thread couldn't get requested lock for table %s"),
  ER_UNUSED2(1151, "HY000", "Too many delayed threads in use"),
  ER_ABORTING_CONNECTION(1152, "08S01", "Aborted connection %ld to db"),
  ER_NET_PACKET_TOO_LARGE(1153, "08S01", "Got a packet bigger than 'max_allowed_packet' bytes"),
  ER_NET_READ_ERROR_FROM_PIPE(1154, "08S01", "Got a read error from the connection pipe"),
  ER_NET_FCNTL_ERROR(1155, "08S01", "Got an error from fcntl()"),
  ER_NET_PACKETS_OUT_OF_ORDER(1156, "08S01", "Got packets out of order"),
  ER_NET_UNCOMPRESS_ERROR(1157, "08S01", "Couldn't uncompress communication packet"),
  ER_NET_READ_ERROR(1158, "08S01", "Got an error reading communication packets"),
  ER_NET_READ_INTERRUPTED(1159, "08S01", "Got timeout reading communication packets"),
  ER_NET_ERROR_ON_WRITE(1160, "08S01", "Got an error writing communication packets"),
  ER_NET_WRITE_INTERRUPTED(1161, "08S01", "Got timeout writing communication packets"),
  ER_TOO_LONG_STRING(1162, "42000", "Result string is longer than 'max_allowed_packet' bytes"),
  ER_TABLE_CANT_HANDLE_BLOB(1163, "42000", "The used table type doesn't support BLOB/TEXT columns"),
  ER_TABLE_CANT_HANDLE_AUTO_INCREMENT(1164, "42000", "The used table type doesn't support AUTO_INCREMENT columns"),
  ER_UNUSED3(1165, "HY000", "INSERT DELAYED can't be used with table '%s' because it is locked with LOCK TABLES"),
  ER_WRONG_COLUMN_NAME(1166, "42000", "Incorrect column name '%s'"),
  ER_WRONG_KEY_COLUMN(1167, "42000", "The used storage engine can't index column '%s'"),
  ER_WRONG_MRG_TABLE(1168, "HY000", "Unable to open underlying table which is differently defined or of non-MyISAM "
          + "type or doesn't exist"),
  ER_DUP_UNIQUE(1169, "23000", "Can't write, because of unique constraint, to table '%s'"),
  ER_BLOB_KEY_WITHOUT_LENGTH(1170, "42000", "BLOB/TEXT column '%s' used in key specification without a key length"),
  ER_PRIMARY_CANT_HAVE_NULL(1171, "42000", "All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, "
          + "use UNIQUE instead"),
  ER_TOO_MANY_ROWS(1172, "42000", "Result consisted of more than one row"),
  ER_REQUIRES_PRIMARY_KEY(1173, "42000", "This table type requires a primary key"),
  ER_NO_RAID_COMPILED(1174, "HY000", "This version of MySQL is not compiled with RAID support"),
  ER_UPDATE_WITHOUT_KEY_IN_SAFE_MODE(1175, "HY000", "You are using safe update mode and you tried to update a table "
          + "without a WHERE that uses a KEY column. %s"),
  ER_KEY_DOES_NOT_EXITS(1176, "42000", "Key '%s' doesn't exist in table '%s'"),
  ER_CHECK_NO_SUCH_TABLE(1177, "42000", "Can't open table"),
  ER_CHECK_NOT_IMPLEMENTED(1178, "42000", "The storage engine for the table doesn't support %s"),
  ER_CANT_DO_THIS_DURING_AN_TRANSACTION(1179, "25000", "You are not allowed to execute this command in a transaction"),
  ER_ERROR_DURING_COMMIT(1180, "HY000", "Got error %s during COMMIT"),
  ER_ERROR_DURING_ROLLBACK(1181, "HY000", "Got error %s during ROLLBACK"),
  ER_ERROR_DURING_FLUSH_LOGS(1182, "HY000", "Got error %s during FLUSH_LOGS"),
  ER_ERROR_DURING_CHECKPOINT(1183, "HY000", "Got error %s during CHECKPOINT"),
  ER_NEW_ABORTING_CONNECTION(1184, "08S01", "Aborted connection %u to db"),
  ER_DUMP_NOT_IMPLEMENTED(1185, "HY000", "The storage engine for the table does not support binary table dump"),
  ER_FLUSH_MASTER_BINLOG_CLOSED(1186, "HY000", "Binlog closed, cannot RESET MASTER"),
  ER_INDEX_REBUILD(1187, "HY000", "Failed rebuilding the index of dumped table '%s'"),
  ER_MASTER(1188, "HY000", "Error from master"),
  ER_MASTER_NET_READ(1189, "08S01", "Net error reading from master"),
  ER_MASTER_NET_WRITE(1190, "08S01", "Net error writing to master"),
  ER_FT_MATCHING_KEY_NOT_FOUND(1191, "HY000", "Can't find FULLTEXT index matching the column list"),
  ER_LOCK_OR_ACTIVE_TRANSACTION(1192, "HY000", "Can't execute the given command because you have active locked "
          + "tables or an active transaction"),
  ER_UNKNOWN_SYSTEM_VARIABLE(1193, "HY000", "Unknown system variable '%s'"),
  ER_CRASHED_ON_USAGE(1194, "HY000", "Table '%s' is marked as crashed and should be repaired"),
  ER_CRASHED_ON_REPAIR(1195, "HY000", "Table '%s' is marked as crashed and last (automatic?) repair failed"),
  ER_WARNING_NOT_COMPLETE_ROLLBACK(1196, "HY000", "Some non-transactional changed tables couldn't be rolled back"),
  ER_TRANS_CACHE_FULL(1197, "HY000", "Multi-statement transaction required more than 'max_binlog_cache_size' bytes "
          + "of storage; increase this mysqld variable and try again"),
  ER_SLAVE_MUST_STOP(1198, "HY000", "This operation cannot be performed with a running slave; run STOP SLAVE first"),
  ER_SLAVE_NOT_RUNNING(1199, "HY000", "This operation requires a running slave; configure slave and do START SLAVE"),
  ER_BAD_SLAVE(1200, "HY000", "The server is not configured as slave; fix in config file or with CHANGE MASTER TO"),
  ER_MASTER_INFO(1201, "HY000", "Could not initialize master info structure; more error messages can be found in the"
          + "MySQL error log"),
  ER_SLAVE_THREAD(1202, "HY000", "Could not create slave thread; check system resources"),
  ER_TOO_MANY_USER_CONNECTIONS(1203, "42000", "User %s already has more than 'max_user_connections' active connections"),
  ER_SET_CONSTANTS_ONLY(1204, "HY000", "You may only use constant expressions with SET"),
  ER_LOCK_WAIT_TIMEOUT(1205, "HY000", "Lock wait timeout exceeded; try restarting transaction"),
  ER_LOCK_TABLE_FULL(1206, "HY000", "The total number of locks exceeds the lock table size"),
  ER_READ_ONLY_TRANSACTION(1207, "25000", "Update locks cannot be acquired during a READ UNCOMMITTED transaction"),
  ER_DROP_DB_WITH_READ_LOCK(1208, "HY000", "DROP DATABASE not allowed while thread is holding global read lock"),
  ER_CREATE_DB_WITH_READ_LOCK(1209, "HY000", "CREATE DATABASE not allowed while thread is holding global read lock"),
  ER_WRONG_ARGUMENTS(1210, "HY000", "Incorrect arguments to %s"),
  ER_NO_PERMISSION_TO_CREATE_USER(1211, "42000", "'%s'@'%s' is not allowed to create new users"),
  ER_UNION_TABLES_IN_DIFFERENT_DIR(1212, "HY000", "Incorrect table definition; all MERGE tables must be in the same database"),
  ER_LOCK_DEADLOCK(1213, "40001", "Deadlock found when trying to get lock; try restarting transaction"),
  ER_TABLE_CANT_HANDLE_FT(1214, "HY000", "The used table type doesn't support FULLTEXT indexes"),
  ER_CANNOT_ADD_FOREIGN(1215, "HY000", "Cannot add foreign key constraint"),
  ER_NO_REFERENCED_ROW(1216, "23000", "Cannot add or update a child row"),
  ER_ROW_IS_REFERENCED(1217, "23000", "Cannot delete or update a parent row"),
  ER_CONNECT_TO_MASTER(1218, "08S01", "Error connecting to master"),
  ER_QUERY_ON_MASTER(1219, "HY000", "Error running query on master"),
  ER_ERROR_WHEN_EXECUTING_COMMAND(1220, "HY000", "Error when executing command %s"),
  ER_WRONG_USAGE(1221, "HY000", "Incorrect usage of %s and %s"),
  ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT(1222, "21000", "The used SELECT statements have a different number of columns"),
  ER_CANT_UPDATE_WITH_READLOCK(1223, "HY000", "Can't execute the query because you have a conflicting read lock"),
  ER_MIXING_NOT_ALLOWED(1224, "HY000", "Mixing of transactional and non-transactional tables is disabled"),
  ER_DUP_ARGUMENT(1225, "HY000", "Option '%s' used twice in statement"),
  ER_USER_LIMIT_REACHED(1226, "42000", "User '%s' has exceeded the '%s' resource (current value"),
  ER_SPECIFIC_ACCESS_DENIED_ERROR(1227, "42000", "Access denied; you need (at least one of) the %s privilege(s) for "
          + "this operation"),
  ER_LOCAL_VARIABLE(1228, "HY000", "Variable '%s' is a SESSION variable and can't be used with SET GLOBAL"),
  ER_GLOBAL_VARIABLE(1229, "HY000", "Variable '%s' is a GLOBAL variable and should be set with SET GLOBAL"),
  ER_NO_DEFAULT(1230, "42000", "Variable '%s' doesn't have a default value"),
  ER_WRONG_VALUE_FOR_VAR(1231, "42000", "Variable '%s' can't be set to the value of '%s'"),
  ER_WRONG_TYPE_FOR_VAR(1232, "42000", "Incorrect argument type to variable '%s'"),
  ER_VAR_CANT_BE_READ(1233, "HY000", "Variable '%s' can only be set, not read"),
  ER_CANT_USE_OPTION_HERE(1234, "42000", "Incorrect usage/placement of '%s'"),
  ER_NOT_SUPPORTED_YET(1235, "42000", "This version of MySQL doesn't yet support '%s'"),
  ER_MASTER_FATAL_ERROR_READING_BINLOG(1236, "HY000", "Got fatal error %s from master when reading data from binary log"),
  ER_SLAVE_IGNORED_TABLE(1237, "HY000", "Slave SQL thread ignored the query because of replicate-*-table rules"),
  ER_INCORRECT_GLOBAL_LOCAL_VAR(1238, "HY000", "Variable '%s' is a %s variable"),
  ER_WRONG_FK_DEF(1239, "42000", "Incorrect foreign key definition for '%s'"),
  ER_KEY_REF_DO_NOT_MATCH_TABLE_REF(1240, "HY000", "Key reference and table reference don't match"),
  ER_OPERAND_COLUMNS(1241, "21000", "Operand should contain %s column(s)"),
  ER_SUBQUERY_NO_1_ROW(1242, "21000", "Subquery returns more than 1 row"),
  ER_UNKNOWN_STMT_HANDLER(1243, "HY000", "Unknown prepared statement handler (%.*s) given to %s"),
  ER_CORRUPT_HELP_DB(1244, "HY000", "Help database is corrupt or does not exist"),
  ER_CYCLIC_REFERENCE(1245, "HY000", "Cyclic reference on subqueries"),
  ER_AUTO_CONVERT(1246, "HY000", "Converting column '%s' from %s to %s"),
  ER_ILLEGAL_REFERENCE(1247, "42S22", "Reference '%s' not supported (%s)"),
  ER_DERIVED_MUST_HAVE_ALIAS(1248, "42000", "Every derived table must have its own alias"),
  ER_SELECT_REDUCED(1249, "01000", "Select %u was reduced during optimization"),
  ER_TABLENAME_NOT_ALLOWED_HERE(1250, "42000", "Table '%s' from one of the SELECTs cannot be used in %s"),
  ER_NOT_SUPPORTED_AUTH_MODE(1251, "08004", "Client does not support authentication protocol requested by server; "
          + "consider upgrading MySQL client"),
  ER_SPATIAL_CANT_HAVE_NULL(1252, "42000", "All parts of a SPATIAL index must be NOT NULL"),
  ER_COLLATION_CHARSET_MISMATCH(1253, "42000", "COLLATION '%s' is not valid for CHARACTER SET '%s'"),
  ER_SLAVE_WAS_RUNNING(1254, "HY000", "Slave is already running"),
  ER_SLAVE_WAS_NOT_RUNNING(1255, "HY000", "Slave already has been stopped"),
  ER_TOO_BIG_FOR_UNCOMPRESS(1256, "HY000", "Uncompressed data size too large; the maximum size is %s (probably, "
          + "length of uncompressed data was corrupted)"),
  ER_ZLIB_Z_MEM_ERROR(1257, "HY000", "ZLIB"),
  ER_ZLIB_Z_BUF_ERROR(1258, "HY000", "ZLIB"),
  ER_ZLIB_Z_DATA_ERROR(1259, "HY000", "ZLIB"),
  ER_CUT_VALUE_GROUP_CONCAT(1260, "HY000", "Row %u was cut by GROUP_CONCAT()"),
  ER_WARN_TOO_FEW_RECORDS(1261, "01000", "Row %ld doesn't contain data for all columns"),
  ER_WARN_TOO_MANY_RECORDS(1262, "01000", "Row %ld was truncated; it contained more data than there were input columns"),
  ER_WARN_NULL_TO_NOTNULL(1263, "22004", "Column set to default value; NULL supplied to NOT NULL column '%s' at row %s"),
  ER_WARN_DATA_OUT_OF_RANGE(1264, "22003", "Out of range value for column '%s' at row %s"),
  WARN_DATA_TRUNCATED(1265, "01000", "Data truncated for column '%s' at row %ld"),
  ER_WARN_USING_OTHER_HANDLER(1266, "HY000", "Using storage engine %s for table '%s'"),
  ER_CANT_AGGREGATE_2COLLATIONS(1267, "HY000", "Illegal mix of collations (%s,%s) and (%s,%s) for operation '%s'"),
  ER_DROP_USER(1268, "HY000", "Cannot drop one or more of the requested users"),
  ER_REVOKE_GRANTS(1269, "HY000", "Can't revoke all privileges for one or more of the requested users"),
  ER_CANT_AGGREGATE_3COLLATIONS(1270, "HY000", "Illegal mix of collations (%s,%s), (%s,%s), (%s,%s) for operation '%s'"),
  ER_CANT_AGGREGATE_NCOLLATIONS(1271, "HY000", "Illegal mix of collations for operation '%s'"),
  ER_VARIABLE_IS_NOT_STRUCT(1272, "HY000", "Variable '%s' is not a variable component (can't be used as XXX.variable_name)"),
  ER_UNKNOWN_COLLATION(1273, "HY000", "Unknown collation"),
  ER_SLAVE_IGNORED_SSL_PARAMS(1274, "HY000", "SSL parameters in CHANGE MASTER are ignored because this MySQL slave "
          + "was compiled without SSL support; they can be used later if MySQL slave with SSL is started"),
  ER_SERVER_IS_IN_SECURE_AUTH_MODE(1275, "HY000", "Server is running in --secure-auth mode, but '%s'@'%s' has a "
          + "password in the old format; please change the password to the new format"),
  ER_WARN_FIELD_RESOLVED(1276, "HY000", "Field or reference '%s%s%s%s%s' of SELECT #%s was resolved in SELECT #%s"),
  ER_BAD_SLAVE_UNTIL_COND(1277, "HY000", "Incorrect parameter or combination of parameters for START SLAVE UNTIL"),
  ER_MISSING_SKIP_SLAVE(1278, "HY000", "It is recommended to use --skip-slave-start when doing step-by-step "
          + "replication with START SLAVE UNTIL; otherwise, you will get problems if you get an unexpected slave's mysqld restart"),
  ER_UNTIL_COND_IGNORED(1279, "HY000", "SQL thread is not to be started so UNTIL options are ignored"),
  ER_WRONG_NAME_FOR_INDEX(1280, "42000", "Incorrect index name '%s'"),
  ER_WRONG_NAME_FOR_CATALOG(1281, "42000", "Incorrect catalog name '%s'"),
  ER_WARN_QC_RESIZE(1282, "HY000", "Query cache failed to set size %lu; new query cache size is %lu"),
  ER_BAD_FT_COLUMN(1283, "HY000", "Column '%s' cannot be part of FULLTEXT index"),
  ER_UNKNOWN_KEY_CACHE(1284, "HY000", "Unknown key cache '%s'"),
  ER_WARN_HOSTNAME_WONT_WORK(1285, "HY000", "MySQL is started in --skip-name-resolve mode; you must restart it "
          + "without this switch for this grant to work"),
  ER_UNKNOWN_STORAGE_ENGINE(1286, "42000", "Unknown storage engine '%s'"),
  ER_WARN_DEPRECATED_SYNTAX(1287, "HY000", "'%s' is deprecated and will be removed in a future release. Please use %s instead"),
  ER_NON_UPDATABLE_TABLE(1288, "HY000", "The target table %s of the %s is not updatable"),
  ER_FEATURE_DISABLED(1289, "HY000", "The '%s' feature is disabled; you need MySQL built with '%s' to have it working"),
  ER_OPTION_PREVENTS_STATEMENT(1290, "HY000", "The MySQL server is running with the %s option so it cannot execute "
          + "this statement"),
  ER_DUPLICATED_VALUE_IN_TYPE(1291, "HY000", "Column '%s' has duplicated value '%s' in %s"),
  ER_TRUNCATED_WRONG_VALUE(1292, "22007", "Truncated incorrect %s value"),
  ER_TRUNCATED_WRONG_TIME_VALUE(1292, "22007", "Incorrect time value: '%s'"),
  ER_TRUNCATED_WRONG_DATETIME_VALUE(1292, "22007", "Incorrect datetime value: '%s'"),
  ER_TOO_MUCH_AUTO_TIMESTAMP_COLS(1293, "HY000", "Incorrect table definition; there can be only one TIMESTAMP column"
          + "with CURRENT_TIMESTAMP in DEFAULT or ON UPDATE clause"),
  ER_INVALID_ON_UPDATE(1294, "HY000", "Invalid ON UPDATE clause for '%s' column"),
  ER_UNSUPPORTED_PS(1295, "HY000", "This command is not supported in the prepared statement protocol yet"),
  ER_GET_ERRMSG(1296, "HY000", "Got error %s '%s' from %s"),
  ER_GET_TEMPORARY_ERRMSG(1297, "HY000", "Got temporary error %s '%s' from %s"),
  ER_UNKNOWN_TIME_ZONE(1298, "HY000", "Unknown or incorrect time zone"),
  ER_WARN_INVALID_TIMESTAMP(1299, "HY000", "Invalid TIMESTAMP value in column '%s' at row %s"),
  ER_INVALID_CHARACTER_STRING(1300, "HY000", "Invalid %s character string"),
  ER_WARN_ALLOWED_PACKET_OVERFLOWED(1301, "HY000", "Result of %s() was larger than max_allowed_packet (%ld)-truncated"),
  ER_CONFLICTING_DECLARATIONS(1302, "HY000", "Conflicting declarations"),
  ER_SP_NO_RECURSIVE_CREATE(1303, "2F003", "Can't create a %s from within another stored routine"),
  ER_SP_ALREADY_EXISTS(1304, "42000", "%s %s already exists"),
  ER_SP_DOES_NOT_EXIST(1305, "42000", "%s %s does not exist"),
  ER_SP_DROP_FAILED(1306, "HY000", "Failed to DROP %s %s"),
  ER_SP_STORE_FAILED(1307, "HY000", "Failed to CREATE %s %s"),
  ER_SP_LILABEL_MISMATCH(1308, "42000", "%s with no matching label"),
  ER_SP_LABEL_REDEFINE(1309, "42000", "Redefining label %s"),
  ER_SP_LABEL_MISMATCH(1310, "42000", "End-label %s without match"),
  ER_SP_UNINIT_VAR(1311, "01000", "Referring to uninitialized variable %s"),
  ER_SP_BADSELECT(1312, "0A000", "PROCEDURE %s can't return a result set in the given context"),
  ER_SP_BADRETURN(1313, "42000", "RETURN is only allowed in a FUNCTION"),
  ER_SP_BADSTATEMENT(1314, "0A000", "%s is not allowed in stored procedures"),
  ER_UPDATE_LOG_DEPRECATED_IGNORED(1315, "42000", "The update log is deprecated and replaced by the binary log; SET "
          + "SQL_LOG_UPDATE has been ignored."),
  ER_UPDATE_LOG_DEPRECATED_TRANSLATED(1316, "42000", "The update log is deprecated and replaced by the binary log; "
          + "SET SQL_LOG_UPDATE has been translated to SET SQL_LOG_BIN."),
  ER_QUERY_INTERRUPTED(1317, "70100", "Query execution was interrupted"),
  ER_SP_WRONG_NO_OF_ARGS(1318, "42000", "Incorrect number of arguments for %s %s; expected %u, got %u"),
  ER_SP_COND_MISMATCH(1319, "42000", "Undefined CONDITION"),
  ER_SP_NORETURN(1320, "42000", "No RETURN found in FUNCTION %s"),
  ER_SP_NORETURNEND(1321, "2F005", "FUNCTION %s ended without RETURN"),
  ER_SP_BAD_CURSOR_QUERY(1322, "42000", "Cursor statement must be a SELECT"),
  ER_SP_BAD_CURSOR_SELECT(1323, "42000", "Cursor SELECT must not have INTO"),
  ER_SP_CURSOR_MISMATCH(1324, "42000", "Undefined CURSOR"),
  ER_SP_CURSOR_ALREADY_OPEN(1325, "24000", "Cursor is already open"),
  ER_SP_CURSOR_NOT_OPEN(1326, "24000", "Cursor is not open"),
  ER_SP_UNDECLARED_VAR(1327, "42000", "Undeclared variable"),
  ER_SP_WRONG_NO_OF_FETCH_ARGS(1328, "HY000", "Incorrect number of FETCH variables"),
  ER_SP_FETCH_NO_DATA(1329, "02000", "No data - zero rows fetched, selected, or processed"),
  ER_SP_DUP_PARAM(1330, "42000", "Duplicate parameter"),
  ER_SP_DUP_VAR(1331, "42000", "Duplicate variable"),
  ER_SP_DUP_COND(1332, "42000", "Duplicate condition"),
  ER_SP_DUP_CURS(1333, "42000", "Duplicate cursor"),
  ER_SP_CANT_ALTER(1334, "HY000", "Failed to ALTER %s %s"),
  ER_SP_SUBSELECT_NYI(1335, "0A000", "Subquery value not supported"),
  ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG(1336, "0A000", "%s is not allowed in stored function or trigger"),
  ER_SP_VARCOND_AFTER_CURSHNDLR(1337, "42000", "Variable or condition declaration after cursor or handler declaration"),
  ER_SP_CURSOR_AFTER_HANDLER(1338, "42000", "Cursor declaration after handler declaration"),
  ER_SP_CASE_NOT_FOUND(1339, "20000", "Case not found for CASE statement"),
  ER_FPARSER_TOO_BIG_FILE(1340, "HY000", "Configuration file '%s' is too big"),
  ER_FPARSER_BAD_HEADER(1341, "HY000", "Malformed file type header in file '%s'"),
  ER_FPARSER_EOF_IN_COMMENT(1342, "HY000", "Unexpected end of file while parsing comment '%s'"),
  ER_FPARSER_ERROR_IN_PARAMETER(1343, "HY000", "Error while parsing parameter '%s' (line"),
  ER_FPARSER_EOF_IN_UNKNOWN_PARAMETER(1344, "HY000", "Unexpected end of file while skipping unknown parameter '%s'"),
  ER_VIEW_NO_EXPLAIN(1345, "HY000", "EXPLAIN/SHOW can not be issued; lacking privileges for underlying table"),
  ER_FRM_UNKNOWN_TYPE(1346, "HY000", "File '%s' has unknown type '%s' in its header"),
  ER_WRONG_OBJECT(1347, "HY000", "'%s.%s' is not %s"),
  ER_NONUPDATEABLE_COLUMN(1348, "HY000", "Column '%s' is not updatable"),
  ER_VIEW_SELECT_DERIVED(1349, "HY000", "View's SELECT contains a subquery in the FROM clause"),
  ER_VIEW_SELECT_DERIVED_UNUSED(1349, "HY000", "View's SELECT contains a subquery in the FROM clause"),
  ER_VIEW_SELECT_CLAUSE(1350, "HY000", "View's SELECT contains a '%s' clause"),
  ER_VIEW_SELECT_VARIABLE(1351, "HY000", "View's SELECT contains a variable or parameter"),
  ER_VIEW_SELECT_TMPTABLE(1352, "HY000", "View's SELECT refers to a temporary table '%s'"),
  ER_VIEW_WRONG_LIST(1353, "HY000", "View's SELECT and view's field list have different column counts"),
  ER_WARN_VIEW_MERGE(1354, "HY000", "View merge algorithm can't be used here for now (assumed undefined algorithm)"),
  ER_WARN_VIEW_WITHOUT_KEY(1355, "HY000", "View being updated does not have complete key of underlying table in it"),
  ER_VIEW_INVALID(1356, "HY000", "View '%s.%s' references invalid table(s) or column(s) or function(s) or "
          + "definer/invoker of view lack rights to use them"),
  ER_SP_NO_DROP_SP(1357, "HY000", "Can't drop or alter a %s from within another stored routine"),
  ER_SP_GOTO_IN_HNDLR(1358, "HY000", "GOTO is not allowed in a stored procedure handler"),
  ER_TRG_ALREADY_EXISTS(1359, "HY000", "Trigger already exists"),
  ER_TRG_DOES_NOT_EXIST(1360, "HY000", "Trigger does not exist"),
  ER_TRG_ON_VIEW_OR_TEMP_TABLE(1361, "HY000", "Trigger's '%s' is view or temporary table"),
  ER_TRG_CANT_CHANGE_ROW(1362, "HY000", "Updating of %s row is not allowed in %strigger"),
  ER_TRG_NO_SUCH_ROW_IN_TRG(1363, "HY000", "There is no %s row in %s trigger"),
  ER_NO_DEFAULT_FOR_FIELD(1364, "HY000", "Field '%s' doesn't have a default value"),
  ER_DIVISION_BY_ZERO(1365, "22012", "Division by 0"),
  ER_TRUNCATED_WRONG_VALUE_FOR_FIELD(1366, "HY000", "Incorrect %s value for column '%s' at row %s"),
  ER_ILLEGAL_VALUE_FOR_TYPE(1367, "22007", "Illegal %s '%s' value found during parsing"),
  ER_VIEW_NONUPD_CHECK(1368, "HY000", "CHECK OPTION on non-updatable view '%s.%s'"),
  ER_VIEW_CHECK_FAILED(1369, "HY000", "CHECK OPTION failed '%s.%s'"),
  ER_PROCACCESS_DENIED_ERROR(1370, "42000", "%s command denied to user '%s'@'%s' for routine '%s'"),
  ER_RELAY_LOG_FAIL(1371, "HY000", "Failed purging old relay logs"),
  ER_PASSWD_LENGTH(1372, "HY000", "Password hash should be a %s-digit hexadecimal number"),
  ER_UNKNOWN_TARGET_BINLOG(1373, "HY000", "Target log not found in binlog index"),
  ER_IO_ERR_LOG_INDEX_READ(1374, "HY000", "I/O error reading log index file"),
  ER_BINLOG_PURGE_PROHIBITED(1375, "HY000", "Server configuration does not permit binlog purge"),
  ER_FSEEK_FAIL(1376, "HY000", "Failed on fseek()"),
  ER_BINLOG_PURGE_FATAL_ERR(1377, "HY000", "Fatal error during log purge"),
  ER_LOG_IN_USE(1378, "HY000", "A purgeable log is in use, will not purge"),
  ER_LOG_PURGE_UNKNOWN_ERR(1379, "HY000", "Unknown error during log purge"),
  ER_RELAY_LOG_INIT(1380, "HY000", "Failed initializing relay log position"),
  ER_NO_BINARY_LOGGING(1381, "HY000", "You are not using binary logging"),
  ER_RESERVED_SYNTAX(1382, "HY000", "The '%s' syntax is reserved for purposes internal to the MySQL server"),
  ER_WSAS_FAILED(1383, "HY000", "WSAStartup Failed"),
  ER_DIFF_GROUPS_PROC(1384, "HY000", "Can't handle procedures with different groups yet"),
  ER_NO_GROUP_FOR_PROC(1385, "HY000", "Select must have a group with this procedure"),
  ER_ORDER_WITH_PROC(1386, "HY000", "Can't use ORDER clause with this procedure"),
  ER_LOGGING_PROHIBIT_CHANGING_OF(1387, "HY000", "Binary logging and replication forbid changing the global server %s"),
  ER_NO_FILE_MAPPING(1388, "HY000", "Can't map file"),
  ER_WRONG_MAGIC(1389, "HY000", "Wrong magic in %s"),
  ER_PS_MANY_PARAM(1390, "HY000", "Prepared statement contains too many placeholders"),
  ER_KEY_PART_0(1391, "HY000", "Key part '%s' length cannot be 0"),
  ER_VIEW_CHECKSUM(1392, "HY000", "View text checksum failed"),
  ER_VIEW_MULTIUPDATE(1393, "HY000", "Can not modify more than one base table through a join view '%s.%s'"),
  ER_VIEW_NO_INSERT_FIELD_LIST(1394, "HY000", "Can not insert into join view '%s.%s' without fields list"),
  ER_VIEW_DELETE_MERGE_VIEW(1395, "HY000", "Can not delete from join view '%s.%s'"),
  ER_CANNOT_USER(1396, "HY000", "Operation %s failed for %s"),
  ER_XAER_NOTA(1397, "XAE04", "XAER_NOTA"),
  ER_XAER_INVAL(1398, "XAE05", "XAER_INVAL"),
  ER_XAER_RMFAIL(1399, "XAE07", "XAER_RMFAIL"),
  ER_XAER_OUTSIDE(1400, "XAE09", "XAER_OUTSIDE"),
  ER_XAER_RMERR(1401, "XAE03", "XAER_RMERR"),
  ER_XA_RBROLLBACK(1402, "XA100", "XA_RBROLLBACK"),
  ER_NONEXISTING_PROC_GRANT(1403, "42000", "There is no such grant defined for user '%s' on host '%s' on routine '%s'"),
  ER_PROC_AUTO_GRANT_FAIL(1404, "HY000", "Failed to grant EXECUTE and ALTER ROUTINE privileges"),
  ER_PROC_AUTO_REVOKE_FAIL(1405, "HY000", "Failed to revoke all privileges to dropped routine"),
  ER_DATA_TOO_LONG(1406, "22001", "Data too long for column '%s' at row %s"),
  ER_SP_BAD_SQLSTATE(1407, "42000", "Bad SQLSTATE"),
  ER_STARTUP(1408, "HY000", "%s"),
  ER_LOAD_FROM_FIXED_SIZE_ROWS_TO_VAR(1409, "HY000", "Can't load value from file with fixed size rows to variable"),
  ER_CANT_CREATE_USER_WITH_GRANT(1410, "42000", "You are not allowed to create a user with GRANT"),
  ER_WRONG_VALUE_FOR_TYPE(1411, "HY000", "Incorrect %s value"),
  ER_TABLE_DEF_CHANGED(1412, "HY000", "Table definition has changed, please retry transaction"),
  ER_SP_DUP_HANDLER(1413, "42000", "Duplicate handler declared in the same block"),
  ER_SP_NOT_VAR_ARG(1414, "42000", "OUT or INOUT argument %s for routine %s is not a variable or NEW pseudo-variable"
          + "in BEFORE trigger"),
  ER_SP_NO_RETSET(1415, "0A000", "Not allowed to return a result set from a %s"),
  ER_CANT_CREATE_GEOMETRY_OBJECT(1416, "22003", "Cannot get geometry object from data you send to the GEOMETRY field"),
  ER_FAILED_ROUTINE_BREAK_BINLOG(1417, "HY000", "A routine failed and has neither NO SQL nor READS SQL DATA in its "
          + "declaration and binary logging is enabled; if non-transactional tables were updated, the binary log will miss "
          + "their changes"),
  ER_BINLOG_UNSAFE_ROUTINE(1418, "HY000", "This function has none of DETERMINISTIC, NO SQL, or READS SQL DATA in its"
          + "declaration and binary logging is enabled (you *might* want to use the less safe "
          + "log_bin_trust_function_creators variable)"),
  ER_BINLOG_CREATE_ROUTINE_NEED_SUPER(1419, "HY000", "You do not have the SUPER privilege and binary logging is "
          + "enabled (you *might* want to use the less safe log_bin_trust_function_creators variable)"),
  ER_EXEC_STMT_WITH_OPEN_CURSOR(1420, "HY000", "You can't execute a prepared statement which has an open cursor "
          + "associated with it. Reset the statement to re-execute it."),
  ER_STMT_HAS_NO_OPEN_CURSOR(1421, "HY000", "The statement (%lu) has no open cursor."),
  ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG(1422, "HY000", "Explicit or implicit commit is not allowed in stored function "
          + "or trigger."),
  ER_NO_DEFAULT_FOR_VIEW_FIELD(1423, "HY000", "Field of view '%s.%s' underlying table doesn't have a default value"),
  ER_SP_NO_RECURSION(1424, "HY000", "Recursive stored functions and triggers are not allowed."),
  ER_TOO_BIG_SCALE(1425, "42000", "Too big scale %s specified for column '%s'. Maximum is %s."),
  ER_TOO_BIG_PRECISION(1426, "42000", "Too-big precision %s specified for '%s'. Maximum is %s."),
  ER_M_BIGGER_THAN_D(1427, "42000", "For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '%s')."),
  ER_WRONG_LOCK_OF_SYSTEM_TABLE(1428, "HY000", "You can't combine write-locking of system tables with other tables "
          + "or lock types"),
  ER_CONNECT_TO_FOREIGN_DATA_SOURCE(1429, "HY000", "Unable to connect to foreign data source"),
  ER_QUERY_ON_FOREIGN_DATA_SOURCE(1430, "HY000", "There was a problem processing the query on the foreign data "
          + "source. Data source error"),
  ER_FOREIGN_DATA_SOURCE_DOESNT_EXIST(1431, "HY000", "The foreign data source you are trying to reference does not "
          + "exist. Data source error"),
  ER_FOREIGN_DATA_STRING_INVALID_CANT_CREATE(1432, "HY000", "Can't create federated table. The data source "
          + "connection string '%s' is not in the correct format"),
  ER_FOREIGN_DATA_STRING_INVALID(1433, "HY000", "The data source connection string '%s' is not in the correct format"),
  ER_CANT_CREATE_FEDERATED_TABLE(1434, "HY000", "Can't create federated table. Foreign data src error"),
  ER_TRG_IN_WRONG_SCHEMA(1435, "HY000", "Trigger in wrong schema"),
  ER_STACK_OVERRUN_NEED_MORE(1436, "HY000", "Thread stack overrun"),
  ER_TOO_LONG_BODY(1437, "42000", "Routine body for '%s' is too long"),
  ER_WARN_CANT_DROP_DEFAULT_KEYCACHE(1438, "HY000", "Cannot drop default keycache"),
  ER_TOO_BIG_DISPLAYWIDTH(1439, "42000", "Display width out of range for column '%s' (max = %s)"),
  ER_XAER_DUPID(1440, "XAE08", "XAER_DUPID"),
  ER_DATETIME_FUNCTION_OVERFLOW(1441, "22008", "Datetime function"),
  ER_CANT_UPDATE_USED_TABLE_IN_SF_OR_TRG(1442, "HY000", "Can't update table '%s' in stored function/trigger because "
          + "it is already used by statement which invoked this stored function/trigger."),
  ER_VIEW_PREVENT_UPDATE(1443, "HY000", "The definition of table '%s' prevents operation %s on table '%s'."),
  ER_PS_NO_RECURSION(1444, "HY000", "The prepared statement contains a stored routine call that refers to that same "
          + "statement. It's not allowed to execute a prepared statement in such a recursive manner"),
  ER_SP_CANT_SET_AUTOCOMMIT(1445, "HY000", "Not allowed to set autocommit from a stored function or trigger"),
  ER_MALFORMED_DEFINER(1446, "HY000", "Definer is not fully qualified"),
  ER_VIEW_FRM_NO_USER(1447, "HY000", "View '%s'.'%s' has no definer information (old table format). Current user is "
          + "used as definer. Please recreate the view!"),
  ER_VIEW_OTHER_USER(1448, "HY000", "You need the SUPER privilege for creation view with '%s'@'%s' definer"),
  ER_NO_SUCH_USER(1449, "HY000", "The user specified as a definer ('%s'@'%s') does not exist"),
  ER_FORBID_SCHEMA_CHANGE(1450, "HY000", "Changing schema from '%s' to '%s' is not allowed."),
  ER_ROW_IS_REFERENCED_2(1451, "23000", "Cannot delete or update a parent row"),
  ER_NO_REFERENCED_ROW_2(1452, "23000", "Cannot add or update a child row"),
  ER_SP_BAD_VAR_SHADOW(1453, "42000", "Variable '%s' must be quoted with `...`, or renamed"),
  ER_TRG_NO_DEFINER(1454, "HY000", "No definer attribute for trigger '%s'.'%s'. The trigger will be activated under "
          + "the authorization of the invoker, which may have insufficient privileges. Please recreate the trigger."),
  ER_OLD_FILE_FORMAT(1455, "HY000", "'%s' has an old format, you should re-create the '%s' object(s)"),
  ER_SP_RECURSION_LIMIT(1456, "HY000", "Recursive limit %s (as set by the max_sp_recursion_depth variable) was "
          + "exceeded for routine %s"),
  ER_SP_PROC_TABLE_CORRUPT(1457, "HY000", "Failed to load routine %s. The table mysql.proc is missing, corrupt, or "
          + "contains bad data (internal code %s)"),
  ER_SP_WRONG_NAME(1458, "42000", "Incorrect routine name '%s'"),
  ER_TABLE_NEEDS_UPGRADE(1459, "HY000", "Table upgrade required. Please do \"REPAIR TABLE `%s`\"or dump/reload to "
          + "fix it!"),
  ER_SP_NO_AGGREGATE(1460, "42000", "AGGREGATE is not supported for stored functions"),
  ER_MAX_PREPARED_STMT_COUNT_REACHED(1461, "42000", "Can't create more than max_prepared_stmt_count statements "
          + "(current value"),
  ER_VIEW_RECURSIVE(1462, "HY000", "`%s`.`%s` contains view recursion"),
  ER_NON_GROUPING_FIELD_USED(1463, "42000", "Non-grouping field '%s' is used in %s clause"),
  ER_TABLE_CANT_HANDLE_SPKEYS(1464, "HY000", "The used table type doesn't support SPATIAL indexes"),
  ER_NO_TRIGGERS_ON_SYSTEM_SCHEMA(1465, "HY000", "Triggers can not be created on system tables"),
  ER_REMOVED_SPACES(1466, "HY000", "Leading spaces are removed from name '%s'"),
  ER_AUTOINC_READ_FAILED(1467, "HY000", "Failed to read auto-increment value from storage engine"),
  ER_USERNAME(1468, "HY000", "user name"),
  ER_HOSTNAME(1469, "HY000", "host name"),
  ER_WRONG_STRING_LENGTH(1470, "HY000", "String '%s' is too long for %s (should be no longer than %s)"),
  ER_NON_INSERTABLE_TABLE(1471, "HY000", "The target table %s of the %s is not insertable-into"),
  ER_ADMIN_WRONG_MRG_TABLE(1472, "HY000", "Table '%s' is differently defined or of non-MyISAM type or doesn't exist"),
  ER_TOO_HIGH_LEVEL_OF_NESTING_FOR_SELECT(1473, "HY000", "Too high level of nesting for select"),
  ER_NAME_BECOMES_EMPTY(1474, "HY000", "Name '%s' has become ''"),
  ER_AMBIGUOUS_FIELD_TERM(1475, "HY000", "First character of the FIELDS TERMINATED string is ambiguous; please use "
          + "non-optional and non-empty FIELDS ENCLOSED BY"),
  ER_FOREIGN_SERVER_EXISTS(1476, "HY000", "The foreign server, %s, you are trying to create already exists."),
  ER_FOREIGN_SERVER_DOESNT_EXIST(1477, "HY000", "The foreign server name you are trying to reference does not exist."
          + "Data source error"),
  ER_ILLEGAL_HA_CREATE_OPTION(1478, "HY000", "Table storage engine '%s' does not support the create option '%s'"),
  ER_PARTITION_REQUIRES_VALUES_ERROR(1479, "HY000", "Syntax error"),
  ER_PARTITION_WRONG_VALUES_ERROR(1480, "HY000", "Only %s PARTITIONING can use VALUES %s in partition definition"),
  ER_PARTITION_MAXVALUE_ERROR(1481, "HY000", "MAXVALUE can only be used in last partition definition"),
  ER_PARTITION_SUBPARTITION_ERROR(1482, "HY000", "Subpartitions can only be hash partitions and by key"),
  ER_PARTITION_SUBPART_MIX_ERROR(1483, "HY000", "Must define subpartitions on all partitions if on one partition"),
  ER_PARTITION_WRONG_NO_PART_ERROR(1484, "HY000", "Wrong number of partitions defined, mismatch with previous setting"),
  ER_PARTITION_WRONG_NO_SUBPART_ERROR(1485, "HY000", "Wrong number of subpartitions defined, mismatch with previous setting"),
  ER_WRONG_EXPR_IN_PARTITION_FUNC_ERROR(1486, "HY000", "Constant, random or timezone-dependent expressions in (sub)"
          + "partitioning function are not allowed"),
  ER_NO_CONST_EXPR_IN_RANGE_OR_LIST_ERROR(1487, "HY000", "Expression in RANGE/LIST VALUES must be constant"),
  ER_FIELD_NOT_FOUND_PART_ERROR(1488, "HY000", "Field in list of fields for partition function not found in table"),
  ER_LIST_OF_FIELDS_ONLY_IN_HASH_ERROR(1489, "HY000", "List of fields is only allowed in KEY partitions"),
  ER_INCONSISTENT_PARTITION_INFO_ERROR(1490, "HY000", "The partition info in the frm file is not consistent with "
          + "what can be written into the frm file"),
  ER_PARTITION_FUNC_NOT_ALLOWED_ERROR(1491, "HY000", "The %s function returns the wrong type"),
  ER_PARTITIONS_MUST_BE_DEFINED_ERROR(1492, "HY000", "For %s partitions each partition must be defined"),
  ER_RANGE_NOT_INCREASING_ERROR(1493, "HY000", "VALUES LESS THAN value must be strictly increasing for each partition"),
  ER_INCONSISTENT_TYPE_OF_FUNCTIONS_ERROR(1494, "HY000", "VALUES value must be of same type as partition function"),
  ER_MULTIPLE_DEF_CONST_IN_LIST_PART_ERROR(1495, "HY000", "Multiple definition of same constant in list partitioning"),
  ER_PARTITION_ENTRY_ERROR(1496, "HY000", "Partitioning can not be used stand-alone in query"),
  ER_MIX_HANDLER_ERROR(1497, "HY000", "The mix of handlers in the partitions is not allowed in this version of MySQL"),
  ER_PARTITION_NOT_DEFINED_ERROR(1498, "HY000", "For the partitioned engine it is necessary to define all %s"),
  ER_TOO_MANY_PARTITIONS_ERROR(1499, "HY000", "Too many partitions (including subpartitions) were defined"),
  ER_SUBPARTITION_ERROR(1500, "HY000", "It is only possible to mix RANGE/LIST partitioning with HASH/KEY "
          + "partitioning for subpartitioning"),
  ER_CANT_CREATE_HANDLER_FILE(1501, "HY000", "Failed to create specific handler file"),
  ER_BLOB_FIELD_IN_PART_FUNC_ERROR(1502, "HY000", "A BLOB field is not allowed in partition function"),
  ER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF(1503, "HY000", "A %s must include all columns in the table's partitioning function"),
  ER_NO_PARTS_ERROR(1504, "HY000", "Number of %s = 0 is not an allowed value"),
  ER_PARTITION_MGMT_ON_NONPARTITIONED(1505, "HY000", "Partition management on a not partitioned table is not possible"),
  ER_FOREIGN_KEY_ON_PARTITIONED(1506, "HY000", "Foreign keys are not yet supported in conjunction with partitioning"),
  ER_DROP_PARTITION_NON_EXISTENT(1507, "HY000", "Error in list of partitions to %s"),
  ER_DROP_LAST_PARTITION(1508, "HY000", "Cannot remove all partitions, use DROP TABLE instead"),
  ER_COALESCE_ONLY_ON_HASH_PARTITION(1509, "HY000", "COALESCE PARTITION can only be used on HASH/KEY partitions"),
  ER_REORG_HASH_ONLY_ON_SAME_NO(1510, "HY000", "REORGANIZE PARTITION can only be used to reorganize partitions not "
          + "to change their numbers"),
  ER_REORG_NO_PARAM_ERROR(1511, "HY000", "REORGANIZE PARTITION without parameters can only be used on "
          + "auto-partitioned tables using HASH PARTITIONs"),
  ER_ONLY_ON_RANGE_LIST_PARTITION(1512, "HY000", "%s PARTITION can only be used on RANGE/LIST partitions"),
  ER_ADD_PARTITION_SUBPART_ERROR(1513, "HY000", "Trying to Add partition(s) with wrong number of subpartitions"),
  ER_ADD_PARTITION_NO_NEW_PARTITION(1514, "HY000", "At least one partition must be added"),
  ER_COALESCE_PARTITION_NO_PARTITION(1515, "HY000", "At least one partition must be coalesced"),
  ER_REORG_PARTITION_NOT_EXIST(1516, "HY000", "More partitions to reorganize than there are partitions"),
  ER_SAME_NAME_PARTITION(1517, "HY000", "Duplicate partition name %s"),
  ER_NO_BINLOG_ERROR(1518, "HY000", "It is not allowed to shut off binlog on this command"),
  ER_CONSECUTIVE_REORG_PARTITIONS(1519, "HY000", "When reorganizing a set of partitions they must be in consecutive order"),
  ER_REORG_OUTSIDE_RANGE(1520, "HY000", "Reorganize of range partitions cannot change total ranges except for last "
          + "partition where it can extend the range"),
  ER_PARTITION_FUNCTION_FAILURE(1521, "HY000", "Partition function not supported in this version for this handler"),
  ER_PART_STATE_ERROR(1522, "HY000", "Partition state cannot be defined from CREATE/ALTER TABLE"),
  ER_LIMITED_PART_RANGE(1523, "HY000", "The %s handler only supports 32 bit integers in VALUES"),
  ER_PLUGIN_IS_NOT_LOADED(1524, "HY000", "Plugin '%s' is not loaded"),
  ER_WRONG_VALUE(1525, "HY000", "Incorrect %s value"),
  ER_NO_PARTITION_FOR_GIVEN_VALUE(1526, "HY000", "Table has no partition for value %s"),
  ER_FILEGROUP_OPTION_ONLY_ONCE(1527, "HY000", "It is not allowed to specify %s more than once"),
  ER_CREATE_FILEGROUP_FAILED(1528, "HY000", "Failed to create %s"),
  ER_DROP_FILEGROUP_FAILED(1529, "HY000", "Failed to drop %s"),
  ER_TABLESPACE_AUTO_EXTEND_ERROR(1530, "HY000", "The handler doesn't support autoextend of tablespaces"),
  ER_WRONG_SIZE_NUMBER(1531, "HY000", "A size parameter was incorrectly specified, either number or on the form 10M"),
  ER_SIZE_OVERFLOW_ERROR(1532, "HY000", "The size number was correct but we don't allow the digit part to be more "
          + "than 2 billion"),
  ER_ALTER_FILEGROUP_FAILED(1533, "HY000", "Failed to alter"),
  ER_BINLOG_ROW_LOGGING_FAILED(1534, "HY000", "Writing one row to the row-based binary log failed"),
  ER_BINLOG_ROW_WRONG_TABLE_DEF(1535, "HY000", "Table definition on master and slave does not match"),
  ER_BINLOG_ROW_RBR_TO_SBR(1536, "HY000", "Slave running with --log-slave-updates must use row-based binary logging "
          + "to be able to replicate row-based binary log events"),
  ER_EVENT_ALREADY_EXISTS(1537, "HY000", "Event '%s' already exists"),
  ER_EVENT_STORE_FAILED(1538, "HY000", "Failed to store event %s. Error code %s from storage engine."),
  ER_EVENT_DOES_NOT_EXIST(1539, "HY000", "Unknown event '%s'"),
  ER_EVENT_CANT_ALTER(1540, "HY000", "Failed to alter event '%s'"),
  ER_EVENT_DROP_FAILED(1541, "HY000", "Failed to drop %s"),
  ER_EVENT_INTERVAL_NOT_POSITIVE_OR_TOO_BIG(1542, "HY000", "INTERVAL is either not positive or too big"),
  ER_EVENT_ENDS_BEFORE_STARTS(1543, "HY000", "ENDS is either invalid or before STARTS"),
  ER_EVENT_EXEC_TIME_IN_THE_PAST(1544, "HY000", "Event execution time is in the past. Event has been disabled"),
  ER_EVENT_OPEN_TABLE_FAILED(1545, "HY000", "Failed to open mysql.event"),
  ER_EVENT_NEITHER_M_EXPR_NOR_M_AT(1546, "HY000", "No datetime expression provided"),
  ER_OBSOLETE_COL_COUNT_DOESNT_MATCH_CORRUPTED(1547, "HY000", "Column count of mysql.%s is wrong. Expected %s, found"
          + "%s. The table is probably corrupted"),
  ER_OBSOLETE_CANNOT_LOAD_FROM_TABLE(1548, "HY000", "Cannot load from mysql.%s. The table is probably corrupted"),
  ER_EVENT_CANNOT_DELETE(1549, "HY000", "Failed to delete the event from mysql.event"),
  ER_EVENT_COMPILE_ERROR(1550, "HY000", "Error during compilation of event's body"),
  ER_EVENT_SAME_NAME(1551, "HY000", "Same old and new event name"),
  ER_EVENT_DATA_TOO_LONG(1552, "HY000", "Data for column '%s' too long"),
  ER_DROP_INDEX_FK(1553, "HY000", "Cannot drop index '%s'"),
  ER_WARN_DEPRECATED_SYNTAX_WITH_VER(1554, "HY000", "The syntax '%s' is deprecated and will be removed in MySQL %s. "
          + "Please use %s instead"),
  ER_CANT_WRITE_LOCK_LOG_TABLE(1555, "HY000", "You can't write-lock a log table. Only read access is possible"),
  ER_CANT_LOCK_LOG_TABLE(1556, "HY000", "You can't use locks with log tables."),
  ER_FOREIGN_DUPLICATE_KEY_OLD_UNUSED(1557, "23000", "Upholding foreign key constraints for table '%s', entry '%s', "
          + "key %s would lead to a duplicate entry"),
  ER_COL_COUNT_DOESNT_MATCH_PLEASE_UPDATE(1558, "HY000", "Column count of mysql.%s is wrong. Expected %s, found %s. "
          + "Created with MySQL %s, now running %s. Please use mysql_upgrade to fix this error."),
  ER_TEMP_TABLE_PREVENTS_SWITCH_OUT_OF_RBR(1559, "HY000", "Cannot switch out of the row-based binary log format when"
          + "the session has open temporary tables"),
  ER_STORED_FUNCTION_PREVENTS_SWITCH_BINLOG_FORMAT(1560, "HY000", "Cannot change the binary logging format inside a "
          + "stored function or trigger"),
  ER_NDB_CANT_SWITCH_BINLOG_FORMAT(1561, "HY000", "The NDB cluster engine does not support changing the binlog "
          + "format on the fly yet"),
  ER_PARTITION_NO_TEMPORARY(1562, "HY000", "Cannot create temporary table with partitions"),
  ER_PARTITION_CONST_DOMAIN_ERROR(1563, "HY000", "Partition constant is out of partition function domain"),
  ER_PARTITION_FUNCTION_IS_NOT_ALLOWED(1564, "HY000", "This partition function is not allowed"),
  ER_DDL_LOG_ERROR(1565, "HY000", "Error in DDL log"),
  ER_NULL_IN_VALUES_LESS_THAN(1566, "HY000", "Not allowed to use NULL value in VALUES LESS THAN"),
  ER_WRONG_PARTITION_NAME(1567, "HY000", "Incorrect partition name"),
  ER_CANT_CHANGE_TX_CHARACTERISTICS(1568, "25001", "Transaction characteristics can't be changed while a transaction"
          + "is in progress"),
  ER_DUP_ENTRY_AUTOINCREMENT_CASE(1569, "HY000", "ALTER TABLE causes auto_increment resequencing, resulting in "
          + "duplicate entry '%s' for key '%s'"),
  ER_EVENT_MODIFY_QUEUE_ERROR(1570, "HY000", "Internal scheduler error %s"),
  ER_EVENT_SET_VAR_ERROR(1571, "HY000", "Error during starting/stopping of the scheduler. Error code %u"),
  ER_PARTITION_MERGE_ERROR(1572, "HY000", "Engine cannot be used in partitioned tables"),
  ER_CANT_ACTIVATE_LOG(1573, "HY000", "Cannot activate '%s' log"),
  ER_RBR_NOT_AVAILABLE(1574, "HY000", "The server was not built with row-based replication"),
  ER_BASE64_DECODE_ERROR(1575, "HY000", "Decoding of base64 string failed"),
  ER_EVENT_RECURSION_FORBIDDEN(1576, "HY000", "Recursion of EVENT DDL statements is forbidden when body is present"),
  ER_EVENTS_DB_ERROR(1577, "HY000", "Cannot proceed because system tables used by Event Scheduler were found damaged"
          + "at server start"),
  ER_ONLY_INTEGERS_ALLOWED(1578, "HY000", "Only integers allowed as number here"),
  ER_UNSUPORTED_LOG_ENGINE(1579, "HY000", "This storage engine cannot be used for log tables"),
  ER_BAD_LOG_STATEMENT(1580, "HY000", "You cannot '%s' a log table if logging is enabled"),
  ER_CANT_RENAME_LOG_TABLE(1581, "HY000", "Cannot rename '%s'. When logging enabled, rename to/from log table must "
          + "rename two tables"),
  ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT(1582, "42000", "Incorrect parameter count in the call to native function '%s'"),
  ER_WRONG_PARAMETERS_TO_NATIVE_FCT(1583, "42000", "Incorrect parameters in the call to native function '%s'"),
  ER_WRONG_PARAMETERS_TO_STORED_FCT(1584, "42000", "Incorrect parameters in the call to stored function %s"),
  ER_NATIVE_FCT_NAME_COLLISION(1585, "HY000", "This function '%s' has the same name as a native function"),
  ER_DUP_ENTRY_WITH_KEY_NAME(1586, "23000", "Duplicate entry '%s' for key '%s'"),
  ER_BINLOG_PURGE_EMFILE(1587, "HY000", "Too many files opened, please execute the command again"),
  ER_EVENT_CANNOT_CREATE_IN_THE_PAST(1588, "HY000", "Event execution time is in the past and ON COMPLETION NOT "
          + "PRESERVE is set. The event was dropped immediately after creation."),
  ER_EVENT_CANNOT_ALTER_IN_THE_PAST(1589, "HY000", "Event execution time is in the past and ON COMPLETION NOT "
          + "PRESERVE is set. The event was not changed. Specify a time in the future."),
  ER_SLAVE_INCIDENT(1590, "HY000", "The incident %s occured on the master. Message"),
  ER_NO_PARTITION_FOR_GIVEN_VALUE_SILENT(1591, "HY000", "Table has no partition for some existing values"),
  ER_BINLOG_UNSAFE_STATEMENT(1592, "HY000", "Unsafe statement written to the binary log using statement format since"
          + "BINLOG_FORMAT = STATEMENT. %s"),
  ER_SLAVE_FATAL_ERROR(1593, "HY000", "Fatal error"),
  ER_SLAVE_RELAY_LOG_READ_FAILURE(1594, "HY000", "Relay log read failure"),
  ER_SLAVE_RELAY_LOG_WRITE_FAILURE(1595, "HY000", "Relay log write failure"),
  ER_SLAVE_CREATE_EVENT_FAILURE(1596, "HY000", "Failed to create %s"),
  ER_SLAVE_MASTER_COM_FAILURE(1597, "HY000", "Master command %s failed"),
  ER_BINLOG_LOGGING_IMPOSSIBLE(1598, "HY000", "Binary logging not possible. Message"),
  ER_VIEW_NO_CREATION_CTX(1599, "HY000", "View `%s`.`%s` has no creation context"),
  ER_VIEW_INVALID_CREATION_CTX(1600, "HY000", "Creation context of view `%s`.`%s' is invalid"),
  ER_SR_INVALID_CREATION_CTX(1601, "HY000", "Creation context of stored routine `%s`.`%s` is invalid"),
  ER_TRG_CORRUPTED_FILE(1602, "HY000", "Corrupted TRG file for table `%s`.`%s`"),
  ER_TRG_NO_CREATION_CTX(1603, "HY000", "Triggers for table `%s`.`%s` have no creation context"),
  ER_TRG_INVALID_CREATION_CTX(1604, "HY000", "Trigger creation context of table `%s`.`%s` is invalid"),
  ER_EVENT_INVALID_CREATION_CTX(1605, "HY000", "Creation context of event `%s`.`%s` is invalid"),
  ER_TRG_CANT_OPEN_TABLE(1606, "HY000", "Cannot open table for trigger `%s`.`%s`"),
  ER_CANT_CREATE_SROUTINE(1607, "HY000", "Cannot create stored routine `%s`. Check warnings"),
  ER_NEVER_USED(1608, "HY000", "Ambiguous slave modes combination. %s"),
  ER_NO_FORMAT_DESCRIPTION_EVENT_BEFORE_BINLOG_STATEMENT(1609, "HY000", "The BINLOG statement of type `%s` was not "
          + "preceded by a format description BINLOG statement."),
  ER_SLAVE_CORRUPT_EVENT(1610, "HY000", "Corrupted replication event was detected"),
  ER_LOAD_DATA_INVALID_COLUMN(1611, "HY000", "Invalid column reference (%s) in LOAD DATA"),
  ER_LOAD_DATA_INVALID_COLUMN_UNUSED(1611, "HY000", "Invalid column reference (%s) in LOAD DATA"),
  ER_LOG_PURGE_NO_FILE(1612, "HY000", "Being purged log %s was not found"),
  ER_XA_RBTIMEOUT(1613, "XA106", "XA_RBTIMEOUT"),
  ER_XA_RBDEADLOCK(1614, "XA102", "XA_RBDEADLOCK"),
  ER_NEED_REPREPARE(1615, "HY000", "Prepared statement needs to be re-prepared"),
  ER_DELAYED_NOT_SUPPORTED(1616, "HY000", "DELAYED option not supported for table '%s'"),
  WARN_NO_MASTER_INFO(1617, "HY000", "The master info structure does not exist"),
  WARN_OPTION_IGNORED(1618, "HY000", "<%s> option ignored"),
  WARN_PLUGIN_DELETE_BUILTIN(1619, "HY000", "Built-in plugins cannot be deleted"),
  ER_PLUGIN_DELETE_BUILTIN(1619, "HY000", "Built-in plugins cannot be deleted"),
  WARN_PLUGIN_BUSY(1620, "HY000", "Plugin is busy and will be uninstalled on shutdown"),
  ER_VARIABLE_IS_READONLY(1621, "HY000", "variable '%s' is read-only"),
  ER_WARN_ENGINE_TRANSACTION_ROLLBACK(1622, "HY000", "Storage engine %s does not support rollback for this statement"
          + ". Transaction rolled back and must be restarted"),
  ER_SLAVE_HEARTBEAT_FAILURE(1623, "HY000", "Unexpected master's heartbeat data"),
  ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE(1624, "HY000", "The requested value for the heartbeat period is either "
          + "negative or exceeds the maximum allowed (%s seconds)."),
  ER_NDB_REPLICATION_SCHEMA_ERROR(1625, "HY000", "Bad schema for mysql.ndb_replication table. Message"),
  ER_CONFLICT_FN_PARSE_ERROR(1626, "HY000", "Error in parsing conflict function. Message"),
  ER_EXCEPTIONS_WRITE_ERROR(1627, "HY000", "Write to exceptions table failed. Message"),
  ER_TOO_LONG_TABLE_COMMENT(1628, "HY000", "Comment for table '%s' is too long (max = %s)"),
  ER_TOO_LONG_FIELD_COMMENT(1629, "HY000", "Comment for field '%s' is too long (max = %s)"),
  ER_FUNC_INEXISTENT_NAME_COLLISION(1630, "42000", "FUNCTION %s does not exist. Check the 'Function Name Parsing and"
          + "Resolution' section in the Reference Manual"),
  ER_DATABASE_NAME(1631, "HY000", "Database"),
  ER_TABLE_NAME(1632, "HY000", "Table"),
  ER_PARTITION_NAME(1633, "HY000", "Partition"),
  ER_SUBPARTITION_NAME(1634, "HY000", "Subpartition"),
  ER_TEMPORARY_NAME(1635, "HY000", "Temporary"),
  ER_RENAMED_NAME(1636, "HY000", "Renamed"),
  ER_TOO_MANY_CONCURRENT_TRXS(1637, "HY000", "Too many active concurrent transactions"),
  WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED(1638, "HY000", "Non-ASCII separator arguments are not fully supported"),
  ER_DEBUG_SYNC_TIMEOUT(1639, "HY000", "debug sync point wait timed out"),
  ER_DEBUG_SYNC_HIT_LIMIT(1640, "HY000", "debug sync point hit limit reached"),
  ER_DUP_SIGNAL_SET(1641, "42000", "Duplicate condition information item '%s'"),
  ER_SIGNAL_WARN(1642, "01000", "Unhandled user-defined warning condition"),
  ER_SIGNAL_NOT_FOUND(1643, "02000", "Unhandled user-defined not found condition"),
  ER_SIGNAL_EXCEPTION(1644, "HY000", "Unhandled user-defined exception condition"),
  ER_RESIGNAL_WITHOUT_ACTIVE_HANDLER(1645, "0K000", "RESIGNAL when handler not active"),
  ER_SIGNAL_BAD_CONDITION_TYPE(1646, "HY000", "SIGNAL/RESIGNAL can only use a CONDITION defined with SQLSTATE"),
  WARN_COND_ITEM_TRUNCATED(1647, "HY000", "Data truncated for condition item '%s'"),
  ER_COND_ITEM_TOO_LONG(1648, "HY000", "Data too long for condition item '%s'"),
  ER_UNKNOWN_LOCALE(1649, "HY000", "Unknown locale"),
  ER_SLAVE_IGNORE_SERVER_IDS(1650, "HY000", "The requested server id %s clashes with the slave startup option "
          + "--replicate-same-server-id"),
  ER_QUERY_CACHE_DISABLED(1651, "HY000", "Query cache is disabled; restart the server with query_cache_type=1 to "
          + "enable it"),
  ER_SAME_NAME_PARTITION_FIELD(1652, "HY000", "Duplicate partition field name '%s'"),
  ER_PARTITION_COLUMN_LIST_ERROR(1653, "HY000", "Inconsistency in usage of column lists for partitioning"),
  ER_WRONG_TYPE_COLUMN_VALUE_ERROR(1654, "HY000", "Partition column values of incorrect type"),
  ER_TOO_MANY_PARTITION_FUNC_FIELDS_ERROR(1655, "HY000", "Too many fields in '%s'"),
  ER_MAXVALUE_IN_VALUES_IN(1656, "HY000", "Cannot use MAXVALUE as value in VALUES IN"),
  ER_TOO_MANY_VALUES_ERROR(1657, "HY000", "Cannot have more than one value for this type of %s partitioning"),
  ER_ROW_SINGLE_PARTITION_FIELD_ERROR(1658, "HY000", "Row expressions in VALUES IN only allowed for multi-field "
          + "column partitioning"),
  ER_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD(1659, "HY000", "Field '%s' is of a not allowed type for this type of "
          + "partitioning"),
  ER_PARTITION_FIELDS_TOO_LONG(1660, "HY000", "The total length of the partitioning fields is too large"),
  ER_BINLOG_ROW_ENGINE_AND_STMT_ENGINE(1661, "HY000", "Cannot execute statement"),
  ER_BINLOG_ROW_MODE_AND_STMT_ENGINE(1662, "HY000", "Cannot execute statement"),
  ER_BINLOG_UNSAFE_AND_STMT_ENGINE(1663, "HY000", "Cannot execute statement"),
  ER_BINLOG_ROW_INJECTION_AND_STMT_ENGINE(1664, "HY000", "Cannot execute statement"),
  ER_BINLOG_STMT_MODE_AND_ROW_ENGINE(1665, "HY000", "Cannot execute statement"),
  ER_BINLOG_ROW_INJECTION_AND_STMT_MODE(1666, "HY000", "Cannot execute statement"),
  ER_BINLOG_MULTIPLE_ENGINES_AND_SELF_LOGGING_ENGINE(1667, "HY000", "Cannot execute statement"),
  ER_BINLOG_UNSAFE_LIMIT(1668, "HY000", "The statement is unsafe because it uses a LIMIT clause. This is unsafe "
          + "because the set of rows included cannot be predicted."),
  ER_UNUSED4(1669, "HY000", "The statement is unsafe because it uses INSERT DELAYED. This is unsafe because the "
          + "times when rows are inserted cannot be predicted."),
  ER_BINLOG_UNSAFE_SYSTEM_TABLE(1670, "HY000", "The statement is unsafe because it uses the general log, slow query "
          + "log, or performance_schema table(s). This is unsafe because system tables may differ on slaves."),
  ER_BINLOG_UNSAFE_AUTOINC_COLUMNS(1671, "HY000", "Statement is unsafe because it invokes a trigger or a stored "
          + "function that inserts into an AUTO_INCREMENT column. Inserted values cannot be logged correctly."),
  ER_BINLOG_UNSAFE_UDF(1672, "HY000", "Statement is unsafe because it uses a UDF which may not return the same value"
          + "on the slave."),
  ER_BINLOG_UNSAFE_SYSTEM_VARIABLE(1673, "HY000", "Statement is unsafe because it uses a system variable that may "
          + "have a different value on the slave."),
  ER_BINLOG_UNSAFE_SYSTEM_FUNCTION(1674, "HY000", "Statement is unsafe because it uses a system function that may "
          + "return a different value on the slave."),
  ER_BINLOG_UNSAFE_NONTRANS_AFTER_TRANS(1675, "HY000", "Statement is unsafe because it accesses a non-transactional "
          + "table after accessing a transactional table within the same transaction."),
  ER_MESSAGE_AND_STATEMENT(1676, "HY000", "%s Statement"),
  ER_SLAVE_CONVERSION_FAILED(1677, "HY000", "Column %s of table '%s.%s' cannot be converted from type '%s' to type '%s'"),
  ER_SLAVE_CANT_CREATE_CONVERSION(1678, "HY000", "Can't create conversion table for table '%s.%s'"),
  ER_INSIDE_TRANSACTION_PREVENTS_SWITCH_BINLOG_FORMAT(1679, "HY000", "Cannot modify @@session.binlog_format inside a transaction"),
  ER_PATH_LENGTH(1680, "HY000", "The path specified for %s is too long."),
  ER_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT(1681, "HY000", "'%s' is deprecated and will be removed in a future release."),
  ER_WRONG_NATIVE_TABLE_STRUCTURE(1682, "HY000", "Native table '%s'.'%s' has the wrong structure"),
  ER_WRONG_PERFSCHEMA_USAGE(1683, "HY000", "Invalid performance_schema usage."),
  ER_WARN_I_S_SKIPPED_TABLE(1684, "HY000", "Table '%s'.'%s' was skipped since its definition is being modified by "
          + "concurrent DDL statement"),
  ER_INSIDE_TRANSACTION_PREVENTS_SWITCH_BINLOG_DIRECT(1685, "HY000", "Cannot modify @@session"
          + ".binlog_direct_non_transactional_updates inside a transaction"),
  ER_STORED_FUNCTION_PREVENTS_SWITCH_BINLOG_DIRECT(1686, "HY000", "Cannot change the binlog direct flag inside a "
          + "stored function or trigger"),
  ER_SPATIAL_MUST_HAVE_GEOM_COL(1687, "42000", "A SPATIAL index may only contain a geometrical type column"),
  ER_TOO_LONG_INDEX_COMMENT(1688, "HY000", "Comment for index '%s' is too long (max = %s)"),
  ER_LOCK_ABORTED(1689, "HY000", "Wait on a lock was aborted due to a pending exclusive lock"),
  ER_DATA_OUT_OF_RANGE(1690, "22003", "%s value is out of range in '%s'"),
  ER_WRONG_SPVAR_TYPE_IN_LIMIT(1691, "HY000", "A variable of a non-integer based type in LIMIT clause"),
  ER_BINLOG_UNSAFE_MULTIPLE_ENGINES_AND_SELF_LOGGING_ENGINE(1692, "HY000", "Mixing self-logging and non-self-logging"
          + "engines in a statement is unsafe."),
  ER_BINLOG_UNSAFE_MIXED_STATEMENT(1693, "HY000", "Statement accesses nontransactional table as well as "
          + "transactional or temporary table, and writes to any of them."),
  ER_INSIDE_TRANSACTION_PREVENTS_SWITCH_SQL_LOG_BIN(1694, "HY000", "Cannot modify @@session.sql_log_bin inside a transaction"),
  ER_STORED_FUNCTION_PREVENTS_SWITCH_SQL_LOG_BIN(1695, "HY000", "Cannot change the sql_log_bin inside a stored "
          + "function or trigger"),
  ER_FAILED_READ_FROM_PAR_FILE(1696, "HY000", "Failed to read from the .par file"),
  ER_VALUES_IS_NOT_INT_TYPE_ERROR(1697, "HY000", "VALUES value for partition '%s' must have type INT"),
  ER_ACCESS_DENIED_NO_PASSWORD_ERROR(1698, "28000", "Access denied for user '%s'@'%s'"),
  ER_SET_PASSWORD_AUTH_PLUGIN(1699, "HY000", "SET PASSWORD has no significance for users authenticating via plugins"),
  ER_GRANT_PLUGIN_USER_EXISTS(1700, "HY000", "GRANT with IDENTIFIED WITH is illegal because the user %-.*s already exists"),
  ER_TRUNCATE_ILLEGAL_FK(1701, "42000", "Cannot truncate a table referenced in a foreign key constraint (%s)"),
  ER_PLUGIN_IS_PERMANENT(1702, "HY000", "Plugin '%s' is force_plus_permanent and can not be unloaded"),
  ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE_MIN(1703, "HY000", "The requested value for the heartbeat period is less "
          + "than 1 millisecond. The value is reset to 0, meaning that heartbeating will effectively be disabled."),
  ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE_MAX(1704, "HY000", "The requested value for the heartbeat period exceeds the"
          + "value of `slave_net_timeout' seconds. A sensible value for the period should be less than the timeout."),
  ER_STMT_CACHE_FULL(1705, "HY000", "Multi-row statements required more than 'max_binlog_stmt_cache_size' bytes of "
          + "storage; increase this mysqld variable and try again"),
  ER_MULTI_UPDATE_KEY_CONFLICT(1706, "HY000", "Primary key/partition key update is not allowed since the table is "
          + "updated both as '%s' and '%s'."),
  ER_TABLE_NEEDS_REBUILD(1707, "HY000", "Table rebuild required. Please do \"ALTER TABLE `%s` FORCE\"or dump/reload"
          + "to fix it!"),
  WARN_OPTION_BELOW_LIMIT(1708, "HY000", "The value of '%s' should be no less than the value of '%s'"),
  ER_INDEX_COLUMN_TOO_LONG(1709, "HY000", "Index column size too large. The maximum column size is %lu bytes."),
  ER_ERROR_IN_TRIGGER_BODY(1710, "HY000", "Trigger '%s' has an error in its body"),
  ER_ERROR_IN_UNKNOWN_TRIGGER_BODY(1711, "HY000", "Unknown trigger has an error in its body"),
  ER_INDEX_CORRUPT(1712, "HY000", "Index %s is corrupted"),
  ER_UNDO_RECORD_TOO_BIG(1713, "HY000", "Undo log record is too big."),
  ER_BINLOG_UNSAFE_INSERT_IGNORE_SELECT(1714, "HY000", "INSERT IGNORE... SELECT is unsafe because the order in which"
          + "rows are retrieved by the SELECT determines which (if any) rows are ignored. This order cannot be predicted and"
          + "may differ on master and the slave."),
  ER_BINLOG_UNSAFE_INSERT_SELECT_UPDATE(1715, "HY000", "INSERT... SELECT... ON DUPLICATE KEY UPDATE is unsafe "
          + "because the order in which rows are retrieved by the SELECT determines which (if any) rows are updated. This "
          + "order cannot be predicted and may differ on master and the slave."),
  ER_BINLOG_UNSAFE_REPLACE_SELECT(1716, "HY000", "REPLACE... SELECT is unsafe because the order in which rows are "
          + "retrieved by the SELECT determines which (if any) rows are replaced. This order cannot be predicted and may "
          + "differ on master and the slave."),
  ER_BINLOG_UNSAFE_CREATE_IGNORE_SELECT(1717, "HY000", "CREATE... IGNORE SELECT is unsafe because the order in which"
          + "rows are retrieved by the SELECT determines which (if any) rows are ignored. This order cannot be predicted and"
          + "may differ on master and the slave."),
  ER_BINLOG_UNSAFE_CREATE_REPLACE_SELECT(1718, "HY000", "CREATE... REPLACE SELECT is unsafe because the order in "
          + "which rows are retrieved by the SELECT determines which (if any) rows are replaced. This order cannot be "
          + "predicted and may differ on master and the slave."),
  ER_BINLOG_UNSAFE_UPDATE_IGNORE(1719, "HY000", "UPDATE IGNORE is unsafe because the order in which rows are updated"
          + "determines which (if any) rows are ignored. This order cannot be predicted and may differ on master and the slave."),
  ER_PLUGIN_NO_UNINSTALL(1720, "HY000", "Plugin '%s' is marked as not dynamically uninstallable. You have to stop "
          + "the server to uninstall it."),
  ER_PLUGIN_NO_INSTALL(1721, "HY000", "Plugin '%s' is marked as not dynamically installable. You have to stop the "
          + "server to install it."),
  ER_BINLOG_UNSAFE_WRITE_AUTOINC_SELECT(1722, "HY000", "Statements writing to a table with an auto-increment column "
          + "after selecting from another table are unsafe because the order in which rows are retrieved determines what (if "
          + "any) rows will be written. This order cannot be predicted and may differ on master and the slave."),
  ER_BINLOG_UNSAFE_CREATE_SELECT_AUTOINC(1723, "HY000", "CREATE TABLE... SELECT... on a table with an auto-increment"
          + "column is unsafe because the order in which rows are retrieved by the SELECT determines which (if any) rows are"
          + "inserted. This order cannot be predicted and may differ on master and the slave."),
  ER_BINLOG_UNSAFE_INSERT_TWO_KEYS(1724, "HY000", "INSERT... ON DUPLICATE KEY UPDATE on a table with more than one "
          + "UNIQUE KEY is unsafe"),
  ER_TABLE_IN_FK_CHECK(1725, "HY000", "Table is being used in foreign key check."),
  ER_UNSUPPORTED_ENGINE(1726, "HY000", "Storage engine '%s' does not support system tables. [%s.%s]"),
  ER_BINLOG_UNSAFE_AUTOINC_NOT_FIRST(1727, "HY000", "INSERT into autoincrement field which is not the first part in "
          + "the composed primary key is unsafe."),
  ER_CANNOT_LOAD_FROM_TABLE_V2(1728, "HY000", "Cannot load from %s.%s. The table is probably corrupted"),
  ER_MASTER_DELAY_VALUE_OUT_OF_RANGE(1729, "HY000", "The requested value %s for the master delay exceeds the maximum %u"),
  ER_ONLY_FD_AND_RBR_EVENTS_ALLOWED_IN_BINLOG_STATEMENT(1730, "HY000", "Only Format_description_log_event and row "
          + "events are allowed in BINLOG statements (but %s was provided)"),
  ER_PARTITION_EXCHANGE_DIFFERENT_OPTION(1731, "HY000", "Non matching attribute '%s' between partition and table"),
  ER_PARTITION_EXCHANGE_PART_TABLE(1732, "HY000", "Table to exchange with partition is partitioned"),
  ER_PARTITION_EXCHANGE_TEMP_TABLE(1733, "HY000", "Table to exchange with partition is temporary"),
  ER_PARTITION_INSTEAD_OF_SUBPARTITION(1734, "HY000", "Subpartitioned table, use subpartition instead of partition"),
  ER_UNKNOWN_PARTITION(1735, "HY000", "Unknown partition '%s' in table '%s'"),
  ER_TABLES_DIFFERENT_METADATA(1736, "HY000", "Tables have different definitions"),
  ER_ROW_DOES_NOT_MATCH_PARTITION(1737, "HY000", "Found a row that does not match the partition"),
  ER_BINLOG_CACHE_SIZE_GREATER_THAN_MAX(1738, "HY000", "Option binlog_cache_size (%lu) is greater than "
          + "max_binlog_cache_size (%lu); setting binlog_cache_size equal to max_binlog_cache_size."),
  ER_WARN_INDEX_NOT_APPLICABLE(1739, "HY000", "Cannot use %s access on index '%s' due to type or collation "
          + "conversion on field '%s'"),
  ER_PARTITION_EXCHANGE_FOREIGN_KEY(1740, "HY000", "Table to exchange with partition has foreign key references"),
  ER_NO_SUCH_KEY_VALUE(1741, "HY000", "Key value '%s' was not found in table '%s.%s'"),
  ER_RPL_INFO_DATA_TOO_LONG(1742, "HY000", "Data for column '%s' too long"),
  ER_NETWORK_READ_EVENT_CHECKSUM_FAILURE(1743, "HY000", "Replication event checksum verification failed while "
          + "reading from network."),
  ER_BINLOG_READ_EVENT_CHECKSUM_FAILURE(1744, "HY000", "Replication event checksum verification failed while reading"
          + "from a log file."),
  ER_BINLOG_STMT_CACHE_SIZE_GREATER_THAN_MAX(1745, "HY000", "Option binlog_stmt_cache_size (%lu) is greater than "
          + "max_binlog_stmt_cache_size (%lu); setting binlog_stmt_cache_size equal to max_binlog_stmt_cache_size."),
  ER_CANT_UPDATE_TABLE_IN_CREATE_TABLE_SELECT(1746, "HY000", "Can't update table '%s' while '%s' is being created."),
  ER_PARTITION_CLAUSE_ON_NONPARTITIONED(1747, "HY000", "PARTITION () clause on non partitioned table"),
  ER_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET(1748, "HY000", "Found a row not matching the given partition set"),
  ER_NO_SUCH_PARTITION__UNUSED(1749, "HY000", "partition '%s' doesn't exist"),
  ER_CHANGE_RPL_INFO_REPOSITORY_FAILURE(1750, "HY000", "Failure while changing the type of replication repository"),
  ER_WARNING_NOT_COMPLETE_ROLLBACK_WITH_CREATED_TEMP_TABLE(1751, "HY000", "The creation of some temporary tables "
          + "could not be rolled back."),
  ER_WARNING_NOT_COMPLETE_ROLLBACK_WITH_DROPPED_TEMP_TABLE(1752, "HY000", "Some temporary tables were dropped, but "
          + "these operations could not be rolled back."),
  ER_MTS_FEATURE_IS_NOT_SUPPORTED(1753, "HY000", "%s is not supported in multi-threaded slave mode. %s"),
  ER_MTS_UPDATED_DBS_GREATER_MAX(1754, "HY000", "The number of modified databases exceeds the maximum %s; the "
          + "database names will not be included in the replication event metadata."),
  ER_MTS_CANT_PARALLEL(1755, "HY000", "Cannot execute the current event group in the parallel mode. Encountered "
          + "event %s, relay-log name %s, position %s which prevents execution of this event group in parallel mode. Reason"),
  ER_MTS_INCONSISTENT_DATA(1756, "HY000", "%s"),
  ER_FULLTEXT_NOT_SUPPORTED_WITH_PARTITIONING(1757, "HY000", "FULLTEXT index is not supported for partitioned tables."),
  ER_DA_INVALID_CONDITION_NUMBER(1758, "35000", "Invalid condition number"),
  ER_INSECURE_PLAIN_TEXT(1759, "HY000", "Sending passwords in plain text without SSL/TLS is extremely insecure."),
  ER_INSECURE_CHANGE_MASTER(1760, "HY000", "Storing MySQL user name or password information in the master info "
          + "repository is not secure and is therefore not recommended. Please consider using the USER and PASSWORD "
          + "connection options for START SLAVE; see the 'START SLAVE Syntax' in the MySQL Manual for more information."),
  ER_FOREIGN_DUPLICATE_KEY_WITH_CHILD_INFO(1761, "23000", "Foreign key constraint for table '%s', record '%s' would "
          + "lead to a duplicate entry in table '%s', key '%s'"),
  ER_FOREIGN_DUPLICATE_KEY_WITHOUT_CHILD_INFO(1762, "23000", "Foreign key constraint for table '%s', record '%s' "
          + "would lead to a duplicate entry in a child table"),
  ER_SQLTHREAD_WITH_SECURE_SLAVE(1763, "HY000", "Setting authentication options is not possible when only the Slave "
          + "SQL Thread is being started."),
  ER_TABLE_HAS_NO_FT(1764, "HY000", "The table does not have FULLTEXT index to support this query"),
  ER_VARIABLE_NOT_SETTABLE_IN_SF_OR_TRIGGER(1765, "HY000", "The system variable %s cannot be set in stored functions"
          + "or triggers."),
  ER_VARIABLE_NOT_SETTABLE_IN_TRANSACTION(1766, "HY000", "The system variable %s cannot be set when there is an "
          + "ongoing transaction."),
  ER_GTID_NEXT_IS_NOT_IN_GTID_NEXT_LIST(1767, "HY000", "The system variable @@SESSION.GTID_NEXT has the value %s, "
          + "which is not listed in @@SESSION.GTID_NEXT_LIST."),
  ER_CANT_CHANGE_GTID_NEXT_IN_TRANSACTION_WHEN_GTID_NEXT_LIST_IS_NULL(1768, "HY000", "The system variable @@SESSION"
          + ".GTID_NEXT cannot change inside a transaction."),
  ER_CANT_CHANGE_GTID_NEXT_IN_TRANSACTION(1768, "HY000", "The system variable @@SESSION.GTID_NEXT cannot change "
          + "inside a transaction."),
  ER_SET_STATEMENT_CANNOT_INVOKE_FUNCTION(1769, "HY000", "The statement 'SET %s' cannot invoke a stored function."),
  ER_GTID_NEXT_CANT_BE_AUTOMATIC_IF_GTID_NEXT_LIST_IS_NON_NULL(1770, "HY000", "The system variable @@SESSION"
          + ".GTID_NEXT cannot be 'AUTOMATIC' when @@SESSION.GTID_NEXT_LIST is non-NULL."),
  ER_SKIPPING_LOGGED_TRANSACTION(1771, "HY000", "Skipping transaction %s because it has already been executed and logged."),
  ER_MALFORMED_GTID_SET_SPECIFICATION(1772, "HY000", "Malformed GTID set specification '%s'."),
  ER_MALFORMED_GTID_SET_ENCODING(1773, "HY000", "Malformed GTID set encoding."),
  ER_MALFORMED_GTID_SPECIFICATION(1774, "HY000", "Malformed GTID specification '%s'."),
  ER_GNO_EXHAUSTED(1775, "HY000", "Impossible to generate Global Transaction Identifier"),
  ER_BAD_SLAVE_AUTO_POSITION(1776, "HY000", "Parameters MASTER_LOG_FILE, MASTER_LOG_POS, RELAY_LOG_FILE and "
          + "RELAY_LOG_POS cannot be set when MASTER_AUTO_POSITION is active."),
  ER_AUTO_POSITION_REQUIRES_GTID_MODE_ON(1777, "HY000", "CHANGE MASTER TO MASTER_AUTO_POSITION = 1 can only be "
          + "executed when @@GLOBAL.GTID_MODE = ON."),
  ER_AUTO_POSITION_REQUIRES_GTID_MODE_NOT_OFF(1777, "HY000", "CHANGE MASTER TO MASTER_AUTO_POSITION = 1 cannot be "
          + "executed because @@GLOBAL.GTID_MODE = OFF."),
  ER_CANT_DO_IMPLICIT_COMMIT_IN_TRX_WHEN_GTID_NEXT_IS_SET(1778, "HY000", "Cannot execute statements with implicit "
          + "commit inside a transaction when @@SESSION.GTID_NEXT == 'UUID"),
  ER_GTID_MODE_2_OR_3_REQUIRES_ENFORCE_GTID_CONSISTENCY_ON(1779, "HY000", "@@GLOBAL.GTID_MODE = ON or UPGRADE_STEP_2"
          + "requires @@GLOBAL.ENFORCE_GTID_CONSISTENCY = 1."),
  ER_GTID_MODE_ON_REQUIRES_ENFORCE_GTID_CONSISTENCY_ON(1779, "HY000", "GTID_MODE = ON requires "
          + "ENFORCE_GTID_CONSISTENCY = ON."),
  ER_GTID_MODE_REQUIRES_BINLOG(1780, "HY000", "@@GLOBAL.GTID_MODE = ON or ON_PERMISSIVE or OFF_PERMISSIVE requires "
          + "--log-bin and --log-slave-updates."),
  ER_CANT_SET_GTID_NEXT_TO_GTID_WHEN_GTID_MODE_IS_OFF(1781, "HY000", "@@SESSION.GTID_NEXT cannot be set to UUID"),
  ER_CANT_SET_GTID_NEXT_TO_ANONYMOUS_WHEN_GTID_MODE_IS_ON(1782, "HY000", "@@SESSION.GTID_NEXT cannot be set to "
          + "ANONYMOUS when @@GLOBAL.GTID_MODE = ON."),
  ER_CANT_SET_GTID_NEXT_LIST_TO_NON_NULL_WHEN_GTID_MODE_IS_OFF(1783, "HY000", "@@SESSION.GTID_NEXT_LIST cannot be "
          + "set to a non-NULL value when @@GLOBAL.GTID_MODE = OFF."),
  ER_FOUND_GTID_EVENT_WHEN_GTID_MODE_IS_OFF(1784, "HY000", "Found a Gtid_log_event or Previous_gtids_log_event when "
          + "@@GLOBAL.GTID_MODE = OFF."),
  ER_FOUND_GTID_EVENT_WHEN_GTID_MODE_IS_OFF__UNUSED(1784, "HY000", "Found a Gtid_log_event when @@GLOBAL.GTID_MODE=OFF."),
  ER_GTID_UNSAFE_NON_TRANSACTIONAL_TABLE(1785, "HY000", "Statement violates GTID consistency"),
  ER_GTID_UNSAFE_CREATE_SELECT(1786, "HY000", "Statement violates GTID consistency"),
  ER_GTID_UNSAFE_CREATE_DROP_TEMPORARY_TABLE_IN_TRANSACTION(1787, "HY000", "Statement violates GTID consistency"),
  ER_GTID_MODE_CAN_ONLY_CHANGE_ONE_STEP_AT_A_TIME(1788, "HY000", "The value of @@GLOBAL.GTID_MODE can only be "
          + "changed one step at a time"),
  ER_MASTER_HAS_PURGED_REQUIRED_GTIDS(1789, "HY000", "The slave is connecting using CHANGE MASTER TO "
          + "MASTER_AUTO_POSITION = 1, but the master has purged binary logs containing GTIDs that the slave requires."),
  ER_CANT_SET_GTID_NEXT_WHEN_OWNING_GTID(1790, "HY000", "@@SESSION.GTID_NEXT cannot be changed by a client that owns"
          + "a GTID. The client owns %s. Ownership is released on COMMIT or ROLLBACK."),
  ER_UNKNOWN_EXPLAIN_FORMAT(1791, "HY000", "Unknown EXPLAIN format name"),
  ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION(1792, "25006", "Cannot execute statement in a READ ONLY transaction."),
  ER_TOO_LONG_TABLE_PARTITION_COMMENT(1793, "HY000", "Comment for table partition '%s' is too long (max = %lu)"),
  ER_SLAVE_CONFIGURATION(1794, "HY000", "Slave is not configured or failed to initialize properly. You must at least"
          + "set --server-id to enable either a master or a slave. Additional error messages can be found in the MySQL error log."),
  ER_INNODB_FT_LIMIT(1795, "HY000", "InnoDB presently supports one FULLTEXT index creation at a time"),
  ER_INNODB_NO_FT_TEMP_TABLE(1796, "HY000", "Cannot create FULLTEXT index on temporary InnoDB table"),
  ER_INNODB_FT_WRONG_DOCID_COLUMN(1797, "HY000", "Column '%s' is of wrong type for an InnoDB FULLTEXT index"),
  ER_INNODB_FT_WRONG_DOCID_INDEX(1798, "HY000", "Index '%s' is of wrong type for an InnoDB FULLTEXT index"),
  ER_INNODB_ONLINE_LOG_TOO_BIG(1799, "HY000", "Creating index '%s' required more than "
          + "'innodb_online_alter_log_max_size' bytes of modification log. Please try again."),
  ER_UNKNOWN_ALTER_ALGORITHM(1800, "HY000", "Unknown ALGORITHM '%s'"),
  ER_UNKNOWN_ALTER_LOCK(1801, "HY000", "Unknown LOCK type '%s'"),
  ER_MTS_CHANGE_MASTER_CANT_RUN_WITH_GAPS(1802, "HY000", "CHANGE MASTER cannot be executed when the slave was "
          + "stopped with an error or killed in MTS mode. Consider using RESET SLAVE or START SLAVE UNTIL."),
  ER_MTS_RECOVERY_FAILURE(1803, "HY000", "Cannot recover after SLAVE errored out in parallel execution mode. "
          + "Additional error messages can be found in the MySQL error log."),
  ER_MTS_RESET_WORKERS(1804, "HY000", "Cannot clean up worker info tables. Additional error messages can be found in"
          + "the MySQL error log."),
  ER_COL_COUNT_DOESNT_MATCH_CORRUPTED_V2(1805, "HY000", "Column count of %s.%s is wrong. Expected %s, found %s. The "
          + "table is probably corrupted"),
  ER_SLAVE_SILENT_RETRY_TRANSACTION(1806, "HY000", "Slave must silently retry current transaction"),
  ER_DISCARD_FK_CHECKS_RUNNING(1807, "HY000", "There is a foreign key check running on table '%s'. Cannot discard "
          + "the table."),
  ER_TABLE_SCHEMA_MISMATCH(1808, "HY000", "Schema mismatch (%s)"),
  ER_TABLE_IN_SYSTEM_TABLESPACE(1809, "HY000", "Table '%s' in system tablespace"),
  ER_IO_READ_ERROR(1810, "HY000", "IO Read error"),
  ER_IO_WRITE_ERROR(1811, "HY000", "IO Write error"),
  ER_TABLESPACE_MISSING(1812, "HY000", "Tablespace is missing for table %s."),
  ER_TABLESPACE_EXISTS(1813, "HY000", "Tablespace '%s' exists."),
  ER_TABLESPACE_DISCARDED(1814, "HY000", "Tablespace has been discarded for table '%s'"),
  ER_INTERNAL_ERROR(1815, "HY000", "Internal error"),
  ER_INNODB_IMPORT_ERROR(1816, "HY000", "ALTER TABLE %s IMPORT TABLESPACE failed with error %lu "),
  ER_INNODB_INDEX_CORRUPT(1817, "HY000", "Index corrupt"),
  ER_INVALID_YEAR_COLUMN_LENGTH(1818, "HY000", "Supports only YEAR or YEAR(4) column."),
  ER_NOT_VALID_PASSWORD(1819, "HY000", "Your password does not satisfy the current policy requirements"),
  ER_MUST_CHANGE_PASSWORD(1820, "HY000", "You must reset your password using ALTER USER statement before executing "
          + "this statement."),
  ER_FK_NO_INDEX_CHILD(1821, "HY000", "Failed to add the foreign key constaint. Missing index for constraint '%s' in"
          + "the foreign table '%s'"),
  ER_FK_NO_INDEX_PARENT(1822, "HY000", "Failed to add the foreign key constaint. Missing index for constraint '%s' "
          + "in the referenced table '%s'"),
  ER_FK_FAIL_ADD_SYSTEM(1823, "HY000", "Failed to add the foreign key constraint '%s' to system tables"),
  ER_FK_CANNOT_OPEN_PARENT(1824, "HY000", "Failed to open the referenced table '%s'"),
  ER_FK_INCORRECT_OPTION(1825, "HY000", "Failed to add the foreign key constraint on table '%s'. Incorrect options "
          + "in FOREIGN KEY constraint '%s'"),
  ER_FK_DUP_NAME(1826, "HY000", "Duplicate foreign key constraint name '%s'"),
  ER_PASSWORD_FORMAT(1827, "HY000", "The password hash doesn't have the expected format. Check if the correct "
          + "password algorithm is being used with the PASSWORD() function."),
  ER_FK_COLUMN_CANNOT_DROP(1828, "HY000", "Cannot drop column '%s'"),
  ER_FK_COLUMN_CANNOT_DROP_CHILD(1829, "HY000", "Cannot drop column '%s'"),
  ER_FK_COLUMN_NOT_NULL(1830, "HY000", "Column '%s' cannot be NOT NULL"),
  ER_DUP_INDEX(1831, "HY000", "Duplicate index '%s' defined on the table '%s.%s'. This is deprecated and will be "
          + "disallowed in a future release."),
  ER_FK_COLUMN_CANNOT_CHANGE(1832, "HY000", "Cannot change column '%s'"),
  ER_FK_COLUMN_CANNOT_CHANGE_CHILD(1833, "HY000", "Cannot change column '%s'"),
  ER_FK_CANNOT_DELETE_PARENT(1834, "HY000", "Cannot delete rows from table which is parent in a foreign key "
          + "constraint '%s' of table '%s'"),
  ER_UNUSED5(1834, "HY000", "Cannot delete rows from table which is parent in a foreign key constraint '%s' of table '%s'"),
  ER_MALFORMED_PACKET(1835, "HY000", "Malformed communication packet."),
  ER_READ_ONLY_MODE(1836, "HY000", "Running in read-only mode"),
  ER_GTID_NEXT_TYPE_UNDEFINED_GROUP(1837, "HY000", "When @@SESSION.GTID_NEXT is set to a GTID, you must explicitly "
          + "set it to a different value after a COMMIT or ROLLBACK. Please check GTID_NEXT variable manual page for detailed"
          + "explanation. Current @@SESSION.GTID_NEXT is '%s'."),
  ER_VARIABLE_NOT_SETTABLE_IN_SP(1838, "HY000", "The system variable %s cannot be set in stored procedures."),
  ER_CANT_SET_GTID_PURGED_WHEN_GTID_MODE_IS_OFF(1839, "HY000", "@@GLOBAL.GTID_PURGED can only be set when @@GLOBAL"
          + ".GTID_MODE = ON."),
  ER_CANT_SET_GTID_PURGED_WHEN_GTID_EXECUTED_IS_NOT_EMPTY(1840, "HY000", "@@GLOBAL.GTID_PURGED can only be set when "
          + "@@GLOBAL.GTID_EXECUTED is empty."),
  ER_CANT_SET_GTID_PURGED_WHEN_OWNED_GTIDS_IS_NOT_EMPTY(1841, "HY000", "@@GLOBAL.GTID_PURGED can only be set when "
          + "there are no ongoing transactions (not even in other clients)."),
  ER_GTID_PURGED_WAS_CHANGED(1842, "HY000", "@@GLOBAL.GTID_PURGED was changed from '%s' to '%s'."),
  ER_GTID_EXECUTED_WAS_CHANGED(1843, "HY000", "@@GLOBAL.GTID_EXECUTED was changed from '%s' to '%s'."),
  ER_BINLOG_STMT_MODE_AND_NO_REPL_TABLES(1844, "HY000", "Cannot execute statement"),
  ER_ALTER_OPERATION_NOT_SUPPORTED(1845, "0A000", "%s is not supported for this operation. Try %s."),
  ER_ALTER_OPERATION_NOT_SUPPORTED_REASON(1846, "0A000", "%s is not supported. Reason"),
  ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_COPY(1847, "HY000", "COPY algorithm requires a lock"),
  ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_PARTITION(1848, "HY000", "Partition specific operations do not yet support"
          + "LOCK/ALGORITHM"),
  ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_FK_RENAME(1849, "HY000", "Columns participating in a foreign key are renamed"),
  ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_COLUMN_TYPE(1850, "HY000", "Cannot change column type INPLACE"),
  ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_FK_CHECK(1851, "HY000", "Adding foreign keys needs foreign_key_checks=OFF"),
  ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_IGNORE(1852, "HY000", "Creating unique indexes with IGNORE requires COPY "
          + "algorithm to remove duplicate rows"),
  ER_UNUSED6(1852, "HY000", "Creating unique indexes with IGNORE requires COPY algorithm to remove duplicate rows"),
  ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_NOPK(1853, "HY000", "Dropping a primary key is not allowed without also "
          + "adding a new primary key"),
  ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_AUTOINC(1854, "HY000", "Adding an auto-increment column requires a lock"),
  ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_HIDDEN_FTS(1855, "HY000", "Cannot replace hidden FTS_DOC_ID with a "
          + "user-visible one"),
  ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_CHANGE_FTS(1856, "HY000", "Cannot drop or rename FTS_DOC_ID"),
  ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_FTS(1857, "HY000", "Fulltext index creation requires a lock"),
  ER_SQL_SLAVE_SKIP_COUNTER_NOT_SETTABLE_IN_GTID_MODE(1858, "HY000", "sql_slave_skip_counter can not be set when the"
          + "server is running with @@GLOBAL.GTID_MODE = ON. Instead, for each transaction that you want to skip, generate "
          + "an empty transaction with the same GTID as the transaction"),
  ER_DUP_UNKNOWN_IN_INDEX(1859, "23000", "Duplicate entry for key '%s'"),
  ER_IDENT_CAUSES_TOO_LONG_PATH(1860, "HY000", "Long database name and identifier for object resulted in path length"
          + "exceeding %s characters. Path"),
  ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_NOT_NULL(1861, "HY000", "cannot silently convert NULL values, as required "
          + "in this SQL_MODE"),
  ER_MUST_CHANGE_PASSWORD_LOGIN(1862, "HY000", "Your password has expired. To log in you must change it using a "
          + "client that supports expired passwords."),
  ER_ROW_IN_WRONG_PARTITION(1863, "HY000", "Found a row in wrong partition %s"),
  ER_MTS_EVENT_BIGGER_PENDING_JOBS_SIZE_MAX(1864, "HY000", "Cannot schedule event %s, relay-log name %s, position %s"
          + "to Worker thread because its size %lu exceeds %lu of slave_pending_jobs_size_max."),
  ER_INNODB_NO_FT_USES_PARSER(1865, "HY000", "Cannot CREATE FULLTEXT INDEX WITH PARSER on InnoDB table"),
  ER_BINLOG_LOGICAL_CORRUPTION(1866, "HY000", "The binary log file '%s' is logically corrupted"),
  ER_WARN_PURGE_LOG_IN_USE(1867, "HY000", "file %s was not purged because it was being read by %s thread(s), purged "
          + "only %s out of %s files."),
  ER_WARN_PURGE_LOG_IS_ACTIVE(1868, "HY000", "file %s was not purged because it is the active log file."),
  ER_AUTO_INCREMENT_CONFLICT(1869, "HY000", "Auto-increment value in UPDATE conflicts with internally generated values"),
  WARN_ON_BLOCKHOLE_IN_RBR(1870, "HY000", "Row events are not logged for %s statements that modify BLACKHOLE tables "
          + "in row format. Table(s)"),
  ER_SLAVE_MI_INIT_REPOSITORY(1871, "HY000", "Slave failed to initialize master info structure from the repository"),
  ER_SLAVE_RLI_INIT_REPOSITORY(1872, "HY000", "Slave failed to initialize relay log info structure from the repository"),
  ER_ACCESS_DENIED_CHANGE_USER_ERROR(1873, "28000", "Access denied trying to change to user '%s'@'%s' (using password"),
  ER_INNODB_READ_ONLY(1874, "HY000", "InnoDB is in read only mode."),
  ER_STOP_SLAVE_SQL_THREAD_TIMEOUT(1875, "HY000", "STOP SLAVE command execution is incomplete"),
  ER_STOP_SLAVE_IO_THREAD_TIMEOUT(1876, "HY000", "STOP SLAVE command execution is incomplete"),
  ER_TABLE_CORRUPT(1877, "HY000", "Operation cannot be performed. The table '%s.%s' is missing, corrupt or contains "
          + "bad data."),
  ER_TEMP_FILE_WRITE_FAILURE(1878, "HY000", "Temporary file write failure."),
  ER_INNODB_FT_AUX_NOT_HEX_ID(1879, "HY000", "Upgrade index name failed, please use create index(alter table) "
          + "algorithm copy to rebuild index."),
  ER_OLD_TEMPORALS_UPGRADED(1880, "HY000", "TIME/TIMESTAMP/DATETIME columns of old format have been upgraded to the "
          + "new format."),
  ER_INNODB_FORCED_RECOVERY(1881, "HY000", "Operation not allowed when innodb_forced_recovery > 0."),
  ER_AES_INVALID_IV(1882, "HY000", "The initialization vector supplied to %s is too short. Must be at least %s bytes long"),
  ER_PLUGIN_CANNOT_BE_UNINSTALLED(1883, "HY000", "Plugin '%s' cannot be uninstalled now. %s"),
  ER_GTID_UNSAFE_BINLOG_SPLITTABLE_STATEMENT_AND_GTID_GROUP(1884, "HY000", "Cannot execute statement because it "
          + "needs to be written to the binary log as multiple statements, and this is not allowed when @@SESSION.GTID_NEXT "
          + "== 'UUID"),
  ER_SLAVE_HAS_MORE_GTIDS_THAN_MASTER(1885, "HY000", "Slave has more GTIDs than the master has, using the master's "
          + "SERVER_UUID. This may indicate that the end of the binary log was truncated or that the last binary log file was"
          + "lost, e.g., after a power or disk failure when sync_binlog != 1. The master may or may not have rolled back "
          + "transactions that were already replicated to the slave. Suggest to replicate any transactions that master has "
          + "rolled back from slave to master, and/or commit empty transactions on master to account for transactions that "
          + "have been committed on master but are not included in GTID_EXECUTED."),
  ER_MISSING_KEY(1886, "HY000", "The table '%s.%s' does not have the necessary key(s) defined on it. Please check "
          + "the table definition and create index(s) accordingly."),
  WARN_NAMED_PIPE_ACCESS_EVERYONE(1887, "HY000", "Setting named_pipe_full_access_group='%s' is insecure. Consider "
          + "using a Windows group with fewer members."),
  ER_SLAVE_IO_THREAD_MUST_STOP(1906, "HY000", "This operation cannot be performed with a running slave io thread; "
          + "run STOP SLAVE IO_THREAD first."),
  ER_FILE_CORRUPT(3000, "HY000", "File %s is corrupted"),
  ER_ERROR_ON_MASTER(3001, "HY000", "Query partially completed on the master (error on master"),
  ER_INCONSISTENT_ERROR(3002, "HY000", "Query caused different errors on master and slave. Error on master"),
  ER_STORAGE_ENGINE_NOT_LOADED(3003, "HY000", "Storage engine for table '%s'.'%s' is not loaded."),
  ER_GET_STACKED_DA_WITHOUT_ACTIVE_HANDLER(3004, "0Z002", "GET STACKED DIAGNOSTICS when handler not active"),
  ER_WARN_LEGACY_SYNTAX_CONVERTED(3005, "HY000", "%s is no longer supported. The statement was converted to %s."),
  ER_BINLOG_UNSAFE_FULLTEXT_PLUGIN(3006, "HY000", "Statement is unsafe because it uses a fulltext parser plugin "
          + "which may not return the same value on the slave."),
  ER_CANNOT_DISCARD_TEMPORARY_TABLE(3007, "HY000", "Cannot DISCARD/IMPORT tablespace associated with temporary table"),
  ER_FK_DEPTH_EXCEEDED(3008, "HY000", "Foreign key cascade delete/update exceeds max depth of %s."),
  ER_COL_COUNT_DOESNT_MATCH_PLEASE_UPDATE_V2(3009, "HY000", "Column count of %s.%s is wrong. Expected %s, found %s. "
          + "Created with MySQL %s, now running %s. Please use mysql_upgrade to fix this error."),
  ER_WARN_TRIGGER_DOESNT_HAVE_CREATED(3010, "HY000", "Trigger %s.%s.%s does not have CREATED attribute."),
  ER_REFERENCED_TRG_DOES_NOT_EXIST(3011, "HY000", "Referenced trigger '%s' for the given action time and event type "
          + "does not exist."),
  ER_EXPLAIN_NOT_SUPPORTED(3012, "HY000", "EXPLAIN FOR CONNECTION command is supported only for "
          + "SELECT/UPDATE/INSERT/DELETE/REPLACE"),
  ER_INVALID_FIELD_SIZE(3013, "HY000", "Invalid size for column '%s'."),
  ER_MISSING_HA_CREATE_OPTION(3014, "HY000", "Table storage engine '%s' found required create option missing"),
  ER_ENGINE_OUT_OF_MEMORY(3015, "HY000", "Out of memory in storage engine '%s'."),
  ER_PASSWORD_EXPIRE_ANONYMOUS_USER(3016, "HY000", "The password for anonymous user cannot be expired."),
  ER_SLAVE_SQL_THREAD_MUST_STOP(3017, "HY000", "This operation cannot be performed with a running slave sql thread; "
          + "run STOP SLAVE SQL_THREAD first"),
  ER_NO_FT_MATERIALIZED_SUBQUERY(3018, "HY000", "Cannot create FULLTEXT index on materialized subquery"),
  ER_INNODB_UNDO_LOG_FULL(3019, "HY000", "Undo Log error"),
  ER_INVALID_ARGUMENT_FOR_LOGARITHM(3020, "2201E", "Invalid argument for logarithm"),
  ER_SLAVE_CHANNEL_IO_THREAD_MUST_STOP(3021, "HY000", "This operation cannot be performed with a running slave io "
          + "thread; run STOP SLAVE IO_THREAD FOR CHANNEL '%s' first."),
  ER_WARN_OPEN_TEMP_TABLES_MUST_BE_ZERO(3022, "HY000", "This operation may not be safe when the slave has temporary "
          + "tables. The tables will be kept open until the server restarts or until the tables are deleted by any replicated"
          + "DROP statement. Suggest to wait until slave_open_temp_tables = 0."),
  ER_WARN_ONLY_MASTER_LOG_FILE_NO_POS(3023, "HY000", "CHANGE MASTER TO with a MASTER_LOG_FILE clause but no "
          + "MASTER_LOG_POS clause may not be safe. The old position value may not be valid for the new binary log file."),
  ER_QUERY_TIMEOUT(3024, "HY000", "Query execution was interrupted, maximum statement execution time exceeded"),
  ER_NON_RO_SELECT_DISABLE_TIMER(3025, "HY000", "Select is not a read only statement, disabling timer"),
  ER_DUP_LIST_ENTRY(3026, "HY000", "Duplicate entry '%s'."),
  ER_SQL_MODE_NO_EFFECT(3027, "HY000", "'%s' mode no longer has any effect. Use STRICT_ALL_TABLES or "
          + "STRICT_TRANS_TABLES instead."),
  ER_AGGREGATE_ORDER_FOR_UNION(3028, "HY000", "Expression #%u of ORDER BY contains aggregate function and applies to"
          + "a UNION"),
  ER_AGGREGATE_ORDER_NON_AGG_QUERY(3029, "HY000", "Expression #%u of ORDER BY contains aggregate function and "
          + "applies to the result of a non-aggregated query"),
  ER_SLAVE_WORKER_STOPPED_PREVIOUS_THD_ERROR(3030, "HY000", "Slave worker has stopped after at least one previous "
          + "worker encountered an error when slave-preserve-commit-order was enabled. To preserve commit order, the last "
          + "transaction executed by this thread has not been committed. When restarting the slave after fixing any failed "
          + "threads, you should fix this worker as well."),
  ER_DONT_SUPPORT_SLAVE_PRESERVE_COMMIT_ORDER(3031, "HY000", "slave_preserve_commit_order is not supported %s."),
  ER_SERVER_OFFLINE_MODE(3032, "HY000", "The server is currently in offline mode"),
  ER_GIS_DIFFERENT_SRIDS(3033, "HY000", "Binary geometry function %s given two geometries of different srids"),
  ER_GIS_UNSUPPORTED_ARGUMENT(3034, "HY000", "Calling geometry function %s with unsupported types of arguments."),
  ER_GIS_UNKNOWN_ERROR(3035, "HY000", "Unknown GIS error occured in function %s."),
  ER_GIS_UNKNOWN_EXCEPTION(3036, "HY000", "Unknown exception caught in GIS function %s."),
  ER_GIS_INVALID_DATA(3037, "22023", "Invalid GIS data provided to function %s."),
  ER_BOOST_GEOMETRY_EMPTY_INPUT_EXCEPTION(3038, "HY000", "The geometry has no data in function %s."),
  ER_BOOST_GEOMETRY_CENTROID_EXCEPTION(3039, "HY000", "Unable to calculate centroid because geometry is empty in "
          + "function %s."),
  ER_BOOST_GEOMETRY_OVERLAY_INVALID_INPUT_EXCEPTION(3040, "HY000", "Geometry overlay calculation error"),
  ER_BOOST_GEOMETRY_TURN_INFO_EXCEPTION(3041, "HY000", "Geometry turn info calculation error"),
  ER_BOOST_GEOMETRY_SELF_INTERSECTION_POINT_EXCEPTION(3042, "HY000", "Analysis procedures of intersection points "
          + "interrupted unexpectedly in function %s."),
  ER_BOOST_GEOMETRY_UNKNOWN_EXCEPTION(3043, "HY000", "Unknown exception thrown in function %s."),
  ER_STD_BAD_ALLOC_ERROR(3044, "HY000", "Memory allocation error"),
  ER_STD_DOMAIN_ERROR(3045, "HY000", "Domain error"),
  ER_STD_LENGTH_ERROR(3046, "HY000", "Length error"),
  ER_STD_INVALID_ARGUMENT(3047, "HY000", "Invalid argument error"),
  ER_STD_OUT_OF_RANGE_ERROR(3048, "HY000", "Out of range error"),
  ER_STD_OVERFLOW_ERROR(3049, "HY000", "Overflow error error"),
  ER_STD_RANGE_ERROR(3050, "HY000", "Range error"),
  ER_STD_UNDERFLOW_ERROR(3051, "HY000", "Underflow error"),
  ER_STD_LOGIC_ERROR(3052, "HY000", "Logic error"),
  ER_STD_RUNTIME_ERROR(3053, "HY000", "Runtime error"),
  ER_STD_UNKNOWN_EXCEPTION(3054, "HY000", "Unknown exception: %s"),
  ER_GIS_DATA_WRONG_ENDIANESS(3055, "HY000", "Geometry byte string must be little endian."),
  ER_CHANGE_MASTER_PASSWORD_LENGTH(3056, "HY000", "The password provided for the replication user exceeds the "
          + "maximum length of 32 characters"),
  ER_USER_LOCK_WRONG_NAME(3057, "42000", "Incorrect user-level lock name '%s'."),
  ER_USER_LOCK_DEADLOCK(3058, "HY000", "Deadlock found when trying to get user-level lock; try rolling back "
          + "transaction/releasing locks and restarting lock acquisition."),
  ER_REPLACE_INACCESSIBLE_ROWS(3059, "HY000", "REPLACE cannot be executed as it requires deleting rows that are not "
          + "in the view"),
  ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_GIS(3060, "HY000", "Do not support online operation on table with GIS index"),
  ER_ILLEGAL_USER_VAR(3061, "42000", "User variable name '%s' is illegal"),
  ER_GTID_MODE_OFF(3062, "HY000", "Cannot %s when GTID_MODE = OFF."),
  ER_UNSUPPORTED_BY_REPLICATION_THREAD(3063, "HY000", "Cannot %s from a replication slave thread."),
  ER_INCORRECT_TYPE(3064, "HY000", "Incorrect type for argument %s in function %s."),
  ER_FIELD_IN_ORDER_NOT_SELECT(3065, "HY000", "Expression #%u of ORDER BY clause is not in SELECT list, references "
          + "column '%s' which is not in SELECT list; this is incompatible with %s"),
  ER_AGGREGATE_IN_ORDER_NOT_SELECT(3066, "HY000", "Expression #%u of ORDER BY clause is not in SELECT list, contains"
          + "aggregate function; this is incompatible with %s"),
  ER_INVALID_RPL_WILD_TABLE_FILTER_PATTERN(3067, "HY000", "Supplied filter list contains a value which is not in the"
          + "required format 'db_pattern.table_pattern'"),
  ER_NET_OK_PACKET_TOO_LARGE(3068, "08S01", "OK packet too large"),
  ER_INVALID_JSON_DATA(3069, "HY000", "Invalid JSON data provided to function %s"),
  ER_INVALID_GEOJSON_MISSING_MEMBER(3070, "HY000", "Invalid GeoJSON data provided to function %s"),
  ER_INVALID_GEOJSON_WRONG_TYPE(3071, "HY000", "Invalid GeoJSON data provided to function %s"),
  ER_INVALID_GEOJSON_UNSPECIFIED(3072, "HY000", "Invalid GeoJSON data provided to function %s"),
  ER_DIMENSION_UNSUPPORTED(3073, "HY000", "Unsupported number of coordinate dimensions in function %s"),
  ER_SLAVE_CHANNEL_DOES_NOT_EXIST(3074, "HY000", "Slave channel '%s' does not exist."),
  ER_SLAVE_MULTIPLE_CHANNELS_HOST_PORT(3075, "HY000", "A slave channel '%s' already exists for the given host and "
          + "port combination."),
  ER_SLAVE_CHANNEL_NAME_INVALID_OR_TOO_LONG(3076, "HY000", "Couldn't create channel"),
  ER_SLAVE_NEW_CHANNEL_WRONG_REPOSITORY(3077, "HY000", "To have multiple channels, repository cannot be of type "
          + "FILE; Please check the repository configuration and convert them to TABLE."),
  ER_SLAVE_CHANNEL_DELETE(3078, "HY000", "Cannot delete slave info objects for channel '%s'."),
  ER_SLAVE_MULTIPLE_CHANNELS_CMD(3079, "HY000", "Multiple channels exist on the slave. Please provide channel name "
          + "as an argument."),
  ER_SLAVE_MAX_CHANNELS_EXCEEDED(3080, "HY000", "Maximum number of replication channels allowed exceeded."),
  ER_SLAVE_CHANNEL_MUST_STOP(3081, "HY000", "This operation cannot be performed with running replication threads; "
          + "run STOP SLAVE FOR CHANNEL '%s' first"),
  ER_SLAVE_CHANNEL_NOT_RUNNING(3082, "HY000", "This operation requires running replication threads; configure slave "
          + "and run START SLAVE FOR CHANNEL '%s'"),
  ER_SLAVE_CHANNEL_WAS_RUNNING(3083, "HY000", "Replication thread(s) for channel '%s' are already runnning."),
  ER_SLAVE_CHANNEL_WAS_NOT_RUNNING(3084, "HY000", "Replication thread(s) for channel '%s' are already stopped."),
  ER_SLAVE_CHANNEL_SQL_THREAD_MUST_STOP(3085, "HY000", "This operation cannot be performed with a running slave sql "
          + "thread; run STOP SLAVE SQL_THREAD FOR CHANNEL '%s' first."),
  ER_SLAVE_CHANNEL_SQL_SKIP_COUNTER(3086, "HY000", "When sql_slave_skip_counter > 0, it is not allowed to start more"
          + "than one SQL thread by using 'START SLAVE [SQL_THREAD]'. Value of sql_slave_skip_counter can only be used by "
          + "one SQL thread at a time. Please use 'START SLAVE [SQL_THREAD] FOR CHANNEL' to start the SQL thread which will "
          + "use the value of sql_slave_skip_counter."),
  ER_WRONG_FIELD_WITH_GROUP_V2(3087, "HY000", "Expression #%u of %s is not in GROUP BY clause and contains "
          + "nonaggregated column '%s' which is not functionally dependent on columns in GROUP BY clause; this is "
          + "incompatible with sql_mode=only_full_group_by"),
  ER_MIX_OF_GROUP_FUNC_AND_FIELDS_V2(3088, "HY000", "In aggregated query without GROUP BY, expression #%u of %s "
          + "contains nonaggregated column '%s'; this is incompatible with sql_mode=only_full_group_by"),
  ER_WARN_DEPRECATED_SYSVAR_UPDATE(3089, "HY000", "Updating '%s' is deprecated. It will be made read-only in a "
          + "future release."),
  ER_WARN_DEPRECATED_SQLMODE(3090, "HY000", "Changing sql mode '%s' is deprecated. It will be removed in a future release."),
  ER_CANNOT_LOG_PARTIAL_DROP_DATABASE_WITH_GTID(3091, "HY000", "DROP DATABASE failed; some tables may have been "
          + "dropped but the database directory remains. The GTID has not been added to GTID_EXECUTED and the statement was "
          + "not written to the binary log. Fix this as follows"),
  ER_GROUP_REPLICATION_CONFIGURATION(3092, "HY000", "The server is not configured properly to be an active member of"
          + "the group. Please see more details on error log."),
  ER_GROUP_REPLICATION_RUNNING(3093, "HY000", "The START GROUP_REPLICATION command failed since the group is already running."),
  ER_GROUP_REPLICATION_APPLIER_INIT_ERROR(3094, "HY000", "The START GROUP_REPLICATION command failed as the applier "
          + "module failed to start."),
  ER_GROUP_REPLICATION_STOP_APPLIER_THREAD_TIMEOUT(3095, "HY000", "The STOP GROUP_REPLICATION command execution is incomplete"),
  ER_GROUP_REPLICATION_COMMUNICATION_LAYER_SESSION_ERROR(3096, "HY000", "The START GROUP_REPLICATION command failed "
          + "as there was an error when initializing the group communication layer."),
  ER_GROUP_REPLICATION_COMMUNICATION_LAYER_JOIN_ERROR(3097, "HY000", "The START GROUP_REPLICATION command failed as "
          + "there was an error when joining the communication group."),
  ER_BEFORE_DML_VALIDATION_ERROR(3098, "HY000", "The table does not comply with the requirements by an external plugin."),
  ER_PREVENTS_VARIABLE_WITHOUT_RBR(3099, "HY000", "Cannot change the value of variable %s without binary log format "
          + "as ROW."),
  ER_RUN_HOOK_ERROR(3100, "HY000", "Error on observer while running replication hook '%s'."),
  ER_TRANSACTION_ROLLBACK_DURING_COMMIT(3101, "HY000", "Plugin instructed the server to rollback the current "
          + "transaction."),
  ER_GENERATED_COLUMN_FUNCTION_IS_NOT_ALLOWED(3102, "HY000", "Expression of generated column '%s' contains a "
          + "disallowed function."),
  ER_KEY_BASED_ON_GENERATED_COLUMN(3103, "HY000", "Key/Index cannot be defined on a virtual generated column."),
  ER_UNSUPPORTED_ALTER_INPLACE_ON_VIRTUAL_COLUMN(3103, "HY000", "INPLACE ADD or DROP of virtual columns cannot be "
          + "combined with other ALTER TABLE actions"),
  ER_WRONG_FK_OPTION_FOR_GENERATED_COLUMN(3104, "HY000", "Cannot define foreign key with %s clause on a generated column."),
  ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN(3105, "HY000", "The value specified for generated column '%s' in table "
          + "'%s' is not allowed."),
  ER_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN(3106, "HY000", "'%s' is not supported for generated columns."),
  ER_GENERATED_COLUMN_NON_PRIOR(3107, "HY000", "Generated column can refer only to generated columns defined prior "
          + "to it."),
  ER_DEPENDENT_BY_GENERATED_COLUMN(3108, "HY000", "Column '%s' has a generated column dependency."),
  ER_GENERATED_COLUMN_REF_AUTO_INC(3109, "HY000", "Generated column '%s' cannot refer to auto-increment column."),
  ER_FEATURE_NOT_AVAILABLE(3110, "HY000", "The '%s' feature is not available; you need to remove '%s' or use MySQL "
          + "built with '%s'"),
  ER_CANT_SET_GTID_MODE(3111, "HY000", "SET @@GLOBAL.GTID_MODE = %s is not allowed because %s."),
  ER_CANT_USE_AUTO_POSITION_WITH_GTID_MODE_OFF(3112, "HY000", "The replication receiver thread%s cannot start in "
          + "AUTO_POSITION mode"),
  ER_CANT_REPLICATE_ANONYMOUS_WITH_AUTO_POSITION(3113, "HY000", "Cannot replicate anonymous transaction when "
          + "AUTO_POSITION = 1, at file %s, position %lld."),
  ER_CANT_REPLICATE_ANONYMOUS_WITH_GTID_MODE_ON(3114, "HY000", "Cannot replicate anonymous transaction when @@GLOBAL"
          + ".GTID_MODE = ON, at file %s, position %lld."),
  ER_CANT_REPLICATE_GTID_WITH_GTID_MODE_OFF(3115, "HY000", "Cannot replicate GTID-transaction when @@GLOBAL"
          + ".GTID_MODE = OFF, at file %s, position %lld."),
  ER_CANT_SET_ENFORCE_GTID_CONSISTENCY_ON_WITH_ONGOING_GTID_VIOLATING_TRANSACTIONS(3116, "HY000", "Cannot set "
          + "ENFORCE_GTID_CONSISTENCY = ON because there are ongoing transactions that violate GTID consistency."),
  ER_SET_ENFORCE_GTID_CONSISTENCY_WARN_WITH_ONGOING_GTID_VIOLATING_TRANSACTIONS(3117, "HY000", "There are ongoing "
          + "transactions that violate GTID consistency."),
  ER_ACCOUNT_HAS_BEEN_LOCKED(3118, "HY000", "Access denied for user '%s'@'%s'. Account is locked."),
  ER_WRONG_TABLESPACE_NAME(3119, "42000", "Incorrect tablespace name `%s`"),
  ER_TABLESPACE_IS_NOT_EMPTY(3120, "HY000", "Tablespace `%s` is not empty."),
  ER_WRONG_FILE_NAME(3121, "HY000", "Incorrect File Name '%s'."),
  ER_BOOST_GEOMETRY_INCONSISTENT_TURNS_EXCEPTION(3122, "HY000", "Inconsistent intersection points."),
  ER_WARN_OPTIMIZER_HINT_SYNTAX_ERROR(3123, "HY000", "Optimizer hint syntax error"),
  ER_WARN_BAD_MAX_EXECUTION_TIME(3124, "HY000", "Unsupported MAX_EXECUTION_TIME"),
  ER_WARN_UNSUPPORTED_MAX_EXECUTION_TIME(3125, "HY000", "MAX_EXECUTION_TIME hint is supported by top-level "
          + "standalone SELECT statements only"),
  ER_WARN_CONFLICTING_HINT(3126, "HY000", "Hint %s is ignored as conflicting/duplicated"),
  ER_WARN_UNKNOWN_QB_NAME(3127, "HY000", "Query block name %s is not found for %s hint"),
  ER_UNRESOLVED_HINT_NAME(3128, "HY000", "Unresolved name %s for %s hint"),
  ER_WARN_DEPRECATED_SQLMODE_UNSET(3129, "HY000", "Unsetting sql mode '%s' is deprecated. It will be made read-only "
          + "in a future release."),
  ER_WARN_ON_MODIFYING_GTID_EXECUTED_TABLE(3129, "HY000", "Please do not modify the %s table. This is a mysql "
          + "internal system table to store GTIDs for committed transactions. Modifying it can lead to an inconsistent GTID state."),
  ER_PLUGGABLE_PROTOCOL_COMMAND_NOT_SUPPORTED(3130, "HY000", "Command not supported by pluggable protocols"),
  ER_LOCKING_SERVICE_WRONG_NAME(3131, "42000", "Incorrect locking service lock name '%s'."),
  ER_LOCKING_SERVICE_DEADLOCK(3132, "HY000", "Deadlock found when trying to get locking service lock; try releasing "
          + "locks and restarting lock acquisition."),
  ER_LOCKING_SERVICE_TIMEOUT(3133, "HY000", "Service lock wait timeout exceeded."),
  ER_GIS_MAX_POINTS_IN_GEOMETRY_OVERFLOWED(3134, "HY000", "Parameter %s exceeds the maximum number of points in a "
          + "geometry (%lu) in function %s."),
  ER_SQL_MODE_MERGED(3135, "HY000", "'NO_ZERO_DATE', 'NO_ZERO_IN_DATE' and 'ERROR_FOR_DIVISION_BY_ZERO' sql modes "
          + "should be used with strict mode. They will be merged with strict mode in a future release."),
  ER_VTOKEN_PLUGIN_TOKEN_MISMATCH(3136, "HY000", "Version token mismatch for %.*s. Correct value %.*s"),
  ER_VTOKEN_PLUGIN_TOKEN_NOT_FOUND(3137, "HY000", "Version token %.*s not found."),
  ER_CANT_SET_VARIABLE_WHEN_OWNING_GTID(3138, "HY000", "Variable %s cannot be changed by a client that owns a GTID. "
          + "The client owns %s. Ownership is released on COMMIT or ROLLBACK."),
  ER_SLAVE_CHANNEL_OPERATION_NOT_ALLOWED(3139, "HY000", "%s cannot be performed on channel '%s'."),
  ER_INVALID_JSON_TEXT(3140, "22032", "Invalid JSON text"),
  ER_INVALID_JSON_TEXT_IN_PARAM(3141, "22032", "Invalid JSON text in argument %u to function %s"),
  ER_INVALID_JSON_BINARY_DATA(3142, "HY000", "The JSON binary value contains invalid data."),
  ER_INVALID_JSON_PATH(3143, "42000", "Invalid JSON path expression. The error is around character position %u.%s"),
  ER_INVALID_JSON_CHARSET(3144, "22032", "Cannot create a JSON value from a string with CHARACTER SET '%s'."),
  ER_INVALID_JSON_CHARSET_IN_FUNCTION(3145, "22032", "Invalid JSON character data provided to function %s"),
  ER_INVALID_TYPE_FOR_JSON(3146, "22032", "Invalid data type for JSON data in argument %u to function %s; a JSON "
          + "string or JSON type is required."),
  ER_INVALID_CAST_TO_JSON(3147, "22032", "Cannot CAST value to JSON."),
  ER_INVALID_JSON_PATH_CHARSET(3148, "42000", "A path expression must be encoded in the utf8 character set. The path"
          + "expression '%s' is encoded in character set '%s'."),
  ER_INVALID_JSON_PATH_WILDCARD(3149, "42000", "In this situation, path expressions may not contain the * and ** tokens."),
  ER_JSON_VALUE_TOO_BIG(3150, "22032", "The JSON value is too big to be stored in a JSON column."),
  ER_JSON_KEY_TOO_BIG(3151, "22032", "The JSON object contains a key name that is too long."),
  ER_JSON_USED_AS_KEY(3152, "42000", "JSON column '%s' cannot be used in key specification."),
  ER_JSON_VACUOUS_PATH(3153, "42000", "The path expression '$' is not allowed in this context."),
  ER_JSON_BAD_ONE_OR_ALL_ARG(3154, "42000", "The oneOrAll argument to %s may take these values"),
  ER_NUMERIC_JSON_VALUE_OUT_OF_RANGE(3155, "22003", "Out of range JSON value for CAST to %s%s from column %s at row %ld"),
  ER_INVALID_JSON_VALUE_FOR_CAST(3156, "22018", "Invalid JSON value for CAST to %s%s from column %s at row %ld"),
  ER_JSON_DOCUMENT_TOO_DEEP(3157, "22032", "The JSON document exceeds the maximum depth."),
  ER_JSON_DOCUMENT_NULL_KEY(3158, "22032", "JSON documents may not contain NULL member names."),
  ER_SECURE_TRANSPORT_REQUIRED(3159, "HY000", "Connections using insecure transport are prohibited while "
          + "--require_secure_transport=ON."),
  ER_NO_SECURE_TRANSPORTS_CONFIGURED(3160, "HY000", "No secure transports (SSL or Shared Memory) are configured, "
          + "unable to set --require_secure_transport=ON."),
  ER_DISABLED_STORAGE_ENGINE(3161, "HY000", "Storage engine %s is disabled (Table creation is disallowed)."),
  ER_USER_DOES_NOT_EXIST(3162, "HY000", "User %s does not exist."),
  ER_USER_ALREADY_EXISTS(3163, "HY000", "User %s already exists."),
  ER_AUDIT_API_ABORT(3164, "HY000", "Aborted by Audit API ('%s';%s)."),
  ER_INVALID_JSON_PATH_ARRAY_CELL(3165, "42000", "A path expression is not a path to a cell in an array."),
  ER_BUFPOOL_RESIZE_INPROGRESS(3166, "HY000", "Another buffer pool resize is already in progress."),
  ER_FEATURE_DISABLED_SEE_DOC(3167, "HY000", "The '%s' feature is disabled; see the documentation for '%s'"),
  ER_SERVER_ISNT_AVAILABLE(3168, "HY000", "Server isn't available"),
  ER_SESSION_WAS_KILLED(3169, "HY000", "Session was killed"),
  ER_CAPACITY_EXCEEDED(3170, "HY000", "Memory capacity of %llu bytes for '%s' exceeded. %s"),
  ER_CAPACITY_EXCEEDED_IN_RANGE_OPTIMIZER(3171, "HY000", "Range optimization was not done for this query."),
  ER_TABLE_NEEDS_UPG_PART(3172, "HY000", "Partitioning upgrade required. Please dump/reload to fix it or do"),
  ER_CANT_WAIT_FOR_EXECUTED_GTID_SET_WHILE_OWNING_A_GTID(3173, "HY000", "The client holds ownership of the GTID %s. "
          + "Therefore, WAIT_FOR_EXECUTED_GTID_SET cannot wait for this GTID."),
  ER_CANNOT_ADD_FOREIGN_BASE_COL_VIRTUAL(3174, "HY000", "Cannot add foreign key on the base column of indexed "
          + "virtual column."),
  ER_CANNOT_CREATE_VIRTUAL_INDEX_CONSTRAINT(3175, "HY000", "Cannot create index on virtual column whose base column "
          + "has foreign constraint."),
  ER_ERROR_ON_MODIFYING_GTID_EXECUTED_TABLE(3176, "HY000", "Please do not modify the %s table with an XA transaction"
          + ". This is an internal system table used to store GTIDs for committed transactions. Although modifying it can "
          + "lead to an inconsistent GTID state, if neccessary you can modify it with a non-XA transaction."),
  ER_LOCK_REFUSED_BY_ENGINE(3177, "HY000", "Lock acquisition refused by storage engine."),
  ER_UNSUPPORTED_ALTER_ONLINE_ON_VIRTUAL_COLUMN(3178, "HY000", "ADD COLUMN col...VIRTUAL, ADD INDEX(col)"),
  ER_MASTER_KEY_ROTATION_NOT_SUPPORTED_BY_SE(3179, "HY000", "Master key rotation is not supported by storage engine."),
  ER_MASTER_KEY_ROTATION_ERROR_BY_SE(3180, "HY000", "Encryption key rotation error reported by SE"),
  ER_MASTER_KEY_ROTATION_BINLOG_FAILED(3181, "HY000", "Write to binlog failed. However, master key rotation has been"
          + "completed successfully."),
  ER_MASTER_KEY_ROTATION_SE_UNAVAILABLE(3182, "HY000", "Storage engine is not available."),
  ER_TABLESPACE_CANNOT_ENCRYPT(3183, "HY000", "This tablespace can't be encrypted."),
  ER_INVALID_ENCRYPTION_OPTION(3184, "HY000", "Invalid encryption option."),
  ER_CANNOT_FIND_KEY_IN_KEYRING(3185, "HY000", "Can't find master key from keyring, please check in the server log "
          + "if a keyring plugin is loaded and initialized successfully."),
  ER_CAPACITY_EXCEEDED_IN_PARSER(3186, "HY000", "Parser bailed out for this query."),
  ER_UNSUPPORTED_ALTER_ENCRYPTION_INPLACE(3187, "HY000", "Cannot alter encryption attribute by inplace algorithm."),
  ER_KEYRING_UDF_KEYRING_SERVICE_ERROR(3188, "HY000", "Function '%s' failed because underlying keyring service "
          + "returned an error. Please check if a keyring plugin is installed and that provided arguments are valid for the "
          + "keyring you are using."),
  ER_USER_COLUMN_OLD_LENGTH(3189, "HY000", "It seems that your db schema is old. The %s column is 77 characters long"
          + "and should be 93 characters long. Please run mysql_upgrade."),
  ER_CANT_RESET_MASTER(3190, "HY000", "RESET MASTER is not allowed because %s."),
  ER_GROUP_REPLICATION_MAX_GROUP_SIZE(3191, "HY000", "The START GROUP_REPLICATION command failed since the group "
          + "already has 9 members."),
  ER_CANNOT_ADD_FOREIGN_BASE_COL_STORED(3192, "HY000", "Cannot add foreign key on the base column of stored column."),
  ER_TABLE_REFERENCED(3193, "HY000", "Cannot complete the operation because table is referenced by another connection."),
  ER_PARTITION_ENGINE_DEPRECATED_FOR_TABLE(3194, "HY000", "The partition engine, used by table '%s.%s', is "
          + "deprecated and will be removed in a future release. Please use native partitioning instead."),
  ER_WARN_USING_GEOMFROMWKB_TO_SET_SRID_ZERO(3195, "01000", "%s(geometry) is deprecated and will be replaced by "
          + "st_srid(geometry, 0) in a future version. Use %s(st_aswkb(geometry), 0) instead."),
  ER_WARN_USING_GEOMFROMWKB_TO_SET_SRID(3196, "01000", "%s(geometry, srid) is deprecated and will be replaced by "
          + "st_srid(geometry, srid) in a future version. Use %s(st_aswkb(geometry), srid) instead."),
  ER_XA_RETRY(3197, "HY000", "The resource manager is not able to commit the transaction branch at this time. Please"
          + "retry later."),
  ER_KEYRING_AWS_UDF_AWS_KMS_ERROR(3198, "HY000", "Function %s failed due to"),
  ER_BINLOG_UNSAFE_XA(3199, "HY000", "Statement is unsafe because it is being used inside a XA transaction. "
          + "Concurrent XA transactions may deadlock on slaves when replicated using statements."),
  ER_UDF_ERROR(3200, "HY000", "%s UDF failed; %s"),
  ER_KEYRING_MIGRATION_FAILURE(3201, "HY000", "Can not perform keyring migration "),
  ER_KEYRING_ACCESS_DENIED_ERROR(3202, "42000", "Access denied; you need %s privileges for this operation"),
  ER_KEYRING_MIGRATION_STATUS(3203, "HY000", "Keyring migration %s."),
  ER_PLUGIN_FAILED_TO_OPEN_TABLES(3204, "HY000", "Failed to open the %s filter tables."),
  ER_PLUGIN_FAILED_TO_OPEN_TABLE(3205, "HY000", "Failed to open '%s.%s' %s table."),
  ER_AUDIT_LOG_NO_KEYRING_PLUGIN_INSTALLED(3206, "HY000", "No keyring plugin installed."),
  ER_AUDIT_LOG_ENCRYPTION_PASSWORD_HAS_NOT_BEEN_SET(3207, "HY000", "Audit log encryption password has not been set; "
          + "it will be generated automatically. Use audit_log_encryption_password_get to obtain the password or "
          + "audit_log_encryption_password_set to set a new one."),
  ER_AUDIT_LOG_COULD_NOT_CREATE_AES_KEY(3208, "HY000", "Could not create AES key. OpenSSL's EVP_BytesToKey function failed."),
  ER_AUDIT_LOG_ENCRYPTION_PASSWORD_CANNOT_BE_FETCHED(3209, "HY000", "Audit log encryption password cannot be fetched"
          + "from the keyring. Password used so far is used for encryption."),
  ER_AUDIT_LOG_JSON_FILTERING_NOT_ENABLED(3210, "HY000", "Audit Log filtering has not been installed."),
  ER_AUDIT_LOG_UDF_INSUFFICIENT_PRIVILEGE(3211, "HY000", "Request ignored for '%s'@'%s'. SUPER_ACL needed to perform operation"),
  ER_AUDIT_LOG_SUPER_PRIVILEGE_REQUIRED(3212, "HY000", "SUPER privilege required for '%s'@'%s' user."),
  ER_COULD_NOT_REINITIALIZE_AUDIT_LOG_FILTERS(3213, "HY000", "Could not reinitialize audit log filters."),
  ER_AUDIT_LOG_UDF_INVALID_ARGUMENT_TYPE(3214, "HY000", "Invalid argument type"),
  ER_AUDIT_LOG_UDF_INVALID_ARGUMENT_COUNT(3215, "HY000", "Invalid argument count"),
  ER_AUDIT_LOG_HAS_NOT_BEEN_INSTALLED(3216, "HY000", "audit_log plugin has not been installed using INSTALL PLUGIN syntax."),
  ER_AUDIT_LOG_UDF_READ_INVALID_MAX_ARRAY_LENGTH_ARG_TYPE(3217, "HY000", "Invalid \"max_array_length\"argument type."),
  ER_AUDIT_LOG_UDF_READ_INVALID_MAX_ARRAY_LENGTH_ARG_VALUE(3218, "HY000", "Invalid \"max_array_length\"argument value."),
  ER_AUDIT_LOG_JSON_FILTER_PARSING_ERROR(3219, "HY000", "%s"),
  ER_AUDIT_LOG_JSON_FILTER_NAME_CANNOT_BE_EMPTY(3220, "HY000", "Filter name cannot be empty."),
  ER_AUDIT_LOG_JSON_USER_NAME_CANNOT_BE_EMPTY(3221, "HY000", "User cannot be empty."),
  ER_AUDIT_LOG_JSON_FILTER_DOES_NOT_EXISTS(3222, "HY000", "Specified filter has not been found."),
  ER_AUDIT_LOG_USER_FIRST_CHARACTER_MUST_BE_ALPHANUMERIC(3223, "HY000", "First character of the user name must be alphanumeric."),
  ER_AUDIT_LOG_USER_NAME_INVALID_CHARACTER(3224, "HY000", "Invalid character in the user name."),
  ER_AUDIT_LOG_HOST_NAME_INVALID_CHARACTER(3225, "HY000", "Invalid character in the host name."),
  WARN_DEPRECATED_MAXDB_SQL_MODE_FOR_TIMESTAMP(3226, "HY000", "With the MAXDB SQL mode enabled, TIMESTAMP is identical with DATETIME. "
          + "The MAXDB SQL mode is deprecated and will be removed in a future release."
          + "Please disable the MAXDB SQL mode and use DATETIME instead."),
  ER_XA_REPLICATION_FILTERS(3227, "HY000", "The use of replication filters with XA transactions is not supported, and can lead to an undefined state in the replication slave."),
  ER_CANT_OPEN_ERROR_LOG(3228, "HY000", "Could not open file '%s' for error logging%s%s"),
  ER_GROUPING_ON_TIMESTAMP_IN_DST(3229, "HY000", "Grouping on temporal is non-deterministic for timezones having DST. Please consider switching to UTC for this query."),
  ER_CANT_START_SERVER_NAMED_PIPE(3230, "HY000", "Can't start server "),

  // Jimsql errorcode
  ER_SYSTEM_INTERNAL(5000, "SYSTEM_INTERNAL", "system internal error"),
  ER_SYSTEM_PLUGIN_UNSUPPORTED(5001, "SYSTEM_PLUGIN_UNSUPPORTED", "unsupported plugin type '%s'"),
  ER_SYSTEM_PLUGIN_DUPLICATE(5002, "SYSTEM_PLUGIN_DUPLICATE", "duplicate name '%s' of plugin '%s'"),
  ER_SYSTEM_PLUGIN_LOAD(5003, "SYSTEM_PLUGIN_LOAD", "plugin load error"),
  ER_SYSTEM_REFCNT_ARGUMENT(5004, "SYSTEM_REFCNT_ARGUMENT", "refCnt %s argument '%s' is not positive"),
  ER_SYSTEM_REFCNT_STATE(5005, "SYSTEM_REFCNT_STATE", "refCnt state '%s' is not positive"),
  ER_SYSTEM_OUTOF_CAPACITY(5006, "SYSTEM_OUTOF_CAPACITY", "index '%s' out of bounds, expected: range(%s, %s)"),
  ER_SYSTEM_SEQ_INVALID(5007, "SYSTEM_SEQ_INVALID", "invalid sequence id, expected: %s, actual: %s"),
  ER_SYSTEM_CANCELLED(5008, "SYSTEM_CANCELLED", "system cancelled"),
  ER_SYSTEM_PARSE_ERROR(5009, "SYSTEM_PARSE_ERROR", "sql parse error: %s"),
  ER_SYSTEM_COLLATION_INVALID(5010, "SYSTEM_COLLATION_INVALID", "invalid collation name '%s'"),
  ER_SYSTEM_VALUE_TYPE_INVALID(5011, "SYSTEM_VALUE_TYPE_INVALID", "invalid value type"),
  ER_SYSTEM_VALUE_CONVERT_ERROR(5012, "SYSTEM_VALUE_CONVERT_ERROR", "value type '%s' cant convert to '%s', value is '%s'"),
  ER_SYSTEM_VALUE_OPERATION_ERROR(5013, "SYSTEM_VALUE_OPERATION_ERROR", "value types(%s) unsupported operation '%s'"),
  ER_SYSTEM_VALUE_TOO_LONG(5014, "SYSTEM_VALUE_TOO_LONG", "value too long, field length %s, value length %s"),
  ER_SYSTEM_VALUE_OVER_FLOW(5015, "SYSTEM_VALUE_OVER_FLOW", "%s value is out of range in '%s'"),
  ER_SYSTEM_VALUE_TRUNCATED(5016, "SYSTEM_VALUE_TRUNCATED", "value truncated"),
  ER_SYSTEM_EXPR_REWRITE_STACK_INVALID(5017, "SYSTEM_EXPR_REWRITE_STACK_INVALID", "expression rewrite stack is invalid with size '%s'"),
  ER_SYSTEM_FUNC_OPERATION_ERROR(5018, "SYSTEM_FUNC_OPERATION_ERROR", "function(%s) unsupported operation '%s'"),
  ER_SYSTEM_VALUE_OVER_FLOW_UP(5019, "SYSTEM_VALUE_OVER_FLOW_UP", "%s value is out of range in '%s'"),
  ER_SYSTEM_VALUE_OVER_FLOW_DOWN(5020, "SYSTEM_VALUE_OVER_FLOW_DOWN", "%s value is out of range in '%s'"),

  ER_META_NO_MASTER(6000, "META_NO_MASTER", "meta has no master leader"),
  ER_META_GET_CATALOG_LIST(6001, "META_GET_CATALOG_LIST", "meta get catalogs from '%s' error"),
  ER_META_GET_CATALOG(6002, "META_GET_CATALOG", "meta get catalog from cluster '%s' error"),
  ER_META_CREATE_CATALOG(6003, "META_CREATE_CATALOG", "meta create catalog from '%s' error"),
  ER_META_DELETE_CATALOG(6004, "META_DELETE_CATALOG", "meta delete catalog from '%s' error"),
  ER_META_GET_TABLE_LIST(6005, "META_GET_TABLE_LIST", "meta get tables from '%s' error"),
  ER_META_GET_TABLE(6006, "META_GET_TABLE", "meta get table of '%s' from '%s' error"),
  ER_META_CREATE_TABLE(6007, "META_CREATE_TABLE", "meta create table of '%s' from '%s' error"),
  ER_META_UPDATE_TABLE(6008, "META_UPDATE_TABLE", "meta update table of '%s' from '%s' error"),
  ER_META_DELETE_TABLE(6009, "META_DELETE_TABLE", "meta delete table of '%s' from '%s' error"),
  ER_META_GET_ROUTE(6010, "META_GET_ROUTE", "meta get route of table '%s' error"),
  ER_META_GET_NODE_LIST(6011, "META_GET_NODE_LIST", "meta get nodes from '%s' error"),
  ER_META_GET_NODE(6012, "META_GET_NODE", "meta get node '%s' error"),
  ER_META_SHARD_NOT_EXIST(6013, "META_SHARD_NOT_FOUND", "meta not found shards exist"),
  ER_META_NODE_NOT_EXIST(6014, "META_NODE_NOT_EXIST", "meta not found node '%s'"),
  ER_META_INVALID_REPLICA_NUMBER(6015, "META_INVALID_REPLICA_NUMBER", "Invalid replica number '%s'"),
  ER_META_UNKNOWN_TYPELEN(6016, "ER_META_UNKNOWN_TYPELEN", "Unknown length for type '%s'"),
  ER_META_UNKNOWN_FRACTIONLEN(6017, "ER_META_UNKNOWN_FRACTIONLEN", "Unknown length for type '%s' with fraction '%s'"),
  ER_META_UNSUPPORTED_PARTITION(6018, "ER_META_UNSUPPORTED_PARTITION", "Unsupported partition by multi columns and Non-Primary Key"),
  ER_META_UNSUPPORTED_PARTITION_VALUE(6019, "ER_META_UNSUPPORTED_PARTITION_VALUE", "Unsupported partition multi values and operator Non-Less Than"),
  ER_META_UNSUPPORTED_PARTITION_TYPE(6020, "ER_META_UNSUPPORTED_PARTITION_TYPE", "Unsupported partition data type %s"),
  ER_META_STORE_ERROR(6021, "ER_META_STORE_ERROR", "meta storage error : '%s'"),
  ER_META_CREATE_RANGE(6022, "CREATE_RANGE", "table %s:[code: %s, error: %s]"),
  ER_META_DELETE_RANGE(6023, "DELETE_RANGE", "table %s:[code: %s, error: %s]"),
  ER_META_GET_TABLE_STAT(6024, "META_GET_TABLE_STAT", "meta get all table stats error"),
  ER_META_GET_AUTO_INCR(6025, "META_GET_AUTO_INCR", "meta get auto incr id error"),
  ER_META_GET_TABLE_STATS(6016, "META_GET_TABLE_STAT", "meta get all table stats error"),

  ER_RPC_CONNECT_INACTIVE(7000, "RPC_CONNECT_INACTIVE", "rpc connection '%s' is inactive"),
  ER_RPC_CONNECT_TIMEOUT(7001, "RPC_CONNECT_TIMEOUT", "rpc connect to '%s' timeout"),
  ER_RPC_CONNECT_UNKNOWN(7002, "RPC_CONNECT_UNKNOWN", "rpc connect to '%s' unknow host"),
  ER_RPC_CONNECT_INTERRUPTED(7003, "RPC_CONNECT_INTERRUPTED", "rpc connect to '%s' interrupted, error: %s"),
  ER_RPC_CONNECT_ERROR(7004, "RPC_CONNECT_ERROR", "rpc connect to '%s' error"),
  ER_RPC_REQUEST_TIMEOUT(7005, "RPC_REQUEST_TIMEOUT", "rpc request '%s' of connection '%s' is timeout"),
  ER_RPC_REQUEST_CODEC(7006, "RPC_REQUEST_CODEC", "rpc request codec error"),
  ER_RPC_REQUEST_INVALID(7007, "RPC_REQUEST_INVALID", "rpc request command must not be null"),
  ER_RPC_REQUEST_ERROR(7008, "RPC_REQUEST_ERROR", "rpc connection '%s' send command '%s' error"),

  ER_SHARD_LEADER_CHANGE(7030, "SHARD_LEADER_CHANGE", "db shard route leader change, shard: '%s', oldLeader: '%s', newLeader: '%s'"),
  ER_SHARD_ROUTE_CHANGE(7031, "SHARD_ROUTE_CHANGE", "db shard route change, node: '%s', shard: '%s'"),
  ER_SHARD_NOT_EXIST(7032, "SHARD_NOT_FOUND", "db shard for key '%s' not exist"),
  ER_SHARD_NOT_INCLUDE(7033, "SHARD_NOT_INCLUDE", "db shard not include key '%s', node: '%s', shard: '%s'"),
  ER_SHARD_STALE_EPOCH(7034, "SHARD_STALE_EPOCH", "db shard has stale epoch, node: '%s', shard: '%s'"),
  ER_SHARD_ENTRY_LARGE(7035, "SHARD_ENTRY_LARGE", "db shard response '%s' EntryTooLarge, node: '%s', shard: '%s'"),
  ER_SHARD_TIME_OUT(7036, "SHARD_TIME_OUT", "db shard response %s' Timeout, node: '%s', shard: '%s'"),
  ER_SHARD_RAFT_FAIL(7037, "SHARD_RAFT_FAIL", "db shard response RaftFail, node: '%s', shard: '%s'"),
  ER_SHARD_RESPONSE_ERROR(7038, "SHARD_RESPONSE_ERROR", "db shard response '%s' error: %s"),
  ER_SHARD_RESPONSE_CODE(7039, "SHARD_RESPONSE_CODE", "db command '%s' response code '%s' error"),
  ER_SHARD_RESPONSE_AFFECTED(7040, "SHARD_RESPONSE_AFFECTED", "db response '%s' not equal request '%s'"),
  ER_SHARD_RESPONSE_VERSION_DUP(7041, "SHARD_RESPONSE_VERSION_DUP", "db response duplicate version '%s'"),

  ER_CONTEXT_TIMEOUT(7101, "CONTEXT EXEC TIMEOUT", "%s exec time out"),

  ER_TXN_STATE_INVALID(8000, "TXN_STATE_INVALID", "transaction state '%s' error"),
  ER_TXN_ROW_SIZE(8001, "TXN_ROW_SIZE", "transaction expected result size '%s', but get '%s'"),
  ER_TXN_VERSION_CONFLICT(8002, "TXN_VERSION_CONFLICT", "transaction version conflict, expected: '%s', actual: %s'"),
  ER_TXN_STATUS_CONFLICT(8003, "TXN_STATUS_CONFLICT", "transaction status conflict, status: %s'"),
  ER_TXN_CONFLICT(8004, "TXN_CONFLICT", "transaction occur conflict, expected: %s', actual: %s'");

  private final int code;
  private final String state;
  private final String message;

  ErrorCode(int code, String state, String message) {
    this.code = code;
    this.state = state;
    this.message = message;
  }

  public final int getCode() {
    return this.code;
  }

  public final String getState() {
    return state;
  }

  public final String getMessage() {
    return message;
  }

  @Override
  public final String toString() {
    StringBuilder buf = new StringBuilder(32);
    buf.append('(')
            .append(this.code)
            .append(')')
            .append(this.state);
    return buf.toString();
  }
}
