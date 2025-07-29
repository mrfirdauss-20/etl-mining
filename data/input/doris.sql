-- Create database
CREATE DATABASE IF NOT EXISTS data_mart
CHARACTER SET utf8mb4 
COLLATE utf8mb4_unicode_ci;
USE data_mart;

CREATE TABLE `production_weather_summary_daily` (
  `date` date NULL,
  `fuel_consumption` double SUM NULL,
  `tons_extracted` bigint SUM NULL,
  `fuel_efficiency` double REPLACE NULL,
  `precipitation_sum` double SUM NULL,
  `weather_impact` double REPLACE NULL
) ENGINE=OLAP
AGGREGATE KEY(`date`)
DISTRIBUTED BY HASH(`date`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);

-- raw.equipment_sensors definition
CREATE DATABASE IF NOT EXISTS raw
CHARACTER SET utf8mb4 
COLLATE utf8mb4_unicode_ci;
USE raw;
CREATE TABLE `equipment_sensors` (
  `timestamp` datetime NOT NULL,
  `equipment_id` varchar(50) NOT NULL,
  `status` varchar(50) NULL,
  `fuel_consumption` float NULL,
  `maintenance_alert` boolean NULL
) ENGINE=OLAP
DUPLICATE KEY(`timestamp`, `equipment_id`)
DISTRIBUTED BY HASH(`equipment_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);

-- raw.mines definition

CREATE TABLE `mines` (
  `mine_id` int NOT NULL,
  `mine_code` varchar(10) NOT NULL,
  `mine_name` varchar(50) NOT NULL,
  `location` varchar(100) NOT NULL,
  `operational_status` varchar(20) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`mine_id`)
COMMENT 'Table of mining sites'
DISTRIBUTED BY RANDOM BUCKETS AUTO
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);


CREATE TABLE `production_logs` (
  `log_id` int NOT NULL,
  `date` date NOT NULL,
  `mine_id` int NOT NULL,
  `shift` varchar(10) NOT NULL,
  `tons_extracted` decimal(10,2) NULL,
  `quality_grade` decimal(3,1) NULL
) ENGINE=OLAP
DUPLICATE KEY(`log_id`)
COMMENT 'Daily production log'
DISTRIBUTED BY RANDOM BUCKETS AUTO
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);

-- raw.weather definition

CREATE TABLE `weather` (
  `time` date NOT NULL,
  `latitude` float NULL,
  `longitude` float NULL,
  `generationtime_ms` float NULL,
  `utc_offset_seconds` int NULL,
  `timezone` varchar(50) NULL,
  `timezone_abbreviation` varchar(10) NULL,
  `elevation` int NULL,
  `temperature_2m_mean` float NULL,
  `precipitation_sum` float NULL
) ENGINE=OLAP
DUPLICATE KEY(`time`)
DISTRIBUTED BY HASH(`time`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);

-- `transform`.equipment_utilization_daily definition
CREATE DATABASE IF NOT EXISTS transform
CHARACTER SET utf8mb4 
COLLATE utf8mb4_unicode_ci;
USE transform;

CREATE TABLE `equipment_utilization_daily` (
  `usage_date` date NOT NULL,
  `equipment_id` varchar(64) NOT NULL,
  `utilization_percentage` decimal(5,2) REPLACE NOT NULL,
  `active_count` bigint SUM NOT NULL,
  `idle_count` bigint SUM NOT NULL,
  `maintenance_count` bigint SUM NOT NULL,
  `fuel_consumption` double SUM NOT NULL
) ENGINE=OLAP
AGGREGATE KEY(`usage_date`, `equipment_id`)
DISTRIBUTED BY HASH(`usage_date`, `equipment_id`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);

-- `transform`.production_summary_daily definition

CREATE TABLE `production_summary_daily` (
  `date` date NOT NULL,
  `mine_id` varchar(64) NOT NULL,
  `mine_name` varchar(255) REPLACE NOT NULL,
  `location` varchar(255) REPLACE NOT NULL,
  `tons_extracted` bigint SUM NOT NULL,
  `quality_grade` double REPLACE NOT NULL
) ENGINE=OLAP
AGGREGATE KEY(`date`, `mine_id`)
DISTRIBUTED BY HASH(`date`, `mine_id`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);