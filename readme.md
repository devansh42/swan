# Proposal for Bench Routes Storage Engine
## Requirements
* It should be a Time Series DB
* It should use binary encoding for persisting data to optimally use resources
* It should be well suited for Write Intensive Load
* It should provide fast read times for visulaization purposes

## Swan

### Brief 
Swan is a timeseries storage engine specially designed for workloads like bench routes inspired by LevelDB and Influx DB.

#### Writes
* Swan writes are highly inspired by level db
* All writes are done in a golang map (for now atleast).
* All the writes are flushed to a Log File in background at the same time
* After a certain limit (say 2MB) this file would be converted to a Chunk File and a new log file is created for futher usage
* Log File is Row based, so that writes can be fast
* Chunk File is column based to make read fasters ( motivated by InfluxDB )
* Every Chunk file has a level defined
* Chunk file at level x can contain only 10^x MB of data
* There would be n number of levels (can be tweaked)
* After exceeding its level bounderies this chunk file will merge it self to the chunck file of level x+1 (motivated by level db compactation)
* After merge chunk file at level x is deleted
* This process continues for level x+1 then and so on, until it reaches to level n.
* In every chunk file data is sorted on basis for keys and then on basis of timestamps ( key -> timestamp )

#### Reads
* As all the data is presented in golang map at will be searched first
* After that it would search chunck files
* Due to sorted column based data present in chunck, it will be very fast to read the data

#### Updates
Swan doesn't supports updates yet

#### Deletion
Swan doesn't supports deletion yet

### Data Format Chunk File
Column oriented records sorted by, key_name > time_stamp
	
	Header: [start_time][end_time][record_count][key_count]
	[key1][key2][key3]...
	[record_count(key1)][record_count(key2)][record_count(key3)]...
	[ts_k1_r1][ts_k1_r2][ts_k1_rn][ts_k2_r1][ts_k2_r2][ts_k2_rn]....
	[k1_r1_ln][k1_r2_ln][k1_rn_ln][k2_r1_ln][k2_r2_ln][k2_rn_ln]
	[k1_r1][k1_r2][k1_rn][k2_r1][k2_r2][k2_rn]
	
