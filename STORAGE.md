## MizuDB Storage Engine

#### Components

- Catalog per database
- Write-Ahead-Logging (WAL) per database
- Copy-on-Write (COW) Parquet file per database
- Local Object Storage backed by t4 (github.com/XiangpengHao/t4)
- Concurrency control (TO DO at a later date, storage engine v2, use MVCC)

#### t4 structure

- t4 uses a single file as a local object store
- t4 file organization
    - key-value pairs for metadata and data files
- Right now t4 has a max file size of 4GB per key, this may become an issue in the future
  I will likely need to implement a more sophisticated file organization scheme, maybe including multiple parquet files
  per DB.
  For now, I will just use a single parquet file per DB.

key:value structure

```
{ "database_name.parquet": "<parquet file as bytes>" }
{ "database_name_catalog.json": "<database catalog json as bytes>" }
{ "database_name_wal.arrow": "<arrow record batches as bytes>" }
```

#### Catalog structure

- catalog is stored in the same t4 object store with per database keys
- catalog is updated atomically using a copy-on-write approach (COW) similar to the parquet file
- Catalog should be updated immediately after a DDL operation (no need to wait for WAL flush)
- Catalog should be updated during WAL flush for DML operations
- JSON file metadata
    - Database name
    - Database location
    - Database size
    - Database creation time
    - Database last modified time
    - Database schema (DDL)

#### Parquet structure

- Parquet files are stored in the same t4 object store with per database keys
- Parquet files are stored in a single file per database
- Parquet files are updated atomically using a copy-on-write approach (COW)
- In order to support COW, we need to have a new key and atomic pointer swap operation
    - t4 should make the full read-modify-write operation pretty cheap due to locality and using SSDs
    - (Idea: Need to implement a put_if_match operation in t4)
- Inspiration: https://www.uber.com/us/en/blog/fast-copy-on-write-within-apache-parquet/
- Inspiration: https://datafusion.apache.org/blog/2025/08/15/external-parquet-indexes/

#### WAL structure

- WAL files are stored in the same t4 object store as the parquet file

Theoretically, the WAL should function like so

```
oldest  | <BEGIN>
        | <RECORD BATCH WITH OP>
        | <COMMIT>
        | <BEGIN>
        | <RECORD BATCH WITH OP>
newest  | <COMMIT>
```

- The WAL file should be using arrow format for the data, each transaction should be a single arrow record batch
- For non-transaction operations such as row inserts, we use GroupCommit; there should be a 5ms delay between group
  commits (WAL flush with fsync)