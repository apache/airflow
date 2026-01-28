/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// NOTE: The "required" and "optional" keywords for the service methods are purely for documentation

namespace java org.apache.hadoop.hbase.thrift2.generated
namespace cpp apache.hadoop.hbase.thrift2
namespace rb Apache.Hadoop.Hbase.Thrift2
namespace py hbase
namespace perl Hbase

struct TTimeRange {
  1: required i64 minStamp,
  2: required i64 maxStamp
}

/**
 * Addresses a single cell or multiple cells
 * in a HBase table by column family and optionally
 * a column qualifier and timestamp
 */
struct TColumn {
  1: required binary family,
  2: optional binary qualifier,
  3: optional i64 timestamp
}

/**
 * Represents a single cell and its value.
 */
struct TColumnValue {
  1: required binary family,
  2: required binary qualifier,
  3: required binary value,
  4: optional i64 timestamp,
  5: optional binary tags,
  6: optional byte type
}

/**
 * Represents a single cell and the amount to increment it by
 */
struct TColumnIncrement {
  1: required binary family,
  2: required binary qualifier,
  3: optional i64 amount = 1
}

/**
 * if no Result is found, row and columnValues will not be set.
 */
struct TResult {
  1: optional binary row,
  2: required list<TColumnValue> columnValues,
  3: optional bool stale = false
  4: optional bool partial = false
}

/**
 * Specify type of delete:
 *  - DELETE_COLUMN means exactly one version will be removed,
 *  - DELETE_COLUMNS means previous versions will also be removed.
 */
enum TDeleteType {
  DELETE_COLUMN = 0,
  DELETE_COLUMNS = 1,
  DELETE_FAMILY = 2,
  DELETE_FAMILY_VERSION = 3
}

/**
 * Specify Durability:
 *  - SKIP_WAL means do not write the Mutation to the WAL.
 *  - ASYNC_WAL means write the Mutation to the WAL asynchronously,
 *  - SYNC_WAL means write the Mutation to the WAL synchronously,
 *  - FSYNC_WAL means Write the Mutation to the WAL synchronously and force the entries to disk.
 */

enum TDurability {
  USE_DEFAULT = 0,
  SKIP_WAL = 1,
  ASYNC_WAL = 2,
  SYNC_WAL = 3,
  FSYNC_WAL = 4
}
struct TAuthorization {
 1: optional list<string> labels
}

struct TCellVisibility {
 1: optional string expression
}

/**
 * Specify Consistency:
 *  - STRONG means reads only from primary region
 *  - TIMELINE means reads might return values from secondary region replicas
 */
enum TConsistency {
  STRONG = 1,
  TIMELINE = 2
}

/**
 * Used to perform Get operations on a single row.
 *
 * The scope can be further narrowed down by specifying a list of
 * columns or column families.
 *
 * To get everything for a row, instantiate a Get object with just the row to get.
 * To further define the scope of what to get you can add a timestamp or time range
 * with an optional maximum number of versions to return.
 *
 * If you specify a time range and a timestamp the range is ignored.
 * Timestamps on TColumns are ignored.
 */
struct TGet {
  1: required binary row,
  2: optional list<TColumn> columns,

  3: optional i64 timestamp,
  4: optional TTimeRange timeRange,

  5: optional i32 maxVersions,
  6: optional binary filterString,
  7: optional map<binary, binary> attributes
  8: optional TAuthorization authorizations
  9: optional TConsistency consistency
  10: optional i32 targetReplicaId
  11: optional bool cacheBlocks
  12: optional i32 storeLimit
  13: optional i32 storeOffset
  14: optional bool existence_only
  15: optional binary filterBytes

}

/**
 * Used to perform Put operations for a single row.
 *
 * Add column values to this object and they'll be added.
 * You can provide a default timestamp if the column values
 * don't have one. If you don't provide a default timestamp
 * the current time is inserted.
 *
 * You can specify how this Put should be written to the write-ahead Log (WAL)
 * by changing the durability. If you don't provide durability, it defaults to
 * column family's default setting for durability.
 */
struct TPut {
  1: required binary row,
  2: required list<TColumnValue> columnValues
  3: optional i64 timestamp,
  5: optional map<binary, binary> attributes,
  6: optional TDurability durability,
  7: optional TCellVisibility cellVisibility
}

/**
 * Used to perform Delete operations on a single row.
 *
 * The scope can be further narrowed down by specifying a list of
 * columns or column families as TColumns.
 *
 * Specifying only a family in a TColumn will delete the whole family.
 * If a timestamp is specified all versions with a timestamp less than
 * or equal to this will be deleted. If no timestamp is specified the
 * current time will be used.
 *
 * Specifying a family and a column qualifier in a TColumn will delete only
 * this qualifier. If a timestamp is specified only versions equal
 * to this timestamp will be deleted. If no timestamp is specified the
 * most recent version will be deleted.  To delete all previous versions,
 * specify the DELETE_COLUMNS TDeleteType.
 *
 * The top level timestamp is only used if a complete row should be deleted
 * (i.e. no columns are passed) and if it is specified it works the same way
 * as if you had added a TColumn for every column family and this timestamp
 * (i.e. all versions older than or equal in all column families will be deleted)
 *
 * You can specify how this Delete should be written to the write-ahead Log (WAL)
 * by changing the durability. If you don't provide durability, it defaults to
 * column family's default setting for durability.
 */
struct TDelete {
  1: required binary row,
  2: optional list<TColumn> columns,
  3: optional i64 timestamp,
  4: optional TDeleteType deleteType = 1,
  6: optional map<binary, binary> attributes,
  7: optional TDurability durability,
  8: optional TCellVisibility cellVisibility

}

/**
 * Used to perform Increment operations for a single row.
 *
 * You can specify how this Increment should be written to the write-ahead Log (WAL)
 * by changing the durability. If you don't provide durability, it defaults to
 * column family's default setting for durability.
 */
struct TIncrement {
  1: required binary row,
  2: required list<TColumnIncrement> columns,
  4: optional map<binary, binary> attributes,
  5: optional TDurability durability
  6: optional TCellVisibility cellVisibility
  7: optional bool returnResults
}

/* 
 * Used to perform append operation 
 */
struct TAppend {
  1: required binary row,
  2: required list<TColumnValue> columns,
  3: optional map<binary, binary> attributes,
  4: optional TDurability durability
  5: optional TCellVisibility cellVisibility
  6: optional bool returnResults
}

enum TReadType {
  DEFAULT = 1,
  STREAM = 2,
  PREAD = 3
}

/**
 * Any timestamps in the columns are ignored but the colFamTimeRangeMap included, use timeRange to select by timestamp.
 * Max versions defaults to 1.
 */
struct TScan {
  1: optional binary startRow,
  2: optional binary stopRow,
  3: optional list<TColumn> columns
  4: optional i32 caching,
  5: optional i32 maxVersions=1,
  6: optional TTimeRange timeRange,
  7: optional binary filterString,
  8: optional i32 batchSize,
  9: optional map<binary, binary> attributes
  10: optional TAuthorization authorizations
  11: optional bool reversed
  12: optional bool cacheBlocks
  13: optional map<binary,TTimeRange> colFamTimeRangeMap
  14: optional TReadType readType
  15: optional i32 limit
  16: optional TConsistency consistency
  17: optional i32 targetReplicaId
  18: optional binary filterBytes

}

/**
 * Atomic mutation for the specified row. It can be either Put or Delete.
 */
union TMutation {
  1: TPut put
  2: TDelete deleteSingle
}

/**
 * A TRowMutations object is used to apply a number of Mutations to a single row.
 */
struct TRowMutations {
  1: required binary row
  2: required list<TMutation> mutations
}

struct THRegionInfo {
  1: required i64 regionId
  2: required binary tableName
  3: optional binary startKey
  4: optional binary endKey
  5: optional bool offline
  6: optional bool split
  7: optional i32 replicaId
}

struct TServerName {
  1: required string hostName
  2: optional i32 port
  3: optional i64 startCode
}

struct THRegionLocation {
  1: required TServerName serverName
  2: required THRegionInfo regionInfo
}

/**
 * Thrift wrapper around
 * org.apache.hadoop.hbase.CompareOperator.
 */
enum TCompareOperator {
  LESS = 0,
  LESS_OR_EQUAL = 1,
  EQUAL = 2,
  NOT_EQUAL = 3,
  GREATER_OR_EQUAL = 4,
  GREATER = 5,
  NO_OP = 6
}

/**
 * Thrift wrapper around
 * org.apache.hadoop.hbase.regionserver.BloomType
 */
enum TBloomFilterType {
/**
   * Bloomfilters disabled
   */
  NONE = 0,
  /**
   * Bloom enabled with Table row as Key
   */
  ROW = 1,
  /**
   * Bloom enabled with Table row &amp; column (family+qualifier) as Key
   */
  ROWCOL = 2,
  /**
   * Bloom enabled with Table row prefix as Key, specify the length of the prefix
   */
  ROWPREFIX_FIXED_LENGTH = 3,
}

/**
 * Thrift wrapper around
 * org.apache.hadoop.hbase.io.compress.Algorithm
 */
enum TCompressionAlgorithm {
  LZO = 0,
  GZ = 1,
  NONE = 2,
  SNAPPY = 3,
  LZ4 = 4,
  BZIP2 = 5,
  ZSTD = 6
}

/**
 * Thrift wrapper around
 * org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
 */
enum TDataBlockEncoding {
/** Disable data block encoding. */
  NONE = 0,
  // id 1 is reserved for the BITSET algorithm to be added later
  PREFIX = 2,
  DIFF  = 3,
  FAST_DIFF = 4,
  // id 5 is reserved for the COPY_KEY algorithm for benchmarking
  // COPY_KEY(5, "org.apache.hadoop.hbase.io.encoding.CopyKeyDataBlockEncoder"),
  // PREFIX_TREE(6, "org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeCodec"),
  ROW_INDEX_V1 = 7
}

/**
 * Thrift wrapper around
 * org.apache.hadoop.hbase.KeepDeletedCells
 */
enum TKeepDeletedCells {
  /** Deleted Cells are not retained. */
  FALSE = 0,
  /**
   * Deleted Cells are retained until they are removed by other means
   * such TTL or VERSIONS.
   * If no TTL is specified or no new versions of delete cells are
   * written, they are retained forever.
   */
  TRUE = 1,
  /**
   * Deleted Cells are retained until the delete marker expires due to TTL.
   * This is useful when TTL is combined with MIN_VERSIONS and one
   * wants to keep a minimum number of versions around but at the same
   * time remove deleted cells after the TTL.
   */
  TTL = 2
}

/**
 * Thrift wrapper around
 * org.apache.hadoop.hbase.TableName
 */
struct TTableName {
  /** namespace name */
  1: optional binary ns
  /** tablename */
  2: required binary qualifier
}

/**
 * Thrift wrapper around
 * org.apache.hadoop.hbase.client.ColumnFamilyDescriptor
 */
struct TColumnFamilyDescriptor {
  1: required binary name
  2: optional map<binary, binary> attributes
  3: optional map<string, string> configuration
  4: optional i32 blockSize
  5: optional TBloomFilterType bloomnFilterType
  6: optional TCompressionAlgorithm compressionType
  7: optional i16 dfsReplication
  8: optional TDataBlockEncoding dataBlockEncoding
  9: optional TKeepDeletedCells keepDeletedCells
  10: optional i32 maxVersions
  11: optional i32 minVersions
  12: optional i32 scope
  13: optional i32 timeToLive
  14: optional bool blockCacheEnabled
  15: optional bool cacheBloomsOnWrite
  16: optional bool cacheDataOnWrite
  17: optional bool cacheIndexesOnWrite
  18: optional bool compressTags
  19: optional bool evictBlocksOnClose
  20: optional bool inMemory

}

/**
 * Thrift wrapper around
 * org.apache.hadoop.hbase.client.TableDescriptor
 */
struct TTableDescriptor {
 1: required TTableName tableName
 2: optional list<TColumnFamilyDescriptor> columns
 3: optional map<binary, binary> attributes
 4: optional TDurability durability
}

/**
 * Thrift wrapper around
 * org.apache.hadoop.hbase.NamespaceDescriptor
 */
struct TNamespaceDescriptor {
1: required string name
2: optional map<string, string> configuration
}

enum TLogType {
  SLOW_LOG = 1,
  LARGE_LOG = 2
}

enum TFilterByOperator {
  AND,
  OR
}

/**
 * Thrift wrapper around
 * org.apache.hadoop.hbase.client.LogQueryFilter
 */
struct TLogQueryFilter {
  1: optional string regionName
  2: optional string clientAddress
  3: optional string tableName
  4: optional string userName
  5: optional i32 limit = 10
  6: optional TLogType logType = 1
  7: optional TFilterByOperator filterByOperator = TFilterByOperator.OR
}


/**
 * Thrift wrapper around
 * org.apache.hadoop.hbase.client.OnlineLogRecord
 */
struct TOnlineLogRecord {
  1: required i64 startTime
  2: required i32 processingTime
  3: required i32 queueTime
  4: required i64 responseSize
  5: required string clientAddress
  6: required string serverClass
  7: required string methodName
  8: required string callDetails
  9: required string param
  10: required string userName
  11: required i32 multiGetsCount
  12: required i32 multiMutationsCount
  13: required i32 multiServiceCalls
  14: optional string regionName
  15: optional i64 blockBytesScanned
  16: optional i64 fsReadTime
}

//
// Exceptions
//

/**
 * A TIOError exception signals that an error occurred communicating
 * to the HBase master or a HBase region server. Also used to return
 * more general HBase error conditions.
 */
exception TIOError {
  1: optional string message
  2: optional bool canRetry
}

/**
 * A TIllegalArgument exception indicates an illegal or invalid
 * argument was passed into a procedure.
 */
exception TIllegalArgument {
  1: optional string message
}

/**
 * Specify type of thrift server: thrift and thrift2
 */
enum TThriftServerType {
  ONE = 1,
  TWO = 2
}

enum TPermissionScope {
  TABLE = 0,
  NAMESPACE = 1
}

/**
 * TAccessControlEntity for permission control
 */
struct TAccessControlEntity {
 1: required string username
 2: required TPermissionScope scope
 4: required string actions
 5: optional string tableName
 6: optional string nsName
}

service THBaseService {

  /**
   * Test for the existence of columns in the table, as specified in the TGet.
   *
   * @return true if the specified TGet matches one or more keys, false if not
   */
  bool exists(
    /** the table to check on */
    1: required binary table,

    /** the TGet to check for */
    2: required TGet tget
  ) throws (1:TIOError io)


  /**
  * Test for the existence of columns in the table, as specified by the TGets.
  *
  * This will return an array of booleans. Each value will be true if the related Get matches
  * one or more keys, false if not.
  */
  list<bool> existsAll(
    /** the table to check on */
    1: required binary table,

    /** a list of TGets to check for */
    2: required list<TGet> tgets
  ) throws (1:TIOError io)

  /**
   * Method for getting data from a row.
   *
   * If the row cannot be found an empty Result is returned.
   * This can be checked by the empty field of the TResult
   *
   * @return the result
   */
  TResult get(
    /** the table to get from */
    1: required binary table,

    /** the TGet to fetch */
    2: required TGet tget
  ) throws (1: TIOError io)

  /**
   * Method for getting multiple rows.
   *
   * If a row cannot be found there will be a null
   * value in the result list for that TGet at the
   * same position.
   *
   * So the Results are in the same order as the TGets.
   */
  list<TResult> getMultiple(
    /** the table to get from */
    1: required binary table,

    /** a list of TGets to fetch, the Result list
        will have the Results at corresponding positions
        or null if there was an error */
    2: required list<TGet> tgets
  ) throws (1: TIOError io)

  /**
   * Commit a TPut to a table.
   */
  void put(
    /** the table to put data in */
    1: required binary table,

    /** the TPut to put */
    2: required TPut tput
  ) throws (1: TIOError io)

  /**
   * Atomically checks if a row/family/qualifier value matches the expected
   * value. If it does, it adds the TPut.
   *
   * @return true if the new put was executed, false otherwise
   */
  bool checkAndPut(
    /** to check in and put to */
    1: required binary table,

    /** row to check */
    2: required binary row,

    /** column family to check */
    3: required binary family,

    /** column qualifier to check */
    4: required binary qualifier,

    /** the expected value, if not provided the
        check is for the non-existence of the
        column in question */
    5: binary value,

    /** the TPut to put if the check succeeds */
    6: required TPut tput
  ) throws (1: TIOError io)

  /**
   * Commit a List of Puts to the table.
   */
  void putMultiple(
    /** the table to put data in */
    1: required binary table,

    /** a list of TPuts to commit */
    2: required list<TPut> tputs
  ) throws (1: TIOError io)

  /**
   * Deletes as specified by the TDelete.
   *
   * Note: "delete" is a reserved keyword and cannot be used in Thrift
   * thus the inconsistent naming scheme from the other functions.
   */
  void deleteSingle(
    /** the table to delete from */
    1: required binary table,

    /** the TDelete to delete */
    2: required TDelete tdelete
  ) throws (1: TIOError io)

  /**
   * Bulk commit a List of TDeletes to the table.
   *
   * Throws a TIOError if any of the deletes fail.
   *
   * Always returns an empty list for backwards compatibility.
   */
  list<TDelete> deleteMultiple(
    /** the table to delete from */
    1: required binary table,

    /** list of TDeletes to delete */
    2: required list<TDelete> tdeletes
  ) throws (1: TIOError io)

  /**
   * Atomically checks if a row/family/qualifier value matches the expected
   * value. If it does, it adds the delete.
   *
   * @return true if the new delete was executed, false otherwise
   */
  bool checkAndDelete(
    /** to check in and delete from */
    1: required binary table,

    /** row to check */
    2: required binary row,

    /** column family to check */
    3: required binary family,

    /** column qualifier to check */
    4: required binary qualifier,

    /** the expected value, if not provided the
        check is for the non-existence of the
        column in question */
    5: binary value,

    /** the TDelete to execute if the check succeeds */
    6: required TDelete tdelete
  ) throws (1: TIOError io)

  TResult increment(
    /** the table to increment the value on */
    1: required binary table,

    /** the TIncrement to increment */
    2: required TIncrement tincrement
  ) throws (1: TIOError io)

  TResult append(
    /** the table to append the value on */
    1: required binary table,

    /** the TAppend to append */
    2: required TAppend tappend
  ) throws (1: TIOError io)

  /**
   * Get a Scanner for the provided TScan object.
   *
   * @return Scanner Id to be used with other scanner procedures
   */
  i32 openScanner(
    /** the table to get the Scanner for */
    1: required binary table,

    /** the scan object to get a Scanner for */
    2: required TScan tscan,
  ) throws (1: TIOError io)

  /**
   * Grabs multiple rows from a Scanner.
   *
   * @return Between zero and numRows TResults
   */
  list<TResult> getScannerRows(
    /** the Id of the Scanner to return rows from. This is an Id returned from the openScanner function. */
    1: required i32 scannerId,

    /** number of rows to return */
    2: i32 numRows = 1
  ) throws (
    1: TIOError io,

    /** if the scannerId is invalid */
    2: TIllegalArgument ia
  )

  /**
   * Closes the scanner. Should be called to free server side resources timely.
   * Typically close once the scanner is not needed anymore, i.e. after looping
   * over it to get all the required rows.
   */
  void closeScanner(
    /** the Id of the Scanner to close **/
    1: required i32 scannerId
  ) throws (
    1: TIOError io,

    /** if the scannerId is invalid */
    2: TIllegalArgument ia
  )

  /**
   * mutateRow performs multiple mutations atomically on a single row.
  */
  void mutateRow(
  /** table to apply the mutations */
    1: required binary table,

    /** mutations to apply */
    2: required TRowMutations trowMutations
  ) throws (1: TIOError io)

  /**
   * Get results for the provided TScan object.
   * This helper function opens a scanner, get the results and close the scanner.
   *
   * @return between zero and numRows TResults
   */
  list<TResult> getScannerResults(
    /** the table to get the Scanner for */
    1: required binary table,

    /** the scan object to get a Scanner for */
    2: required TScan tscan,

    /** number of rows to return */
    3: i32 numRows = 1
  ) throws (
    1: TIOError io
  )

  /**
   * Given a table and a row get the location of the region that
   * would contain the given row key.
   *
   * reload = true means the cache will be cleared and the location
   * will be fetched from meta.
   */
  THRegionLocation getRegionLocation(
    1: required binary table,
    2: required binary row,
    3: bool reload,
  ) throws (
    1: TIOError io
  )

  /**
   * Get all of the region locations for a given table.
   **/
  list<THRegionLocation> getAllRegionLocations(
    1: required binary table,
  ) throws (
    1: TIOError io
  )

  /**
   * Atomically checks if a row/family/qualifier value matches the expected
   * value. If it does, it mutates the row.
   *
   * @return true if the row was mutated, false otherwise
   */
  bool checkAndMutate(
    /** to check in and delete from */
    1: required binary table,

    /** row to check */
    2: required binary row,

    /** column family to check */
    3: required binary family,

    /** column qualifier to check */
    4: required binary qualifier,

    /** comparison to make on the value */
    5: required TCompareOperator compareOperator,

    /** the expected value to be compared against, if not provided the
        check is for the non-existence of the column in question */
    6: binary value,

    /** row mutations to execute if the value matches */
    7: required TRowMutations rowMutations
  ) throws (1: TIOError io)

  /**
  * Get a table descriptor.
  * @return the TableDescriptor of the giving tablename
  **/
  TTableDescriptor getTableDescriptor(
    /** the tablename of the table to get tableDescriptor*/
    1: required TTableName table
  ) throws (1: TIOError io)

  /**
  * Get table descriptors of tables.
  * @return the TableDescriptor of the giving tablename
  **/
  list<TTableDescriptor> getTableDescriptors(
    /** the tablename list of the tables to get tableDescriptor*/
    1: required list<TTableName> tables
  ) throws (1: TIOError io)

  /**
  *
  * @return true if table exists already, false if not
  **/
  bool tableExists(
    /** the tablename of the tables to check*/
    1: TTableName tableName
  ) throws (1: TIOError io)

  /**
  * Get table descriptors of tables that match the given pattern
  * @return the tableDescriptors of the matching table
  **/
  list<TTableDescriptor> getTableDescriptorsByPattern(
    /** The regular expression to match against */
    1: optional string regex
    /** set to false if match only against userspace tables */
    2: required bool includeSysTables
  ) throws (1: TIOError io)

  /**
  * Get table descriptors of tables in the given namespace
  * @return the tableDescriptors in the namespce
  **/
  list<TTableDescriptor> getTableDescriptorsByNamespace(
    /** The namesapce's name */
    1: required string name
  ) throws (1: TIOError io)

  /**
  * Get table names of tables that match the given pattern
  * @return the table names of the matching table
  **/
  list<TTableName> getTableNamesByPattern(
    /** The regular expression to match against */
    1: optional string regex
    /** set to false if match only against userspace tables */
    2: required bool includeSysTables
  ) throws (1: TIOError io)

  /**
  * Get table names of tables in the given namespace
  * @return the table names of the matching table
  **/
  list<TTableName> getTableNamesByNamespace(
    /** The namesapce's name */
    1: required string name
  ) throws (1: TIOError io)

  /**
  * Creates a new table with an initial set of empty regions defined by the specified split keys.
  * The total number of regions created will be the number of split keys plus one. Synchronous
  * operation.
  **/
  void createTable(
    /** table descriptor for table */
    1: required TTableDescriptor desc
    /** rray of split keys for the initial regions of the table */
    2: optional list<binary> splitKeys
  ) throws (1: TIOError io)

  /**
  * Deletes a table. Synchronous operation.
  **/
  void deleteTable(
    /** the tablename to delete */
    1: required TTableName tableName
  ) throws (1: TIOError io)

  /**
  * Truncate a table. Synchronous operation.
  **/
  void truncateTable(
    /** the tablename to truncate */
    1: required TTableName tableName
    /** whether to  preserve previous splits*/
    2: required bool preserveSplits
  ) throws (1: TIOError io)

  /**
  * Enalbe a table
  **/
  void enableTable(
    /** the tablename to enable */
    1: required TTableName tableName
  ) throws (1: TIOError io)

  /**
  * Disable a table
  **/
  void disableTable(
    /** the tablename to disable */
    1: required TTableName tableName
  ) throws (1: TIOError io)

  /**
  *
  * @return true if table is enabled, false if not
  **/
  bool isTableEnabled(
    /** the tablename to check */
    1: required TTableName tableName
  ) throws (1: TIOError io)

 /**
  *
  * @return true if table is disabled, false if not
  **/
  bool isTableDisabled(
    /** the tablename to check */
    1: required TTableName tableName
  ) throws (1: TIOError io)

 /**
  *
  * @return true if table is available, false if not
  **/
  bool isTableAvailable(
    /** the tablename to check */
    1: required TTableName tableName
  ) throws (1: TIOError io)

  /**
  * Add a column family to an existing table. Synchronous operation.
  **/
  void addColumnFamily(
    /** the tablename to add column family to */
    1: required TTableName tableName
    /** column family descriptor of column family to be added */
    2: required TColumnFamilyDescriptor column
  ) throws (1: TIOError io)

  /**
  * Delete a column family from a table. Synchronous operation.
  **/
  void deleteColumnFamily(
    /** the tablename to delete column family from */
    1: required TTableName tableName
    /** name of column family to be deleted */
    2: required binary column
  ) throws (1: TIOError io)

  /**
  * Modify an existing column family on a table. Synchronous operation.
  **/
  void modifyColumnFamily(
     /** the tablename to modify column family */
    1: required TTableName tableName
    /** column family descriptor of column family to be modified */
    2: required TColumnFamilyDescriptor column
  ) throws (1: TIOError io)

  /**
  * Modify an existing table
  **/
  void modifyTable(
    /** the descriptor of the table to modify */
    1: required TTableDescriptor desc
  ) throws (1: TIOError io)

  /**
  * Create a new namespace. Blocks until namespace has been successfully created or an exception is
  * thrown
  **/
  void createNamespace(
    /** descriptor which describes the new namespace */
    1: required TNamespaceDescriptor namespaceDesc
  ) throws (1: TIOError io)

  /**
  * Modify an existing namespace.  Blocks until namespace has been successfully modified or an
  * exception is thrown
  **/
  void modifyNamespace(
    /** descriptor which describes the new namespace */
    1: required TNamespaceDescriptor namespaceDesc
  ) throws (1: TIOError io)

  /**
  * Delete an existing namespace. Only empty namespaces (no tables) can be removed.
  * Blocks until namespace has been successfully deleted or an
  * exception is thrown.
  **/
  void deleteNamespace(
    /** namespace name */
    1: required string name
  ) throws (1: TIOError io)

  /**
  *  Get a namespace descriptor by name.
  *  @retrun the descriptor
  **/
  TNamespaceDescriptor getNamespaceDescriptor(
    /** name of namespace descriptor */
    1: required string name
  ) throws (1: TIOError io)

  /**
  * @return all namespaces
  **/
  list<TNamespaceDescriptor> listNamespaceDescriptors(
  ) throws (1: TIOError io)

  /**
  * @return all namespace names
  **/
  list<string> listNamespaces(
  ) throws (1: TIOError io)

  /**
   * Get the type of this thrift server.
   *
   * @return the type of this thrift server
   */
  TThriftServerType getThriftServerType()

  /**
   * Returns the cluster ID for this cluster.
   */
  string getClusterId()

  /**
   * Retrieves online slow RPC logs from the provided list of
   * RegionServers
   *
   * @return online slowlog response list
   * @throws TIOError if a remote or network exception occurs
   */
  list<TOnlineLogRecord> getSlowLogResponses(
   /** @param serverNames Server names to get slowlog responses from */
    1: set<TServerName> serverNames
   /** @param logQueryFilter filter to be used if provided */
    2: TLogQueryFilter logQueryFilter
  ) throws (1: TIOError io)

  /**
   * Clears online slow/large RPC logs from the provided list of
   * RegionServers
   *
   * @return List of booleans representing if online slowlog response buffer is cleaned
   *   from each RegionServer
   * @throws TIOError if a remote or network exception occurs
   */
  list<bool> clearSlowLogResponses(
    /** @param serverNames Set of Server names to clean slowlog responses from */
    1: set<TServerName> serverNames
  ) throws (1: TIOError io)

  /**
   *  Grant permissions in table or namespace level.
   */
  bool grant(
    1: required TAccessControlEntity info
  ) throws (1: TIOError io)

  /**
   *  Revoke permissions in table or namespace level.
   */
   bool revoke(
    1: required TAccessControlEntity info
   ) throws (1: TIOError io)
}
