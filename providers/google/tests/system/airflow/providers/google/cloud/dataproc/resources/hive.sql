/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
*/

-- MySQL dump 10.13  Distrib 5.7.35, for Linux (x86_64)
--
-- Host: localhost    Database: hive-7e773be5
-- ------------------------------------------------------
-- Server version 5.7.34-google-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Current Database: `hive-7e773be5`
--

CREATE DATABASE /*!32312 IF NOT EXISTS*/ `hive-7e773be5` /*!40100 DEFAULT CHARACTER SET utf8 */;

USE `hive-7e773be5`;

--
-- Table structure for table `AUX_TABLE`
--

DROP TABLE IF EXISTS `AUX_TABLE`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `AUX_TABLE` (
  `MT_KEY1` varchar(128) NOT NULL,
  `MT_KEY2` bigint(20) NOT NULL,
  `MT_COMMENT` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`MT_KEY1`,`MT_KEY2`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `AUX_TABLE`
--

LOCK TABLES `AUX_TABLE` WRITE;
/*!40000 ALTER TABLE `AUX_TABLE` DISABLE KEYS */;
INSERT INTO `AUX_TABLE` VALUES ('Cleaner',0,NULL),('CompactionHistory',0,NULL),('HouseKeeper',0,NULL),('Initiator',0,NULL),('MaterializationRebuild',0,NULL),('WriteSetCleaner',0,NULL);
/*!40000 ALTER TABLE `AUX_TABLE` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `BUCKETING_COLS`
--

DROP TABLE IF EXISTS `BUCKETING_COLS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `BUCKETING_COLS` (
  `SD_ID` bigint(20) NOT NULL,
  `BUCKET_COL_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `INTEGER_IDX` int(11) NOT NULL,
  PRIMARY KEY (`SD_ID`,`INTEGER_IDX`),
  KEY `BUCKETING_COLS_N49` (`SD_ID`),
  CONSTRAINT `BUCKETING_COLS_FK1` FOREIGN KEY (`SD_ID`) REFERENCES `SDS` (`SD_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `BUCKETING_COLS`
--

LOCK TABLES `BUCKETING_COLS` WRITE;
/*!40000 ALTER TABLE `BUCKETING_COLS` DISABLE KEYS */;
/*!40000 ALTER TABLE `BUCKETING_COLS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `CDS`
--

DROP TABLE IF EXISTS `CDS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `CDS` (
  `CD_ID` bigint(20) NOT NULL,
  PRIMARY KEY (`CD_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `CDS`
--

LOCK TABLES `CDS` WRITE;
/*!40000 ALTER TABLE `CDS` DISABLE KEYS */;
/*!40000 ALTER TABLE `CDS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `COLUMNS_V2`
--

DROP TABLE IF EXISTS `COLUMNS_V2`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `COLUMNS_V2` (
  `CD_ID` bigint(20) NOT NULL,
  `COMMENT` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `COLUMN_NAME` varchar(767) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `TYPE_NAME` mediumtext,
  `INTEGER_IDX` int(11) NOT NULL,
  PRIMARY KEY (`CD_ID`,`COLUMN_NAME`),
  KEY `COLUMNS_V2_N49` (`CD_ID`),
  CONSTRAINT `COLUMNS_V2_FK1` FOREIGN KEY (`CD_ID`) REFERENCES `CDS` (`CD_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `COLUMNS_V2`
--

LOCK TABLES `COLUMNS_V2` WRITE;
/*!40000 ALTER TABLE `COLUMNS_V2` DISABLE KEYS */;
/*!40000 ALTER TABLE `COLUMNS_V2` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `COMPACTION_QUEUE`
--

DROP TABLE IF EXISTS `COMPACTION_QUEUE`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `COMPACTION_QUEUE` (
  `CQ_ID` bigint(20) NOT NULL,
  `CQ_DATABASE` varchar(128) NOT NULL,
  `CQ_TABLE` varchar(128) NOT NULL,
  `CQ_PARTITION` varchar(767) DEFAULT NULL,
  `CQ_STATE` char(1) NOT NULL,
  `CQ_TYPE` char(1) NOT NULL,
  `CQ_TBLPROPERTIES` varchar(2048) DEFAULT NULL,
  `CQ_WORKER_ID` varchar(128) DEFAULT NULL,
  `CQ_START` bigint(20) DEFAULT NULL,
  `CQ_RUN_AS` varchar(128) DEFAULT NULL,
  `CQ_HIGHEST_WRITE_ID` bigint(20) DEFAULT NULL,
  `CQ_META_INFO` varbinary(2048) DEFAULT NULL,
  `CQ_HADOOP_JOB_ID` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`CQ_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `COMPACTION_QUEUE`
--

LOCK TABLES `COMPACTION_QUEUE` WRITE;
/*!40000 ALTER TABLE `COMPACTION_QUEUE` DISABLE KEYS */;
/*!40000 ALTER TABLE `COMPACTION_QUEUE` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `COMPLETED_COMPACTIONS`
--

DROP TABLE IF EXISTS `COMPLETED_COMPACTIONS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `COMPLETED_COMPACTIONS` (
  `CC_ID` bigint(20) NOT NULL,
  `CC_DATABASE` varchar(128) NOT NULL,
  `CC_TABLE` varchar(128) NOT NULL,
  `CC_PARTITION` varchar(767) DEFAULT NULL,
  `CC_STATE` char(1) NOT NULL,
  `CC_TYPE` char(1) NOT NULL,
  `CC_TBLPROPERTIES` varchar(2048) DEFAULT NULL,
  `CC_WORKER_ID` varchar(128) DEFAULT NULL,
  `CC_START` bigint(20) DEFAULT NULL,
  `CC_END` bigint(20) DEFAULT NULL,
  `CC_RUN_AS` varchar(128) DEFAULT NULL,
  `CC_HIGHEST_WRITE_ID` bigint(20) DEFAULT NULL,
  `CC_META_INFO` varbinary(2048) DEFAULT NULL,
  `CC_HADOOP_JOB_ID` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`CC_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `COMPLETED_COMPACTIONS`
--

LOCK TABLES `COMPLETED_COMPACTIONS` WRITE;
/*!40000 ALTER TABLE `COMPLETED_COMPACTIONS` DISABLE KEYS */;
/*!40000 ALTER TABLE `COMPLETED_COMPACTIONS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `COMPLETED_TXN_COMPONENTS`
--

DROP TABLE IF EXISTS `COMPLETED_TXN_COMPONENTS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `COMPLETED_TXN_COMPONENTS` (
  `CTC_TXNID` bigint(20) NOT NULL,
  `CTC_DATABASE` varchar(128) NOT NULL,
  `CTC_TABLE` varchar(256) DEFAULT NULL,
  `CTC_PARTITION` varchar(767) DEFAULT NULL,
  `CTC_TIMESTAMP` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `CTC_WRITEID` bigint(20) DEFAULT NULL,
  `CTC_UPDATE_DELETE` char(1) NOT NULL,
  KEY `COMPLETED_TXN_COMPONENTS_IDX` (`CTC_DATABASE`,`CTC_TABLE`,`CTC_PARTITION`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `COMPLETED_TXN_COMPONENTS`
--

LOCK TABLES `COMPLETED_TXN_COMPONENTS` WRITE;
/*!40000 ALTER TABLE `COMPLETED_TXN_COMPONENTS` DISABLE KEYS */;
/*!40000 ALTER TABLE `COMPLETED_TXN_COMPONENTS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `CTLGS`
--

DROP TABLE IF EXISTS `CTLGS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `CTLGS` (
  `CTLG_ID` bigint(20) NOT NULL,
  `NAME` varchar(256) DEFAULT NULL,
  `DESC` varchar(4000) DEFAULT NULL,
  `LOCATION_URI` varchar(4000) NOT NULL,
  PRIMARY KEY (`CTLG_ID`),
  UNIQUE KEY `UNIQUE_CATALOG` (`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `CTLGS`
--

LOCK TABLES `CTLGS` WRITE;
/*!40000 ALTER TABLE `CTLGS` DISABLE KEYS */;
INSERT INTO `CTLGS` VALUES (1,'hive','Default catalog for Hive','gs://gcs-bucket-wj-dm-service-1-022faf65-f2f7-4f8c-b720-5aab8f43e94d/hive-warehouse');
/*!40000 ALTER TABLE `CTLGS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `DATABASE_PARAMS`
--

DROP TABLE IF EXISTS `DATABASE_PARAMS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `DATABASE_PARAMS` (
  `DB_ID` bigint(20) NOT NULL,
  `PARAM_KEY` varchar(180) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `PARAM_VALUE` varchar(4000) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  PRIMARY KEY (`DB_ID`,`PARAM_KEY`),
  KEY `DATABASE_PARAMS_N49` (`DB_ID`),
  CONSTRAINT `DATABASE_PARAMS_FK1` FOREIGN KEY (`DB_ID`) REFERENCES `DBS` (`DB_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `DATABASE_PARAMS`
--

LOCK TABLES `DATABASE_PARAMS` WRITE;
/*!40000 ALTER TABLE `DATABASE_PARAMS` DISABLE KEYS */;
INSERT INTO `DATABASE_PARAMS` VALUES (1,'transient_lastDdlTime','1634563201');
/*!40000 ALTER TABLE `DATABASE_PARAMS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `DBS`
--

DROP TABLE IF EXISTS `DBS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `DBS` (
  `DB_ID` bigint(20) NOT NULL,
  `DESC` varchar(4000) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `DB_LOCATION_URI` varchar(4000) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `OWNER_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `OWNER_TYPE` varchar(10) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `CTLG_NAME` varchar(256) NOT NULL DEFAULT 'hive',
  PRIMARY KEY (`DB_ID`),
  UNIQUE KEY `UNIQUE_DATABASE` (`NAME`,`CTLG_NAME`),
  KEY `CTLG_FK1` (`CTLG_NAME`),
  CONSTRAINT `CTLG_FK1` FOREIGN KEY (`CTLG_NAME`) REFERENCES `CTLGS` (`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `DBS`
--

LOCK TABLES `DBS` WRITE;
/*!40000 ALTER TABLE `DBS` DISABLE KEYS */;
INSERT INTO `DBS` VALUES (1,'Default Hive database','gs://gcs-bucket-wj-dm-service-1-022faf65-f2f7-4f8c-b720-5aab8f43e94d/hive-warehouse','default','public','ROLE','hive');
/*!40000 ALTER TABLE `DBS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `DB_PRIVS`
--

DROP TABLE IF EXISTS `DB_PRIVS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `DB_PRIVS` (
  `DB_GRANT_ID` bigint(20) NOT NULL,
  `CREATE_TIME` int(11) NOT NULL,
  `DB_ID` bigint(20) DEFAULT NULL,
  `GRANT_OPTION` smallint(6) NOT NULL,
  `GRANTOR` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `GRANTOR_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `PRINCIPAL_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `PRINCIPAL_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `DB_PRIV` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `AUTHORIZER` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  PRIMARY KEY (`DB_GRANT_ID`),
  UNIQUE KEY `DBPRIVILEGEINDEX` (`AUTHORIZER`,`DB_ID`,`PRINCIPAL_NAME`,`PRINCIPAL_TYPE`,`DB_PRIV`,`GRANTOR`,`GRANTOR_TYPE`),
  KEY `DB_PRIVS_N49` (`DB_ID`),
  CONSTRAINT `DB_PRIVS_FK1` FOREIGN KEY (`DB_ID`) REFERENCES `DBS` (`DB_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `DB_PRIVS`
--

LOCK TABLES `DB_PRIVS` WRITE;
/*!40000 ALTER TABLE `DB_PRIVS` DISABLE KEYS */;
/*!40000 ALTER TABLE `DB_PRIVS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `DELEGATION_TOKENS`
--

DROP TABLE IF EXISTS `DELEGATION_TOKENS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `DELEGATION_TOKENS` (
  `TOKEN_IDENT` varchar(767) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `TOKEN` varchar(767) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  PRIMARY KEY (`TOKEN_IDENT`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `DELEGATION_TOKENS`
--

LOCK TABLES `DELEGATION_TOKENS` WRITE;
/*!40000 ALTER TABLE `DELEGATION_TOKENS` DISABLE KEYS */;
/*!40000 ALTER TABLE `DELEGATION_TOKENS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `FUNCS`
--

DROP TABLE IF EXISTS `FUNCS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `FUNCS` (
  `FUNC_ID` bigint(20) NOT NULL,
  `CLASS_NAME` varchar(4000) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `CREATE_TIME` int(11) NOT NULL,
  `DB_ID` bigint(20) DEFAULT NULL,
  `FUNC_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `FUNC_TYPE` int(11) NOT NULL,
  `OWNER_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `OWNER_TYPE` varchar(10) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  PRIMARY KEY (`FUNC_ID`),
  UNIQUE KEY `UNIQUEFUNCTION` (`FUNC_NAME`,`DB_ID`),
  KEY `FUNCS_N49` (`DB_ID`),
  CONSTRAINT `FUNCS_FK1` FOREIGN KEY (`DB_ID`) REFERENCES `DBS` (`DB_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `FUNCS`
--

LOCK TABLES `FUNCS` WRITE;
/*!40000 ALTER TABLE `FUNCS` DISABLE KEYS */;
/*!40000 ALTER TABLE `FUNCS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `FUNC_RU`
--

DROP TABLE IF EXISTS `FUNC_RU`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `FUNC_RU` (
  `FUNC_ID` bigint(20) NOT NULL,
  `RESOURCE_TYPE` int(11) NOT NULL,
  `RESOURCE_URI` varchar(4000) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `INTEGER_IDX` int(11) NOT NULL,
  PRIMARY KEY (`FUNC_ID`,`INTEGER_IDX`),
  CONSTRAINT `FUNC_RU_FK1` FOREIGN KEY (`FUNC_ID`) REFERENCES `FUNCS` (`FUNC_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `FUNC_RU`
--

LOCK TABLES `FUNC_RU` WRITE;
/*!40000 ALTER TABLE `FUNC_RU` DISABLE KEYS */;
/*!40000 ALTER TABLE `FUNC_RU` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `GLOBAL_PRIVS`
--

DROP TABLE IF EXISTS `GLOBAL_PRIVS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `GLOBAL_PRIVS` (
  `USER_GRANT_ID` bigint(20) NOT NULL,
  `CREATE_TIME` int(11) NOT NULL,
  `GRANT_OPTION` smallint(6) NOT NULL,
  `GRANTOR` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `GRANTOR_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `PRINCIPAL_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `PRINCIPAL_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `USER_PRIV` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `AUTHORIZER` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  PRIMARY KEY (`USER_GRANT_ID`),
  UNIQUE KEY `GLOBALPRIVILEGEINDEX` (`AUTHORIZER`,`PRINCIPAL_NAME`,`PRINCIPAL_TYPE`,`USER_PRIV`,`GRANTOR`,`GRANTOR_TYPE`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `GLOBAL_PRIVS`
--

LOCK TABLES `GLOBAL_PRIVS` WRITE;
/*!40000 ALTER TABLE `GLOBAL_PRIVS` DISABLE KEYS */;
INSERT INTO `GLOBAL_PRIVS` VALUES (1,1634563201,1,'admin','ROLE','admin','ROLE','All','SQL');
/*!40000 ALTER TABLE `GLOBAL_PRIVS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `HIVE_LOCKS`
--

DROP TABLE IF EXISTS `HIVE_LOCKS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `HIVE_LOCKS` (
  `HL_LOCK_EXT_ID` bigint(20) NOT NULL,
  `HL_LOCK_INT_ID` bigint(20) NOT NULL,
  `HL_TXNID` bigint(20) NOT NULL,
  `HL_DB` varchar(128) NOT NULL,
  `HL_TABLE` varchar(128) DEFAULT NULL,
  `HL_PARTITION` varchar(767) DEFAULT NULL,
  `HL_LOCK_STATE` char(1) NOT NULL,
  `HL_LOCK_TYPE` char(1) NOT NULL,
  `HL_LAST_HEARTBEAT` bigint(20) NOT NULL,
  `HL_ACQUIRED_AT` bigint(20) DEFAULT NULL,
  `HL_USER` varchar(128) NOT NULL,
  `HL_HOST` varchar(128) NOT NULL,
  `HL_HEARTBEAT_COUNT` int(11) DEFAULT NULL,
  `HL_AGENT_INFO` varchar(128) DEFAULT NULL,
  `HL_BLOCKEDBY_EXT_ID` bigint(20) DEFAULT NULL,
  `HL_BLOCKEDBY_INT_ID` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`HL_LOCK_EXT_ID`,`HL_LOCK_INT_ID`),
  KEY `HIVE_LOCK_TXNID_INDEX` (`HL_TXNID`),
  KEY `HL_TXNID_IDX` (`HL_TXNID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `HIVE_LOCKS`
--

LOCK TABLES `HIVE_LOCKS` WRITE;
/*!40000 ALTER TABLE `HIVE_LOCKS` DISABLE KEYS */;
/*!40000 ALTER TABLE `HIVE_LOCKS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `IDXS`
--

DROP TABLE IF EXISTS `IDXS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `IDXS` (
  `INDEX_ID` bigint(20) NOT NULL,
  `CREATE_TIME` int(11) NOT NULL,
  `DEFERRED_REBUILD` bit(1) NOT NULL,
  `INDEX_HANDLER_CLASS` varchar(4000) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `INDEX_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `INDEX_TBL_ID` bigint(20) DEFAULT NULL,
  `LAST_ACCESS_TIME` int(11) NOT NULL,
  `ORIG_TBL_ID` bigint(20) DEFAULT NULL,
  `SD_ID` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`INDEX_ID`),
  UNIQUE KEY `UNIQUEINDEX` (`INDEX_NAME`,`ORIG_TBL_ID`),
  KEY `IDXS_N51` (`SD_ID`),
  KEY `IDXS_N50` (`INDEX_TBL_ID`),
  KEY `IDXS_N49` (`ORIG_TBL_ID`),
  CONSTRAINT `IDXS_FK1` FOREIGN KEY (`ORIG_TBL_ID`) REFERENCES `TBLS` (`TBL_ID`),
  CONSTRAINT `IDXS_FK2` FOREIGN KEY (`SD_ID`) REFERENCES `SDS` (`SD_ID`),
  CONSTRAINT `IDXS_FK3` FOREIGN KEY (`INDEX_TBL_ID`) REFERENCES `TBLS` (`TBL_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `IDXS`
--

LOCK TABLES `IDXS` WRITE;
/*!40000 ALTER TABLE `IDXS` DISABLE KEYS */;
/*!40000 ALTER TABLE `IDXS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `INDEX_PARAMS`
--

DROP TABLE IF EXISTS `INDEX_PARAMS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `INDEX_PARAMS` (
  `INDEX_ID` bigint(20) NOT NULL,
  `PARAM_KEY` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `PARAM_VALUE` varchar(4000) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  PRIMARY KEY (`INDEX_ID`,`PARAM_KEY`),
  KEY `INDEX_PARAMS_N49` (`INDEX_ID`),
  CONSTRAINT `INDEX_PARAMS_FK1` FOREIGN KEY (`INDEX_ID`) REFERENCES `IDXS` (`INDEX_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `INDEX_PARAMS`
--

LOCK TABLES `INDEX_PARAMS` WRITE;
/*!40000 ALTER TABLE `INDEX_PARAMS` DISABLE KEYS */;
/*!40000 ALTER TABLE `INDEX_PARAMS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `I_SCHEMA`
--

DROP TABLE IF EXISTS `I_SCHEMA`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `I_SCHEMA` (
  `SCHEMA_ID` bigint(20) NOT NULL,
  `SCHEMA_TYPE` int(11) NOT NULL,
  `NAME` varchar(256) DEFAULT NULL,
  `DB_ID` bigint(20) DEFAULT NULL,
  `COMPATIBILITY` int(11) NOT NULL,
  `VALIDATION_LEVEL` int(11) NOT NULL,
  `CAN_EVOLVE` bit(1) NOT NULL,
  `SCHEMA_GROUP` varchar(256) DEFAULT NULL,
  `DESCRIPTION` varchar(4000) DEFAULT NULL,
  PRIMARY KEY (`SCHEMA_ID`),
  KEY `DB_ID` (`DB_ID`),
  KEY `UNIQUE_NAME` (`NAME`),
  CONSTRAINT `I_SCHEMA_ibfk_1` FOREIGN KEY (`DB_ID`) REFERENCES `DBS` (`DB_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `I_SCHEMA`
--

LOCK TABLES `I_SCHEMA` WRITE;
/*!40000 ALTER TABLE `I_SCHEMA` DISABLE KEYS */;
/*!40000 ALTER TABLE `I_SCHEMA` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `KEY_CONSTRAINTS`
--

DROP TABLE IF EXISTS `KEY_CONSTRAINTS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `KEY_CONSTRAINTS` (
  `CHILD_CD_ID` bigint(20) DEFAULT NULL,
  `CHILD_INTEGER_IDX` int(11) DEFAULT NULL,
  `CHILD_TBL_ID` bigint(20) DEFAULT NULL,
  `PARENT_CD_ID` bigint(20) DEFAULT NULL,
  `PARENT_INTEGER_IDX` int(11) NOT NULL,
  `PARENT_TBL_ID` bigint(20) NOT NULL,
  `POSITION` bigint(20) NOT NULL,
  `CONSTRAINT_NAME` varchar(400) NOT NULL,
  `CONSTRAINT_TYPE` smallint(6) NOT NULL,
  `UPDATE_RULE` smallint(6) DEFAULT NULL,
  `DELETE_RULE` smallint(6) DEFAULT NULL,
  `ENABLE_VALIDATE_RELY` smallint(6) NOT NULL,
  `DEFAULT_VALUE` varchar(400) DEFAULT NULL,
  PRIMARY KEY (`CONSTRAINT_NAME`,`POSITION`),
  KEY `CONSTRAINTS_PARENT_TABLE_ID_INDEX` (`PARENT_TBL_ID`) USING BTREE,
  KEY `CONSTRAINTS_CONSTRAINT_TYPE_INDEX` (`CONSTRAINT_TYPE`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `KEY_CONSTRAINTS`
--

LOCK TABLES `KEY_CONSTRAINTS` WRITE;
/*!40000 ALTER TABLE `KEY_CONSTRAINTS` DISABLE KEYS */;
/*!40000 ALTER TABLE `KEY_CONSTRAINTS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `MASTER_KEYS`
--

DROP TABLE IF EXISTS `MASTER_KEYS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `MASTER_KEYS` (
  `KEY_ID` int(11) NOT NULL AUTO_INCREMENT,
  `MASTER_KEY` varchar(767) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  PRIMARY KEY (`KEY_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `MASTER_KEYS`
--

LOCK TABLES `MASTER_KEYS` WRITE;
/*!40000 ALTER TABLE `MASTER_KEYS` DISABLE KEYS */;
/*!40000 ALTER TABLE `MASTER_KEYS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `MATERIALIZATION_REBUILD_LOCKS`
--

DROP TABLE IF EXISTS `MATERIALIZATION_REBUILD_LOCKS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `MATERIALIZATION_REBUILD_LOCKS` (
  `MRL_TXN_ID` bigint(20) NOT NULL,
  `MRL_DB_NAME` varchar(128) NOT NULL,
  `MRL_TBL_NAME` varchar(256) NOT NULL,
  `MRL_LAST_HEARTBEAT` bigint(20) NOT NULL,
  PRIMARY KEY (`MRL_TXN_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `MATERIALIZATION_REBUILD_LOCKS`
--

LOCK TABLES `MATERIALIZATION_REBUILD_LOCKS` WRITE;
/*!40000 ALTER TABLE `MATERIALIZATION_REBUILD_LOCKS` DISABLE KEYS */;
/*!40000 ALTER TABLE `MATERIALIZATION_REBUILD_LOCKS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `METASTORE_DB_PROPERTIES`
--

DROP TABLE IF EXISTS `METASTORE_DB_PROPERTIES`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `METASTORE_DB_PROPERTIES` (
  `PROPERTY_KEY` varchar(255) NOT NULL,
  `PROPERTY_VALUE` varchar(1000) NOT NULL,
  `DESCRIPTION` varchar(1000) DEFAULT NULL,
  PRIMARY KEY (`PROPERTY_KEY`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `METASTORE_DB_PROPERTIES`
--

LOCK TABLES `METASTORE_DB_PROPERTIES` WRITE;
/*!40000 ALTER TABLE `METASTORE_DB_PROPERTIES` DISABLE KEYS */;
/*!40000 ALTER TABLE `METASTORE_DB_PROPERTIES` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `MIN_HISTORY_LEVEL`
--

DROP TABLE IF EXISTS `MIN_HISTORY_LEVEL`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `MIN_HISTORY_LEVEL` (
  `MHL_TXNID` bigint(20) NOT NULL,
  `MHL_MIN_OPEN_TXNID` bigint(20) NOT NULL,
  PRIMARY KEY (`MHL_TXNID`),
  KEY `MIN_HISTORY_LEVEL_IDX` (`MHL_MIN_OPEN_TXNID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `MIN_HISTORY_LEVEL`
--

LOCK TABLES `MIN_HISTORY_LEVEL` WRITE;
/*!40000 ALTER TABLE `MIN_HISTORY_LEVEL` DISABLE KEYS */;
/*!40000 ALTER TABLE `MIN_HISTORY_LEVEL` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `MV_CREATION_METADATA`
--

DROP TABLE IF EXISTS `MV_CREATION_METADATA`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `MV_CREATION_METADATA` (
  `MV_CREATION_METADATA_ID` bigint(20) NOT NULL,
  `CAT_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `DB_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `TBL_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `TXN_LIST` text,
  `MATERIALIZATION_TIME` bigint(20) NOT NULL,
  PRIMARY KEY (`MV_CREATION_METADATA_ID`),
  KEY `MV_UNIQUE_TABLE` (`TBL_NAME`,`DB_NAME`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `MV_CREATION_METADATA`
--

LOCK TABLES `MV_CREATION_METADATA` WRITE;
/*!40000 ALTER TABLE `MV_CREATION_METADATA` DISABLE KEYS */;
/*!40000 ALTER TABLE `MV_CREATION_METADATA` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `MV_TABLES_USED`
--

DROP TABLE IF EXISTS `MV_TABLES_USED`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `MV_TABLES_USED` (
  `MV_CREATION_METADATA_ID` bigint(20) NOT NULL,
  `TBL_ID` bigint(20) NOT NULL,
  KEY `MV_TABLES_USED_FK1` (`MV_CREATION_METADATA_ID`),
  KEY `MV_TABLES_USED_FK2` (`TBL_ID`),
  CONSTRAINT `MV_TABLES_USED_FK1` FOREIGN KEY (`MV_CREATION_METADATA_ID`) REFERENCES `MV_CREATION_METADATA` (`MV_CREATION_METADATA_ID`),
  CONSTRAINT `MV_TABLES_USED_FK2` FOREIGN KEY (`TBL_ID`) REFERENCES `TBLS` (`TBL_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `MV_TABLES_USED`
--

LOCK TABLES `MV_TABLES_USED` WRITE;
/*!40000 ALTER TABLE `MV_TABLES_USED` DISABLE KEYS */;
/*!40000 ALTER TABLE `MV_TABLES_USED` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `NEXT_COMPACTION_QUEUE_ID`
--

DROP TABLE IF EXISTS `NEXT_COMPACTION_QUEUE_ID`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `NEXT_COMPACTION_QUEUE_ID` (
  `NCQ_NEXT` bigint(20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `NEXT_COMPACTION_QUEUE_ID`
--

LOCK TABLES `NEXT_COMPACTION_QUEUE_ID` WRITE;
/*!40000 ALTER TABLE `NEXT_COMPACTION_QUEUE_ID` DISABLE KEYS */;
INSERT INTO `NEXT_COMPACTION_QUEUE_ID` VALUES (1);
/*!40000 ALTER TABLE `NEXT_COMPACTION_QUEUE_ID` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `NEXT_LOCK_ID`
--

DROP TABLE IF EXISTS `NEXT_LOCK_ID`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `NEXT_LOCK_ID` (
  `NL_NEXT` bigint(20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `NEXT_LOCK_ID`
--

LOCK TABLES `NEXT_LOCK_ID` WRITE;
/*!40000 ALTER TABLE `NEXT_LOCK_ID` DISABLE KEYS */;
INSERT INTO `NEXT_LOCK_ID` VALUES (1);
/*!40000 ALTER TABLE `NEXT_LOCK_ID` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `NEXT_TXN_ID`
--

DROP TABLE IF EXISTS `NEXT_TXN_ID`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `NEXT_TXN_ID` (
  `NTXN_NEXT` bigint(20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `NEXT_TXN_ID`
--

LOCK TABLES `NEXT_TXN_ID` WRITE;
/*!40000 ALTER TABLE `NEXT_TXN_ID` DISABLE KEYS */;
INSERT INTO `NEXT_TXN_ID` VALUES (1);
/*!40000 ALTER TABLE `NEXT_TXN_ID` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `NEXT_WRITE_ID`
--

DROP TABLE IF EXISTS `NEXT_WRITE_ID`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `NEXT_WRITE_ID` (
  `NWI_DATABASE` varchar(128) NOT NULL,
  `NWI_TABLE` varchar(256) NOT NULL,
  `NWI_NEXT` bigint(20) NOT NULL,
  UNIQUE KEY `NEXT_WRITE_ID_IDX` (`NWI_DATABASE`,`NWI_TABLE`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `NEXT_WRITE_ID`
--

LOCK TABLES `NEXT_WRITE_ID` WRITE;
/*!40000 ALTER TABLE `NEXT_WRITE_ID` DISABLE KEYS */;
/*!40000 ALTER TABLE `NEXT_WRITE_ID` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `NOTIFICATION_LOG`
--

DROP TABLE IF EXISTS `NOTIFICATION_LOG`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `NOTIFICATION_LOG` (
  `NL_ID` bigint(20) NOT NULL,
  `EVENT_ID` bigint(20) NOT NULL,
  `EVENT_TIME` int(11) NOT NULL,
  `EVENT_TYPE` varchar(32) NOT NULL,
  `CAT_NAME` varchar(256) DEFAULT NULL,
  `DB_NAME` varchar(128) DEFAULT NULL,
  `TBL_NAME` varchar(256) DEFAULT NULL,
  `MESSAGE` longtext,
  `MESSAGE_FORMAT` varchar(16) DEFAULT NULL,
  PRIMARY KEY (`NL_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `NOTIFICATION_LOG`
--

LOCK TABLES `NOTIFICATION_LOG` WRITE;
/*!40000 ALTER TABLE `NOTIFICATION_LOG` DISABLE KEYS */;
/*!40000 ALTER TABLE `NOTIFICATION_LOG` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `NOTIFICATION_SEQUENCE`
--

DROP TABLE IF EXISTS `NOTIFICATION_SEQUENCE`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `NOTIFICATION_SEQUENCE` (
  `NNI_ID` bigint(20) NOT NULL,
  `NEXT_EVENT_ID` bigint(20) NOT NULL,
  PRIMARY KEY (`NNI_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `NOTIFICATION_SEQUENCE`
--

LOCK TABLES `NOTIFICATION_SEQUENCE` WRITE;
/*!40000 ALTER TABLE `NOTIFICATION_SEQUENCE` DISABLE KEYS */;
INSERT INTO `NOTIFICATION_SEQUENCE` VALUES (1,1);
/*!40000 ALTER TABLE `NOTIFICATION_SEQUENCE` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `NUCLEUS_TABLES`
--

DROP TABLE IF EXISTS `NUCLEUS_TABLES`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `NUCLEUS_TABLES` (
  `CLASS_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `TABLE_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `TYPE` varchar(4) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `OWNER` varchar(2) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `VERSION` varchar(20) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `INTERFACE_NAME` varchar(255) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  PRIMARY KEY (`CLASS_NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `NUCLEUS_TABLES`
--

LOCK TABLES `NUCLEUS_TABLES` WRITE;
/*!40000 ALTER TABLE `NUCLEUS_TABLES` DISABLE KEYS */;
/*!40000 ALTER TABLE `NUCLEUS_TABLES` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `PARTITIONS`
--

DROP TABLE IF EXISTS `PARTITIONS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `PARTITIONS` (
  `PART_ID` bigint(20) NOT NULL,
  `CREATE_TIME` int(11) NOT NULL,
  `LAST_ACCESS_TIME` int(11) NOT NULL,
  `PART_NAME` varchar(767) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `SD_ID` bigint(20) DEFAULT NULL,
  `TBL_ID` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`PART_ID`),
  UNIQUE KEY `UNIQUEPARTITION` (`PART_NAME`,`TBL_ID`),
  KEY `PARTITIONS_N49` (`TBL_ID`),
  KEY `PARTITIONS_N50` (`SD_ID`),
  CONSTRAINT `PARTITIONS_FK1` FOREIGN KEY (`TBL_ID`) REFERENCES `TBLS` (`TBL_ID`),
  CONSTRAINT `PARTITIONS_FK2` FOREIGN KEY (`SD_ID`) REFERENCES `SDS` (`SD_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `PARTITIONS`
--

LOCK TABLES `PARTITIONS` WRITE;
/*!40000 ALTER TABLE `PARTITIONS` DISABLE KEYS */;
/*!40000 ALTER TABLE `PARTITIONS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `PARTITION_EVENTS`
--

DROP TABLE IF EXISTS `PARTITION_EVENTS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `PARTITION_EVENTS` (
  `PART_NAME_ID` bigint(20) NOT NULL,
  `CAT_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `DB_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `EVENT_TIME` bigint(20) NOT NULL,
  `EVENT_TYPE` int(11) NOT NULL,
  `PARTITION_NAME` varchar(767) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `TBL_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  PRIMARY KEY (`PART_NAME_ID`),
  KEY `PARTITIONEVENTINDEX` (`PARTITION_NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `PARTITION_EVENTS`
--

LOCK TABLES `PARTITION_EVENTS` WRITE;
/*!40000 ALTER TABLE `PARTITION_EVENTS` DISABLE KEYS */;
/*!40000 ALTER TABLE `PARTITION_EVENTS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `PARTITION_KEYS`
--

DROP TABLE IF EXISTS `PARTITION_KEYS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `PARTITION_KEYS` (
  `TBL_ID` bigint(20) NOT NULL,
  `PKEY_COMMENT` varchar(4000) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `PKEY_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `PKEY_TYPE` varchar(767) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `INTEGER_IDX` int(11) NOT NULL,
  PRIMARY KEY (`TBL_ID`,`PKEY_NAME`),
  KEY `PARTITION_KEYS_N49` (`TBL_ID`),
  CONSTRAINT `PARTITION_KEYS_FK1` FOREIGN KEY (`TBL_ID`) REFERENCES `TBLS` (`TBL_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `PARTITION_KEYS`
--

LOCK TABLES `PARTITION_KEYS` WRITE;
/*!40000 ALTER TABLE `PARTITION_KEYS` DISABLE KEYS */;
/*!40000 ALTER TABLE `PARTITION_KEYS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `PARTITION_KEY_VALS`
--

DROP TABLE IF EXISTS `PARTITION_KEY_VALS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `PARTITION_KEY_VALS` (
  `PART_ID` bigint(20) NOT NULL,
  `PART_KEY_VAL` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `INTEGER_IDX` int(11) NOT NULL,
  PRIMARY KEY (`PART_ID`,`INTEGER_IDX`),
  KEY `PARTITION_KEY_VALS_N49` (`PART_ID`),
  CONSTRAINT `PARTITION_KEY_VALS_FK1` FOREIGN KEY (`PART_ID`) REFERENCES `PARTITIONS` (`PART_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `PARTITION_KEY_VALS`
--

LOCK TABLES `PARTITION_KEY_VALS` WRITE;
/*!40000 ALTER TABLE `PARTITION_KEY_VALS` DISABLE KEYS */;
/*!40000 ALTER TABLE `PARTITION_KEY_VALS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `PARTITION_PARAMS`
--

DROP TABLE IF EXISTS `PARTITION_PARAMS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `PARTITION_PARAMS` (
  `PART_ID` bigint(20) NOT NULL,
  `PARAM_KEY` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `PARAM_VALUE` varchar(4000) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  PRIMARY KEY (`PART_ID`,`PARAM_KEY`),
  KEY `PARTITION_PARAMS_N49` (`PART_ID`),
  CONSTRAINT `PARTITION_PARAMS_FK1` FOREIGN KEY (`PART_ID`) REFERENCES `PARTITIONS` (`PART_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `PARTITION_PARAMS`
--

LOCK TABLES `PARTITION_PARAMS` WRITE;
/*!40000 ALTER TABLE `PARTITION_PARAMS` DISABLE KEYS */;
/*!40000 ALTER TABLE `PARTITION_PARAMS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `PART_COL_PRIVS`
--

DROP TABLE IF EXISTS `PART_COL_PRIVS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `PART_COL_PRIVS` (
  `PART_COLUMN_GRANT_ID` bigint(20) NOT NULL,
  `COLUMN_NAME` varchar(767) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `CREATE_TIME` int(11) NOT NULL,
  `GRANT_OPTION` smallint(6) NOT NULL,
  `GRANTOR` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `GRANTOR_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `PART_ID` bigint(20) DEFAULT NULL,
  `PRINCIPAL_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `PRINCIPAL_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `PART_COL_PRIV` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `AUTHORIZER` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  PRIMARY KEY (`PART_COLUMN_GRANT_ID`),
  KEY `PART_COL_PRIVS_N49` (`PART_ID`),
  KEY `PARTITIONCOLUMNPRIVILEGEINDEX` (`AUTHORIZER`,`PART_ID`,`COLUMN_NAME`,`PRINCIPAL_NAME`,`PRINCIPAL_TYPE`,`PART_COL_PRIV`,`GRANTOR`,`GRANTOR_TYPE`),
  CONSTRAINT `PART_COL_PRIVS_FK1` FOREIGN KEY (`PART_ID`) REFERENCES `PARTITIONS` (`PART_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `PART_COL_PRIVS`
--

LOCK TABLES `PART_COL_PRIVS` WRITE;
/*!40000 ALTER TABLE `PART_COL_PRIVS` DISABLE KEYS */;
/*!40000 ALTER TABLE `PART_COL_PRIVS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `PART_COL_STATS`
--

DROP TABLE IF EXISTS `PART_COL_STATS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `PART_COL_STATS` (
  `CS_ID` bigint(20) NOT NULL,
  `CAT_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `DB_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `TABLE_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `PARTITION_NAME` varchar(767) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `COLUMN_NAME` varchar(767) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `COLUMN_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `PART_ID` bigint(20) NOT NULL,
  `LONG_LOW_VALUE` bigint(20) DEFAULT NULL,
  `LONG_HIGH_VALUE` bigint(20) DEFAULT NULL,
  `DOUBLE_HIGH_VALUE` double(53,4) DEFAULT NULL,
  `DOUBLE_LOW_VALUE` double(53,4) DEFAULT NULL,
  `BIG_DECIMAL_LOW_VALUE` varchar(4000) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `BIG_DECIMAL_HIGH_VALUE` varchar(4000) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `NUM_NULLS` bigint(20) NOT NULL,
  `NUM_DISTINCTS` bigint(20) DEFAULT NULL,
  `BIT_VECTOR` blob,
  `AVG_COL_LEN` double(53,4) DEFAULT NULL,
  `MAX_COL_LEN` bigint(20) DEFAULT NULL,
  `NUM_TRUES` bigint(20) DEFAULT NULL,
  `NUM_FALSES` bigint(20) DEFAULT NULL,
  `LAST_ANALYZED` bigint(20) NOT NULL,
  PRIMARY KEY (`CS_ID`),
  KEY `PART_COL_STATS_FK` (`PART_ID`),
  KEY `PCS_STATS_IDX` (`CAT_NAME`,`DB_NAME`,`TABLE_NAME`,`COLUMN_NAME`,`PARTITION_NAME`) USING BTREE,
  CONSTRAINT `PART_COL_STATS_FK` FOREIGN KEY (`PART_ID`) REFERENCES `PARTITIONS` (`PART_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `PART_COL_STATS`
--

LOCK TABLES `PART_COL_STATS` WRITE;
/*!40000 ALTER TABLE `PART_COL_STATS` DISABLE KEYS */;
/*!40000 ALTER TABLE `PART_COL_STATS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `PART_PRIVS`
--

DROP TABLE IF EXISTS `PART_PRIVS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `PART_PRIVS` (
  `PART_GRANT_ID` bigint(20) NOT NULL,
  `CREATE_TIME` int(11) NOT NULL,
  `GRANT_OPTION` smallint(6) NOT NULL,
  `GRANTOR` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `GRANTOR_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `PART_ID` bigint(20) DEFAULT NULL,
  `PRINCIPAL_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `PRINCIPAL_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `PART_PRIV` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `AUTHORIZER` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  PRIMARY KEY (`PART_GRANT_ID`),
  KEY `PARTPRIVILEGEINDEX` (`AUTHORIZER`,`PART_ID`,`PRINCIPAL_NAME`,`PRINCIPAL_TYPE`,`PART_PRIV`,`GRANTOR`,`GRANTOR_TYPE`),
  KEY `PART_PRIVS_N49` (`PART_ID`),
  CONSTRAINT `PART_PRIVS_FK1` FOREIGN KEY (`PART_ID`) REFERENCES `PARTITIONS` (`PART_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `PART_PRIVS`
--

LOCK TABLES `PART_PRIVS` WRITE;
/*!40000 ALTER TABLE `PART_PRIVS` DISABLE KEYS */;
/*!40000 ALTER TABLE `PART_PRIVS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `REPL_TXN_MAP`
--

DROP TABLE IF EXISTS `REPL_TXN_MAP`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `REPL_TXN_MAP` (
  `RTM_REPL_POLICY` varchar(256) NOT NULL,
  `RTM_SRC_TXN_ID` bigint(20) NOT NULL,
  `RTM_TARGET_TXN_ID` bigint(20) NOT NULL,
  PRIMARY KEY (`RTM_REPL_POLICY`,`RTM_SRC_TXN_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `REPL_TXN_MAP`
--

LOCK TABLES `REPL_TXN_MAP` WRITE;
/*!40000 ALTER TABLE `REPL_TXN_MAP` DISABLE KEYS */;
/*!40000 ALTER TABLE `REPL_TXN_MAP` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ROLES`
--

DROP TABLE IF EXISTS `ROLES`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ROLES` (
  `ROLE_ID` bigint(20) NOT NULL,
  `CREATE_TIME` int(11) NOT NULL,
  `OWNER_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `ROLE_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  PRIMARY KEY (`ROLE_ID`),
  UNIQUE KEY `ROLEENTITYINDEX` (`ROLE_NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ROLES`
--

LOCK TABLES `ROLES` WRITE;
/*!40000 ALTER TABLE `ROLES` DISABLE KEYS */;
INSERT INTO `ROLES` VALUES (1,1634563201,'admin','admin'),(2,1634563201,'public','public');
/*!40000 ALTER TABLE `ROLES` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ROLE_MAP`
--

DROP TABLE IF EXISTS `ROLE_MAP`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ROLE_MAP` (
  `ROLE_GRANT_ID` bigint(20) NOT NULL,
  `ADD_TIME` int(11) NOT NULL,
  `GRANT_OPTION` smallint(6) NOT NULL,
  `GRANTOR` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `GRANTOR_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `PRINCIPAL_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `PRINCIPAL_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `ROLE_ID` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`ROLE_GRANT_ID`),
  UNIQUE KEY `USERROLEMAPINDEX` (`PRINCIPAL_NAME`,`ROLE_ID`,`GRANTOR`,`GRANTOR_TYPE`),
  KEY `ROLE_MAP_N49` (`ROLE_ID`),
  CONSTRAINT `ROLE_MAP_FK1` FOREIGN KEY (`ROLE_ID`) REFERENCES `ROLES` (`ROLE_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ROLE_MAP`
--

LOCK TABLES `ROLE_MAP` WRITE;
/*!40000 ALTER TABLE `ROLE_MAP` DISABLE KEYS */;
/*!40000 ALTER TABLE `ROLE_MAP` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `RUNTIME_STATS`
--

DROP TABLE IF EXISTS `RUNTIME_STATS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `RUNTIME_STATS` (
  `RS_ID` bigint(20) NOT NULL,
  `CREATE_TIME` bigint(20) NOT NULL,
  `WEIGHT` bigint(20) NOT NULL,
  `PAYLOAD` blob,
  PRIMARY KEY (`RS_ID`),
  KEY `IDX_RUNTIME_STATS_CREATE_TIME` (`CREATE_TIME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `RUNTIME_STATS`
--

LOCK TABLES `RUNTIME_STATS` WRITE;
/*!40000 ALTER TABLE `RUNTIME_STATS` DISABLE KEYS */;
/*!40000 ALTER TABLE `RUNTIME_STATS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SCHEMA_VERSION`
--

DROP TABLE IF EXISTS `SCHEMA_VERSION`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SCHEMA_VERSION` (
  `SCHEMA_VERSION_ID` bigint(20) NOT NULL,
  `SCHEMA_ID` bigint(20) DEFAULT NULL,
  `VERSION` int(11) NOT NULL,
  `CREATED_AT` bigint(20) NOT NULL,
  `CD_ID` bigint(20) DEFAULT NULL,
  `STATE` int(11) NOT NULL,
  `DESCRIPTION` varchar(4000) DEFAULT NULL,
  `SCHEMA_TEXT` mediumtext,
  `FINGERPRINT` varchar(256) DEFAULT NULL,
  `SCHEMA_VERSION_NAME` varchar(256) DEFAULT NULL,
  `SERDE_ID` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`SCHEMA_VERSION_ID`),
  KEY `CD_ID` (`CD_ID`),
  KEY `SERDE_ID` (`SERDE_ID`),
  KEY `UNIQUE_VERSION` (`SCHEMA_ID`,`VERSION`),
  CONSTRAINT `SCHEMA_VERSION_ibfk_1` FOREIGN KEY (`SCHEMA_ID`) REFERENCES `I_SCHEMA` (`SCHEMA_ID`),
  CONSTRAINT `SCHEMA_VERSION_ibfk_2` FOREIGN KEY (`CD_ID`) REFERENCES `CDS` (`CD_ID`),
  CONSTRAINT `SCHEMA_VERSION_ibfk_3` FOREIGN KEY (`SERDE_ID`) REFERENCES `SERDES` (`SERDE_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SCHEMA_VERSION`
--

LOCK TABLES `SCHEMA_VERSION` WRITE;
/*!40000 ALTER TABLE `SCHEMA_VERSION` DISABLE KEYS */;
/*!40000 ALTER TABLE `SCHEMA_VERSION` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SDS`
--

DROP TABLE IF EXISTS `SDS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SDS` (
  `SD_ID` bigint(20) NOT NULL,
  `CD_ID` bigint(20) DEFAULT NULL,
  `INPUT_FORMAT` varchar(4000) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `IS_COMPRESSED` bit(1) NOT NULL,
  `IS_STOREDASSUBDIRECTORIES` bit(1) NOT NULL,
  `LOCATION` varchar(4000) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `NUM_BUCKETS` int(11) NOT NULL,
  `OUTPUT_FORMAT` varchar(4000) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `SERDE_ID` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`SD_ID`),
  KEY `SDS_N49` (`SERDE_ID`),
  KEY `SDS_N50` (`CD_ID`),
  CONSTRAINT `SDS_FK1` FOREIGN KEY (`SERDE_ID`) REFERENCES `SERDES` (`SERDE_ID`),
  CONSTRAINT `SDS_FK2` FOREIGN KEY (`CD_ID`) REFERENCES `CDS` (`CD_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SDS`
--

LOCK TABLES `SDS` WRITE;
/*!40000 ALTER TABLE `SDS` DISABLE KEYS */;
/*!40000 ALTER TABLE `SDS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SD_PARAMS`
--

DROP TABLE IF EXISTS `SD_PARAMS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SD_PARAMS` (
  `SD_ID` bigint(20) NOT NULL,
  `PARAM_KEY` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `PARAM_VALUE` mediumtext CHARACTER SET latin1 COLLATE latin1_bin,
  PRIMARY KEY (`SD_ID`,`PARAM_KEY`),
  KEY `SD_PARAMS_N49` (`SD_ID`),
  CONSTRAINT `SD_PARAMS_FK1` FOREIGN KEY (`SD_ID`) REFERENCES `SDS` (`SD_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SD_PARAMS`
--

LOCK TABLES `SD_PARAMS` WRITE;
/*!40000 ALTER TABLE `SD_PARAMS` DISABLE KEYS */;
/*!40000 ALTER TABLE `SD_PARAMS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SEQUENCE_TABLE`
--

DROP TABLE IF EXISTS `SEQUENCE_TABLE`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SEQUENCE_TABLE` (
  `SEQUENCE_NAME` varchar(255) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `NEXT_VAL` bigint(20) NOT NULL,
  PRIMARY KEY (`SEQUENCE_NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SEQUENCE_TABLE`
--

LOCK TABLES `SEQUENCE_TABLE` WRITE;
/*!40000 ALTER TABLE `SEQUENCE_TABLE` DISABLE KEYS */;
INSERT INTO `SEQUENCE_TABLE` VALUES ('org.apache.hadoop.hive.metastore.model.MDatabase',6),('org.apache.hadoop.hive.metastore.model.MGlobalPrivilege',6),('org.apache.hadoop.hive.metastore.model.MNotificationLog',1),('org.apache.hadoop.hive.metastore.model.MRole',6);
/*!40000 ALTER TABLE `SEQUENCE_TABLE` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SERDES`
--

DROP TABLE IF EXISTS `SERDES`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SERDES` (
  `SERDE_ID` bigint(20) NOT NULL,
  `NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `SLIB` varchar(4000) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `DESCRIPTION` varchar(4000) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `SERIALIZER_CLASS` varchar(4000) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `DESERIALIZER_CLASS` varchar(4000) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `SERDE_TYPE` int(11) DEFAULT NULL,
  PRIMARY KEY (`SERDE_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SERDES`
--

LOCK TABLES `SERDES` WRITE;
/*!40000 ALTER TABLE `SERDES` DISABLE KEYS */;
/*!40000 ALTER TABLE `SERDES` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SERDE_PARAMS`
--

DROP TABLE IF EXISTS `SERDE_PARAMS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SERDE_PARAMS` (
  `SERDE_ID` bigint(20) NOT NULL,
  `PARAM_KEY` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `PARAM_VALUE` mediumtext CHARACTER SET latin1 COLLATE latin1_bin,
  PRIMARY KEY (`SERDE_ID`,`PARAM_KEY`),
  KEY `SERDE_PARAMS_N49` (`SERDE_ID`),
  CONSTRAINT `SERDE_PARAMS_FK1` FOREIGN KEY (`SERDE_ID`) REFERENCES `SERDES` (`SERDE_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SERDE_PARAMS`
--

LOCK TABLES `SERDE_PARAMS` WRITE;
/*!40000 ALTER TABLE `SERDE_PARAMS` DISABLE KEYS */;
/*!40000 ALTER TABLE `SERDE_PARAMS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SKEWED_COL_NAMES`
--

DROP TABLE IF EXISTS `SKEWED_COL_NAMES`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SKEWED_COL_NAMES` (
  `SD_ID` bigint(20) NOT NULL,
  `SKEWED_COL_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `INTEGER_IDX` int(11) NOT NULL,
  PRIMARY KEY (`SD_ID`,`INTEGER_IDX`),
  KEY `SKEWED_COL_NAMES_N49` (`SD_ID`),
  CONSTRAINT `SKEWED_COL_NAMES_FK1` FOREIGN KEY (`SD_ID`) REFERENCES `SDS` (`SD_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SKEWED_COL_NAMES`
--

LOCK TABLES `SKEWED_COL_NAMES` WRITE;
/*!40000 ALTER TABLE `SKEWED_COL_NAMES` DISABLE KEYS */;
/*!40000 ALTER TABLE `SKEWED_COL_NAMES` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SKEWED_COL_VALUE_LOC_MAP`
--

DROP TABLE IF EXISTS `SKEWED_COL_VALUE_LOC_MAP`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SKEWED_COL_VALUE_LOC_MAP` (
  `SD_ID` bigint(20) NOT NULL,
  `STRING_LIST_ID_KID` bigint(20) NOT NULL,
  `LOCATION` varchar(4000) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  PRIMARY KEY (`SD_ID`,`STRING_LIST_ID_KID`),
  KEY `SKEWED_COL_VALUE_LOC_MAP_N49` (`STRING_LIST_ID_KID`),
  KEY `SKEWED_COL_VALUE_LOC_MAP_N50` (`SD_ID`),
  CONSTRAINT `SKEWED_COL_VALUE_LOC_MAP_FK1` FOREIGN KEY (`SD_ID`) REFERENCES `SDS` (`SD_ID`),
  CONSTRAINT `SKEWED_COL_VALUE_LOC_MAP_FK2` FOREIGN KEY (`STRING_LIST_ID_KID`) REFERENCES `SKEWED_STRING_LIST` (`STRING_LIST_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SKEWED_COL_VALUE_LOC_MAP`
--

LOCK TABLES `SKEWED_COL_VALUE_LOC_MAP` WRITE;
/*!40000 ALTER TABLE `SKEWED_COL_VALUE_LOC_MAP` DISABLE KEYS */;
/*!40000 ALTER TABLE `SKEWED_COL_VALUE_LOC_MAP` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SKEWED_STRING_LIST`
--

DROP TABLE IF EXISTS `SKEWED_STRING_LIST`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SKEWED_STRING_LIST` (
  `STRING_LIST_ID` bigint(20) NOT NULL,
  PRIMARY KEY (`STRING_LIST_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SKEWED_STRING_LIST`
--

LOCK TABLES `SKEWED_STRING_LIST` WRITE;
/*!40000 ALTER TABLE `SKEWED_STRING_LIST` DISABLE KEYS */;
/*!40000 ALTER TABLE `SKEWED_STRING_LIST` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SKEWED_STRING_LIST_VALUES`
--

DROP TABLE IF EXISTS `SKEWED_STRING_LIST_VALUES`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SKEWED_STRING_LIST_VALUES` (
  `STRING_LIST_ID` bigint(20) NOT NULL,
  `STRING_LIST_VALUE` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `INTEGER_IDX` int(11) NOT NULL,
  PRIMARY KEY (`STRING_LIST_ID`,`INTEGER_IDX`),
  KEY `SKEWED_STRING_LIST_VALUES_N49` (`STRING_LIST_ID`),
  CONSTRAINT `SKEWED_STRING_LIST_VALUES_FK1` FOREIGN KEY (`STRING_LIST_ID`) REFERENCES `SKEWED_STRING_LIST` (`STRING_LIST_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SKEWED_STRING_LIST_VALUES`
--

LOCK TABLES `SKEWED_STRING_LIST_VALUES` WRITE;
/*!40000 ALTER TABLE `SKEWED_STRING_LIST_VALUES` DISABLE KEYS */;
/*!40000 ALTER TABLE `SKEWED_STRING_LIST_VALUES` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SKEWED_VALUES`
--

DROP TABLE IF EXISTS `SKEWED_VALUES`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SKEWED_VALUES` (
  `SD_ID_OID` bigint(20) NOT NULL,
  `STRING_LIST_ID_EID` bigint(20) NOT NULL,
  `INTEGER_IDX` int(11) NOT NULL,
  PRIMARY KEY (`SD_ID_OID`,`INTEGER_IDX`),
  KEY `SKEWED_VALUES_N50` (`SD_ID_OID`),
  KEY `SKEWED_VALUES_N49` (`STRING_LIST_ID_EID`),
  CONSTRAINT `SKEWED_VALUES_FK1` FOREIGN KEY (`SD_ID_OID`) REFERENCES `SDS` (`SD_ID`),
  CONSTRAINT `SKEWED_VALUES_FK2` FOREIGN KEY (`STRING_LIST_ID_EID`) REFERENCES `SKEWED_STRING_LIST` (`STRING_LIST_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SKEWED_VALUES`
--

LOCK TABLES `SKEWED_VALUES` WRITE;
/*!40000 ALTER TABLE `SKEWED_VALUES` DISABLE KEYS */;
/*!40000 ALTER TABLE `SKEWED_VALUES` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SORT_COLS`
--

DROP TABLE IF EXISTS `SORT_COLS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SORT_COLS` (
  `SD_ID` bigint(20) NOT NULL,
  `COLUMN_NAME` varchar(767) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `ORDER` int(11) NOT NULL,
  `INTEGER_IDX` int(11) NOT NULL,
  PRIMARY KEY (`SD_ID`,`INTEGER_IDX`),
  KEY `SORT_COLS_N49` (`SD_ID`),
  CONSTRAINT `SORT_COLS_FK1` FOREIGN KEY (`SD_ID`) REFERENCES `SDS` (`SD_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SORT_COLS`
--

LOCK TABLES `SORT_COLS` WRITE;
/*!40000 ALTER TABLE `SORT_COLS` DISABLE KEYS */;
/*!40000 ALTER TABLE `SORT_COLS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TABLE_PARAMS`
--

DROP TABLE IF EXISTS `TABLE_PARAMS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TABLE_PARAMS` (
  `TBL_ID` bigint(20) NOT NULL,
  `PARAM_KEY` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `PARAM_VALUE` mediumtext CHARACTER SET latin1 COLLATE latin1_bin,
  PRIMARY KEY (`TBL_ID`,`PARAM_KEY`),
  KEY `TABLE_PARAMS_N49` (`TBL_ID`),
  CONSTRAINT `TABLE_PARAMS_FK1` FOREIGN KEY (`TBL_ID`) REFERENCES `TBLS` (`TBL_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TABLE_PARAMS`
--

LOCK TABLES `TABLE_PARAMS` WRITE;
/*!40000 ALTER TABLE `TABLE_PARAMS` DISABLE KEYS */;
/*!40000 ALTER TABLE `TABLE_PARAMS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TAB_COL_STATS`
--

DROP TABLE IF EXISTS `TAB_COL_STATS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TAB_COL_STATS` (
  `CS_ID` bigint(20) NOT NULL,
  `CAT_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `DB_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `TABLE_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `COLUMN_NAME` varchar(767) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `COLUMN_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `TBL_ID` bigint(20) NOT NULL,
  `LONG_LOW_VALUE` bigint(20) DEFAULT NULL,
  `LONG_HIGH_VALUE` bigint(20) DEFAULT NULL,
  `DOUBLE_HIGH_VALUE` double(53,4) DEFAULT NULL,
  `DOUBLE_LOW_VALUE` double(53,4) DEFAULT NULL,
  `BIG_DECIMAL_LOW_VALUE` varchar(4000) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `BIG_DECIMAL_HIGH_VALUE` varchar(4000) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `NUM_NULLS` bigint(20) NOT NULL,
  `NUM_DISTINCTS` bigint(20) DEFAULT NULL,
  `BIT_VECTOR` blob,
  `AVG_COL_LEN` double(53,4) DEFAULT NULL,
  `MAX_COL_LEN` bigint(20) DEFAULT NULL,
  `NUM_TRUES` bigint(20) DEFAULT NULL,
  `NUM_FALSES` bigint(20) DEFAULT NULL,
  `LAST_ANALYZED` bigint(20) NOT NULL,
  PRIMARY KEY (`CS_ID`),
  KEY `TAB_COL_STATS_FK` (`TBL_ID`),
  KEY `TAB_COL_STATS_IDX` (`CAT_NAME`,`DB_NAME`,`TABLE_NAME`,`COLUMN_NAME`) USING BTREE,
  CONSTRAINT `TAB_COL_STATS_FK` FOREIGN KEY (`TBL_ID`) REFERENCES `TBLS` (`TBL_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TAB_COL_STATS`
--

LOCK TABLES `TAB_COL_STATS` WRITE;
/*!40000 ALTER TABLE `TAB_COL_STATS` DISABLE KEYS */;
/*!40000 ALTER TABLE `TAB_COL_STATS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TBLS`
--

DROP TABLE IF EXISTS `TBLS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TBLS` (
  `TBL_ID` bigint(20) NOT NULL,
  `CREATE_TIME` int(11) NOT NULL,
  `DB_ID` bigint(20) DEFAULT NULL,
  `LAST_ACCESS_TIME` int(11) NOT NULL,
  `OWNER` varchar(767) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `OWNER_TYPE` varchar(10) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `RETENTION` int(11) NOT NULL,
  `SD_ID` bigint(20) DEFAULT NULL,
  `TBL_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `TBL_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `VIEW_EXPANDED_TEXT` mediumtext,
  `VIEW_ORIGINAL_TEXT` mediumtext,
  `IS_REWRITE_ENABLED` bit(1) NOT NULL DEFAULT b'0',
  PRIMARY KEY (`TBL_ID`),
  UNIQUE KEY `UNIQUETABLE` (`TBL_NAME`,`DB_ID`),
  KEY `TBLS_N50` (`SD_ID`),
  KEY `TBLS_N49` (`DB_ID`),
  CONSTRAINT `TBLS_FK1` FOREIGN KEY (`SD_ID`) REFERENCES `SDS` (`SD_ID`),
  CONSTRAINT `TBLS_FK2` FOREIGN KEY (`DB_ID`) REFERENCES `DBS` (`DB_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TBLS`
--

LOCK TABLES `TBLS` WRITE;
/*!40000 ALTER TABLE `TBLS` DISABLE KEYS */;
/*!40000 ALTER TABLE `TBLS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TBL_COL_PRIVS`
--

DROP TABLE IF EXISTS `TBL_COL_PRIVS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TBL_COL_PRIVS` (
  `TBL_COLUMN_GRANT_ID` bigint(20) NOT NULL,
  `COLUMN_NAME` varchar(767) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `CREATE_TIME` int(11) NOT NULL,
  `GRANT_OPTION` smallint(6) NOT NULL,
  `GRANTOR` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `GRANTOR_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `PRINCIPAL_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `PRINCIPAL_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `TBL_COL_PRIV` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `TBL_ID` bigint(20) DEFAULT NULL,
  `AUTHORIZER` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  PRIMARY KEY (`TBL_COLUMN_GRANT_ID`),
  KEY `TABLECOLUMNPRIVILEGEINDEX` (`AUTHORIZER`,`TBL_ID`,`COLUMN_NAME`,`PRINCIPAL_NAME`,`PRINCIPAL_TYPE`,`TBL_COL_PRIV`,`GRANTOR`,`GRANTOR_TYPE`),
  KEY `TBL_COL_PRIVS_N49` (`TBL_ID`),
  CONSTRAINT `TBL_COL_PRIVS_FK1` FOREIGN KEY (`TBL_ID`) REFERENCES `TBLS` (`TBL_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TBL_COL_PRIVS`
--

LOCK TABLES `TBL_COL_PRIVS` WRITE;
/*!40000 ALTER TABLE `TBL_COL_PRIVS` DISABLE KEYS */;
/*!40000 ALTER TABLE `TBL_COL_PRIVS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TBL_PRIVS`
--

DROP TABLE IF EXISTS `TBL_PRIVS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TBL_PRIVS` (
  `TBL_GRANT_ID` bigint(20) NOT NULL,
  `CREATE_TIME` int(11) NOT NULL,
  `GRANT_OPTION` smallint(6) NOT NULL,
  `GRANTOR` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `GRANTOR_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `PRINCIPAL_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `PRINCIPAL_TYPE` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `TBL_PRIV` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `TBL_ID` bigint(20) DEFAULT NULL,
  `AUTHORIZER` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  PRIMARY KEY (`TBL_GRANT_ID`),
  KEY `TBL_PRIVS_N49` (`TBL_ID`),
  KEY `TABLEPRIVILEGEINDEX` (`AUTHORIZER`,`TBL_ID`,`PRINCIPAL_NAME`,`PRINCIPAL_TYPE`,`TBL_PRIV`,`GRANTOR`,`GRANTOR_TYPE`),
  CONSTRAINT `TBL_PRIVS_FK1` FOREIGN KEY (`TBL_ID`) REFERENCES `TBLS` (`TBL_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TBL_PRIVS`
--

LOCK TABLES `TBL_PRIVS` WRITE;
/*!40000 ALTER TABLE `TBL_PRIVS` DISABLE KEYS */;
/*!40000 ALTER TABLE `TBL_PRIVS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TXNS`
--

DROP TABLE IF EXISTS `TXNS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TXNS` (
  `TXN_ID` bigint(20) NOT NULL,
  `TXN_STATE` char(1) NOT NULL,
  `TXN_STARTED` bigint(20) NOT NULL,
  `TXN_LAST_HEARTBEAT` bigint(20) NOT NULL,
  `TXN_USER` varchar(128) NOT NULL,
  `TXN_HOST` varchar(128) NOT NULL,
  `TXN_AGENT_INFO` varchar(128) DEFAULT NULL,
  `TXN_META_INFO` varchar(128) DEFAULT NULL,
  `TXN_HEARTBEAT_COUNT` int(11) DEFAULT NULL,
  `TXN_TYPE` int(11) DEFAULT NULL,
  PRIMARY KEY (`TXN_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TXNS`
--

LOCK TABLES `TXNS` WRITE;
/*!40000 ALTER TABLE `TXNS` DISABLE KEYS */;
/*!40000 ALTER TABLE `TXNS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TXN_COMPONENTS`
--

DROP TABLE IF EXISTS `TXN_COMPONENTS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TXN_COMPONENTS` (
  `TC_TXNID` bigint(20) NOT NULL,
  `TC_DATABASE` varchar(128) NOT NULL,
  `TC_TABLE` varchar(128) DEFAULT NULL,
  `TC_PARTITION` varchar(767) DEFAULT NULL,
  `TC_OPERATION_TYPE` char(1) NOT NULL,
  `TC_WRITEID` bigint(20) DEFAULT NULL,
  KEY `TC_TXNID_INDEX` (`TC_TXNID`),
  CONSTRAINT `TXN_COMPONENTS_ibfk_1` FOREIGN KEY (`TC_TXNID`) REFERENCES `TXNS` (`TXN_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TXN_COMPONENTS`
--

LOCK TABLES `TXN_COMPONENTS` WRITE;
/*!40000 ALTER TABLE `TXN_COMPONENTS` DISABLE KEYS */;
/*!40000 ALTER TABLE `TXN_COMPONENTS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TXN_TO_WRITE_ID`
--

DROP TABLE IF EXISTS `TXN_TO_WRITE_ID`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TXN_TO_WRITE_ID` (
  `T2W_TXNID` bigint(20) NOT NULL,
  `T2W_DATABASE` varchar(128) NOT NULL,
  `T2W_TABLE` varchar(256) NOT NULL,
  `T2W_WRITEID` bigint(20) NOT NULL,
  UNIQUE KEY `TBL_TO_TXN_ID_IDX` (`T2W_DATABASE`,`T2W_TABLE`,`T2W_TXNID`),
  UNIQUE KEY `TBL_TO_WRITE_ID_IDX` (`T2W_DATABASE`,`T2W_TABLE`,`T2W_WRITEID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TXN_TO_WRITE_ID`
--

LOCK TABLES `TXN_TO_WRITE_ID` WRITE;
/*!40000 ALTER TABLE `TXN_TO_WRITE_ID` DISABLE KEYS */;
/*!40000 ALTER TABLE `TXN_TO_WRITE_ID` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TYPES`
--

DROP TABLE IF EXISTS `TYPES`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TYPES` (
  `TYPES_ID` bigint(20) NOT NULL,
  `TYPE_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `TYPE1` varchar(767) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `TYPE2` varchar(767) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  PRIMARY KEY (`TYPES_ID`),
  UNIQUE KEY `UNIQUE_TYPE` (`TYPE_NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TYPES`
--

LOCK TABLES `TYPES` WRITE;
/*!40000 ALTER TABLE `TYPES` DISABLE KEYS */;
/*!40000 ALTER TABLE `TYPES` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TYPE_FIELDS`
--

DROP TABLE IF EXISTS `TYPE_FIELDS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TYPE_FIELDS` (
  `TYPE_NAME` bigint(20) NOT NULL,
  `COMMENT` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `FIELD_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `FIELD_TYPE` varchar(767) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `INTEGER_IDX` int(11) NOT NULL,
  PRIMARY KEY (`TYPE_NAME`,`FIELD_NAME`),
  KEY `TYPE_FIELDS_N49` (`TYPE_NAME`),
  CONSTRAINT `TYPE_FIELDS_FK1` FOREIGN KEY (`TYPE_NAME`) REFERENCES `TYPES` (`TYPES_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TYPE_FIELDS`
--

LOCK TABLES `TYPE_FIELDS` WRITE;
/*!40000 ALTER TABLE `TYPE_FIELDS` DISABLE KEYS */;
/*!40000 ALTER TABLE `TYPE_FIELDS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `VERSION`
--

DROP TABLE IF EXISTS `VERSION`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `VERSION` (
  `VER_ID` bigint(20) NOT NULL,
  `SCHEMA_VERSION` varchar(127) NOT NULL,
  `VERSION_COMMENT` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`VER_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `VERSION`
--

LOCK TABLES `VERSION` WRITE;
/*!40000 ALTER TABLE `VERSION` DISABLE KEYS */;
INSERT INTO `VERSION` VALUES (1,'3.1.0','Hive release version 3.1.0');
/*!40000 ALTER TABLE `VERSION` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `WM_MAPPING`
--

DROP TABLE IF EXISTS `WM_MAPPING`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `WM_MAPPING` (
  `MAPPING_ID` bigint(20) NOT NULL,
  `RP_ID` bigint(20) NOT NULL,
  `ENTITY_TYPE` varchar(128) NOT NULL,
  `ENTITY_NAME` varchar(128) NOT NULL,
  `POOL_ID` bigint(20) DEFAULT NULL,
  `ORDERING` int(11) DEFAULT NULL,
  PRIMARY KEY (`MAPPING_ID`),
  UNIQUE KEY `UNIQUE_WM_MAPPING` (`RP_ID`,`ENTITY_TYPE`,`ENTITY_NAME`),
  KEY `WM_MAPPING_FK2` (`POOL_ID`),
  CONSTRAINT `WM_MAPPING_FK1` FOREIGN KEY (`RP_ID`) REFERENCES `WM_RESOURCEPLAN` (`RP_ID`),
  CONSTRAINT `WM_MAPPING_FK2` FOREIGN KEY (`POOL_ID`) REFERENCES `WM_POOL` (`POOL_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `WM_MAPPING`
--

LOCK TABLES `WM_MAPPING` WRITE;
/*!40000 ALTER TABLE `WM_MAPPING` DISABLE KEYS */;
/*!40000 ALTER TABLE `WM_MAPPING` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `WM_POOL`
--

DROP TABLE IF EXISTS `WM_POOL`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `WM_POOL` (
  `POOL_ID` bigint(20) NOT NULL,
  `RP_ID` bigint(20) NOT NULL,
  `PATH` varchar(767) NOT NULL,
  `ALLOC_FRACTION` double DEFAULT NULL,
  `QUERY_PARALLELISM` int(11) DEFAULT NULL,
  `SCHEDULING_POLICY` varchar(767) DEFAULT NULL,
  PRIMARY KEY (`POOL_ID`),
  UNIQUE KEY `UNIQUE_WM_POOL` (`RP_ID`,`PATH`),
  CONSTRAINT `WM_POOL_FK1` FOREIGN KEY (`RP_ID`) REFERENCES `WM_RESOURCEPLAN` (`RP_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `WM_POOL`
--

LOCK TABLES `WM_POOL` WRITE;
/*!40000 ALTER TABLE `WM_POOL` DISABLE KEYS */;
/*!40000 ALTER TABLE `WM_POOL` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `WM_POOL_TO_TRIGGER`
--

DROP TABLE IF EXISTS `WM_POOL_TO_TRIGGER`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `WM_POOL_TO_TRIGGER` (
  `POOL_ID` bigint(20) NOT NULL,
  `TRIGGER_ID` bigint(20) NOT NULL,
  PRIMARY KEY (`POOL_ID`,`TRIGGER_ID`),
  KEY `WM_POOL_TO_TRIGGER_FK2` (`TRIGGER_ID`),
  CONSTRAINT `WM_POOL_TO_TRIGGER_FK1` FOREIGN KEY (`POOL_ID`) REFERENCES `WM_POOL` (`POOL_ID`),
  CONSTRAINT `WM_POOL_TO_TRIGGER_FK2` FOREIGN KEY (`TRIGGER_ID`) REFERENCES `WM_TRIGGER` (`TRIGGER_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `WM_POOL_TO_TRIGGER`
--

LOCK TABLES `WM_POOL_TO_TRIGGER` WRITE;
/*!40000 ALTER TABLE `WM_POOL_TO_TRIGGER` DISABLE KEYS */;
/*!40000 ALTER TABLE `WM_POOL_TO_TRIGGER` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `WM_RESOURCEPLAN`
--

DROP TABLE IF EXISTS `WM_RESOURCEPLAN`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `WM_RESOURCEPLAN` (
  `RP_ID` bigint(20) NOT NULL,
  `NAME` varchar(128) NOT NULL,
  `QUERY_PARALLELISM` int(11) DEFAULT NULL,
  `STATUS` varchar(20) NOT NULL,
  `DEFAULT_POOL_ID` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`RP_ID`),
  UNIQUE KEY `UNIQUE_WM_RESOURCEPLAN` (`NAME`),
  KEY `WM_RESOURCEPLAN_FK1` (`DEFAULT_POOL_ID`),
  CONSTRAINT `WM_RESOURCEPLAN_FK1` FOREIGN KEY (`DEFAULT_POOL_ID`) REFERENCES `WM_POOL` (`POOL_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `WM_RESOURCEPLAN`
--

LOCK TABLES `WM_RESOURCEPLAN` WRITE;
/*!40000 ALTER TABLE `WM_RESOURCEPLAN` DISABLE KEYS */;
/*!40000 ALTER TABLE `WM_RESOURCEPLAN` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `WM_TRIGGER`
--

DROP TABLE IF EXISTS `WM_TRIGGER`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `WM_TRIGGER` (
  `TRIGGER_ID` bigint(20) NOT NULL,
  `RP_ID` bigint(20) NOT NULL,
  `NAME` varchar(128) NOT NULL,
  `TRIGGER_EXPRESSION` varchar(1024) DEFAULT NULL,
  `ACTION_EXPRESSION` varchar(1024) DEFAULT NULL,
  `IS_IN_UNMANAGED` bit(1) NOT NULL DEFAULT b'0',
  PRIMARY KEY (`TRIGGER_ID`),
  UNIQUE KEY `UNIQUE_WM_TRIGGER` (`RP_ID`,`NAME`),
  CONSTRAINT `WM_TRIGGER_FK1` FOREIGN KEY (`RP_ID`) REFERENCES `WM_RESOURCEPLAN` (`RP_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `WM_TRIGGER`
--

LOCK TABLES `WM_TRIGGER` WRITE;
/*!40000 ALTER TABLE `WM_TRIGGER` DISABLE KEYS */;
/*!40000 ALTER TABLE `WM_TRIGGER` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `WRITE_SET`
--

DROP TABLE IF EXISTS `WRITE_SET`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `WRITE_SET` (
  `WS_DATABASE` varchar(128) NOT NULL,
  `WS_TABLE` varchar(128) NOT NULL,
  `WS_PARTITION` varchar(767) DEFAULT NULL,
  `WS_TXNID` bigint(20) NOT NULL,
  `WS_COMMIT_ID` bigint(20) NOT NULL,
  `WS_OPERATION_TYPE` char(1) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `WRITE_SET`
--

LOCK TABLES `WRITE_SET` WRITE;
/*!40000 ALTER TABLE `WRITE_SET` DISABLE KEYS */;
/*!40000 ALTER TABLE `WRITE_SET` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2021-10-18 13:43:15
