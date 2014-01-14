 ------------------------------------------------------------------------
   -- Copyright 2014 Expedia, Inc
   --
   -- Description:
   --   Table definition for STGLDR.RPT_HEXDM_SEGMENT_EXP_LIST
   --
   -- Change History:
   --   Date         Author         Description
   --   ----------   ----------     ------------------------------------
   --   2014-01-14   agurumurthi   Creation for jira - 95015
 -----------------------------------------------------------------------

CALL DBADM.SP_DROP_TABLE('STGLDR','RPT_HEXDM_SEGMENT_EXP_LIST');

CREATE TABLE STGLDR.RPT_HEXDM_SEGMENT_EXP_LIST
 (
         EXPERIMENT_CODE        VARCHAR(100)
        ,EXPERIMENT_NAME        VARCHAR(200)
        ,VERSION_NUMBER         SMALLINT
        ,VARIANT_CODE           VARCHAR(100)
        ,VARIANT_NAME           VARCHAR(200)
        ,STATUS                 VARCHAR(50)
        ,EXPERIMENT_TEST_ID     VARCHAR(100)
        ,TEST_MANAGER           VARCHAR(100)
        ,PRODUCT_MANAGER        VARCHAR(100)
        ,POD                    VARCHAR(100)
        ,START_DATE             DATE
        ,END_DATE               VARCHAR(100)
         LATEST_LOCAL_DATE      DATE
 )
 IN TS_STAGE00
 COMPRESS YES;

  CALL SYSPROC.ADMIN_CMD ('RUNSTATS ON TABLE STGLDR.RPT_HEXDM_SEGMENT_EXP_LIST ON ALL COLUMNS AND INDEXES ALL SET PROFILE ONLY');

  GRANT DELETE,INSERT,SELECT,UPDATE ON STGLDR.RPT_HEXDM_SEGMENT_EXP_LIST TO ROLE ORDMRW;
  GRANT SELECT on STGLDR.RPT_HEXDM_SEGMENT_EXP_LIST to ROLE ORDMR;
  GRANT DELETE,INSERT,SELECT,UPDATE ON STGLDR.RPT_HEXDM_SEGMENT_EXP_LIST TO ROLE ORETLR;
  GRANT SELECT ON STGLDR.RPT_HEXDM_SEGMENT_EXP_LIST TO ROLE ORETLRW;
  