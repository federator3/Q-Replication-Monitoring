--#SET TERMINATOR  ; 
-- ---------------------------------------------------------------------
-- Q Capture Monitor Parameters Table QREP_MON_CAPTURE_PARM
-- Creation of a parameters table for the SQL based Q Capture monitor 
-- qrep_monitor_capture.sql and insert of 1 row with parameter values.
--
-- COLUMN_NAME            SUBTYPE DESCRIPTION
-- CLAT_THRESH_WARNING    C-LAT   Seconds, after which the Capture 
--                                latency will be reported as WARNING
-- CLAT_THRESH_ERROR      C-LAT   Seconds, after which the Capture 
--                                latency will be reported as ERROR
-- MAXDEPTH_GLOBAL        notused for future use

-- ---------------------------------------------------------------------
--  - The schema of the table must be the same as the Capture schema
--  - The table must only have 1 single row
--  - A trigger prevents multiple rows
-- ---------------------------------------------------------------------
-- Customization: (search "-- change")
-- SET CURRENT SCHEMA to reflect your Q Capture schema
-- ---------------------------------------------------------------------
-- Status: testing - no warrenty
-- 
-- ---------------------------------------------------------------------
-- Änderungen / Ergänzungen
--  - 21.12.2020: Initial version
--  - 22.01.2021: Table name changed from QREP_MONITOR_CAPTURE_PARMS 
--                to QREP_MON_CAPTURE_PARM
-- ---------------------------------------------------------------------
-- TODO: 
-- ---------------------------------------------------------------------

-- change before execution ---------------------------------------------
-- connect to '<apply_server>';
-- set current schema = '<apply_schema>';
-- change before execution ---------------------------------------------

DROP TRIGGER QREP_MON_CAPTURE_PARM_1_ROW;
DROP TABLE QREP_MON_CAPTURE_PARM;

CREATE TABLE QREP_MON_CAPTURE_PARM (
 CLAT_THRESH_WARNING  INT not null with default 300
,CLAT_THRESH_ERROR    INT not null with default 3600
,MAXDEPTH_GLOBAL        INT not null with default 100000
) 
-- ORGANIZE BY ROW
;

-- EN
-- COMMENT ON COLUMN QREP_MON_CAPTURE_PARM.CLAT_THRESH_WARNING IS 
-- 'Seconds, after which the Capture latency will be reported as WARNING';
-- COMMENT ON COLUMN QREP_MON_CAPTURE_PARM.CLAT_THRESH_ERROR IS 
-- 'Seconds, after which the Capture latency will be reported as ERROR';
-- COMMENT ON COLUMN QREP_MON_CAPTURE_PARM.MAXDEPTH_GLOBAL IS 
-- 'for future use';

-- DE
COMMENT ON COLUMN QREP_MON_CAPTURE_PARM.CLAT_THRESH_WARNING IS 
'Warning Threshold fuer die Capture-Latency: Anzahl Sekunden, 
 nach denen die Capture Latency als WARNING reportet wird';
COMMENT ON COLUMN QREP_MON_CAPTURE_PARM.CLAT_THRESH_ERROR IS 
'Error Threshold fuer die Capture-Latency: Anzahl Sekunden, 
 nach denen die Capture Latency als ERROR reportet wird';
COMMENT ON COLUMN QREP_MON_CAPTURE_PARM.MAXDEPTH_GLOBAL IS 
'for future use';

-- Trigger to prevent that more than 1 row is inserted into
-- QREP_MON_CAPTURE_PARM
CREATE TRIGGER QREP_MON_CAPTURE_PARM_1_ROW
  NO CASCADE BEFORE INSERT ON QREP_MON_CAPTURE_PARM
  FOR EACH ROW MODE DB2SQL     
  WHEN ((select count(*) from QREP_MON_CAPTURE_PARM) <> 0) 
   SIGNAL SQLSTATE '85101'
    ('No more than one row allowed in table QREP_MON_CAPTURE_PARM')
;

INSERT INTO QREP_MON_CAPTURE_PARM (
 CLAT_THRESH_WARNING
,CLAT_THRESH_ERROR
,MAXDEPTH_GLOBAL        
) VALUES (
  500
, 5400
, 130000
);

