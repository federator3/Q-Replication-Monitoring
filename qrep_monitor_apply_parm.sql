--#SET TERMINATOR  ; 
-- ---------------------------------------------------------------------
-- Q Appply Monitor Parameters Table QREP_MON_APPLY_PARM
-- Creation of a parameters table for the SQL based Q Apply monitor 
-- qrep_monitor_APPLY.sql and insert of 1 row with parameter values.
--
-- COLUMN_NAME            SUBTYPE DESCRIPTION
-- ALAT_THRESH_WARNING    A-LAT   Seconds, after which an E2E latency
--                                will be reported as WARNING
-- ALAT_THRESH_ERROR      A-LAT   Seconds, after which an E2E latency
--                                will be reported as ERROR
-- ALAT_CAUSE_CAP_QDEPTH  A-LAT   For A-LAT errors and warning CAPTURE
--                                will be determined as cause when the
--                                QDEPTH is less than or equal to
--                                ALAT_CAUSE_CAP_QDEPTH
-- ALAT_CAUSE_APP_QDEPTH  A-LAT   For A-LAT errors and warning APPLY
--                                will be determined as cause when the
--                                QDEPTH is greater than or equal to
--                                ALAT_CAUSE_APP_QDEPTH 
-- EXCEPTION_TIME_WINDOW  A-EXC   Interval in hours to look for 
--                                exceptions (last n hours)
-- HEARTBEAT_TIME_WINDOW  A-CHB   Interval in minutes to look for 
--                                Capture heartbeat messages
--                                (last n minutes)
-- MAXDEPTH_GLOBAL        notused for future use
-- ---------------------------------------------------------------------
--  - The schema of the table must be the same as the Apply schema
--  - The table must only have 1 single row
--  - A trigger prevents multiple rows
-- ---------------------------------------------------------------------
-- Customization: (search "-- change")
-- SET CURRENT SCHEMA to reflect your Q Apply schema
-- ---------------------------------------------------------------------
-- Status: testing - no warrenty
-- 
-- ---------------------------------------------------------------------
-- Änderungen / Ergänzungen
--  - 21.12.2020: Initial version
--  - 22.01.2021: Table name changed from QREP_MONITOR_APPLY_PARMS 
--                to QREP_MON_APPLY_PARM
-- ---------------------------------------------------------------------
-- TODO: 
-- ---------------------------------------------------------------------

-- change before execution ---------------------------------------------
-- connect to '<apply_server>';
-- set current schema = '<apply_schema>';
-- change before execution ---------------------------------------------

DROP TRIGGER QREP_MON_APPLY_PARM_1_ROW;
DROP TABLE QREP_MON_APPLY_PARM;

CREATE TABLE QREP_MON_APPLY_PARM (
 ALAT_THRESH_WARNING    INT not null with default 30
,ALAT_THRESH_ERROR      INT not null with default 600
,ALAT_CAUSE_CAP_QDEPTH  INT not null with default 100
,ALAT_CAUSE_APP_QDEPTH  INT not null with default 60000
,EXCEPTION_TIME_WINDOW  INT not null with default 24
,HEARTBEAT_TIME_WINDOW  INT not null with default 10
,MAXDEPTH_GLOBAL        INT not null with default 100000
)
-- ORGANIZE BY ROW
;

-- EN
-- COMMENT ON COLUMN QREP_MON_APPLY_PARM.ALAT_THRESH_WARNING IS 
-- 'Seconds, after which an E2E latency will be reported as WARNING';
-- COMMENT ON COLUMN QREP_MON_APPLY_PARM.ALAT_THRESH_ERROR IS 
-- 'Seconds, after which an E2E latency will be reported as ERROR';
-- COMMENT ON COLUMN QREP_MON_APPLY_PARM.ALAT_CAUSE_CAP_QDEPTH IS 
-- 'For A-LAT errors and warning CAPTURE will be determined as cause 
--  when the QDEPTH is less than or equal to 
-- ALAT_CAUSE_CAP_QDEPTH';
-- COMMENT ON COLUMN QREP_MON_APPLY_PARM.ALAT_CAUSE_APP_QDEPTH IS 
-- 'For A-LAT errors and warning APPLY will be determined as cause 
--  when the QDEPTH is greater than or equal to 
-- ALAT_CAUSE_APP_QDEPTH ';
-- COMMENT ON COLUMN QREP_MON_APPLY_PARM.EXCEPTION_TIME_WINDOW IS 
-- 'Interval in hours to look for exceptions (last n hours)';
-- COMMENT ON COLUMN QREP_MON_APPLY_PARM.HEARTBEAT_TIME_WINDOW IS 
-- 'Interval in minutes to look for Capture heartbeat messages 
--  (last n minutes)';
-- COMMENT ON COLUMN QREP_MON_APPLY_PARM.MAXDEPTH_GLOBAL IS 
-- 'for future use';

-- DE
COMMENT ON COLUMN QREP_MON_APPLY_PARM.ALAT_THRESH_WARNING IS 
'Warning Threshold fuer die End-to-End-Latency: Anzahl Sekunden, 
 nach denen die E2E Latency als WARNING reportet wird';
COMMENT ON COLUMN QREP_MON_APPLY_PARM.ALAT_THRESH_ERROR IS 
'Error Threshold fuer die End-to-End-Latency: Anzahl Sekunden, 
 nach denen die E2E Latency als ERROR reportet wird';
COMMENT ON COLUMN QREP_MON_APPLY_PARM.ALAT_CAUSE_CAP_QDEPTH IS 
'Bei A-LAT Warning oder Error wird CAPTURE als Verursacher der 
 Latency ausgegeben, wenn die aktuelle QDEPTH kleiner ist als der 
 in dieser Spalte gespeicherte Wert.'; 
COMMENT ON COLUMN QREP_MON_APPLY_PARM.ALAT_CAUSE_APP_QDEPTH IS 
'Bei A-LAT Warning oder Error wird APPLY als Verursacher der 
 Latency ausgegeben, wenn die aktuelle QDEPTH groesser ist als der 
 in dieser Spalte gespeicherte Wert.'; 
COMMENT ON COLUMN QREP_MON_APPLY_PARM.EXCEPTION_TIME_WINDOW IS 
'Intervall in Stunden, die auf Exceptions untersucht werden 
 (die letzten n Stunden)';
COMMENT ON COLUMN QREP_MON_APPLY_PARM.HEARTBEAT_TIME_WINDOW IS 
'Interval in Minuten, die auf Capture Heartbeat Messages untersucht 
 werden (die letzten n Minuten)';
COMMENT ON COLUMN QREP_MON_APPLY_PARM.MAXDEPTH_GLOBAL IS 
'for future use';


-- Trigger to prevent that more than 1 row is inserted into
-- QREP_MON_APPLY_PARM
CREATE TRIGGER QREP_MON_APPLY_PARM_1_ROW
  NO CASCADE BEFORE INSERT ON QREP_MON_APPLY_PARM
  FOR EACH ROW MODE DB2SQL     
  WHEN ((select count(*) from QREP_MON_APPLY_PARM) <> 0) 
   SIGNAL SQLSTATE '85101'
    ('No more than one row allowed in table QREP_MON_APPLY_PARM')
;

INSERT INTO QREP_MON_APPLY_PARM (
 ALAT_THRESH_WARNING
,ALAT_THRESH_ERROR
,ALAT_CAUSE_CAP_QDEPTH
,ALAT_CAUSE_APP_QDEPTH
,EXCEPTION_TIME_WINDOW
,HEARTBEAT_TIME_WINDOW
,MAXDEPTH_GLOBAL        
) VALUES (
  600
, 3600
, 100
, 60000
, 24
, 10
, 130000
);

