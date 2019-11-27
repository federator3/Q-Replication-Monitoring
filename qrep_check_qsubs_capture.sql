--#SET TERMINATOR  ;
-- ---------------------------------------------------------------------
-- Q Sub Check Capture
-- Report detects misplaced subscription configurations and anomalies
-- at the Q Capture server. Queries:
--   110: C-TNF - Source table not found
--   120: C-CNF - Subscribed column does not exist in DB2
--   130: C-CNS - Existing source column not subscribed
--   140: C-DCC - Data capture flag missing for source table
-- ---------------------------------------------------------------------
-- Execute this query at the Q Capture server (e.g., after application
-- release activities)
-- ---------------------------------------------------------------------
-- Change before execution: (search "-- change")
--
-- SET CURRENT SCHEMA = '<your Q Capture schema>'
--
-- Query 140: LUW syntax and z/OS syntax not identical
-- Comment the syntax (LUW or z/OS) which is not appropriate for you
-- LUW
-- where st.data_capture != 'Y'
-- DB2 ZOS
-- where st.datacapture != 'Y'
-- ---------------------------------------------------------------------
-- Status: testing
-- ---------------------------------------------------------------------
-- Delete sections "Special: only when used with subscription generator"
-- if not used in conjunction with the Q Replication Subscription
-- Generator
-- ---------------------------------------------------------------------
-- Changes / enhancements
--  - 28.03.2017: Message Type
--  - 06.04.2017: Layout synched with Status Query (Ampel)
--  - 06.04.2017: WITH UR
--  - 27.04.2017: German and English Messages (EN currently commented)
--  - 11.05.2017: Delimiter for FIXIT ADDCOL Signal changed from ; to #
--  - 11.05.2017: Added separate optional output fields SUBNAME, STATE
--  - 11.05.2017: Added subscription state to MTXT
--  - 12.05.2017: Added separate DEBUG output fields SUBNAME, STATE
--                SOURCE_OWNER, SOURCE_NAME, COLNAME
--  - 29.06.2017: DEBUG columns commented to reduce report width
--  - 09.03.2018: TARGET_DBNAME, SOURCE_OWNER added to reported columns
--  - 24.04.2018: New support for TARGETCOLCLAUSE for query 130 (C-CNS)
--                only ASNCLP version
-- ---------------------------------------------------------------------

-- change before execution ---------------------------------------------
-- connect to '<capture_server>';
-- set current schema = '<capture schema>';
-- change before execution ---------------------------------------------

-- uncomment the following line (CREATE VIEW) when using CREATE VIEW -
-- comment when used as query
-- create view G000.QREP_CHECK_QSUBS_CAPTURE as


select

-- uncomment the following line (ordercol) when using CREATE VIEW -
-- comment when used as query
-- x.ordercol,

current timestamp as CHECK_TS,
case
  when length(x.program) <= 18
  then substr(x.program, 1 , 18)
  else substr(x.program, 1 , 16) concat '..'
end as PROGRAM,
x.CURRENT_SERVER,
x.MTYP,
x.SEV,
x.MTXT
-- optional
, x.FIXIT

-- uncomment the following 2 lines when using CREATE VIEW -
-- comment when used as query
-- , x.TARGET_SERVER as TARGET_DBNAME
-- , x.source_owner

-- DEBUG
-- , x.STATE
-- , x.SUBNAME
-- , x.source_name
-- , x.colname

from
(
-- Query 110:
--    DE: Finde alle Subscriptions, deren Quelltabelle nicht in DB2
--    definiert ist, Quelltabelle wurde nach Anlegen der Subscription
--    geloescht
--    EN: Find all subscriptions that have no source table in DB2
--    (e.g., source table was dropped / renamed after the subscription
--    was defined)

select
110 as ordercol,
'ASNQCAP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'C-TNF' as MTYP,
qs.subname,

case
  when qs.state = 'A'
  then 'ERROR'
  else 'WARNING'
end as SEV,

'QSUB ' concat trim(qs.subname) concat ' (' concat qs.state concat ')'

-- DE
concat ' fuer Quell-Tabelle '
concat trim(qs.source_owner) concat '.' concat trim(qs.source_name)
concat ', STATE=' concat trim(qs.state)
concat ', existiert, aber die Tabelle exitiert nicht in DB2! '
concat case when qs.state = 'A' then 'Deactiviere und e' else 'E' end
concat 'ntferne die Subscription.' as MTXT,

-- EN
-- concat ' for source table '
-- concat trim(qs.source_owner) concat '.' concat trim(qs.source_name)
-- concat ', STATE=' concat trim(qs.state)
-- concat ', exists, but the source table does not exist in DB2! '
-- concat case when qs.state = 'A' then 'Deactivate and r' else 'R' end
-- concat 'emove the subscription.' as MTXT,

'DROP QSUB ( SUBNAME "' CONCAT rtrim(qs.subname) CONCAT '" '
CONCAT ' USING REPLQMAP ' CONCAT QQ.PUBQMAPNAME
CONCAT ');' as FIXIT,

qs.TARGET_SERVER,

-- DEBUG
qs.state,
qs.source_owner, qs.source_name,
substr(st.creator , 1 , 18) as tbcreator,
substr(st.name , 1 , 18) as tbname,
cast(null as varchar(128)) as COLNAME

from ibmqrep_subs qs
inner join ibmqrep_sendqueues qq
on qs.sendq = qq.sendq

left outer join sysibm.systables st
on  qs.SOURCE_OWNER = st.CREATOR
and qs.SOURCE_NAME  = st.NAME

where st.creator is null

UNION
-- Query 120:
--    DE: Finde alle Subscriptions, fuer die eine Spalte definiert ist,
--    die nicht in DB2 existiert. Spalte wurde nach Anlegen der
--    Subscription geloescht oder umbenannt
--    EN: Find all subscriptions which include a column which does not
--    exist in DB2 (e.g., source column was removed / renamed after the
--    subscription was defined)

select
120 as ordercol,
'ASNQCAP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'C-CNF' as MTYP,
qs.subname,

'ERROR' as SEV,

'QSUB ' concat trim(qs.subname) concat ' (' concat qs.state concat ')'

-- DE
concat ' enthaelt Quell-Spalte '
concat trim(qs.source_owner) concat '.' concat trim(qs.source_name)
concat '.' concat trim(qc.SRC_COLNAME)
concat ' aber die Spalte existiert nicht in DB2!  '
concat 'Die Subscription ist anzupassen.' as MTXT,

-- EN
-- concat ' contains source column '
-- concat trim(qs.source_owner) concat '.' concat trim(qs.source_name)
-- concat '.' concat trim(qc.SRC_COLNAME)
-- concat ' but the column does not exist in DB2!  '
-- concat 'Modify the subscription.' as MTXT,

'ALTER TABLE ' concat trim(qs.source_owner) concat '.'
concat trim(qs.source_name) CONCAT ' ADD COLUMN '
CONCAT trim(qc.SRC_COLNAME) concat '<datatype>' as FIXIT,

qs.TARGET_SERVER,

-- DEBUG
qs.state,
qs.source_owner, qs.source_name,
substr(sc.tbcreator , 1 , 18) as tbcreator,
substr(sc.tbname , 1 , 18) as tbname,
substr(qc.SRC_COLNAME , 1 , 18) as COLNAME

from ibmqrep_subs qs
inner join ibmqrep_src_cols qc
on  qs.SUBNAME = qc.SUBNAME

-- check if source table exists (only column missing) to prevent report
-- for the same as in query 1
inner join sysibm.systables st
on  qs.SOURCE_OWNER = st.CREATOR
and qs.SOURCE_NAME  = st.NAME

left outer join sysibm.syscolumns sc
on  qs.SOURCE_OWNER = sc.TBCREATOR
and qs.SOURCE_NAME  = sc.TBNAME
and qc.src_colname  = sc.name

where sc.name is null

UNION
-- Query 130:
--    DE: Finde alle Subscriptions, fuer die eine Spalte in DB2
--    existiert, die aber nicht in der Subscription definiert ist.
--    Spalte wurde nach Anlegen der Subscription zur Quelltabelle
--    hinzugefuegt oder umbenannt
--    EN: Find all subscriptions, for which a column exists in DB2
--    which is not included in the subscription. E.g., column was
--    added to the source table after the subscription was defined
--    and REPLADDCOL = 'N'.

select
130 as ordercol,
'ASNQCAP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'C-CNS' as MTYP,
y.subname,

'ERROR' as SEV,

-- DEBUG
-- coalesce(y.sub_has_trgcolsclause, '') concat
-- coalesce(y.col_in_include, '') concat
-- coalesce(y.col_in_exclude, '') concat

'QSUB ' concat trim(y.subname) concat ' (' concat y.state concat ')'

-- DE
-- Special: only when used with subscription generator
-- concat
-- case
--   when sub_has_trgcolsclause is not null
--     then ' mit TRGCOLS ' concat sub_has_trgcolsclause
--     else ''
-- end
-- Special: only when used with subscription generator

concat ' enthaelt die Quell-Spalte '
concat trim(y.tbcreator) concat '.' concat trim(y.tbname) concat '.'
concat trim(y.name)
concat ' nicht. Die Subscription ist anzupassen.' as MTXT,
-- DE

-- EN
-- Special: only when used with subscription generator
-- concat
-- case
--   when sub_has_trgcolsclause is not null
--     then ' with TRGCOLS ' concat sub_has_trgcolsclause
--     else ''
-- end
-- Special: only when used with subscription generator

-- concat ' does not contain source col '
-- concat trim(y.tbcreator) concat '.'
-- concat trim(y.tbname) concat '.'
-- concat trim(y.name)
-- concat '. Modify the subscription.' as MTXT,
-- EN

'INSERT INTO IBMQREP_SIGNAL(SIGNAL_TIME, SIGNAL_TYPE, SIGNAL_SUBTYPE, '
concat 'SIGNAL_INPUT_IN, SIGNAL_STATE) values (CURRENT TIMESTAMP, '
concat '''CMD'', ''ADDCOL'', '''
concat trim(y.subname) concat ';' concat trim(y.name)
concat ''', ''P'' );' as FIXIT,

y.TARGET_SERVER,

-- DEBUG
y.state,
y.source_owner, y.source_name,
substr(y.tbcreator , 1 , 18) as tbcreator,
substr(y.tbname , 1 , 18) as tbname,
substr(y.name , 1 , 18) as COLNAME

from

(

select


qs.subname,
qc.src_colname,
qs.TARGET_SERVER,
qs.state,
qs.source_owner, qs.source_name,
sc.tbcreator, sc.tbname,
sc.name

-- Special: only when used with subscription generator
-- ,case
--   when locate('TRGCOLSINCLUDE' ,
--            replace(upper(qb.targetcolclause), ' ', '') ) <> 0
--   then 'INCLUDE'
--   when locate('TRGCOLSEXCLUDE' ,
--            replace(upper(qb.targetcolclause), ' ', '') ) <> 0
--   then 'EXCLUDE'
--   else null
-- end as sub_has_trgcolsclause
-- ,case
--   when locate('TRGCOLSINCLUDE' ,
--            replace(upper(qb.targetcolclause), ' ', '') ) <> 0
--    and (    locate('(' concat sc.name concat ')' ,
--                 replace(upper(qb.targetcolclause), ' ', '')) <> 0
--          or locate('(' concat sc.name concat ',' ,
--                 replace(upper(qb.targetcolclause), ' ', '')) <> 0
--          or locate(',' concat sc.name concat ',' ,
--                 replace(upper(qb.targetcolclause), ' ', '')) <> 0
--          or locate(',' concat sc.name concat ')' ,
--                 replace(upper(qb.targetcolclause), ' ', '')) <> 0
--        )
--   then 'Y'
--   else 'N'
-- end as COL_IN_INCLUDE
-- ,case
--   when locate('TRGCOLSEXCLUDE' ,
--            replace(upper(qb.targetcolclause), ' ', '') ) <> 0
--    and (    locate('(' concat sc.name concat ')' ,
--                 replace(upper(qb.targetcolclause), ' ', '')) <> 0
--          or locate('(' concat sc.name concat ',' ,
--                 replace(upper(qb.targetcolclause), ' ', '')) <> 0
--          or locate(',' concat sc.name concat ',' ,
--                 replace(upper(qb.targetcolclause), ' ', '')) <> 0
--          or locate(',' concat sc.name concat ')' ,
--                 replace(upper(qb.targetcolclause), ' ', '')) <> 0
--        )
--   then 'Y'
--   else 'N'
-- end as COL_IN_EXCLUDE
-- Special: only when used with subscription generator


from sysibm.syscolumns sc
inner join ibmqrep_subs qs
on   sc.TBCREATOR = qs.SOURCE_OWNER
and  sc.TBNAME    = qs.SOURCE_NAME

left outer join ibmqrep_src_cols qc
on  qs.subname = qc.subname
and sc.name    = qc.src_colname

-- Special: only when used with subscription generator
-- left outer join asnclp_q_base qb
--  on qs.source_owner = qb.tabschema
-- and qs.source_name  = qb.tabname
-- and qs.target_owner = qb.target_schema
-- and qs.target_name  = qb.target_name
-- Special: only when used with subscription generator

) y

-- Special: use this where clause when query used without the
-- Q Replication subscription generator
-- Error condiction: A column exists in the source table which
-- is not part of the subscription

WHERE Y.SRC_COLNAME IS NULL
-- ----------------------------------------------------------

-- Special: only when used with subscription generator
-- Error condictions:
--  a) A column exists in the source table which
--     is not part of the subscription and TARGETCOLCLAUSE is null
--  b) A column exists in the source table which
--     is not part of the subscription and it is mentioned in
--     TRGCOLS INCLUDE
--  c) A column exists in the source table which
--     is not part of the subscription and it is NOT mentioned in
--     TRGCOLS EXCLUDE

-- where (y.sub_has_trgcolsclause is null and y.src_colname is null)
-- or (     y.sub_has_trgcolsclause = 'INCLUDE'
--      and y.col_in_include        = 'Y'
--      and y.src_colname           is null)
-- or (     y.sub_has_trgcolsclause = 'EXCLUDE'
--      and y.col_in_exclude        = 'N'
--      and y.src_colname           is null)
-- Special: only when used with subscription generator

UNION
-- Query 140:
--    DE: Finde alle Subscriptions, deren Quelltabelle kein Data
--    Capture Changes Attribut hat.
--    EN: Find all subscriptions for which the source table does not
--    have the data capture flag

select
140 as ordercol,
'ASNQCAP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'C-DCC' as MTYP,
qs.subname,

'ERROR' as SEV,

'QSUB ' concat trim(qs.subname) concat ' (' concat qs.state concat ')'

-- DE
concat ': Quelltabelle '
concat trim(st.creator) concat '.' concat trim(st.name)
concat 'hat kein DATA CAPTURE Attribut. '
concat 'ALTER TABLE DATA CATRURE CHANGES ist auszufuehren.' as MTXT,

-- EN
-- concat ': Source table '
-- concat trim(st.creator) concat '.' concat trim(st.name)
-- concat 'does not have DATA CAPTURE flag. '
-- concat 'Alter the table.' as MTXT,

'ALTER TABLE ' concat trim(st.creator) concat '.' concat trim(st.name)
concat ' DATA CAPTURE CHANGES;' as FIXIT,

qs.TARGET_SERVER,

-- DEBUG
qs.state,
qs.source_owner, qs.source_name,
substr(st.creator , 1 , 18) as tbcreator,
substr(st.name , 1 , 18) as tbname,
cast(null as varchar(128)) as COLNAME

from ibmqrep_subs qs
inner join sysibm.systables st
on  qs.SOURCE_OWNER = st.CREATOR
and qs.SOURCE_NAME  = st.NAME


-- change before execution ---------------------------------------------

-- DB2 LUW
where st.data_capture != 'Y'

-- DB2 ZOS
-- where st.datacapture != 'Y'

-- change before execution ---------------------------------------------

) x

-- comment the following 2 lines (order by / with ur) when
-- using CREATE VIEW - uncomment when used as query
order by x.ordercol, x.MTXT
with ur
;

-- set current schema = user;