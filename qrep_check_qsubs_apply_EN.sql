--#SET TERMINATOR  ;
-- ---------------------------------------------------------------------
-- Q Sub Check Apply
-- Report detects misplaced subscription configurations and anomalies
-- at the Q Apply server. Queries:
--   210: A-TNF - Target table not found
--   220: A-CNF - Subscribed column does not exist in DB2
--   230: A-CNS - Existing target column not subscribed
--   250: A-GRA - Target table grant missing for Apply user
--   260: A-RIP - Sub for RI parent of a replicated RI child missing
--   261: A-RIC - Sub for RI child of a replicated RI parent missing
--   270: A-BID - Before image column has different data type than
--                after image column
--   280: A-BXN - BLOB and XML target column nullability for CCD targets
-- ---------------------------------------------------------------------
-- Execute this query at the Q Apply server (e.g., after application
-- release activities)
-- ---------------------------------------------------------------------
-- Change before execution: (search "-- change")
--
-- SET CURRENT SCHEMA = '<your Q Apply schema>'
--
-- Query 250: Apply user has to be set in WHERE clause of the query
-- search for 'ta.grantee IN' to change
--
-- Query 250: LUW syntax and z/OS syntax not identical
-- Comment the syntax (LUW or z/OS) which is not appropriate for you
-- LUW
-- ta.refauth
-- DB2 ZOS
-- ta.referencesauth
-- ---------------------------------------------------------------------
-- Status: testing
-- ---------------------------------------------------------------------
-- Changes / enhancements
--  - 28.03.2017: Message Type
--  - 28.03.2017: Query 240: target table REORG PENDING / AREO
--  - 06.04.2017: Layout synched with Status Query (Ampel)
--  - 06.04.2017: WITH UR
--  - 27.04.2017: German and English Messages (EN currently commented)
--  - 11.05.2017: Added separate DEBUG output fields SUBNAME, STATE
--                TARGET_OWNER, TARGET_NAME, COLNAME
--  - 11.05.2017: Added subscription STATE to MTXT
--  - 11.05.2017: Target table authorizations check
--  - 11.05.2017: BugFix: ALTER source_owner.source_name ersetzt durch
--                ALTER target_owner.target_name
--  - 13.06.2017: Query 240: Moved to a separate script
--                (qrep_check_qsubs_apply_reorg_pending.sql) because
--                the elapsed time to query sysproc.admin_get_tab_info
--                is very long
--  - 13.06.2017: Query 250 (GRANT): Added inner join to systables not
--                to list errors twice (Query 210 and Query 250)
--  - 29.06.2017: DEBUG columns commented to reduce report width
--  - 29.06.2017: Improved FIXIT for missing GRANTS. Now a syntactically
--                correct GRANT Statements (with missing privs) is
--                generated
--  - 22.09.2017: Query 250 (GRANT): Added check of REFAUTH for target
--                tables with RI constraints (dependent table). This
--                ought to be obsolete but has to be tested
--  - 11.10.2017: Query 250 (GRANT): Added check of REFAUTH for target
--                tables with RI constraints (parent table). This is the
--                required check for REFAUTH
--  - 14.10.2017: Query 260 (RI): Check if all child tables of a
--                replicated parent table are also replicated (via the
--                same queue)
--  - 14.10.2017: Query 261 (RI): Check if all parent tables of a
--                replicated child table are also replicated (via the
--                same queue)
--  - 19.10.2017: FI Special: Column check eliminated for IDH columns.
--                Search for "FI Special" to comment/delete if not
--                relevant
--  - 25.10.2017: Syntax difference between DB2 LUW and DB2 z/OS covered
--                (REFAUTH / REFERENCESAUTH)
--  - 25.10.2017: z/OS syntax fix. CAST (NULL as ...)
--  - 10.01.2018: Query 230 (target columns): Added support for before
--                image columns
--  - 16.01.2018: Query 270 (Before image data type) added
--  - 19.04.2018: Query 270 (Before images): Check added for before
--                image column NULLABILITY
--  - 19.04.2018: Query 270 (Before images): FIXIT implemented for
--                before image errors
--  - 12.03.2021: New Query 280 - nullability check for XML / BLOB cols
-- ---------------------------------------------------------------------
-- TODO: ALTER RECHTE für Tabellen prüfen, die nicht repliziert werden,
-- aber CHILD in einer RI Beziehung sind, deren Parent repliziert wird
-- ---------------------------------------------------------------------

-- change before execution ---------------------------------------------
-- connect to '<apply_server>';
-- set current schema = '<apply_schema>';
-- change before execution ---------------------------------------------

select
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

-- DEBUG
-- , x.STATE
-- , x.SUBNAME
-- , x.target_owner
-- , x.target_name
-- , x.colname


from
(
-- Query 210:
--    DE: Finde alle Subscriptions, deren Zieltabelle nicht in DB2
--    definiert ist, Zieltabelle wurde nach Anlegen der Subscription
--    gelöscht
--    EN: Find all subscriptions that have no target table in DB2
--    (e.g., target table was dropped / renamed after the subscription
--    was defined)

select
210 as ordercol,
'ASNQAPP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'A-TNF' as MTYP,
qt.subname,
case
  when qt.state = 'A'
  then 'ERROR'
  else 'WARNING'
end as SEV,

'QSUB ' concat trim(qt.subname) concat ' (' concat qt.state concat ')'

-- DE
-- concat ' fuer Ziel-Tabelle '
-- concat trim(qt.target_owner) concat '.' concat trim(qt.target_name)
-- concat ', STATE=' concat trim(qt.state)
-- concat ', existiert, aber die Tabelle exitiert nicht in DB2! '
-- concat case when qt.state = 'A' then 'Deactiviere und e' else 'E' end
-- concat 'ntferne die Subscription.' as MTXT,
-- DE

-- EN
 concat ' for target table '
 concat trim(qt.target_owner) concat '.' concat trim(qt.target_name)
 concat ', STATE=' concat trim(qt.state)
 concat ', exists, but the table does not exist in DB2! '
 concat case when qt.state = 'A' then 'Deactivate and r' else 'R' end
 concat 'emove the subscription.' as MTXT,
-- EN

'DROP QSUB ( SUBNAME "' CONCAT rtrim(qt.subname) CONCAT '" '
CONCAT ' USING REPLQMAP ' CONCAT QQ.REPQMAPNAME
CONCAT ');' as FIXIT,

-- DEBUG
qt.state,
qt.target_owner, qt.target_name,
substr(st.creator , 1 , 18) as tbcreator,
substr(st.name , 1 , 18) as tbname,
cast(null as varchar(128)) as COLNAME

from ibmqrep_targets qt
inner join ibmqrep_recvqueues qq
on qt.recvq = qq.recvq

left outer join sysibm.systables st
on  qt.TARGET_OWNER = st.CREATOR
and qt.TARGET_NAME  = st.NAME

where st.creator is null

UNION
-- Query 220:
--    DE: Finde alle Subscriptions, für die eine Spalte definiert ist,
--    die nicht in DB2 existiert. Spalte wurde nach Anlegen der
--    Subscription gelöscht oder umbenannt
--    EN: Find all subscriptions which include a column which does not
--    exist in DB2 (e.g., target column was removed / renamed after the
--    subscription was defined)

select
220 as ordercol,
'ASNQAPP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'A-CNF' as MTYP,
qt.subname,
'ERROR' as SEV,

'QSUB ' concat trim(qt.subname) concat ' (' concat qt.state concat ')'

-- DE
-- concat ' enthaelt Ziel-Spalte '
-- concat trim(qt.target_owner) concat '.' concat trim(qt.target_name)
-- concat '.' concat trim(qc.TARGET_COLNAME)
-- concat ' aber die Spalte existiert nicht in DB2!  '
-- concat 'Die Subscription ist anzupassen.' as MTXT,
-- DE

-- EN
 concat ' contains target column '
 concat trim(qt.target_owner) concat '.' concat trim(qt.target_name)
 concat '.' concat trim(qc.TARGET_COLNAME)
 concat ' but the column does not exist in DB2!  '
 concat 'Modify the subscription.' as MTXT,
-- EN

'ALTER TABLE ' concat trim(qt.target_owner) concat '.'
concat trim(qt.target_name) CONCAT ' ADD COLUMN '
CONCAT trim(qc.TARGET_COLNAME) concat '<datatype>' as FIXIT,

-- DEBUG
qt.state,
qt.target_owner, qt.target_name,
substr(sc.tbcreator , 1 , 18) as tbcreator,
substr(sc.tbname , 1 , 18) as tbname,
substr(qc.TARGET_COLNAME , 1 , 18) as COLNAME

from ibmqrep_targets qt
inner join ibmqrep_trg_cols qc
on  qt.SUBNAME = qc.SUBNAME
and qt.RECVQ = qc.RECVQ

-- check if source table exists (only column missing) to prevent
-- report for the same as in query 1
inner join sysibm.systables st
on  qt.TARGET_OWNER = st.CREATOR
and qt.TARGET_NAME  = st.NAME

left outer join sysibm.syscolumns sc
on  qt.TARGET_OWNER = sc.TBCREATOR
and qt.TARGET_NAME  = sc.TBNAME
and qc.TARGET_COLNAME  = sc.name

where sc.name is null

UNION
-- Query 230:
--    DE: Finde alle Subscriptions, für die eine Spalte in DB2
--    existiert, die aber nicht in der Subscription definiert ist.
--    Spalte wurde nach Anlegen der Subscription zur Zieltabelle
--    hinzugefügt oder umbenannt
--    EN: Find all subscriptions, for which a column exists in DB2
--    which is not included in the subscription. E.g., column was
--    added to the source table after the subscription was defined
--    and REPLADDCOL = 'N'.

select
230 as ordercol,
'ASNQAPP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'A-CNS' as MTYP,
qt.subname,
'ERROR' as SEV,

'QSUB ' concat trim(qt.subname) concat ' (' concat qt.state concat ')'

-- DE
-- concat ' enthaelt die DB2-Ziel-Spalte '
-- concat trim(sc.tbcreator) concat '.' concat trim(sc.tbname)
-- concat '.'
-- concat trim(sc.name) concat ' ('
-- concat case when sc.nulls = 'N' then 'NOT NULL' else 'nullable' end
-- concat ') nicht. Die Subscription ist anzupassen.' as MTXT,
-- DE

-- EN
 concat ' does not contain target col '
 concat trim(sc.tbcreator) concat '.' concat trim(sc.tbname)
 concat '.'
 concat trim(sc.name)
 concat '. Modify the subscription.' as MTXT,
-- EN

'Source ADDCOL SIGNAL, siehe qrep_check_qsubs_capture.sql' 
concat ' oder BEF_TARG_COLNAME definieren' as FIXIT,

-- DEBUG
qt.state,
qt.target_owner, qt.target_name,
substr(sc.tbcreator , 1 , 18) as tbcreator,
substr(sc.tbname , 1 , 18) as tbname,
substr(sc.name , 1 , 18) as COLNAME

from sysibm.syscolumns sc
inner join ibmqrep_targets qt
on   sc.TBCREATOR = qt.TARGET_OWNER
and  sc.TBNAME    = qt.TARGET_NAME

left outer join ibmqrep_trg_cols qc
on  qt.subname = qc.subname
and qt.recvq   = qc.recvq
and sc.name    = qc.target_colname

left outer join ibmqrep_trg_cols bqc
on  qt.subname = bqc.subname
and qt.recvq   = bqc.recvq
and sc.name    = bqc.BEF_TARG_COLNAME

where coalesce(qc.target_colname , bqc.BEF_TARG_COLNAME) is null

-- 19.10.2017
-- FI Special  ---------------------------------
-- remove or comment if project is not FI/IDH
--  and sc.name not in ('IDH_GLTG_FACH_ADTM',
--                      'IDH_GLTG_FACH_EDTM',
--					  'IDH_GLTG_TECH_ATS',
--					  'IDH_GLTG_TECH_ETS',
--					  'IDH_PRZS_ID_INS',
--					  'TRANS_START')
-- FI Special  ---------------------------------



-- UNION
-- Query 240:
--    DE: Finde alle Subscriptions, deren Zieltabelle
--    REORG PENDING (LUW)
--    bzw. deren Tablespace AREO (z/OS) ist
--    EN: Find all subscriptions, for which the target table is
--    REORG PENDING (LUW)
--    or for which the tablespace is AREO (z/OS)

-- Moved to seperate script "qrep_check_qsubs_apply_reorg_pending.sql"

UNION
-- Query 250:
--    DE: Finde alle Subscriptions, für die dem Apply-User Datenbank-
--    Rechte fehlen
--    EN: Find all subscriptions, for which grants are missing for
--    the apply user.

select
250 as ordercol,
'ASNQAPP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'A-GRA' as MTYP,
y.subname,
'ERROR' as SEV,

'QSUB ' concat trim(y.subname) concat ' (' concat y.state concat ')'

-- DE
-- concat ' Erwartete TABAUTH: '
-- concat y.expected_tabauth
-- concat ' - Tatsaechliche TABAUTH: '
-- concat y.effective_tabauth
-- concat '. Rechte fuer Zieltabelle '
-- concat trim(y.target_owner) concat '.' concat trim(y.target_name)
-- concat ' vergeben!'
--  as MTXT,
-- DE

-- EN
 concat ' Expected TABAUTH: '
 concat y.expected_tabauth
 concat ' - Effective TABAUTH: '
 concat y.effective_tabauth
 concat '. GRANT authorizations for target table '
 concat trim(y.target_owner) concat '.' concat trim(y.target_name)
  as MTXT,
-- EN

'GRANT ' concat substr(y.grantprivstring, 2) concat 'ON '
concat trim(y.target_owner) concat '.'
concat trim(y.target_name) concat ' TO ' concat

-- change before execution BEGIN ---------------------------------------
-- Set: Apply User
--
-- Setting for CLENKE Testumgebung
-- 'DE094692'

-- Setting for Tests
-- 'Q1D26E99'

-- Setting for cutomized Apply User
 'Q1D26' concat substr(current server , 7 , 1) concat '99'
-- change before execution END -----------------------------------------

concat ';'

as FIXIT,

-- DEBUG
y.state,
y.target_owner, y.target_name,
substr(y.TCREATOR , 1 , 18) as tbcreator,
substr(y.TTNAME , 1 , 18) as tbname,
cast(null as varchar(128)) as COLNAME

from

(

select qt.subname, qt.state,
qt.source_owner, qt.source_name,
qt.target_owner, qt.target_name,
ta.TCREATOR, ta.TTNAME,

case
  when qt.target_type = 2 and qt.ccd_condensed = 'N' then 'I/A'
  when qt.target_type = 1 and qt.has_loadphase = 'I'
   and (red.RELNAME is not null or rep.RELNAME is not null)
   then 'I/U/D/A/R'
  else 'I/U/D/A'
end as expected_tabauth,

-- Check: INSERTAUTH
case
  when ta.insertauth in ('Y', 'G') then 'I'
  else coalesce(ta.insertauth, '-')
end
concat
-- Check: UPDATEAUTH
case
  when qt.target_type = 2 and qt.ccd_condensed = 'N'
    then ''
  else
    case
      when ta.updateauth in ('Y', 'G') then '/U'
	  else '/' concat coalesce(ta.updateauth, '-')
	end
end
concat
-- Check: DELETEAUTH
case
  when qt.target_type = 2 and qt.ccd_condensed = 'N'
    then ''
    else
	  case
	    when ta.deleteauth in ('Y', 'G') then '/D'
		else '/' concat coalesce(ta.deleteauth, '-')
      end
end
concat
-- Check: ALTERAUTH
case
  when ta.alterauth  in ('Y', 'G') then '/A'
  else '/' concat coalesce(ta.alterauth, '-')
end
concat
-- Check: REFAUTH
case
  when qt.target_type = 1 and qt.has_loadphase = 'I'
   and (red.RELNAME is not null or rep.RELNAME is not null)
  then
    case
-- change before execution ---------------------------------------------
-- DB2 LUW
	   when ta.refauth  in ('Y', 'G') then '/R'
	   else '/' concat coalesce(ta.refauth, '-')
-- DB2 z/OS
--	   when ta.referencesauth  in ('Y', 'G') then '/R'
--	   else '/' concat coalesce(ta.referencesauth, '-')
-- change before execution ---------------------------------------------
    end
  else ''
end
as effective_tabauth,

case
  when ta.insertauth in ('Y', 'G') then ''
  else ',INSERT '
end
concat
case
  when qt.target_type = 2 and qt.ccd_condensed = 'N'
    then ''
  else
    case
      when ta.updateauth in ('Y', 'G') then ''
	  else ',UPDATE '
	end
end
concat
case
  when qt.target_type = 2 and qt.ccd_condensed = 'N'
    then ''
    else
	  case
	    when ta.deleteauth in ('Y', 'G') then ''
		else ',DELETE '
      end
end
concat
case
  when ta.alterauth  in ('Y', 'G') then ''
  else ',ALTER '
end
concat
case
  when qt.target_type = 1 and qt.has_loadphase = 'I'
   and (red.RELNAME is not null or red.RELNAME is not null)
  then
    case
-- change before execution ---------------------------------------------
-- DB2 LUW
	   when ta.refauth  in ('Y', 'G') then ''
	   else ',REFERENCES '
-- DB2 z/OS
--	   when ta.referencesauth  in ('Y', 'G') then ''
--	   else ',REFERENCES '
-- change before execution ---------------------------------------------
	end
  else ''
end
as grantprivstring

from

ibmqrep_targets qt

-- 13.06.17: only target tables which exist
inner join sysibm.systables st
on  qt.target_owner = st.creator
and qt.target_name  = st.name

-- 22.09.17: check if foreign keys exist (dependent tables)
-- this should be obsolete due to the DB2 documentation (GRANT)
-- but kept until tested
left outer join SYSIBM.SYSRELS red
on  qt.target_owner = red.creator
and qt.target_name  = red.tbname

-- 11.10.17: check if foreign keys exist (parent tables)
-- this is the correct check which is required
left outer join SYSIBM.SYSRELS rep
on  qt.target_owner = rep.reftbcreator
and qt.target_name  = rep.reftbname

left outer join SYSIBM.SYSTABAUTH ta
on  qt.target_owner = ta.TCREATOR
and qt.target_name  = ta.TTNAME

and ta.grantee IN

-- change before execution BEGIN ---------------------------------------
-- Set: Apply User
--
-- Setting for CLENKE Testumgebung
-- ('ASNQAPP', 'PUBLIC')

-- Setting for Tests
-- ('Q1D26E99', 'PUBLIC')

-- Setting for customized Apply User
 ('Q1D26' concat substr(current server , 7 , 1) concat '99', 'PUBLIC')
-- change before execution END -----------------------------------------

) y

where y.expected_tabauth <> y.effective_tabauth

UNION
-- Query 260:
--    DE: Finde alle Child Tabellen replizierter Parent Tabellen
--    in einer RI Beziehung, die nicht (oder über eine andere
--    Queue) replizieren
--    EN: Find all child tables of a replicated parent table
--    which are not being replicated (via the same queue)

select
260 as ordercol,
'ASNQAPP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'A-RIP' as MTYP,
qt.subname,
'ERROR' as SEV,

'QSUB ' concat trim(qt.subname) concat ' (' concat qt.state concat ')'

-- DE
-- concat ' Tabelle '
-- concat trim(st.creator) concat '.' concat trim(st.name)
-- concat ' ist Child in RI-Beziehung. Subscription fuer Parent Tabelle'
-- concat ' fehlt oder repliziert über eine andere Queue.' as MTXT,
-- DE

-- EN
 concat ' Table '
 concat trim(st.creator) concat '.' concat trim(st.name)
 concat ' is child in RI-relationship. Subscription for parent table'
 concat ' missing or replicates via a different queue.' as MTXT,
-- EN

-- DE
-- 'CREATE QSUB fuer Parent Tabelle '
-- concat rtrim(red.reftbcreator) concat '.'
-- concat rtrim(red.reftbname) as FIXIT,
-- DE

-- EN
 'CREATE QSUB for parent table '
 concat rtrim(red.reftbcreator) concat '.'
 concat rtrim(red.reftbname) as FIXIT,
-- EN

-- DEBUG
qt.state,
qt.target_owner, qt.target_name,
substr(st.creator , 1 , 18) as tbcreator,
substr(st.name , 1 , 18) as tbname,
cast(null as varchar(128)) as COLNAME

from ibmqrep_targets qt

-- only target tables which exist
inner join sysibm.systables st
on  qt.target_owner = st.creator
and qt.target_name  = st.name

-- check if target table is dependent table in RI relationship
inner join SYSIBM.SYSRELS red
on  qt.target_owner = red.creator
and qt.target_name  = red.tbname

left outer join ibmqrep_targets qt2
on  red.reftbcreator = qt2.target_owner
and red.reftbname    = qt2.target_name

where qt2.target_name is null or qt.recvq <> qt2.recvq

UNION
-- Query 261:
--    DE: Finde alle Parent Tabellen replizierter Child Tabellen
--    in einer RI Beziehung, die nicht (oder über eine andere
--    Queue) replizieren
--    EN: Find all parent tables of a replicated child table
--    which are not being replicated (via the same queue)

select
261 as ordercol,
'ASNQAPP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'A-RIC' as MTYP,
qt.subname,
'ERROR' as SEV,

'QSUB ' concat trim(qt.subname) concat ' (' concat qt.state concat ')'

-- DE
-- concat ' Tabelle '
-- concat trim(st.creator) concat '.' concat trim(st.name)
-- concat ' ist Parent in RI-Beziehung. Subscription fuer Child Tabelle'
-- concat ' fehlt oder repliziert über eine andere Queue.' as MTXT,
-- DE

-- EN
 concat ' Table '
 concat trim(st.creator) concat '.' concat trim(st.name)
 concat ' is parent in RI-relationship. Subscription for child table'
 concat ' missing or replicates via a different queue.' as MTXT,
-- EN

-- DE
-- 'CREATE QSUB fuer Child Tabelle '
-- concat rtrim(rep.creator) concat '.'
-- concat rtrim(rep.tbname) as FIXIT,
-- DE

-- EN
 'CREATE QSUB for child table '
 concat rtrim(rep.creator) concat '.'
 concat rtrim(rep.tbname) as FIXIT,
-- EN

-- DEBUG
qt.state,
qt.target_owner, qt.target_name,
substr(st.creator , 1 , 18) as tbcreator,
substr(st.name , 1 , 18) as tbname,
cast(null as varchar(128)) as COLNAME

from ibmqrep_targets qt

-- only target tables which exist
inner join sysibm.systables st
on  qt.target_owner = st.creator
and qt.target_name  = st.name

-- check if target table is parent table in RI relationship
inner join SYSIBM.SYSRELS rep
on  qt.target_owner = rep.reftbcreator
and qt.target_name  = rep.reftbname

left outer join ibmqrep_targets qt2
on  rep.creator   = qt2.target_owner
and rep.tbname    = qt2.target_name

where qt2.target_name is null or qt.recvq <> qt2.recvq


UNION
-- Query 270:
--    DE: Finde alle Subscriptions, bei denen die Datentypen der
--    Before und After Image Spalten abweichen
--    EN: Find all subscriptions with a data type mismatch between
--    after image and before image column

select
270 as ordercol,
'ASNQAPP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'A-BID' as MTYP,
y.subname,
'ERROR' as SEV,

case
  when check = 'BEF_IMG_TYPE_MISMATCH'
  then

-- DE
--    'QSUB ' concat trim(y.subname)
--	concat ' (' concat y.state concat ')'
--	concat ' Datentyp-Abweichung fuer Before Image Spalte'
--	concat ' fuer Ziel-Tabelle '
--	concat trim(y.target_owner) concat '.'
--	concat trim(y.target_name)
--	concat ' Before Image Spalte ' concat trim(y.bef_img_colname)
--	concat '. Datentyp angleichen.'
-- DE

-- EN
    'QSUB ' concat trim(y.subname)
	concat ' (' concat y.state concat ')'
	concat ' Data type mismatch for before image column '
	concat ' for target tyble '
	concat trim(y.target_owner) concat '.'
	concat trim(y.target_name)
	concat ' before image column ' concat trim(y.bef_img_colname)
	concat '. Align data type.'
-- EN

  when check = 'BEF_IMG_NOT_NULL'
  then

-- DE
--    'QSUB ' concat trim(y.subname)
--	concat ' (' concat y.state concat ')'
--	concat ' Before Image Spalte ist NOT NULL definiert fuer'
--	concat ' Ziel-Tabelle '
--	concat trim(y.target_owner) concat '.'
--	concat trim(y.target_name)
--	concat ' Before Image Spalte ' concat trim(y.bef_img_colname)
--	concat ' (' concat y.bef_img_nulls concat ')'
--	concat '. Aenderung auf NULLABLE erforderlich.'
-- DE

-- EN
    'QSUB ' concat trim(y.subname)
	concat ' (' concat y.state concat ')'
	concat ' Before image column is defined NOT NULL'
	concat ' for target table '
	concat trim(y.target_owner) concat '.'
	concat trim(y.target_name)
	concat ' before image column ' concat trim(y.bef_img_colname)
	concat '(' concat y.bef_img_nulls concat ')'
	concat '. Change to NULLABLE.'
-- EN

  else NULL
end as MTXT,

case
  when check = 'BEF_IMG_TYPE_MISMATCH'
  then
    'ALTER TABLE '
    concat  trim(y.target_owner) concat '.' concat trim(y.target_name)
	concat ' ALTER COLUMN '
	concat trim(y.bef_img_colname)
    concat ' SET DATA TYPE '
    concat trim(y.aft_img_TYPE)
	concat case
	          when y.aft_img_TYPE in ('CHAR', 'VARCHAR', 'DECIMAL')
			  then ' (' concat trim(char(y.aft_img_length))
			  else ''
		   end
	concat case
	          when y.aft_img_TYPE in ('DECIMAL')
			  then ' , ' concat trim(char(y.aft_img_scale))
			  else ''
		   end
	concat case
	          when y.aft_img_TYPE in ('CHAR', 'VARCHAR', 'DECIMAL')
			  then ')'
			  else ''
		   end
     concat ';'
  when check = 'BEF_IMG_NOT_NULL'
  then
    'ALTER TABLE '
    concat  trim(y.target_owner) concat '.' concat trim(y.target_name)
	concat ' ALTER COLUMN '
	concat trim(y.bef_img_colname)
	concat ' DROP NOT NULL;'
  else NULL
end as FIXIT,

-- DEBUG
y.state,
y.target_owner, y.target_name,
substr(y.tbcreator , 1 , 18) as tbcreator,
substr(y.tbname , 1 , 18) as tbname,
y.bef_img_colname as COLNAME

from

(


select
 c1.subname
,c1.state
,c1.repqmapname
,case
   when (c1.bef_img_TYPE != c2.aft_img_TYPE)
   or   (c1.bef_img_length != c2.aft_img_length)
   or   (c1.bef_img_scale != c2.aft_img_scale)
   then 'BEF_IMG_TYPE_MISMATCH'
   when (c1.bef_img_nulls = 'N')
   then 'BEF_IMG_NOT_NULL'
   else NULL
 end as check
,c1.target_owner as target_owner
,c1.target_name  as target_name
,c1.tbcreator
,c1.tbname
,c1.aft_img_colname
,c1.bef_img_colname
,c1.bef_img_TYPE
,c2.aft_img_TYPE
,c1.bef_img_length
,c2.aft_img_length
,c1.bef_img_scale
,c2.aft_img_scale
,c1.bef_img_nulls
,c2.aft_img_nulls


from

(select
 qt.subname
,qt.state
,rq.repqmapname
,qt.recvq
,qt.TARGET_OWNER
,qt.TARGET_NAME
,sc.tbcreator
,sc.tbname
,qc.TARGET_COLNAME   as aft_img_colname
,qc.BEF_TARG_COLNAME as bef_img_colname
,COLTYPE as bef_img_TYPE
,LENGTH as bef_img_length
,SCALE as bef_img_scale
,NULLS as bef_img_nulls

from sysibm.syscolumns sc,
     ibmqrep_trg_cols  qc,
	 ibmqrep_targets qt,
	 ibmqrep_recvqueues rq

where qt.subname = qc.subname
  and qt.recvq   = qc.recvq
  and qt.TARGET_OWNER = sc.TBCREATOR
  and qt.TARGET_NAME  = sc.TBNAME
  and qc.BEF_TARG_COLNAME  = sc.name
  and qt.recvq = rq.recvq

) c1

left outer join

(select
 qt.subname
,qt.recvq
,qt.TARGET_OWNER
,qt.TARGET_NAME
,qc.TARGET_COLNAME   as aft_img_colname
,qc.BEF_TARG_COLNAME as bef_img_colname
,COLTYPE as aft_img_TYPE
,LENGTH as aft_img_length
,SCALE as aft_img_scale
,NULLS as aft_img_nulls

from sysibm.syscolumns sc,
     ibmqrep_trg_cols qc,
	 ibmqrep_targets qt,
	 ibmqrep_recvqueues rq

where qt.subname = qc.subname
  and qt.recvq   = qc.recvq
  and qt.TARGET_OWNER = sc.TBCREATOR
  and qt.TARGET_NAME  = sc.TBNAME
  and qc.TARGET_COLNAME  = sc.name
  and qt.recvq = rq.recvq

) c2

on  c1.subname               = c2.subname
and c1.recvq                 = c2.recvq
and c1.TARGET_OWNER          = c2.TARGET_OWNER
and c1.TARGET_NAME           = c2.TARGET_NAME
and c1.aft_img_COLNAME       = c2.aft_img_COLNAME
and c1.bef_img_COLNAME       = c2.bef_img_COLNAME

) y

where check is not null

UNION
-- Query 280:
--    DE: Finde alle CCD Subscriptions mit einer BLOB oder XML 
--    Spalte die in der Zieltabelle als NOT NULL definiert ist
--    EN: Find all CCD subscriptions with a BLOB or XML column
--    which is defined as NOT NULL in the target table

select
280 as ordercol,
'ASNQAPP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'A-BXN' as MTYP,
qt.subname,
'ERROR' as SEV,

-- DE
-- 'QSUB ' concat trim(qt.subname) 
-- concat ' (' concat qt.state concat ')'
-- concat ' ist eine'
-- concat case 
--          when qt.ccd_condensed = 'Y' then ' condensed ' 
-- 		 else ' non-condensed' 
-- 	   end
-- concat ' CCD Subscription. Die Zieltabelle enthält die '
-- concat trim(sc.coltype) concat '-Spalte ' concat trim(sc.name) 
-- concat ', die NOT NULL definiert ist. ' 
-- concat case 
--          when qt.ccd_condensed = 'Y' 
--            then ' Dies kann im Rahmen eines Initial Loads zu'
-- 		   concat ' Fehlern führen, wenn DELETE Log Records'
-- 		   concat ' repliziert werden.'
-- 		 else ' Dies ist nicht zulässig wenn DELETE Log Records '
-- 		   concat 'repliziert werden sollen.' 
-- 	   end
--   as MTXT,


-- EN
'QSUB ' concat trim(qt.subname) 
concat ' (' concat qt.state concat ')'
concat ' is a'
concat case 
         when qt.ccd_condensed = 'Y' then ' condensed ' 
		 else ' non-condensed' 
	   end
concat ' CCD subscription. The target table coltains '
concat trim(sc.coltype) concat '-column ' concat trim(sc.name) 
concat ', which is defined as NOT NULL. ' 
concat case 
         when qt.ccd_condensed = 'Y' 
           then ' This can cause errors during initial load '
		   concat ' in case DELETE log records have to be replicated'
		   concat ' during the initial load.'
		 else ' This is not valid in case DELETE log records '
		   concat 'have to be replicated.' 
	   end
  as MTXT,


'ALTER TABLE ' concat trim(sc.tbcreator) concat '.' 
concat trim(sc.tbname) concat ' ALTER COLUMN ' 
concat trim(sc.name)  concat ' DROP NOT NULL' as FIXIT,


-- DEBUG
qt.state,
qt.target_owner, qt.target_name,
substr(sc.tbcreator , 1 , 18) as tbcreator,
substr(sc.tbname , 1 , 18) as tbname,
substr(sc.name , 1 , 18) as COLNAME

from ibmqrep_targets qt,
     ibmqrep_trg_cols qc,
     sysibm.syscolumns sc

where qt.subname = qc.subname
  and qt.recvq   = qc.recvq
  and qt.TARGET_OWNER = sc.TBCREATOR
  and qt.TARGET_NAME = sc.TBNAME
  and qc.target_colname = sc.name
  and qt.target_type = 2
  and sc.coltype in ('XML', 'BLOB', 'CLOB')
  and sc.nulls = 'N'


) x

order by x.ordercol, x.subname, x.colname

with ur

;

-- set current schema = user;