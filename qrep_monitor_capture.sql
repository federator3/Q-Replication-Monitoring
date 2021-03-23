--#SET TERMINATOR  ;
-- ---------------------------------------------------------------------
-- Q Capture Monitor
-- Report zum schnellen Aufspüren von Unterbrechungen und
-- Ausnahmebedingungen bei der Q Replication. Queries:
--   100: C-OPE - Capture operational
--   110: C-LAT - Capture latency
--   130: C-SQU - Send Queue status
--   140: C-SUB - Inactive / new subscriptions
-- ---------------------------------------------------------------------
-- Der Report ist sowohl am Quell- aus auch am Zielsystem periodisch
-- auszuführen (z.B. alle 5 Minuten). Spalte SEVerity zeigt an,
-- ob es sich bei der angezeigten Zeile um eine INFO, eine WARNING
-- oder um einen zu korrigierenden ERROR handelt.
-- ---------------------------------------------------------------------
-- Anpassung vor Inbetriebnahme: (search "-- change")
-- SET CURRENT SCHEMA anpassen und durch
-- verwendete Capture- bzw. Apply-Schema ersetzen
-- ---------------------------------------------------------------------
-- Abschnitte "Special: only when used with subscription generator"
-- löschen, wenn die Query nicht in Verbindung mit dem Q Replication
-- Subscription Generator genutzt wird
-- ---------------------------------------------------------------------
-- NEU: Parameterisierung von Thresholds (z.B. Latency-Threshold über
-- die Tabelle QREP_MON_CAPTURE_PARM, die im Capture Server im
-- gleichen Schema wie die Q Rep Control Tables angelegt werden muss
-- DDL-Skript zum Anlegen der Tabelle: QREP_MON_CAPTURE_PARM.sql
-- ---------------------------------------------------------------------
-- Status: In Erprobung
--
-- ---------------------------------------------------------------------
-- Änderungen / Ergänzungen
--  - 27.04.2017: German and English Messages (EN currently commented)
--  - 03.05.2017: Column heading Change (AMPEL_CHECK_TS -> CHECK_TS)
--  - 03.05.2017: Meldung C-SUB, n von m Subscriptions fuer XYZ inaktiv.
--  - 23.05.2017: Name changed from qrep_Ampel_neu_Capture.sql to
--                qrep_monitor_capture.sql
--  - 24.05.2017: Num Subs per Queue in Q130
--  - 08.06.2017: Message C-LAT (Q110): Memory usage
--  - 22.10.2017: Message C-SUB (Q140): Distinguishing between inactive
--                and new subscriptions
--  - 22.10.2017: Message C-SQU (Q130): Now counting subs by state
--  - 23.04.2018: Message C-SUB (Q140): Option to detect if inactive
--                subs are TEMP_DEACTIVATED = 'Y' via join with
--                ASNCLP_Q_BASE. If all inactive subs have
--                TEMP_DEACTIVATED = 'Y' the severity of message C-SUB
--                is INFO instead of WARNING (only applicable when
--                being used in conjunction with the Q Rep Subscription
--                Generator
--  - 03.12.2020: Added A-OPE Operational State
--  - 09.12.2020: new algorithm to calculate the capture latency
--  - 10.12.2020: Message C-SUB (Q140): now issued for all queues, even
--                for those with no subscription
--  - 13.01.2021: C-LAT Latencies < 0 secs always displayed with a
--                leading 0. Example: 0.1 instead of .1
--  - 15.01.2020: Control of thresholds (e.g. latency threshold) via
--                parameters table QREP_MON_CAPTURE_PARM
-- ---------------------------------------------------------------------

-- change before execution ---------------------------------------------
-- connect to '<capture_server>';
-- set current schema = '<capture_schema>';
-- change before execution ---------------------------------------------

-- uncomment the following line (CREATE VIEW) when using CREATE VIEW -
-- comment when used as query
-- create view QREP_MONITOR_CAPTURE as


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

from

(

-- ---------------------------------------------------------------------
-- Q CAPTURE -----------------------------------------------------------

-- ---------------------------------------------------------------------
-- Query 100:

--    DE: Komponente: Q Capture
--    Ausschnitt: Capture operationaler Status
--    EN: Component: Q Capture
--    Section: Capture operational state

select

100 as ordercol,
'ASNQCAP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'C-OPE' as MTYP,

case when y.MONITOR_TIME <
       y.EXPECTED_TS_LAST_MONITOR_RECORD - 5 seconds
     then 'ERROR'
     else 'INFO'
end as SEV,

case when y.MONITOR_TIME <
       y.EXPECTED_TS_LAST_MONITOR_RECORD - 5 seconds
-- DE
       then 'Q Capture nicht in Betrieb oder gestoert seit '
          concat trim(VARCHAR(y.MONITOR_TIME)) concat '.'
-- EN
--     then 'Q Capture down or not operational since '
--        concat trim(VARCHAR(y.MONITOR_TIME)) concat '.'
-- DE
     else 'Q Capture in Betrieb '
-- EN
--   else 'Q Capture operational '
end as MTXT

from

(

select
cm.monitor_time,
current timestamp - (dec(cp.monitor_interval) / 1000) seconds
  AS EXPECTED_TS_LAST_MONITOR_RECORD

from ibmqrep_capmon cm,
     ibmqrep_capparms cp

-- only the most current rows
where cm.monitor_time = (select max(monitor_time)
                         from ibmqrep_capmon)

) y

UNION

-- ---------------------------------------------------------------------
-- Query 110:
--    DE: Komponente: Q Captuure
--    Ausschnitt: Capture Latency
--    EM: Component: Q Capture
--    Section: Capture Latency

select
110 as ordercol,
'ASNQCAP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'C-LAT' as MTYP,

case when y.CAPTURE_LATENCY_SEC > clat_thresh_error
     then 'ERROR'
     when y.CAPTURE_LATENCY_SEC > clat_thresh_warning
     then 'WARNING'
     else 'INFO'
end as SEV,

case when y.CAPTURE_LATENCY_SEC > clat_thresh_error
-- DE
     then 'Q Capture Latenz > '
             concat trim(varchar(clat_thresh_error))
             concat ' Sekunden. CAPTURE_LATENCY='

-- EN
--     then 'Q Capture latency > '
--           concat trim(varchar(clat_thresh_error))
--           concat ' seconds. CAPTURE_LATENCY='

          concat trim(VARCHAR(y.CAPTURE_LATENCY_SEC))
          concat ' s, MEMORY:'
          concat trim(VARCHAR(y.CURRENT_MEMORY_MB))
          concat '/'
          concat varchar(y.memory_limit)
          concat ' MB, TRANS_SPILLED='
          concat trim(VARCHAR(y.TRANS_SPILLED)) concat '.'


     when y.CAPTURE_LATENCY_SEC > clat_thresh_warning
-- DE
     then 'Q Capture Latenz > '
             concat trim(varchar(clat_thresh_warning))
             concat ' Sekunden. CAPTURE_LATENCY='

-- EN
--     then 'Q Capture latency > '
--           concat trim(varchar(clat_thresh_warning))
--           concat ' seconds. CAPTURE_LATENCY='

          concat trim(VARCHAR(y.CAPTURE_LATENCY_SEC))
          concat ' s, MEMORY:'
          concat trim(VARCHAR(y.CURRENT_MEMORY_MB))
          concat '/'
          concat varchar(y.memory_limit)
          concat ' MB, TRANS_SPILLED='
          concat trim(VARCHAR(y.TRANS_SPILLED)) concat '.'

-- DE
     else 'Q Capture Latenz ok (Cature Latency < '
           concat trim(varchar(clat_thresh_warning))
           concat 's). '
-- EN
--     else 'Q Capture latency ok (Cature Latency < '
--           concat trim(varchar(clat_thresh_warning))
--           concat 's). '

          concat 'CAPTURE_LATENCY=' concat
          case when y.CAPTURE_LATENCY_SEC is null then 'UNKNOWN'
               when y.CAPTURE_LATENCY_SEC < 1 then
                  '0' concat trim(VARCHAR(y.CAPTURE_LATENCY_SEC))
               else trim(VARCHAR(y.CAPTURE_LATENCY_SEC))
          end
          concat ' s'
          concat' , MEMORY: '
          concat trim(VARCHAR(y.CURRENT_MEMORY_MB))
          concat '/'
          concat varchar(y.memory_limit)
          concat ' MB, TRANS_SPILLED='
          concat trim(VARCHAR(y.TRANS_SPILLED)) concat '.'
end as MTXT

from

(

select
current timestamp - (dec(monitor_interval) / 1000) seconds
  AS EXPECTED_TS_LAST_MONITOR_RECORD,
cm.monitor_time,
cp.monitor_interval,
qmp.clat_thresh_warning,
qmp.clat_thresh_error,

-- 09.12.2020: new logic to calculate the capture latency using
-- TIMESTAMPDIFF(2, ...) which calculates the differnce in seconds.
-- Explicitly, TIMESTAMPDIFF(1, ...) - microseconds - was not used
-- to prevent overflows. Instead, the difference in microseconds ist
-- added to the difference in seconds to get a more precise latency
-- value and preventing overflows. Max. difference without overflow:
-- 68 years (due to integer limits)

case
-- a) '1900-01-01-00.00.00.000000'  when log reader is not yet ready
  when cm.current_log_time = '1900-01-01-00.00.00.000000'
    then NULL
  when microsecond(cm.MONITOR_TIME) >= microsecond(cm.current_log_time)
-- difference in seconds plus difference in microseconds (1 digit only)
    then TIMESTAMPDIFF(2, CHAR(cm.MONITOR_TIME - cm.current_log_time))
            + dec((dec(microsecond(cm.MONITOR_TIME)
                - microsecond(cm.current_log_time)) / 1000000) , 2 , 1)
-- b) microsecond(cm.MONITOR_TIME) < microsecond(cm.current_log_time)
-- difference in microseconds (1 digit only) negative in this case.
-- Therefore, the negative value is added to the difference in seconds
-- plus 1
  else TIMESTAMPDIFF(2, CHAR(cm.MONITOR_TIME - cm.current_log_time))
            + 1 + dec((dec(microsecond(cm.MONITOR_TIME)
                - microsecond(cm.current_log_time)) / 1000000) , 2 , 1)
end AS CAPTURE_LATENCY_SEC,

-- this logic calculated the wrong difference between the 2 timestamps
-- when midnight was between the 2 timestamps
-- case
--  when cm.current_log_time <> '1900-01-01-00.00.00.000000' then
--   dec(dec(microsecond(cm.MONITOR_TIME - cm.CURRENT_LOG_TIME))
--               / 1000000
--   + SECOND(cm.MONITOR_TIME - cm.CURRENT_LOG_TIME)
--   + ((MINUTE(cm.MONITOR_TIME - cm.CURRENT_LOG_TIME)*60) )
--   + (HOUR(cm.MONITOR_TIME - cm.CURRENT_LOG_TIME)*3600)
--   + ((DAYS(cm.MONITOR_TIME)
--                   - DAYS(cm.CURRENT_LOG_TIME))*86400) , 12 , 1)
-- else null
-- end as CAPTURE_LATENCY_SEC,

dec(dec(cm.CURRENT_MEMORY) / 1024 / 1024, 5 , 0)
  as CURRENT_MEMORY_MB,
cp.memory_limit,
cm.TRANS_SPILLED
from ibmqrep_capmon cm,
     ibmqrep_capparms cp,
     QREP_MON_CAPTURE_PARM qmp

where cm.monitor_time = (select max(monitor_time)
                         from ibmqrep_capmon)

) y

UNION

-- Query 130:
--    DE: Komponente: Q Capture
--    Ausschnitt: Send Queue Status
--    DE: Component: Q Capture
--    Section: Send Queue Status

select
130 as ordercol,
'ASNQCAP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'C-SQU' as MTYP,
case when y.state <> 'A'
     then 'ERROR'
     else 'INFO'
end as SEV,

case when y.state <> 'A'

     then 'Send Queue ' concat trim(y.SENDQ)

-- DE
          concat ' inaktiv (STATE=' concat y.state concat ') seit '
-- EN
--        concat ' inactive (STATE=' concat y.state concat ') since '

          concat trim(varchar(y.STATE_TIME))
          concat ' (#subs A/I/N/O: '
          concat trim(varchar(coalesce(z.num_subs_a , 0)))
          concat '/' concat trim(varchar(coalesce(z.num_subs_i , 0)))
          concat '/' concat trim(varchar(coalesce(z.num_subs_n , 0)))
          concat '/' concat trim(varchar(coalesce(z.num_subs_o , 0)))
          concat '). XMITQDEPTH=' concat trim(VARCHAR(y.XMITQDEPTH))
          concat '.'

     else 'Send Queue ' concat trim(y.SENDQ)

-- DE
          concat ' aktiv.'
-- EN
--        concat ' active.'

          concat ' (#subs A/I/N/O: '
          concat trim(varchar(coalesce(z.num_subs_a , 0)))
          concat '/' concat trim(varchar(coalesce(z.num_subs_i , 0)))
          concat '/' concat trim(varchar(coalesce(z.num_subs_n , 0)))
          concat '/' concat trim(varchar(coalesce(z.num_subs_o , 0)))
          concat '). XMITQDEPTH=' concat trim(VARCHAR(y.XMITQDEPTH))
          concat '.'
end as MTXT

from

(

select
sq.sendq, sq.state, sq.state_time,
cqm.XMITQDEPTH

from

ibmqrep_sendqueues sq,
ibmqrep_capqmon cqm

where sq.sendq = cqm.sendq
  and cqm.monitor_time = (select max(monitor_time)
                          from ibmqrep_capqmon t
                          where t.sendq = sq.sendq)

) y

left outer join

(

-- 16.11.2017
-- number of subscriptions by state (A/I/N/other) per sendq

select
s2.sendq,
coalesce(max(DECODE(s2.calc_state, 'A', s2.num_subs_calc_state)), 0)
 AS num_subs_a,
coalesce(max(DECODE(s2.calc_state, 'I', s2.num_subs_calc_state)), 0)
 AS num_subs_i,
coalesce(max(DECODE(s2.calc_state, 'N', s2.num_subs_calc_state)), 0)
 AS num_subs_n,
coalesce(max(DECODE(s2.calc_state, 'O', s2.num_subs_calc_state)), 0)
 AS num_subs_o

from

(

select s1.sendq, s1.calc_state,
       sum(num_subs_state) as num_subs_calc_state
from

(
select s.sendq, s.state,
case
  when s.state in ('I', 'A', 'N') then s.state
  else 'O'
end as calc_state,
count(*) as num_subs_state
from ibmqrep_subs s
group by s.sendq, s.state

) s1

group by s1.sendq, s1.calc_state

) s2

group by s2.sendq

) z

on y.sendq = z.sendq


UNION

-- Query 140:
--    DE: Komponente: Q Capture
--    Ausschnitt: Subscription Status
--    DE: Component: Q Capture
--    Section: Subscription state

select
140 as ordercol,
'ASNQCAP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'C-SUB' as MTYP,
case when coalesce(y2.NUM_INACTIVE, 0) = 0 then 'INFO'

-- Special: only when used with subscription generator
--   when coalesce(y2.NUM_INACTIVE, 0) =
--        coalesce(y2.NUM_INACTIVE_TEMP_DEACT , 0) then 'INFO'
-- Special: only when used with subscription generator

     else 'WARNING'
end as SEV,

-- DE
case when y1.num_subs = 0 then
        'Keine Subscriptions fuer SENDQ ' concat trim(y1.SENDQ)
     when (y2.NUM_INACTIVE = 0 or y2.NUM_INACTIVE is null) then
        'Alle relevanten '
        concat trim(VARCHAR(y1.num_subs))
        concat ' Subscriptions fuer SENDQ '
        concat trim(y1.SENDQ) concat ' aktiv. '
     else trim(VARCHAR(y2.NUM_INACTIVE)) concat ' von '
        concat trim(VARCHAR(y1.num_subs))
        concat ' Subscriptions fuer SENDQ '
        concat trim(y1.SENDQ) concat ' inaktiv.'
--      concat ' Davon neu (N): '
--      concat trim(VARCHAR(coalesce(y3.num_new , 0)))
--      concat '.'

-- Special: only when used with subscription generator
--      concat ' Davon TEMP_DEACTIVATED = ''Y'': '
--      concat trim(VARCHAR(coalesce(y2.NUM_INACTIVE_TEMP_DEACT , 0)))
-- Special: only when used with subscription generator


-- EN
-- case when y1.num_subs = 0 then
--        'No subscriptions for SENDQ ' concat trim(y1.SENDQ)
--      when (y2.NUM_INACTIVE = 0 or y2.NUM_INACTIVE is null) then
--        'All '
--        concat trim(VARCHAR(y1.num_subs))
--        concat ' subscriptions for SENDQ '
--        concat trim(y1.SENDQ) concat ' active.'
--      else trim(VARCHAR(y2.NUM_INACTIVE)) concat ' of '
--        concat trim(VARCHAR(y1.num_subs))
--        concat ' subscriptions for SENDQ '
--        concat trim(y1.SENDQ) concat ' inactive.'
--        -- concat ' Among those new (N): '
--        -- concat trim(VARCHAR(y3.num_new))
--        -- concat '.'

-- Special: only when used with subscription generator
--        concat ' Among those TEMP_DEACTIVATED = ''Y'': '
--        concat trim(VARCHAR(coalesce(y2.NUM_INACTIVE_TEMP_DEACT , 0)))
-- Special: only when used with subscription generator

end as MTXT

from

(

select q.sendq, coalesce(s.all_subs, 0) AS NUM_SUBS
from IBMQREP_SENDQUEUES q

left outer join

(

select sendq, count(*) as all_subs
from ibmqrep_subs
group by sendq

)s

on q.sendq = s.sendq

) y1

left outer join

(
-- Anzahl inaktive Subs
select su.sendq,

count(*) AS NUM_INACTIVE

-- Special: only when used with subscription generator
-- , sum(case when temp_deactivated = 'Y' then 1 else 0 end)
--  as NUM_INACTIVE_TEMP_DEACT
-- Special: only when used with subscription generator

from ibmqrep_subs su

-- Special: only when used with subscription generator
-- , asnclp_q_base qb
-- Special: only when used with subscription generator

where su.state <> 'A'

-- Special: only when used with subscription generator
-- and su.source_owner = qb.tabschema
-- and su.source_name  = qb.tabname
-- and su.target_owner = qb.target_schema
-- and su.target_name  = qb.target_name
-- Special: only when used with subscription generator

group by su.sendq

) y2

on y1.sendq = y2.sendq

left outer join

(
-- Anzahl neuen Subs
select su.sendq, count(*) AS NUM_NEW
from ibmqrep_subs su
where su.state = 'N'
group by su.sendq
) y3

on y1.sendq = y3.sendq

) x

-- comment the following 2 lines (order by / with ur) when
-- using CREATE VIEW - uncomment when used as query
order by x.ordercol
with ur
;

-- set current schema = user;