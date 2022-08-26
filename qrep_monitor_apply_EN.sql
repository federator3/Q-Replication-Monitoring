--#SET TERMINATOR  ;
-- ---------------------------------------------------------------------
-- Q Apply Monitor
-- Report to quickly detect disruptions and exceptions for Q Apply
-- Queries:
--   000: A-VER - Apply Monitor version information
--   200: A-OPE - Apply operational
--   210: A-RQU - Receive Queue status
--   220: A-LAT - Apply latency
--   232: A-CHB - Queue Heartbeat
--   235: A-EXC - Number of exceptions within exception time window
--   240: A-SUB - Inactive / new subscriptions
-- ---------------------------------------------------------------------
-- The report should be executed at the Q Apply server periodically
-- (e.g., every 5 minutes). Report column SEVerity indicates, whether
-- the reported row is just an INFOrmation, or if it's a WARNING or
-- even an ERROR situation.
-- ---------------------------------------------------------------------
-- Customization: (search "-- change")
-- SET CURRENT SCHEMA to reflect your Q Capture schema
-- ---------------------------------------------------------------------
-- NEW: Parameterization of thresholds (e.g. latency-threshold via
-- Db2 table QREP_MON_APPLY_PARM, located in the Apply Server
-- (same schema as Q Rep control tables)
-- DDL-script to create the table: QREP_MON_APPLY_PARM.sql
-- ---------------------------------------------------------------------
-- Status: testing - no warrenty
--
-- ---------------------------------------------------------------------
-- Änderungen / Ergänzungen
--  - 27.04.2017: German and English Messages (EN currently commented)
--  - 03.05.2017: Column heading Change (AMPEL_CHECK_TS -> CHECK_TS)
--  - 08.05.2017: OLDEST_TRANS zu Message A-RQU hinzugefügt
--  - 23.05.2017: Name changed from qrep_Ampel_neu_Apply.sql to
--                qrep_monitor_apply.sql
--  - 24.05.2017: Num subs per queue in Q230
--  - 24.05.2017: Message A-LAT (Q210): SUM rows applied
--  - 08.06.2017: Message A-RQU (Q230): Memory usage per Queue
--  - 22.10.2017: Message A-RQU (Q230): Join changed from inner to left
--                outer to list queues in error which have never have
--                been successfully activated before (e.g. due to MQ
--                error) - which have no APPLYMON row
--  - 22.10.2017: Message A-RQU (Q230): Now counting subs by state
--  - 21.11.2017: Message A-LAT (Q210): Wrong text 'CAPTURE_LATENCY'
--                changed to 'END2END_LATENCY'
--  - 02.04.2019: Query 235 (EXC) replaced. New: group by RECVQ, REASON,
--                SQLCODE. New: INFO if no exceptions, WARNING if
--                exceptions.
--  - 02.04.2019: Exception interval changed from 6 hours t0 24 hours
--  - 03.12.2020: Added A-OPE Operational State
--  - 03.12.2020: Order of A-RQU (now Q210) and A-LAT (now 220) flipped
--  - 03.12.2020: A-LAT for every queue
--  - 03.12.2020: A-CHB Heartbeat checking
--  - 10.12.2020: Added sqlcode of most recent exception (only within
--                5 most recent days) to A-RQU (now Q210)
--  - 13.01.2021: A-LAT Latencies < 0 secs always displayed with a
--                leading 0. Example: 0.1 instead of .1
--  - 13.01.2021: A-LAT Added detailed latencies "(E2E=x.ys, C=x.ys,
--                A=x.ys, Q=x.ys)"
--  - 13.01.2021: A-LAT Latency "cause" added (latency_cause)
--  - 15.01.2021: Control of thresholds (e.g. latency threshold) via
--                parameters table QREP_MON_APPLY_PARM
--  - 17.05.2021: Sequence of tables changed in query A-CHB due to
--                SQLCODE=-338 with Db2 z/OS
--  - 20.05.2021: Added Q_PERCENT_FULL to A-RQU and A-LAT
--  - 20.05.2021: ORDER BY changed from x.ordercol to x.ordercol, x.mtxt
--  - 23.06.2021: A-LAT when latency case is UNKNOWN, the severity 
--                remains WARNING, even when end2end_latency eceeds 
--                ALAT_THRESH_ERROR (to prevent alarms when latency 
--                cause cannot be determined). Fine tuning with 
--                ALAT_CAUSE_CAP_QDEPTH and ALAT_CAUSE_APP_QDEPTH
--  - 12.04.2022: A-VER added to visualize the version of the monitor 
--                in the monitor's output
--  - 12.04.2022: A-RQU: Status ERROR when for this queue Apply stopped 
--                the APPLYMON reporting (queue 'stale'), but Apply 
--                continues to report progress for other queues
-- ---------------------------------------------------------------------
-- TODO:
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

from

(
-- ---------------------------------------------------------------------
-- Q APPLY -------------------------------------------------------------

-- Query 000:
--    DE: Komponente: Q Apply
--    Ausschnitt: Apply Monitor version
--    EN: Component: Q Apply
--    Section: Apply Monitor version

select

0 as ordercol,
'ASNQAPP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'A-VER' as MTYP,
'INFO'  as SEV,
'Q Apply Monitor SQL Version 2.0 - 20220412' as MTXT

FROM SYSIBM.SYSDUMMY1

UNION

-- Query 200:
--    DE: Komponente: Q Apply
--    Ausschnitt: Apply operationaler Status
--    EN: Component: Q Apply
--    Section: Apply operational state

select

200 as ordercol,
'ASNQAPP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'A-OPE' as MTYP,

case when y.MONITOR_TIME <
       y.EXPECTED_TS_LAST_MONITOR_RECORD - 5 seconds
     then 'ERROR'
     else 'INFO'
end as SEV,

case when y.MONITOR_TIME <
       y.EXPECTED_TS_LAST_MONITOR_RECORD - 5 seconds
-- DE
--     then 'Q Apply nicht in Betrieb oder gestoert seit '
-- EN
       then 'Q Apply down or not operational since '
          concat trim(VARCHAR(y.MONITOR_TIME)) concat '.'
-- DE
--     else 'Q Apply in Betrieb '
-- EN
       else 'Q Apply operational '
end as MTXT

from

(

select
am.recvq,
am.monitor_time,
current timestamp - (dec(ap.monitor_interval) / 1000) seconds
  AS EXPECTED_TS_LAST_MONITOR_RECORD

from ibmqrep_applymon am,
     ibmqrep_applyparms ap

-- only the most current rows
where am.monitor_time = (select max(monitor_time)
                         from ibmqrep_applymon)

-- only 1 OPE message although there could be multipe APPLYMON rows
-- due to UNION (distinct) and not UNION ALL
) y

UNION

-- Query 220:
--    DE: Komponente: Q Apply
--    Ausschnitt: Apply Latency
--    EN: Component: Q Apply
--    Section: Apply Latency

select

220 as ordercol,
'ASNQAPP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'A-LAT' as MTYP,

case when y.end2end_latency_sec > alat_thresh_error
-- added 23.06.2021 begin
      and latency_cause is not null
-- added 23.06.2021	end
     then 'ERROR'
     when y.end2end_latency_sec > alat_thresh_warning
     then 'WARNING'
     else 'INFO'
end as SEV,

case
when y.end2end_latency_sec > alat_thresh_error
-- DE
--        then 'Q Apply End2End Latenz > '
--         concat trim(varchar(alat_thresh_error))
--         concat ' Sekunden fuer '
--         concat trim(y.recvq)
--         concat '. Verursacher: '

-- EN
         then 'Q Apply End2End latency > '
          concat trim(varchar(alat_thresh_error))
          concat ' seconds for '
          concat trim(y.recvq)
          concat '. Probable cause: '
          concat coalesce(y.latency_cause, 'UNKNOWN')
          concat ' (E2E='
          concat case
                    when y.end2end_latency_sec < 1 then
                       '0' concat trim(VARCHAR(y.end2end_latency_sec))
                    else trim(VARCHAR(y.end2end_latency_sec))
                 end
          concat 's'
          concat ', C='
          concat case
                    when y.capture_latency_sec < 1 then
                       '0' concat trim(VARCHAR(y.capture_latency_sec))
                    else trim(VARCHAR(y.capture_latency_sec))
                 end
          concat 's'
          concat ', A='
          concat case
                    when y.apply_latency_sec < 1 then
                       '0' concat trim(VARCHAR(y.apply_latency_sec))
                    else trim(VARCHAR(y.apply_latency_sec))
                 end
          concat 's'
          concat ', Q='
          concat case
                    when y.qlatency_sec < 1 then
                       '0' concat trim(VARCHAR(y.qlatency_sec))
                    else trim(VARCHAR(y.qlatency_sec))
                 end
          concat 's)'
--        concat ', CURRENT_MEMORY='
--        concat trim(VARCHAR(y.current_memory_mb))
--        concat ' MB'
          concat ', QDEPTH='
          concat trim(VARCHAR(y.qdepth))
          concat ' ('
          concat trim(VARCHAR(y.q_percent_full)) concat '%)'
          concat ', ROWS_APPLIED='
          concat trim(VARCHAR(y.rows_applied))

when y.end2end_latency_sec > alat_thresh_warning
-- DE
--       then 'Q Apply End2End Latenz > '
--        concat trim(varchar(alat_thresh_warning))
--        concat ' Sekunden fuer '
--        concat trim(y.recvq)
--        concat '. Verursacher: '
-- EN
         then 'Q Apply End2End latency > '
          concat trim(varchar(alat_thresh_warning))
          concat ' seconds for '
          concat trim(y.recvq)
          concat '. Probable cause: '
          concat coalesce(y.latency_cause, 'UNKNOWN')
          concat ' (E2E='
          concat case
                    when y.end2end_latency_sec < 1 then
                       '0' concat trim(VARCHAR(y.end2end_latency_sec))
                    else trim(VARCHAR(y.end2end_latency_sec))
                 end
          concat 's'
          concat ', C='
          concat case
                    when y.capture_latency_sec < 1 then
                       '0' concat trim(VARCHAR(y.capture_latency_sec))
                    else trim(VARCHAR(y.capture_latency_sec))
                 end
          concat 's'
          concat ', A='
          concat case
                    when y.apply_latency_sec < 1 then
                       '0' concat trim(VARCHAR(y.apply_latency_sec))
                    else trim(VARCHAR(y.apply_latency_sec))
                 end
          concat 's'
          concat ', Q='
          concat case
                    when y.qlatency_sec < 1 then
                       '0' concat trim(VARCHAR(y.qlatency_sec))
                    else trim(VARCHAR(y.qlatency_sec))
                 end
          concat 's)'
--        concat ', CURRENT_MEMORY='
--        concat trim(VARCHAR(y.current_memory_mb))
--        concat ' MB'
          concat ', QDEPTH='
          concat trim(VARCHAR(y.qdepth))
          concat ' ('
          concat trim(VARCHAR(y.q_percent_full)) concat '%)'
          concat ', ROWS_APPLIED='
          concat trim(VARCHAR(y.rows_applied))

-- DE
-- else 'Q Apply End2End Latenz ok (End2End Latency < '
--       concat trim(varchar(alat_thresh_warning))
--       concat 's) fuer '
-- EN
   else 'Q Apply End2End Latency ok (End2End Latency < '
          concat trim(varchar(alat_thresh_warning))
          concat 's) for '
          concat trim(y.recvq)
          concat ' (E2E='
          concat case
                    when y.end2end_latency_sec < 1 then
                       '0' concat trim(VARCHAR(y.end2end_latency_sec))
                    else trim(VARCHAR(y.end2end_latency_sec))
                 end
          concat 's'
          concat ', C='
          concat case
                    when y.capture_latency_sec < 1 then
                       '0' concat trim(VARCHAR(y.capture_latency_sec))
                    else trim(VARCHAR(y.capture_latency_sec))
                 end
          concat 's'
          concat ', A='
          concat case
                    when y.apply_latency_sec < 1 then
                       '0' concat trim(VARCHAR(y.apply_latency_sec))
                    else trim(VARCHAR(y.apply_latency_sec))
                 end
          concat 's'
          concat ', Q='
          concat case
                    when y.qlatency_sec < 1 then
                       '0' concat trim(VARCHAR(y.qlatency_sec))
                    else trim(VARCHAR(y.qlatency_sec))
                 end
          concat 's)'
--        concat ', CURRENT_MEMORY='
--        concat trim(VARCHAR(y.current_memory_mb))
--        concat ' MB'
          concat ', QDEPTH='
          concat trim(VARCHAR(y.qdepth))
          concat ' ('
          concat trim(VARCHAR(y.q_percent_full)) concat '%)'
          concat ', ROWS_APPLIED='
          concat trim(VARCHAR(y.rows_applied))
end as MTXT


from

(

select
am.recvq,
am.monitor_time,
dec(am.end2end_latency / 1000, 12 , 1) as end2end_latency_sec,
dec(am.capture_latency / 1000, 12 , 1) as capture_latency_sec,
dec(am.apply_latency / 1000, 12 , 1) as apply_latency_sec,
dec(am.qlatency / 1000, 12 , 1) as qlatency_sec,
am.rows_applied,
am.qdepth,
am.q_percent_full,
qmp.alat_thresh_error,
qmp.alat_thresh_warning,
qmp.alat_cause_cap_qdepth,
qmp.alat_cause_app_qdepth,

case
   when end2end_latency / 1000 > qmp.alat_thresh_warning
    and qdepth >= qmp.alat_cause_app_qdepth then 'ASNQAPP'
   when end2end_latency / 1000 > qmp.alat_thresh_warning
    and qdepth <= qmp.alat_cause_cap_qdepth then 'ASNQCAP'
   else NULL
end as latency_cause

from ibmqrep_applymon am
    , (select recvq ,
              max(monitor_time) as latest_record
         from ibmqrep_applymon
        group by recvq ) as lm,
     QREP_MON_APPLY_PARM qmp

where am.monitor_time = lm.latest_record
  and am.recvq = lm.recvq
) y


UNION

-- Query 210:
--    DE: Komponente: Q Apply
--    Ausschnitt: Receive Queue Status
--    EN: Component: Q Apply
--    Section: Receive Queue Status

select
210 as ordercol,
'ASNQAPP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'A-RQU' as MTYP,
case when y.state <> 'A'
     then 'ERROR'
-- stale means: Apply is running (A-OPE is ok), but for this 
-- particular queue no more APPLYMON records are written. Might
-- happen due to Db2 out of memory problem, when even setting
-- RECVQUEUES.STATE = 'I' fails
  	 when y.queue_stale is not NULL
  	 then 'ERROR'
     else 'INFO'
end as SEV,

case when y.state <> 'A'
     then 'Receive Queue ' concat trim(y.RECVQ)
-- DE
--        concat ' inaktiv (STATE=' concat y.state concat ') seit '
-- EN
          concat ' inactive (STATE=' concat y.state concat ') since '

          concat trim(VARCHAR(y.STATE_TIME))
          concat ' (#subs A/I/O: '
          concat trim(varchar(coalesce(z.num_subs_a , 0)))
          concat '/' concat trim(varchar(coalesce(z.num_subs_i , 0)))
          concat '/' concat trim(varchar(coalesce(z.num_subs_o , 0)))
          concat '). QDEPTH='
          concat coalesce(trim(VARCHAR(y.qdepth)) , 'UNKNOWN')
          concat ' (' 		  
		  concat coalesce(trim(VARCHAR(y.q_percent_full)) concat '%)',
  		               'UNKNOWN')		  
          concat ' OLDEST_TRANS='
          CONCAT coalesce(trim(varchar(y.OLDEST_TRANS)) , 'UNKNOWN')
          concat ' MEMORY: '
          concat coalesce(trim(varchar(y.current_memory_mb))
                             , 'UNKNOWN')
          concat '/' concat y.memory_limit concat ' MB'
-- DE
--        concat '. Aktuellste Exception der RECVQ: '
-- EN
          concat '. Most current exception of the RECVQ: '
          concat coalesce(trim(ex.sqlcode)
                         concat ' ('
                         concat varchar(ex.exception_time)
                         concat ')', 'UNKNOWN')
          concat '.'

     else 'Receive Queue ' concat trim(y.RECVQ)
-- DE
--       concat
--       case when y.queue_stale is null
--            then ' aktiv'
-- 		   else ' aktiv, aber kein APPLYMON Fortschritt (stale)'
-- 	  end
-- EN
          concat
          case when y.queue_stale is null
               then ' active'
  			   else ' active, but no progress in APPLYMON (stale)'
  		  end
          concat ' (#subs A/I/O: '
          concat trim(varchar(coalesce(z.num_subs_a , 0)))
          concat '/' concat trim(varchar(coalesce(z.num_subs_i , 0)))
          concat '/' concat trim(varchar(coalesce(z.num_subs_o , 0)))
          concat '). QDEPTH='
          concat coalesce(trim(VARCHAR(y.qdepth)) , 'UNKNOWN')
          concat ' (' 
		  concat coalesce(trim(VARCHAR(y.q_percent_full)) concat '%)',
  		               'UNKNOWN')
          concat ' OLDEST_TRANS='
          CONCAT coalesce(trim(varchar(y.OLDEST_TRANS)) , 'UNKNOWN')
          concat ' MEMORY: '
          concat coalesce(trim(varchar(y.current_memory_mb))
                             , 'UNKNOWN')
          concat '/' concat y.memory_limit concat ' MB'
          concat '.'
end as MTXT

from
-- y
(

select
rq.recvq, rq.state, rq.state_time,
moni.OLDEST_TRANS, moni.qdepth, moni.q_percent_full,
varchar(dec(dec(moni.CURRENT_MEMORY) / 1024 / 1024 , 5 , 0))
  as current_memory_mb,
varchar(rq.memory_limit) as memory_limit,
moni.monitor_time, moni.queue_stale

from

ibmqrep_recvqueues rq

left outer join

-- Most recent monitor data
(

select mon1.recvq, mon1.monitor_time, 
       mon1.expected_ts_last_monitor_record,
	   case 
	     when (mon1.monitor_time <
               mon1.EXPECTED_TS_LAST_MONITOR_RECORD - 5 seconds
			   AND
			   mon1.max_monitor_time_global >= 
			   mon1.EXPECTED_TS_LAST_MONITOR_RECORD - 5 seconds
			  )
         then 1
         else null
	   end as queue_stale,
       mon2.OLDEST_TRANS, mon2.qdepth, mon2.q_percent_full, 
	   mon2.CURRENT_MEMORY
from

(

select recvq, monitor_time, EXPECTED_TS_LAST_MONITOR_RECORD, 
       max_monitor_time_global
from

(
-- highest MONITOR_TIME per queue
select recvq, max(monitor_time) as monitor_time,
current timestamp - (dec(ap.monitor_interval) / 1000) seconds
   AS EXPECTED_TS_LAST_MONITOR_RECORD
  
from ibmqrep_applymon am,
     ibmqrep_applyparms ap
     
group by recvq, 
         current timestamp - (dec(ap.monitor_interval) / 1000) seconds

) qmon

inner join 

(
-- joined with highest MONITOR_TIME over all (all queues)
select max(monitor_time) as max_monitor_time_global
from ibmqrep_applymon

) maxmon

on 1=1

) mon1

-- get APPLYMON details for highest MONITOR_TIME
inner join ibmqrep_applymon mon2

on  mon1.recvq = mon2.recvq
and mon1.monitor_time = mon2.monitor_time

) moni

on  rq.recvq = moni.recvq

) y

left outer join

-- Number of subscriptions
(

-- 16.11.2017
-- number of subscriptions by state (A/I/other) per recvq

select
s2.recvq,
coalesce(max(DECODE(s2.calc_state, 'A', s2.num_subs_calc_state)), 0)
 AS num_subs_a,
coalesce(max(DECODE(s2.calc_state, 'I', s2.num_subs_calc_state)), 0)
 AS num_subs_i,
coalesce(max(DECODE(s2.calc_state, 'O', s2.num_subs_calc_state)), 0)
 AS num_subs_o

from

(

select s1.recvq, s1.calc_state,
       sum(num_subs_state) as num_subs_calc_state
from

(
select s.recvq, s.state,
case
  when s.state in ('I', 'A') then s.state
  else 'O'
end as calc_state,
count(*) as num_subs_state
from ibmqrep_targets s
group by s.recvq, s.state

) s1

group by s1.recvq, s1.calc_state

) s2

group by s2.recvq

) z

on y.recvq = z.recvq

left outer join

-- most current exception - only most recent 5 days
-- added 10.12.2020
(

select e1.exception_time, e1.recvq, e1.sqlcode
from ibmqrep_exceptions e1
inner join
(select recvq, max(exception_time) as maxex
 from ibmqrep_exceptions
 where date(exception_time) > current date - 5 days
 group by recvq) e2
 on e1.recvq = e2.recvq
and e1.exception_time = e2.maxex

) ex

 on y.recvq = ex.recvq

UNION

-- Query 232:
--    DE: Komponente: Q Apply
--    Ausschnitt: Capture Heartbeat messages
--    EN: Component: Q Apply
--    Section: Capture Heartbeat messages

select

232 as ordercol,
'ASNQAPP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'A-CHB' as MTYP,

case when y.num_heartbeats = 0 then 'WARNING'
         else 'INFO'
end as SEV,

case when y.num_heartbeats = 0
-- DE
--         then '0 Heartbeat Messages fuer Queue '
--            concat trim(y.recvq) concat ' in den letzten '
--            concat trim(varchar(heartbeat_time_window))
--            concat ' Minuten. Capture ggf. gestoppt '
--            concat 'oder Send Queue inactiv.'
-- EN
           then '0 Heartbeat messages for queue '
              concat trim(y.recvq) concat ' within the recent '
              concat trim(varchar(heartbeat_time_window))
              concat ' minutes. Capture might be down '
              concat 'or send queue is inactive'
           else trim(y.num_heartbeats) concat ' Heartbeat '
-- DE
--            concat 'Messages fuer Queue ' concat trim(y.recvq)
--            concat ' in den letzten '
--            concat trim(varchar(heartbeat_time_window))
--            concat ' Minuten.'
-- EN
              concat 'messages for Queue ' concat trim(y.recvq)
              concat ' within the recent '
              concat trim(varchar(heartbeat_time_window))
              concat ' minutes.'
end as MTXT


from

(

select
rq.recvq, coalesce(am.num_heartbeats, 0) as num_heartbeats,
qmp.heartbeat_time_window
-- sequence of tables changed due to SQLCODE=-338 with Db2 z/OS
from QREP_MON_APPLY_PARM qmp,
     ibmqrep_recvqueues rq

left outer join

(

select recvq, count(*) as num_heartbeats

from ibmqrep_applymon

where monitor_time >
    (select current timestamp - heartbeat_time_window minutes
       from sysibm.sysdummy1,
            QREP_MON_APPLY_PARM)
  and ( HEARTBEAT_LATENCY > 0
     or END2END_LATENCY > 0 )
group by recvq

) am

on rq.recvq = am.recvq

) y

UNION

-- Query 235:
--    Komponente: Q Apply
--    Ausschnitt: Anzahl Exceptions im Exception Zeitfenster
--    Component: Q Apply
--    Section: Number of exceptions within the exception time window

select
235 as ordercol,
'ASNQAPP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'A-EXC' as MTYP,

case
  when coalesce(num_ex , 0) = 0 then 'INFO'
  else 'WARNING'
end as SEV,

case
  when coalesce(num_ex , 0) = 0 then

-- DE
-- 'Keine Exceptions in den letzen ' concat exception_time_window
--                                   concat ' Stunden.'

-- EN
   'No exceptions in previous ' concat exception_time_window
                                concat ' hours.'

  else

    trim(varchar(num_ex)) concat ' ' concat trim(x.reason)
-- DE
--  concat ' Exceptions fuer Receive Queue ' concat trim(recvq)
--  concat ' in den letzten ' concat exception_time_window
--  concat ' Stunden. '
--  concat 'Details der letzen Exception: '
-- EN
    concat ' exceptions for receive queue ' concat trim(recvq)
    concat ' in previous ' concat exception_time_window
    concat ' hours. '
    concat 'Details of the last exception: '

    concat trim(varchar(x.max_t))
    concat ', SQLCODE '
    concat trim(varchar(x.SQLCODE)) concat ', SUBNAME '
    concat trim(x.subname)

end AS MTXT

from

sysibm.sysdummy1,
QREP_MON_APPLY_PARM

left outer join

(

select a.recvq, a.reason, a.sqlcode, num_ex,
max_t, b.subname

from

(

-- Number of exceptions grouped by RECVQ, REASON
select recvq, reason, sqlcode, count(*) as num_ex,
max(exception_time) as max_t
from ibmqrep_exceptions

 where EXCEPTION_TIME >
    (select current timestamp - EXCEPTION_TIME_WINDOW hours
        from sysibm.sysdummy1,
             QREP_MON_APPLY_PARM)

group by recvq, reason, sqlcode

) a

-- joined to get the SUBNAME of the most recent exception
inner join

ibmqrep_exceptions b

on  a.max_t = b.exception_time
and a.recvq = b.recvq


) x

on 1 = 1


UNION

-- Query 240:
--    DE: Komponente: Q Apply
--    Ausschnitt: Alle inaktiven Subscriptions
--    DE: Component: Q Apply
--    Section: All inactive subscriptions

select
240 as ordercol,
'ASNQAPP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'A-SUB' as MTYP,
'ERROR' AS SEV,

-- DE
-- 'Subscription ' concat trim(st.subname) concat ', Recive Queue '
-- concat trim(st.RECVQ) concat ' inaktiv ('
-- concat st.state concat ') seit '
-- EN
 'Subscription ' concat trim(st.subname) concat ', recive queue '
 concat trim(st.RECVQ) concat ' inactive ('
 concat st.state concat ') since '

concat trim(VARCHAR(st.STATE_TIME)) concat '.'
as MTXT

from ibmqrep_targets st
where st.state <> 'A'

) x

order by x.ordercol, x.mtxt

with ur;

-- set current schema = user;