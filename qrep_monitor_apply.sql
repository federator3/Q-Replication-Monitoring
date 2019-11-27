--#SET TERMINATOR  ; 
-- ---------------------------------------------------------------------
-- Q Apply Monitor (a.k.a. qrep_Ampel_neu_Apply.sql)
-- Report zum schnellen Aufspüren von Unterbrechungen und 
-- Ausnahmebedingungen bei der Q Replication. Queries:
--   210: A-LAT - Apply latency
--   230: A-RQU - Receive Queue status
--   235: A-EXC - Number of exceptions within the last 6 hours
--   240: A-SUB - Inactive / new subscriptions
-- ---------------------------------------------------------------------
-- Der Report ist am Zielsystem periodisch auszufuehren (z.B. alle 
-- 5 Minuten). Spalte SEVerity zeigt an, ob es sich bei der angezeigten 
-- Zeile um eine INFO, eine WARNING oder um einen zu korrigierenden 
-- ERROR handelt.
-- ---------------------------------------------------------------------
-- Anpassung vor Inbetriebnahme: (search "-- change")
-- SET CURRENT SCHEMA anpassen und durch
-- verwendete Capture- bzw. Apply-Schema ersetzen
-- ---------------------------------------------------------------------
-- Status: In Erprobung
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

-- Query 210:
--    DE: Komponente: Q Apply
--    Ausschnitt: Apply Prozess
--    EN: Component: Q Apply
--    Section: Apply Prozess

select 

210 as ordercol,
'ASNQAPP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'A-LAT' as MTYP, 

case when y.MONITOR_TIME < 
       y.EXPECTED_TS_LAST_MONITOR_RECORD - 5 seconds
	 then 'ERROR'
	 when max(y.end2end_latency_sec) > 15 
	 then 'WARNING'
	 else 'INFO'
end as SEV,

case when y.MONITOR_TIME < 
       y.EXPECTED_TS_LAST_MONITOR_RECORD - 5 seconds
-- DE
	   then 'Q Apply nicht in Betrieb oder gestoert seit ' 
	      concat trim(VARCHAR(y.MONITOR_TIME)) concat '.'
	 when max(y.end2end_latency_sec) > 15 
	 then 'Q Apply End2End Latenz > 15 Sekunden. END2END_LATENCY=' 
-- EN
--	   then 'Q Apply not running / interrupted since ' 
--	      concat trim(VARCHAR(y.MONITOR_TIME)) concat '.'
--	 when max(y.end2end_latency_sec) > 15 
--	 then 'Q Apply End2End latency > 15 Sekunden. END2END_LATENCY=' 	 
	 
	      concat trim(VARCHAR(max(y.end2end_latency_sec))) 
		  concat ' s'
--		  concat ', CURRENT_MEMORY=' 
--		  concat trim(VARCHAR(max(y.current_memory_mb))) 
--		  concat ' MB'
		  concat ', ROWS_APPLIED=' 
		  concat trim(VARCHAR(sum(y.rows_applied)))
	 else 'Q Apply ok (End2End Latency <15s). END2END_LATENCY=' concat 
	      case when max(y.end2end_latency_sec) is null then 'UNKNOWN'
               else trim(VARCHAR(max(y.end2end_latency_sec))) 
		  end concat ' s'
--		  concat ', CURRENT_MEMORY=' 
--		  concat trim(VARCHAR(max(y.current_memory_mb))) 
--		  concat ' MB'
		  concat ', ROWS_APPLIED=' 
		  concat trim(VARCHAR(sum(y.rows_applied)))
end as MTXT


from 

(

select 
am.recvq, 
am.monitor_time,
current timestamp - (dec(ap.monitor_interval) / 1000) seconds 
  AS EXPECTED_TS_LAST_MONITOR_RECORD,
dec(max(am.end2end_latency) / 1000, 12 , 1) as end2end_latency_sec,
-- dec(sum(am.CURRENT_MEMORY) / 1024 / 1024, 12 , 2) 
-- as CURRENT_MEMORY_MB,
sum(am.rows_applied) as rows_applied

from ibmqrep_applymon am,
     ibmqrep_applyparms ap

-- only the most current rows 
where am.monitor_time = (select max(monitor_time) 
                         from ibmqrep_applymon)	
group by am.recvq, am.monitor_time, ap.monitor_interval 
--         am.end2end_latency, am.current_memory, am.rows_applied

) y

group by y.recvq, y.monitor_time, y.EXPECTED_TS_LAST_MONITOR_RECORD


UNION

-- Query 230:
--    DE: Komponente: Q Apply
--    Ausschnitt: Receive Queue Status
--    EN: Component: Q Apply
--    Section: Receive Queue Status

select 
230 as ordercol,
'ASNQAPP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'A-RQU' as MTYP, 
case when y.state <> 'A'
     then 'ERROR'
     else 'INFO' 
end as SEV,

case when y.state <> 'A'
     then 'Receive Queue ' concat trim(y.RECVQ) 
-- DE	 
	      concat ' inaktiv (STATE=' concat y.state concat ') seit ' 
-- EN	 
--	      concat ' inactive (STATE=' concat y.state concat ') since '

		  concat trim(VARCHAR(y.STATE_TIME)) 
	      concat ' (#subs A/I/O: ' 
		  concat trim(varchar(coalesce(z.num_subs_a , 0)))
  		  concat '/' concat trim(varchar(coalesce(z.num_subs_i , 0)))
		  concat '/' concat trim(varchar(coalesce(z.num_subs_o , 0)))		  
		  concat '). QDEPTH=' 
		  concat coalesce(trim(VARCHAR(y.qdepth)) , 'UNKNOWN')
		  concat ' OLDEST_TRANS=' 
		  CONCAT coalesce(trim(varchar(y.OLDEST_TRANS)) , 'UNKNOWN')
		  concat ' MEMORY: ' 
		  concat coalesce(trim(varchar(y.current_memory_mb)) 
		                     , 'UNKNOWN') 
		  concat '/' concat y.memory_limit concat ' MB'
		  concat '.'
     else 'Receive Queue ' concat trim(y.RECVQ) 
-- DE
	      concat ' aktiv'
-- EN
-- 	      concat ' active'
	      concat ' (#subs A/I/O: ' 
		  concat trim(varchar(coalesce(z.num_subs_a , 0)))
  		  concat '/' concat trim(varchar(coalesce(z.num_subs_i , 0)))
		  concat '/' concat trim(varchar(coalesce(z.num_subs_o , 0)))
          concat '). QDEPTH=' 
		  concat coalesce(trim(VARCHAR(y.qdepth)) , 'UNKNOWN')
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
moni.OLDEST_TRANS, moni.qdepth, 
varchar(dec(dec(moni.CURRENT_MEMORY) / 1024 / 1024 , 5 , 0)) 
  as current_memory_mb,
varchar(rq.memory_limit) as memory_limit

from 

ibmqrep_recvqueues rq

left outer join 

(

select mon1.recvq, mon1.monitor_time, 
       mon2.OLDEST_TRANS, mon2.qdepth, mon2.CURRENT_MEMORY 
from 

(

select recvq, max(monitor_time) as monitor_time 
from ibmqrep_applymon 
group by recvq

) mon1

inner join ibmqrep_applymon mon2

on  mon1.recvq = mon2.recvq
and mon1.monitor_time = mon2.monitor_time

) moni

on  rq.recvq = moni.recvq

) y						 

left outer join

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
						 
						 
UNION

-- Query 235:
--    Komponente: Q Apply
--    Ausschnitt: Anzahl Exceptions der letzten 24 Stunden
--    Component: Q Apply
--    Section: Number of exceptions within previous 24 hours
						 
select 
235 as ordercol,
'ASNQAPP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'A-EXC' as MTYP, 

case 
  when num_ex = 0 then 'INFO'
  else 'WARNING'
end as SEV,

case  
  when num_ex = 0 then

-- DE						   
  'Keine Exceptions in den letzen 24 Stunden.'

-- EN
--  'No exceptions in previous 24 hours.'
  
  else
  
-- DE						 
trim(varchar(num_ex)) concat ' ' concat trim(x.reason)
concat ' Exceptions fuer '
concat 'Receive Queue '
concat trim(recvq) concat ' in den letzten 24 Stunden. ' 
concat 'Details der letzen Exception: ' 
concat trim(varchar(x.max_t))
 concat ', SQLCODE ' 
concat trim(varchar(x.SQLCODE)) concat ', SUBNAME ' 
concat trim(x.subname) 

-- EN						 
-- trim(varchar(num_ex)) concat ' ' concat trim(x.reason)
-- concat ' exceptions for '
-- concat 'receive Queue '
-- concat trim(recvq) concat ' in previous 24 hours. ' 
-- concat 'Details of the most recent exception: ' 
-- concat trim(varchar(x.max_t)) 
-- concat ', SQLCODE ' 
-- concat trim(varchar(x.SQLCODE)) concat ', SUBNAME ' 
-- concat trim(x.subname) 

end AS MTXT
						 
from

(

select recvq, reason, sqlcode, num_ex, max_t, subname from 

(

select a.recvq, a.reason, a.sqlcode, a.num_ex, a.max_t, b.subname

from

( 

-- Number of exceptions grouped by RECVQ, REASON
select recvq, reason, sqlcode, count(*) as num_ex, 
max(exception_time) as max_t 
from ibmqrep_exceptions  

 where EXCEPTION_TIME > 
    (select current timestamp - 24 hours from sysibm.sysdummy1)

group by recvq, reason, sqlcode 

) a

-- joined to get the SUBNAME of the most recent exception
inner join 

ibmqrep_exceptions b 

on  a.max_t = b.exception_time 
and a.recvq = b.recvq

) y

-- union in case no exception 
union

select recvq, reason, sqlcode, num_ex, max_t, subname

from 

(

-- this inner join returns a row only if the number of exceptions is 0
-- means: info message (no exceptions) only returned if number of 
-- exceptions is 0
select '_NOEX' as recvq, '_NOEX' as reason, 0 as sqlcode, 
count(*) as num_ex, current timestamp as max_t, '_NOEX' as subname
from ibmqrep_exceptions

 where EXCEPTION_TIME > 
    (select current timestamp - 24 hours from sysibm.sysdummy1)

) u 

inner join

(

select 0 as noex from sysibm.sysdummy1

) d

on u.num_ex = d.noex

) x


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

'Subscription ' concat trim(st.subname) concat ', RECVQ ' 
-- DE
concat trim(st.RECVQ) concat ' inaktiv (' 
concat st.state concat ') seit ' 
-- EN
-- concat trim(st.RECVQ) concat ' inactive (' 
-- concat st.state concat ') since ' 

concat trim(VARCHAR(st.STATE_TIME)) concat '.'
as MTXT

from ibmqrep_targets st
where st.state <> 'A'

) x

order by x.ordercol

with ur;

-- set current schema = user;