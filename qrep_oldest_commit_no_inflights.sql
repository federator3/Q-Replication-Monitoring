--#SET TERMINATOR  ; 
-- ---------------------------------------------------------------------
-- Q Apply OLDEST COMMIT NO INFLIGHT 
-- Der Report listet pro Receive Queue die aktuelle OLDEST_COMMIT_LSN
-- und OLDEST_COMMIT_TIME. Ist Apply gestört oder hat Apply seit dem 
-- Start noch keine Daten repliziert, ist die Spalte SEV = ERROR, sonst
-- INFO:
--   910: A-OCO - Oldest Commit No Inflights
-- ---------------------------------------------------------------------
-- Der Report ist am Zielsystem vor dem Start der ELT-Verarbeitung 
-- auszufuehren, die Daten aus CCD Tabellen liest.
-- ---------------------------------------------------------------------
-- Anpassung vor Inbetriebnahme: (search "-- change")
-- SET CURRENT SCHEMA anpassen und durch
-- verwendete Capture- bzw. Apply-Schema ersetzen
-- ---------------------------------------------------------------------
-- Status: In Erprobung
-- 
-- ---------------------------------------------------------------------
-- Änderungen / Ergänzungen
--  - 07.03.2019: First Version
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

case when x.MONITOR_TIME < 
       x.EXPECTED_TS_LAST_MONITOR_RECORD - 5 seconds
	 then 'ERROR'
     when x.OLDEST_COMMIT_LSN = x'00000000000000000000000000000000'
       or x.OLDEST_COMMIT_TIME is NULL
	 then 'ERROR'
	 else 'INFO'
end as SEV,  

x.recvq,
x.OLDEST_COMMIT_LSN,
x.OLDEST_COMMIT_TIME,
x.monitor_time,
x.rows_applied, 
x.end2end_latency_sec,

case when x.MONITOR_TIME < 
       x.EXPECTED_TS_LAST_MONITOR_RECORD - 5 seconds
	 then 'Q Apply nicht in Betrieb oder gestoert seit ' 
	      concat trim(VARCHAR(x.MONITOR_TIME)) concat '.'
     when x.OLDEST_COMMIT_LSN = x'00000000000000000000000000000000'
       or x.OLDEST_COMMIT_TIME is NULL
	 then 'Q Apply hat seit dem Start noch keine Transaktion '
	      concat 'repliziert.'
	 else NULL
end as MTXT


from 

(

-- ---------------------------------------------------------------------
-- Q APPLY -------------------------------------------------------------

-- Query 910:
--    DE: Komponente: Q Apply
--    Ausschnitt: Oldest COMMIT
--    EN: Component: Q Apply
--    Section: Oldest COMMIT

select 

910 as ordercol,
'ASNQAPP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'A-OCO' as MTYP, 

current timestamp - (dec(ap.monitor_interval) / 1000) seconds 
  AS EXPECTED_TS_LAST_MONITOR_RECORD,
 
am.recvq,

am.OLDEST_COMMIT_LSN,
am.OLDEST_COMMIT_TIME,

am.monitor_time,
am.rows_applied, 
dec(am.end2end_latency / 1000, 12 , 1) as end2end_latency_sec


from ibmqrep_applymon am,
     ibmqrep_applyparms ap

-- only the most current rows 

inner join

(
select max(monitor_time) as monitor_time, recvq 
from ibmqrep_applymon
group by recvq
) m

on  am.monitor_time = m.monitor_time
and am.recvq        = m.recvq

) x

order by x.ordercol

with ur;

-- set current schema = user;