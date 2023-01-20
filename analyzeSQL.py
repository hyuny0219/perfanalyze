gen_rpt = """select 'generate report start time : ' || to_char(sysdate, 'yyyy/mm/dd hh24:mi:ss') "Start Gen!" from dual"""

dbinfo = """
select distinct host_name "Host Name",
       dbid               "DB ID",
       db_name            "DB Name",
       instance_number    "Instance#",
       instance_name      "Instance",
       version            "Version",
       parallel           "Cluster ?",
       (select to_char(begin_interval_time, 'yyyy-mm-dd hh24:mi:ss') snap_time 
          from dba_hist_snapshot
         where (instance_number,snap_id) = (select min(instance_number),min(snap_id) 
                                              from dba_hist_snapshot
                                             where snap_id between :snap_start and :snap_end
                                               and instance_number = x.instance_number
                                               and dbid = x.dbid)) "Snap Begin",
       (select to_char(end_interval_time, 'yyyy-mm-dd hh24:mi:ss') snap_time 
         from dba_hist_snapshot
         where (instance_number,snap_id) = (select max(instance_number),max(snap_id) 
                                              from dba_hist_snapshot
                                             where snap_id between :snap_start and :snap_end
                                               and instance_number = x.instance_number
                                               and dbid = x.dbid)) "Snap End"
  from dba_hist_database_instance x
where instance_number = :instno
  and dbid = :dbid
  and parallel=(select decode(value,'TRUE','YES','NO') from dba_hist_parameter
                 where parameter_name='cluster_database' and dbid=:dbid and instance_number=:instno and snap_id=:snap_end)
order by instance_number
"""

awrsnap = """
select snap_id "SnapID",
       to_char(end_interval_time, 'yyyy-mm-dd hh24:mi:ss') "Snap Time",
       snap_level "Snap Level",
       extract (hour   from (end_interval_time-begin_interval_time) )*  3600 +
           extract (minute from (end_interval_time-begin_interval_time) )*    60 +
           extract (second from (end_interval_time-begin_interval_time) ) "interval(s)",
       to_char(startup_time, 'yyyy-mm-dd hh24:mi:ss')  "DB Startup",
       dbid            "DB ID",
       instance_number     "Instance#"
  from dba_hist_snapshot
 where snap_id between :snap_start and :snap_end
   and instance_number = :instno
   and dbid = :dbid
 order by snap_id
"""

load = """
select snap_id,
       to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss')                   "Dates",
       max(to_char(end_interval_time,'dd hh24:mi'))                         "Dates2",
       max(decode(stat_name, 'user commits'         , delta, 0)) +
       max(decode(stat_name, 'user rollbacks'       , delta, 0)) "Transactions",
       max(decode(stat_name, 'redo size'            , delta, 0)) "Redo size",
       decode(max(decode(stat_name, 'redo entries', delta_val, 0)), 0, 0,
                   (1 -
                              (max(decode(stat_name,  'redo log space requests', delta_val, 0))
                               /max(decode(stat_name, 'redo entries', delta_val, 0))
                              ))
             ) "Redo NoWait(%)",
       max(decode(stat_name, 'logons cumulative'    , delta, 0)) "Logons",
       max(decode(stat_name, 'user calls'           , delta, 0)) "User calls",
       max(decode(stat_name, 'execute count'        , delta, 0)) "Executes",
       max(decode(stat_name, 'db block changes'     , delta, 0)) "Block changes",
       decode(max(decode(stat_name, 'consistent gets from cache', delta_val, 0))
                 +max(decode(stat_name, 'db block gets from cache', delta_val, 0)), 0, 0,
                   (1 -
                              (max(decode(stat_name, 'physical reads cache', delta_val, 0))
                               /(max(decode(stat_name, 'consistent gets from cache', delta_val, 0))
                                +max(decode(stat_name, 'db block gets from cache', delta_val, 0)))))
             ) "Buffer Hit(%)",
       decode(max(decode(stat_name, 'consistent gets from cache', delta_val, 0))
             +max(decode(stat_name, 'db block gets from cache', delta_val, 0)), 0, 0,
               (1 -
                         (max(delta_wa)
                         /(max(decode(stat_name, 'consistent gets from cache', delta_val, 0))
                          +max(decode(stat_name, 'db block gets from cache', delta_val, 0)))))
             ) "Buffer Nowait(%)",
       decode(max(delta_pins), 0, 0,
                        (max(delta_pinhits)
                               /max(delta_pins))
             ) "Library Hit(%)",
       decode(max(delta_gets), 0, 0,
                    (1 -
                              (max(delta_misses)
                               /max(delta_gets)
                              ))
             )  "Latch Hit(%)",
       max(decode(stat_name, 'sorts (memory)'       , delta, 0)) "Sorts (memory)",
       max(decode(stat_name, 'sorts (disk)'         , delta, 0)) "Sorts (disk)",
       decode(max(decode(stat_name, 'sorts (memory)', delta_val, 0))
                 +max(decode(stat_name, 'sorts (disk)', delta_val, 0)), 0, 0,
                        (max(decode(stat_name, 'sorts (memory)', delta_val, 0))
                               /(max(decode(stat_name, 'sorts (memory)', delta_val, 0))
                                +max(decode(stat_name, 'sorts (disk)', delta_val, 0))))
             )  "In-memory Sort(%)",
       max(decode(stat_name, 'session logical reads', delta, 0)) "Logical reads",
       max(decode(stat_name, 'physical reads'       , delta, 0)) "Physical reads",
       max(decode(stat_name, 'physical reads direct', delta, 0)) "Physical reads direct",
       max(decode(stat_name, 'physical writes'      , delta, 0)) "Physical writes",
       max(decode(stat_name, 'physical writes direct', delta, 0)) "Physical writes direct",
       max(decode(stat_name, 'parse count (total)'  , delta, 0)) "Parses(total)",
       max(decode(stat_name, 'parse count (hard)'   , delta, 0)) "Parses(hard)",
       decode(max(decode(stat_name, 'parse count (total)', delta_val, 0)), 0, 0,
                    (max(decode(stat_name,  'parse count (hard)', delta_val, 0))
                      /max(decode(stat_name, 'parse count (total)', delta_val, 0))
                              )
             )  "Hard Parse(%)",
       decode(max(decode(stat_name, 'parse count (total)', delta_val, 0)), 0, 0,
                    (1 -
                              (max(decode(stat_name,  'parse count (hard)', delta_val, 0))
                               /max(decode(stat_name, 'parse count (total)', delta_val, 0))
                              ))
             )  "Soft Parse(%)",
       decode(max(decode(stat_name, 'execute count', delta_val, 0)), 0, 0,
                    (1 -
                              (max(decode(stat_name,  'parse count (total)', delta_val, 0))
                               /max(decode(stat_name, 'execute count', delta_val, 0))
                              ))
             )  "Execute to Parse(%)",
       decode(max(decode(stat_name, 'parse time elapsed', delta_val, 0)), 0, 0,
                        (max(decode(stat_name, 'parse time cpu', delta_val, 0))
                               /max(decode(stat_name, 'parse time elapsed', delta_val, 0)))
             )  "Parse CPU to Parse Elapsd(%)",
       decode(max(decode(stat_name, 'CPU used by this session', delta_val, 0)), 0, 0,
                    (1 -
                              (max(decode(stat_name,  'parse time cpu', delta_val, 0))
                               /max(decode(stat_name, 'CPU used by this session', delta_val, 0))
                              ))
             )  "Non-Parse CPU(%)",
       max(decode(stat_name, 'parse time cpu'       , delta, 0))/100 "parse cpu",
       max(decode(stat_name, 'parse time elapsed'   , delta, 0))/100 "Parse ela"
 from (
       select sy.stat_name,
              sy.snap_id,
              sn.end_interval_time,
              nvl(sy.value,0)  - nvl(lag(sy.value) over( partition by sy.stat_name order by sy.snap_id ),0) delta_val,
              case when
                (extract (day    from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))* 86400 +
                 extract (hour   from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))*  3600 +
                 extract (minute from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))*    60 +
                 extract (second from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id )) ) > 0
                then
                  (nvl(sy.value,0)  - nvl(lag(sy.value) over( partition by sy.stat_name order by sy.snap_id ),0))
                  /(extract (day    from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))* 86400 +
                    extract (hour   from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))*  3600 +
                    extract (minute from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))*    60 +
                    extract (second from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))
                   )
                else null
              end delta,
              nvl(wa.wait_cnt,0)  - nvl(lag(wa.wait_cnt) over( order by wa.snap_id ),0) delta_wa,
              nvl(libcache.pins,0)  - nvl(lag(libcache.pins) over( order by libcache.snap_id ),0) delta_pins,
              nvl(libcache.pinhits,0)  - nvl(lag(libcache.pinhits) over( order by libcache.snap_id ),0) delta_pinhits,
              nvl(latch.gets,0)  - nvl(lag(latch.gets) over( order by latch.snap_id ),0) delta_gets,
              nvl(latch.misses,0)  - nvl(lag(latch.misses) over( order by latch.snap_id ),0) delta_misses
         from dba_hist_sysstat sy,
              dba_hist_snapshot sn,
             (select snap_id,
                     instance_number,
                     dbid,
                     sum(nvl(wait_count,0)) wait_cnt
                from dba_hist_waitstat
               group by snap_id, instance_number, dbid) wa,
             (select snap_id,
                     instance_number,
                     dbid,
                     sum(nvl(pins,0)) pins,
                     sum(nvl(pinhits,0)) pinhits
                from dba_hist_librarycache
               group by snap_id, instance_number, dbid) libcache,
             (select snap_id,
                     instance_number,
                     dbid,
                     sum(nvl(gets,0)) gets,
                     sum(nvl(misses,0)) misses
                from dba_hist_latch
               group by snap_id, instance_number, dbid) latch
        where sy.snap_id = sn.snap_id
          and sy.instance_number = sn.instance_number
          and sy.dbid = sn.dbid
          and wa.snap_id = sn.snap_id
          and wa.instance_number = sn.instance_number
          and wa.dbid = sn.dbid
          and libcache.snap_id = sn.snap_id
          and libcache.instance_number = sn.instance_number
          and libcache.dbid = sn.dbid
          and latch.snap_id = sn.snap_id
          and latch.instance_number = sn.instance_number
          and latch.dbid = sn.dbid
          and sy.stat_name in (
                       'redo size'            ,
                       'session logical reads',
                       'db block changes'     ,
                       'physical reads'       ,
                       'physical reads direct',
                       'physical writes'      ,
                       'physical writes direct',
                       'user calls'           ,
                       'parse count (total)'  ,
                       'parse count (hard)'   ,
                       'sorts (memory)'       ,
                       'sorts (disk)'         ,
                       'logons cumulative'    ,
                       'execute count'        ,
                       'user commits'         ,
                       'user rollbacks'       ,
                       'consistent gets from cache',
                       'db block gets from cache',
                       'redo log space requests',
                       'redo entries',
                       'physical reads cache',
                       'parse time cpu',
                       'parse time elapsed',
                       'CPU used by this session'
                      )
          and sn.snap_id between :snap_start and :snap_end
          and sn.instance_number = :instno
          and sn.dbid = :dbid
      )
 where snap_id > :snap_start
group by snap_id, to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss')
order by snap_id
"""

rac_load = """
select snap_id "SnapID",
       to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss')                   "Dates",
       max(to_char(end_interval_time,'dd hh24:mi'))                         "Dates2",
       ((max(decode(stat_name, 'gc cr blocks received', delta, 0)) +
         max(decode(stat_name, 'gc current blocks received', delta, 0)) +
         max(decode(stat_name, 'gc cr blocks served'  , delta, 0)) +
         max(decode(stat_name, 'gc current blocks served', delta, 0))
        ) * (select to_number(value) from dba_hist_parameter
              where parameter_name='db_block_size' and dbid=:dbid and instance_number=:instno and snap_id=:snap_end)
       +(max(decode(stat_name, 'gcs messages sent'  , delta, 0)) +
         max(decode(stat_name, 'ges messages sent', delta, 0)) +
         max(decode(     name, 'gcs msgs received'  , delta_dm, 0)) +
         max(decode(     name, 'ges msgs received', delta_dm, 0))
        ) * 200) / 1048576 "Interconnect Traffic(MB)",
       max(decode(stat_name, 'gc cr blocks received', delta, 0)) +
       max(decode(stat_name, 'gc current blocks received', delta, 0)) "GC blocks received",
       max(decode(stat_name, 'gc cr blocks received', delta, 0)) "GC CR block received",
       max(decode(stat_name, 'gc current blocks received', delta, 0)) "GC Current block received",
       max(decode(stat_name, 'gc cr blocks served'  , delta, 0)) +
       max(decode(stat_name, 'gc current blocks served', delta, 0)) "GC blocks served",
       max(decode(stat_name, 'gc cr blocks served'  , delta, 0)) "GC CR block served",
       max(decode(stat_name, 'gc current blocks served', delta, 0)) "GC Current blocks served",
       max(decode(     name, 'gcs msgs received'  , delta_dm, 0)) +
       max(decode(     name, 'ges msgs received', delta_dm, 0)) "GCS/GES msg received",
       max(decode(stat_name, 'gcs messages sent'  , delta, 0)) +
       max(decode(stat_name, 'ges messages sent', delta, 0)) "GCS/GES msg sent",
       max(decode(stat_name, 'DBWR fusion writes', delta, 0)) "DBWR fusion writes",
       max(decode(stat_name, 'gc blocks lost', delta_val, 0)) "gc lost",
       max(decode(stat_name, 'gc blocks corrupt', delta_val, 0)) "gc corrupt",
       decode(max(decode(stat_name, 'consistent gets from cache', delta_val, 0))
                 +max(decode(stat_name, 'db block gets from cache', delta_val, 0)), 0, 0,
                   (1 -
                              ((max(decode(stat_name, 'physical reads cache', delta_val, 0))
                               +max(decode(stat_name, 'gc cr blocks received', delta_val, 0))
                               +max(decode(stat_name, 'gc current blocks received', delta_val, 0)))
                               /(max(decode(stat_name, 'consistent gets from cache', delta_val, 0))
                                +max(decode(stat_name, 'db block gets from cache', delta_val, 0)))))
             ) "local(%)",
       decode(max(decode(stat_name, 'consistent gets from cache', delta_val, 0))
                 +max(decode(stat_name, 'db block gets from cache', delta_val, 0)), 0, 0,
                   ((max(decode(stat_name, 'gc cr blocks received', delta_val, 0))
                               +max(decode(stat_name, 'gc current blocks received', delta_val, 0)))
                               /(max(decode(stat_name, 'consistent gets from cache', delta_val, 0))
                                +max(decode(stat_name, 'db block gets from cache', delta_val, 0))))
             ) "remote(%)",
       decode(max(decode(stat_name, 'consistent gets from cache', delta_val, 0))
                 +max(decode(stat_name, 'db block gets from cache', delta_val, 0)), 0, 0,
                   ( max(decode(stat_name, 'physical reads cache', delta_val, 0))
                               /(max(decode(stat_name, 'consistent gets from cache', delta_val, 0))
                                +max(decode(stat_name, 'db block gets from cache', delta_val, 0))))
             ) "disk(%)",
       ((max(decode(stat_name, 'gc cr blocks received', delta, 0)) +
         max(decode(stat_name, 'gc current blocks received', delta, 0)) ) 
         * (select to_number(value) from dba_hist_parameter
              where parameter_name='db_block_size' and dbid=:dbid and instance_number=:instno and snap_id=:snap_end)
       +(max(decode(name, 'gcs msgs received'  , delta_dm, 0)) +
         max(decode(name, 'ges msgs received', delta_dm, 0))
        ) * 200) / 1048576 "Traffic:received(MB)",
       ((max(decode(stat_name, 'gc cr blocks served'  , delta, 0)) +
         max(decode(stat_name, 'gc current blocks served', delta, 0))
        ) * (select to_number(value) from dba_hist_parameter
              where parameter_name='db_block_size' and dbid=:dbid and instance_number=:instno and snap_id=:snap_end)
       +(max(decode(stat_name, 'gcs messages sent'  , delta, 0)) +
         max(decode(stat_name, 'ges messages sent', delta, 0)) 
        ) * 200) / 1048576 "Traffic:sent(MB)"
 from (
       select sy.stat_name,
              sy.snap_id,
              sn.end_interval_time,
              nvl(sy.value,0)  - nvl(lag(sy.value) over( partition by sy.stat_name order by sy.snap_id ),0) delta_val,
              case when
                (extract (day    from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))* 86400 +
                 extract (hour   from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))*  3600 +
                 extract (minute from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))*    60 +
                 extract (second from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id )) ) > 0
                then
                  (nvl(sy.value,0)  - nvl(lag(sy.value) over( partition by sy.stat_name order by sy.snap_id ),0))
                  /(extract (day    from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))* 86400 +
                    extract (hour   from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))*  3600 +
                    extract (minute from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))*    60 +
                    extract (second from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))
                   )
                else null
              end delta,
              dm.name,
              case when
                (extract (day    from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id ))* 86400 +
                 extract (hour   from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id ))*  3600 +
                 extract (minute from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id ))*    60 +
                 extract (second from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id )) ) > 0
                then
                  (nvl(dm.value,0) - nvl(lag(dm.value) over( partition by dm.name order by dm.snap_id ),0))
                  /(extract (day    from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id ))* 86400 +
                    extract (hour   from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id ))*  3600 +
                    extract (minute from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id ))*    60 +
                    extract (second from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id )) )
                else null
              end delta_dm,
              nvl(dm.value,0)  - nvl(lag(dm.value) over( partition by dm.name order by dm.snap_id ),0) delta_dm_val,
              nvl(cbs.flushes,0)  - nvl(lag(cbs.flushes) over( order by cbs.snap_id ),0) delta_cbs,
              nvl(cus.flush1,0) - nvl(lag(cus.flush1) over( order by cus.snap_id),0) delta_cusf1,
              nvl(cus.flush10,0) - nvl(lag(cus.flush10) over( order by cus.snap_id),0) delta_cusf10,
              nvl(cus.flush100,0) - nvl(lag(cus.flush100) over( order by cus.snap_id),0) delta_cusf100,
              nvl(cus.flush1000,0) - nvl(lag(cus.flush1000) over( order by cus.snap_id),0) delta_cusf1000,
              nvl(cus.flush10000,0) - nvl(lag(cus.flush10000) over( order by cus.snap_id),0) delta_cusf10000
         from dba_hist_sysstat sy,
              dba_hist_snapshot sn,
              dba_hist_dlm_misc dm,
              dba_hist_cr_block_server cbs,
              dba_hist_current_block_server cus
        where sy.snap_id = sn.snap_id
          and sy.instance_number = sn.instance_number
          and sy.dbid = sn.dbid
          and dm.snap_id = sn.snap_id
          and dm.instance_number = sn.instance_number
          and dm.dbid = sn.dbid
          and cbs.snap_id(+) = sn.snap_id
          and cbs.instance_number(+) = sn.instance_number
          and cbs.dbid(+) = sn.dbid
          and cus.snap_id(+) = sn.snap_id
          and cus.instance_number(+) = sn.instance_number
          and cus.dbid(+) = sn.dbid
          and sy.stat_name in (
                       'DBWR fusion writes'   ,
                       'gcs messages sent'    ,
                       'ges messages sent'    ,
                       'global enqueue gets sync',
                       'global enqueue gets async',
                       'global enqueue get time',
                       'gc cr blocks received',
                       'gc cr block receive time',
                       'gc current blocks received',
                       'gc current block receive time',
                       'gc cr blocks served',
                       'gc cr block build time',
                       'gc cr block flush time',
                       'gc cr block send time',
                       'gc current blocks served',
                       'gc current block pin time',
                       'gc current block flush time',
                       'gc current block send time',
                       'gc blocks lost',
                       'gc blocks corrupt',
                       'consistent gets from cache',
                       'db block gets from cache',
                       'physical reads cache'
                      )
          and dm.name in (
                       'messages sent directly',
                       'messages flow controlled',
                       'messages sent indirectly',
                       'gcs msgs received',
                       'gcs msgs process time(ms)',
                       'ges msgs received',
                       'ges msgs process time(ms)',
                       'msgs sent queued',
                       'msgs sent queue time (ms)',
                       'msgs sent queued on ksxp',
                       'msgs sent queue time on ksxp (ms)',
                       'msgs received queue time (ms)',
                       'msgs received queued'
                      )
          and sn.snap_id between :snap_start and :snap_end
          and sn.instance_number = :instno
          and sn.dbid = :dbid
      )
 where snap_id > :snap_start
group by snap_id, to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss')
order by snap_id
"""

rac_load2 = """
select snap_id,
       to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss') "Dates",
       max(to_char(end_interval_time,'dd hh24:mi')) "Dates2",
       decode(max(decode(stat_name, 'gc cr blocks received', delta_val, 0)), 0, 0,
                  (max(decode(stat_name, 'gc cr block receive time', delta_val, 0))
                    /max(decode(stat_name, 'gc cr blocks received', delta_val, 0))) * 10
             ) "Avg GC cr blk rcv time(ms)" ,
       decode(max(decode(stat_name, 'gc current blocks received', delta_val, 0)), 0, 0,
                  (max(decode(stat_name, 'gc current block receive time', delta_val, 0))
                     /max(decode(stat_name, 'gc current blocks received', delta_val, 0))) * 10
             ) "Avg GC cur blk rcv time(ms)",
       decode(max(decode(stat_name, 'gc cr blocks served', delta_val, 0)), 0, 0,
                  (max(decode(stat_name, 'gc cr block build time', delta_val, 0))
                    /max(decode(stat_name, 'gc cr blocks served', delta_val, 0))) * 10
             ) "Avg GC cr blk bld time(ms)",
       decode(max(delta_cbs), 0, 0,
                  (max(decode(stat_name, 'gc cr block flush time', delta_val, 0))
                    /max(delta_cbs)) * 10
             ) "Avg GC cr blk flush time(ms)",
       decode(max(decode(stat_name, 'gc cr blocks served', delta_val, 0)), 0, 0,
                  (max(decode(stat_name, 'gc cr block send time', delta_val, 0))
                    /max(decode(stat_name, 'gc cr blocks served', delta_val, 0))) * 10
             ) "Avg GC cr blk snd time(ms)",
       decode(max(decode(stat_name, 'gc cr blocks served', delta_val, 0)), 0, 0,
                  (max(delta_cbs)
                    /max(decode(stat_name, 'gc cr blocks served', delta_val, 0))) * 100
             ) "GC log flush for cr blk srvd%",
       decode(max(decode(stat_name, 'gc current blocks served', delta_val, 0)), 0, 0,
                  (max(decode(stat_name, 'gc current block pin time', delta_val, 0))
                    /max(decode(stat_name, 'gc current blocks served', delta_val, 0))) * 10
             ) "Avg GC cu blk pin time(ms)",
       decode(max(decode(stat_name, 'gc current blocks served', delta_val, 0)), 0, 0,
                  (max(decode(stat_name, 'gc current block send time', delta_val, 0))
                    /max(decode(stat_name, 'gc current blocks served', delta_val, 0))) * 10
             ) "Avg GC cu blk snd time(ms)",
       decode(max(delta_cusf1)+max(delta_cusf10)+max(delta_cusf100)+max(delta_cusf1000)+max(delta_cusf10000), 0, 0,
                  (max(decode(stat_name, 'gc current block flush time', delta_val, 0))
                    /(max(delta_cusf1)+max(delta_cusf10)+max(delta_cusf100)+max(delta_cusf1000)+max(delta_cusf10000))) * 10
             ) "Avg GC cu blk flush time(ms)",
       decode(max(decode(stat_name, 'gc current blocks served', delta_val, 0)), 0, 0,
                  ((max(delta_cusf1)+max(delta_cusf10)+max(delta_cusf100)+max(delta_cusf1000)+max(delta_cusf10000))
                    /max(decode(stat_name, 'gc current blocks served', delta_val, 0))) * 100
             ) "GC log flush for cur blk srvd%",
       decode(max(decode(stat_name, 'global enqueue gets async', delta_val, 0))
                 +max(decode(stat_name, 'global enqueue gets sync', delta_val, 0)), 0, 0,
                  (max(decode(stat_name, 'global enqueue get time', delta_val, 0))
                    /(max(decode(stat_name, 'global enqueue gets async', delta_val, 0))
                     +max(decode(stat_name, 'global enqueue gets sync', delta_val, 0)))) * 10
             ) "Avg GE get time(ms)"
 from (
       select sy.stat_name,
              sy.snap_id,
              sn.end_interval_time,
              nvl(sy.value,0)  - nvl(lag(sy.value) over( partition by sy.stat_name order by sy.snap_id ),0) delta_val,
              case when
                (extract (day    from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))* 86400 +
                 extract (hour   from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))*  3600 +
                 extract (minute from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))*    60 +
                 extract (second from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id )) ) > 0
                then
                  (nvl(sy.value,0)  - nvl(lag(sy.value) over( partition by sy.stat_name order by sy.snap_id ),0))
                  /(extract (day    from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))* 86400 +
                    extract (hour   from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))*  3600 +
                    extract (minute from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))*    60 +
                    extract (second from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))
                   )
                else null
              end delta,
              dm.name,
              case when
                (extract (day    from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id ))* 86400 +
                 extract (hour   from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id ))*  3600 +
                 extract (minute from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id ))*    60 +
                 extract (second from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id )) ) > 0
                then
                  (nvl(dm.value,0) - nvl(lag(dm.value) over( partition by dm.name order by dm.snap_id ),0))
                  /(extract (day    from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id ))* 86400 +
                    extract (hour   from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id ))*  3600 +
                    extract (minute from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id ))*    60 +
                    extract (second from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id )) )
                else null
              end delta_dm,
              nvl(dm.value,0)  - nvl(lag(dm.value) over( partition by dm.name order by dm.snap_id ),0) delta_dm_val,
              nvl(cbs.flushes,0)  - nvl(lag(cbs.flushes) over( order by cbs.snap_id ),0) delta_cbs,
              nvl(cus.flush1,0) - nvl(lag(cus.flush1) over( order by cus.snap_id),0) delta_cusf1,
              nvl(cus.flush10,0) - nvl(lag(cus.flush10) over( order by cus.snap_id),0) delta_cusf10,
              nvl(cus.flush100,0) - nvl(lag(cus.flush100) over( order by cus.snap_id),0) delta_cusf100,
              nvl(cus.flush1000,0) - nvl(lag(cus.flush1000) over( order by cus.snap_id),0) delta_cusf1000,
              nvl(cus.flush10000,0) - nvl(lag(cus.flush10000) over( order by cus.snap_id),0) delta_cusf10000
         from dba_hist_sysstat sy,
              dba_hist_snapshot sn,
              dba_hist_dlm_misc dm,
              dba_hist_cr_block_server cbs,
              dba_hist_current_block_server cus
        where sy.snap_id = sn.snap_id
          and sy.instance_number = sn.instance_number
          and sy.dbid = sn.dbid
          and dm.snap_id = sn.snap_id
          and dm.instance_number = sn.instance_number
          and dm.dbid = sn.dbid
          and cbs.snap_id(+) = sn.snap_id
          and cbs.instance_number(+) = sn.instance_number
          and cbs.dbid(+) = sn.dbid
          and cus.snap_id(+) = sn.snap_id
          and cus.instance_number(+) = sn.instance_number
          and cus.dbid(+) = sn.dbid
          and sy.stat_name in (
                       'DBWR fusion writes'   ,
                       'gcs messages sent'    ,
                       'ges messages sent'    ,
                       'global enqueue gets sync',
                       'global enqueue gets async',
                       'global enqueue get time',
                       'gc cr blocks received',
                       'gc cr block receive time',
                       'gc current blocks received',
                       'gc current block receive time',
                       'gc cr blocks served',
                       'gc cr block build time',
                       'gc cr block flush time',
                       'gc cr block send time',
                       'gc current blocks served',
                       'gc current block pin time',
                       'gc current block flush time',
                       'gc current block send time',
                       'gc blocks lost',
                       'gc blocks corrupt',
                       'consistent gets from cache',
                       'db block gets from cache',
                       'physical reads cache'
                      )
          and dm.name in (
                       'messages sent directly',
                       'messages flow controlled',
                       'messages sent indirectly',
                       'gcs msgs received',
                       'gcs msgs process time(ms)',
                       'ges msgs received',
                       'ges msgs process time(ms)',
                       'msgs sent queued',
                       'msgs sent queue time (ms)',
                       'msgs sent queued on ksxp',
                       'msgs sent queue time on ksxp (ms)',
                       'msgs received queue time (ms)',
                       'msgs received queued'
                      )
          and sn.snap_id between :snap_start and :snap_end
          and sn.instance_number = :instno
          and sn.dbid = :dbid
      )
 where snap_id > :snap_start
group by snap_id, to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss')
order by snap_id
"""

rac_message = """
select snap_id,
       to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss') "Dates",
       max(to_char(end_interval_time,'dd hh24:mi'))       "Dates2",
       decode(max(decode(name, 'msgs sent queued',         delta_dm_val, 0)), 0, 0,
             (max(decode(name, 'msgs sent queue time (ms)',delta_dm_val, 0))
             /max(decode(name, 'msgs sent queued',         delta_dm_val, 0)))
             ) "Avg msg sent queue time(ms)",
       decode(max(decode(name, 'msgs sent queued on ksxp', delta_dm_val, 0)), 0, 0,
             (max(decode(name, 'msgs sent queue time on ksxp (ms)',delta_dm_val, 0))
             /max(decode(name, 'msgs sent queued on ksxp', delta_dm_val, 0)))
             ) "Avg msg snt q time on ksxp(ms)",
       decode(max(decode(name, 'msgs received queued',     delta_dm_val, 0)), 0, 0,
             (max(decode(name, 'msgs received queue time (ms)',delta_dm_val, 0))
             /max(decode(name, 'msgs received queued',     delta_dm_val, 0)))
             ) "Avg msg rcvd queue time(ms)",
       decode(max(decode(name, 'gcs msgs received',        delta_dm_val, 0)), 0, 0,
             (max(decode(name, 'gcs msgs process time(ms)',delta_dm_val, 0))
             /max(decode(name, 'gcs msgs received',        delta_dm_val, 0)))
             ) "Avg GCS msg process time(ms)",
       decode(max(decode(name, 'ges msgs received',        delta_dm_val, 0)), 0, 0,
             (max(decode(name, 'ges msgs process time(ms)',delta_dm_val, 0))
             /max(decode(name, 'ges msgs received',        delta_dm_val, 0)))
             ) "Avg GES msg process time(ms)",
       decode(max(decode(name, 'messages sent directly',   delta_dm_val, 0)) +
              max(decode(name, 'messages sent indirectly', delta_dm_val, 0)) +
              max(decode(name, 'messages flow controlled', delta_dm_val, 0)), 0, 0,
             (max(decode(name, 'messages sent directly',   delta_dm_val, 0))
            /(max(decode(name, 'messages sent directly',   delta_dm_val, 0)) +
              max(decode(name, 'messages sent indirectly', delta_dm_val, 0)) +
              max(decode(name, 'messages flow controlled', delta_dm_val, 0))))
             ) "direct sent msgs(%)",
       decode(max(decode(name, 'messages sent directly',   delta_dm_val, 0)) +
              max(decode(name, 'messages sent indirectly', delta_dm_val, 0)) +
              max(decode(name, 'messages flow controlled', delta_dm_val, 0)), 0, 0,
             (max(decode(name, 'messages sent indirectly', delta_dm_val, 0))
            /(max(decode(name, 'messages sent directly',   delta_dm_val, 0)) +
              max(decode(name, 'messages sent indirectly', delta_dm_val, 0)) +
              max(decode(name, 'messages flow controlled', delta_dm_val, 0))))
             ) "indirect sent msgs(%)",
       decode(max(decode(name, 'messages sent directly',   delta_dm_val, 0)) +
              max(decode(name, 'messages sent indirectly', delta_dm_val, 0)) +
              max(decode(name, 'messages flow controlled', delta_dm_val, 0)), 0, 0,
             (max(decode(name, 'messages flow controlled', delta_dm_val, 0))
            /(max(decode(name, 'messages sent directly',   delta_dm_val, 0)) +
              max(decode(name, 'messages sent indirectly', delta_dm_val, 0)) +
              max(decode(name, 'messages flow controlled', delta_dm_val, 0))))
             ) "flow controlled msgs(%)",
       max(case when name='messages sent directly'   then delta_dm_val else 0 end) "msg(direct)",
       max(case when name='messages sent indirectly' then delta_dm_val else 0 end) "msg(indirect)",
       max(case when name='messages flow controlled' then delta_dm_val else 0 end) "msg(controlled)"
 from (
       select sy.stat_name,
              sy.snap_id,
              sn.end_interval_time,
              nvl(sy.value,0)  - nvl(lag(sy.value) over( partition by sy.stat_name order by sy.snap_id ),0) delta_val,
              case when
                (extract (day    from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))* 86400 +
                 extract (hour   from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))*  3600 +
                 extract (minute from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))*    60 +
                 extract (second from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id )) ) > 0
                then
                  (nvl(sy.value,0)  - nvl(lag(sy.value) over( partition by sy.stat_name order by sy.snap_id ),0))
                  /(extract (day    from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))* 86400 +
                    extract (hour   from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))*  3600 +
                    extract (minute from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))*    60 +
                    extract (second from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by sy.stat_name order by sy.snap_id ))
                   )
                else null
              end delta,
              dm.name,
              case when
                (extract (day    from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id ))* 86400 +
                 extract (hour   from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id ))*  3600 +
                 extract (minute from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id ))*    60 +
                 extract (second from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id )) ) > 0
                then
                  (nvl(dm.value,0) - nvl(lag(dm.value) over( partition by dm.name order by dm.snap_id ),0))
                  /(extract (day    from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id ))* 86400 +
                    extract (hour   from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id ))*  3600 +
                    extract (minute from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id ))*    60 +
                    extract (second from sn.end_interval_time - lag(sn.end_interval_time) over ( partition by dm.name order by dm.snap_id )) )
                else null
              end delta_dm,
              nvl(dm.value,0)  - nvl(lag(dm.value) over( partition by dm.name order by dm.snap_id ),0) delta_dm_val,
              nvl(cbs.flushes,0)  - nvl(lag(cbs.flushes) over( order by cbs.snap_id ),0) delta_cbs,
              nvl(cus.flush1,0) - nvl(lag(cus.flush1) over( order by cus.snap_id),0) delta_cusf1,
              nvl(cus.flush10,0) - nvl(lag(cus.flush10) over( order by cus.snap_id),0) delta_cusf10,
              nvl(cus.flush100,0) - nvl(lag(cus.flush100) over( order by cus.snap_id),0) delta_cusf100,
              nvl(cus.flush1000,0) - nvl(lag(cus.flush1000) over( order by cus.snap_id),0) delta_cusf1000,
              nvl(cus.flush10000,0) - nvl(lag(cus.flush10000) over( order by cus.snap_id),0) delta_cusf10000
         from dba_hist_sysstat sy,
              dba_hist_snapshot sn,
              dba_hist_dlm_misc dm,
              dba_hist_cr_block_server cbs,
              dba_hist_current_block_server cus
        where sy.snap_id = sn.snap_id
          and sy.instance_number = sn.instance_number
          and sy.dbid = sn.dbid
          and dm.snap_id = sn.snap_id
          and dm.instance_number = sn.instance_number
          and dm.dbid = sn.dbid
          and cbs.snap_id(+) = sn.snap_id
          and cbs.instance_number(+) = sn.instance_number
          and cbs.dbid(+) = sn.dbid
          and cus.snap_id(+) = sn.snap_id
          and cus.instance_number(+) = sn.instance_number
          and cus.dbid(+) = sn.dbid
          and sy.stat_name in (
                       'DBWR fusion writes'   ,
                       'gcs messages sent'    ,
                       'ges messages sent'    ,
                       'global enqueue gets sync',
                       'global enqueue gets async',
                       'global enqueue get time',
                       'gc cr blocks received',
                       'gc cr block receive time',
                       'gc current blocks received',
                       'gc current block receive time',
                       'gc cr blocks served',
                       'gc cr block build time',
                       'gc cr block flush time',
                       'gc cr block send time',
                       'gc current blocks served',
                       'gc current block pin time',
                       'gc current block flush time',
                       'gc current block send time',
                       'gc blocks lost',
                       'gc blocks corrupt',
                       'consistent gets from cache',
                       'db block gets from cache',
                       'physical reads cache'
                      )
          and dm.name in (
                       'messages sent directly',
                       'messages flow controlled',
                       'messages sent indirectly',
                       'gcs msgs received',
                       'gcs msgs process time(ms)',
                       'ges msgs received',
                       'ges msgs process time(ms)',
                       'msgs sent queued',
                       'msgs sent queue time (ms)',
                       'msgs sent queued on ksxp',
                       'msgs sent queue time on ksxp (ms)',
                       'msgs received queue time (ms)',
                       'msgs received queued'
                      )
          and sn.snap_id between :snap_start and :snap_end
          and sn.instance_number = :instno
          and sn.dbid = :dbid
      )
 where snap_id > :snap_start
group by snap_id, to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss')
order by snap_id
"""

rac_gesstat = """
select b.snap_id      "SnapID",
       b.snap_time    "Timestamp",
       name           "GES Statistic",
       value          "Value",
       value/interval "perSecond"
  from (
        select snap_id,name,
               value-lag(value,1) over (partition by name order by name,snap_id) value
          from dba_hist_dlm_misc
         where snap_id between :snap_start and :snap_end
           and instance_number = :instno
           and dbid = :dbid
       ) a,
       ( select snap_id,
                to_char(end_interval_time, 'yyyy-mm-dd hh24:mi:ss') snap_time,
                extract (hour   from (end_interval_time-begin_interval_time) )*  3600 +
                extract (minute from (end_interval_time-begin_interval_time) )*    60 +
                extract (second from (end_interval_time-begin_interval_time) ) interval
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) b
 where a.snap_id=b.snap_id
 order by name,snap_time
"""

rac_gc_cr_serv = """
select b.snap_id   "SnapID",
       to_char(b.end_interval_time,'yyyy-mm-dd hh24:mi:ss') "Dates",
       to_char(b.end_interval_time,'dd hh24:mi')            "Dates2",
       cr_requests+current_requests                         "total",
       cr_requests                                          "cr",
       current_requests                                     "current",
       data_requests                                        "data block",
       undo_requests                                        "undo block",
       tx_requests                                          "TX table",
       current_results                                      "current result",
       private_results                                      "private",
       zero_results                                         "zero",
       disk_read_results                                    "disk read",
       fail_results                                         "fail",
       fairness_down_converts                               "FDC",
       flushes                                              "flushes",
       light_works                                          "light works",
       errors                                               "errors",
       decode((cr_requests+current_requests),0,0,fairness_down_converts/(cr_requests+current_requests)) "FDC(%)",
       decode(cr_requests,0,0,light_works/cr_requests) "cr light-weight%",
       decode((cr_requests+current_requests),0,0,fail_results/(cr_requests+current_requests)) "request fail%"
  from (
        select snap_id,
               cr_requests-lag(cr_requests) over (order by snap_id)             cr_requests,
               current_requests-lag(current_requests) over (order by snap_id)   current_requests,
               data_requests-lag(data_requests) over (order by snap_id)         data_requests,
               undo_requests-lag(undo_requests) over (order by snap_id)         undo_requests,
               tx_requests-lag(tx_requests) over (order by snap_id)             tx_requests,
               current_results-lag(current_results) over (order by snap_id)     current_results,
               private_results-lag(private_results) over (order by snap_id)     private_results,
               zero_results-lag(zero_results) over (order by snap_id)           zero_results,
               disk_read_results-lag(disk_read_results) over (order by snap_id) disk_read_results,
               fail_results-lag(fail_results) over (order by snap_id)           fail_results,
               fairness_down_converts-lag(fairness_down_converts) over (order by snap_id) fairness_down_converts,
               flushes-lag(flushes) over (order by snap_id)                     flushes,
               light_works-lag(light_works) over (order by snap_id)             light_works,
               errors-lag(errors) over (order by snap_id)                       errors
          from dba_hist_cr_block_server
         where snap_id between :snap_start and :snap_end
           and instance_number = :instno
           and dbid = :dbid
        ) a,
        ( select snap_id,
                 end_interval_time 
            from dba_hist_snapshot
           where snap_id between 1+ :snap_start and :snap_end
             and instance_number = :instno
             and dbid = :dbid
        ) b
where   a.snap_id=b.snap_id
order by 2
"""

rac_gc_cur_srv = """
select b.snap_id   "SnapID",
       to_char(b.end_interval_time,'yyyy-mm-dd hh24:mi:ss') "Dates",
       to_char(b.end_interval_time,'dd hh24:mi')            "Dates2",
       pin1       "pin(<1ms)",
       pin10      "pin(<10ms)",
       pin100     "pin(<100ms)",
       pin1000    "pin(<1s)",
       pin10000   "pin(<10s)",
       pin1+pin10+pin100+pin100+pin1000+pin10000  "pin(total)",
       decode((pin1+pin10+pin100+pin100+pin1000+pin10000),0,0,pin1/(pin1+pin10+pin100+pin100+pin1000+pin10000))       "pin(<1ms)%",
       decode((pin1+pin10+pin100+pin100+pin1000+pin10000),0,0,pin10/(pin1+pin10+pin100+pin100+pin1000+pin10000))      "pin(<10ms)%",
       decode((pin1+pin10+pin100+pin100+pin1000+pin10000),0,0,pin100/(pin1+pin10+pin100+pin100+pin1000+pin10000))     "pin(<100ms)%",
       decode((pin1+pin10+pin100+pin100+pin1000+pin10000),0,0,pin1000/(pin1+pin10+pin100+pin100+pin1000+pin10000))    "pin(<1s)%",
       decode((pin1+pin10+pin100+pin100+pin1000+pin10000),0,0,pin10000/(pin1+pin10+pin100+pin100+pin1000+pin10000))   "pin(<10s)%",
       flush1     "flush(<1ms)",
       flush10    "flush(<10ms)",
       flush100   "flush(<100ms)",
       flush1000  "flush(<1s)",
       flush10000 "flush(<10s)",
       flush1+flush10+flush100+flush100+flush1000+flush10000 "flush(total)",
       decode((flush1+flush10+flush100+flush100+flush1000+flush10000),0,0
          ,flush1/(flush1+flush10+flush100+flush100+flush1000+flush10000))        "flush(<1ms)%",
       decode((flush1+flush10+flush100+flush100+flush1000+flush10000),0,0
          ,flush10/(flush1+flush10+flush100+flush100+flush1000+flush10000))       "flush(<10ms)%",
       decode((flush1+flush10+flush100+flush100+flush1000+flush10000),0,0
          ,flush100/(flush1+flush10+flush100+flush100+flush1000+flush10000))      "flush(<100ms)%",
       decode((flush1+flush10+flush100+flush100+flush1000+flush10000),0,0
          ,flush1000/(flush1+flush10+flush100+flush100+flush1000+flush10000))     "flush(<1s)%",
       decode((flush1+flush10+flush100+flush100+flush1000+flush10000),0,0
          ,flush10000/(flush1+flush10+flush100+flush100+flush1000+flush10000))    "flush(<10s)%"
from    (
        select snap_id,
               pin1-lag(pin1) over (order by snap_id) pin1,
               pin10-lag(pin10) over (order by snap_id) pin10,
               pin100-lag(pin100) over (order by snap_id) pin100,
               pin1000-lag(pin1000) over (order by snap_id) pin1000,
               pin10000-lag(pin10000) over (order by snap_id) pin10000,
               flush1-lag(flush1) over (order by snap_id) flush1,
               flush10-lag(flush10) over (order by snap_id) flush10,
               flush100-lag(flush100) over (order by snap_id) flush100,
               flush1000-lag(flush1000) over (order by snap_id) flush1000,
               flush10000-lag(flush10000) over (order by snap_id) flush10000
          from dba_hist_current_block_server
         where snap_id between  :snap_start and :snap_end
           and instance_number = :instno
           and dbid = :dbid
        ) a,
        ( select snap_id,
                 end_interval_time
            from dba_hist_snapshot
           where snap_id between 1+  :snap_start and :snap_end
             and instance_number = :instno
             and dbid = :dbid
        ) b
where   a.snap_id=b.snap_id
order by 2
"""

rac_gc_inst_tran = """
select b.snap_id   "SnapID",
       to_char(b.end_interval_time,'yyyy-mm-dd hh24:mi:ss') "Dates",
       max(to_char(b.end_interval_time,'dd hh24:mi'))       "Dates2",
       instance    "fr Instance",
       sum(cr_block)+sum(cr_busy) +sum(cr_congested) "cr_recv",
       decode(sum(cr_block)+sum(cr_busy) +sum(cr_congested),0,0,sum(cr_block)    /(sum(cr_block)+sum(cr_busy) +sum(cr_congested))) "cr immediate(%)",
       decode(sum(cr_block)+sum(cr_busy) +sum(cr_congested),0,0,sum(cr_busy)     /(sum(cr_block)+sum(cr_busy) +sum(cr_congested))) "cr busy(%)",
       decode(sum(cr_block)+sum(cr_busy) +sum(cr_congested),0,0,sum(cr_congested)/(sum(cr_block)+sum(cr_busy) +sum(cr_congested))) "cr congsted(%)",
       sum(current_block)+sum(current_busy)+sum(current_congested) "cur_recv",
       decode(sum(current_block)+sum(current_busy)+sum(current_congested),0,0,
              sum(current_block)/(sum(current_block)+sum(current_busy)+sum(current_congested)))      "cur immedidate(%)",
       decode(sum(current_block)+sum(current_busy)+sum(current_congested),0,0,
              sum(current_busy)/(sum(current_block)+sum(current_busy)+sum(current_congested)))       "cur busy(%)",
       decode(sum(current_block)+sum(current_busy)+sum(current_congested),0,0,
              sum(current_congested)/(sum(current_block)+sum(current_busy)+sum(current_congested)))  "cur congst(%)",
       decode(sum(cr_block)+sum(cr_busy)+sum(cr_congested),0,0,(sum(cr_block_time)+sum(cr_busy_time)+sum(cr_congested_time))/(sum(cr_block)+sum(cr_busy)+sum(cr_congested)))/1000 "CR all(ms)",
       decode(sum(cr_block),0,0,sum(cr_block_time)/sum(cr_block))/1000             "CR immed(ms)",
       decode(sum(cr_busy),0,0,sum(cr_busy_time)/sum(cr_busy))/1000                "CR busy(ms)",
       decode(sum(cr_congested),0,0,sum(cr_congested_time)/sum(cr_congested))/1000 "CR congst(ms)",
       decode(sum(current_block)+sum(current_busy)+sum(current_congested),0,0,(sum(cu_block_time)+sum(cu_busy_time)+sum(cu_congested_time))/(sum(current_block)+sum(current_busy)+sum(current_congested)))/1000 "CU all(ms)",
       decode(sum(current_block),0,0,sum(cu_block_time)/sum(current_block))/1000 "CU immed(ms)",
       decode(sum(current_busy),0,0,sum(cu_busy_time)/sum(current_busy))/1000 "CU busy(ms)",
       decode(sum(cu_congested_time),0,0,sum(cu_congested_time)/sum(current_congested))/1000 "CU congst(ms)"
  from (
        select snap_id,
               instance,
               class,
               cr_block-lag(cr_block) over (partition by instance,class order by instance,class,snap_id)                     cr_block,
               cr_busy-lag(cr_busy) over (partition by instance,class order by instance,class,snap_id)                       cr_busy,
               cr_congested-lag(cr_congested) over (partition by instance,class order by instance,class,snap_id)             cr_congested,
               current_block-lag(current_block) over (partition by instance,class order by instance,class,snap_id)           current_block,
               current_busy-lag(current_busy) over (partition by instance,class order by instance,class,snap_id)             current_busy,
               current_congested-lag(current_congested) over (partition by instance,class order by instance,class,snap_id)   current_congested,
               cr_block_time-lag(cr_block_time) over (partition by instance,class order by instance,class,snap_id)           cr_block_time,
               cr_busy_time-lag(cr_busy_time) over (partition by instance,class order by instance,class,snap_id)             cr_busy_time,
               cr_congested_time-lag(cr_congested_time) over (partition by instance,class order by instance,class,snap_id)   cr_congested_time,
               current_block_time-lag(current_block_time) over (partition by instance,class order by instance,class,snap_id) cu_block_time,
               current_busy_time-lag(current_busy_time) over (partition by instance,class order by instance,class,snap_id)   cu_busy_time,
               current_congested_time-lag(current_congested_time) over (partition by instance,class order by instance,class,snap_id) cu_congested_time
          from dba_hist_inst_cache_transfer
         where snap_id between :snap_start and :snap_end
           and instance_number = :instno
           and dbid = :dbid
       ) a,
       ( select snap_id,
                end_interval_time
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) b
 where a.snap_id=b.snap_id
 group by b.snap_id,to_char(b.end_interval_time,'yyyy-mm-dd hh24:mi:ss'),instance
 order by 3,2
"""

rac_ping = """
select b.snap_id              "SnapID",
       to_char(b.end_interval_time,'yyyy-mm-dd hh24:mi:ss')          "Dates",
       to_char(b.end_interval_time,'dd hh24:mi')                     "Dates2",
       target_instance                                               "Target Inst",
       cnt_500b                                                      "Ping Count(500b)",
       round(avg_500b/1000,2)                                        "Ping Latency(500b)(ms)",
       cnt_8k                                                        "Ping Count(8k)",
       round(avg_8k/1000,2)                                          "Ping Latency(8k)(ms)",
       SQRT(ABS(greatest(ss_500b - (avg_500b * avg_500b), 0)))/1000  "Stddev(500b)",
       SQRT(ABS(greatest(ss_8k   - (avg_8k * avg_8k), 0)))/1000      "Stddev(8k)"
  from (
        select snap_id,
               target_instance,
               cnt_500b,
               (wait_500b / greatest(cnt_500b, 1))   avg_500b,
               (waitsq_500b / greatest(cnt_500b, 1)) ss_500b,
               cnt_8k,
               (wait_8k / greatest(cnt_8k, 1))       avg_8k,
               (waitsq_8k / greatest(cnt_8k, 1))     ss_8k
          from (
                select snap_id,
                       target_instance,
                       cnt_500b-lag(cnt_500b)       over (order by target_instance,snap_id) cnt_500b,
                       wait_500b-lag(wait_500b)     over (order by target_instance,snap_id) wait_500b,
                       waitsq_500b-lag(waitsq_500b) over (order by target_instance,snap_id) waitsq_500b,
                       cnt_8k-lag(cnt_8k)           over (order by target_instance,snap_id) cnt_8k,
                       wait_8k-lag(wait_8k)         over (order by target_instance,snap_id) wait_8k,
                       waitsq_8k-lag(waitsq_8k)     over (order by target_instance,snap_id) waitsq_8k
                  from dba_hist_interconnect_pings
                 where snap_id between :snap_start and :snap_end
                   and instance_number = :instno
                   and dbid = :dbid
               )
       ) a,
       ( select snap_id,
                end_interval_time
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) b
 where a.snap_id=b.snap_id
 order by 4,2
"""

os_stat = """
select snap_id,
       to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss')       "dates",
       max(to_char(end_interval_time,'dd hh24:mi'))             "dates2",
       max(decode(stat_name, 'USER_TIME', delta, 0))/100        "usr(s)",
       max(decode(stat_name, 'SYS_TIME', delta, 0))/100         "sys(s)",
       max(decode(stat_name, 'IOWAIT_TIME', delta, 0))/100      "wio(s)",
       max(decode(stat_name, 'IDLE_TIME', delta, 0))/100 - max(decode(stat_name, 'IOWAIT_TIME', delta, 0))/100 "idle(s)",
       max(decode(stat_name, 'BUSY_TIME', delta, 0))/100        "busy(s)",
       max(decode(stat_name, 'NICE_TIME', delta, 0))/100        "nice(s)",
       max(decode(stat_name, 'RSRC_MGR_CPU_WAIT_TIME', delta, 0))/100        "resource mgr wait(s)",
       max(decode(stat_name, 'PHYSICAL_MEMORY_BYTES', value, 0))/1024/1024   "Phy.memory(mb)",
       max(decode(stat_name, 'NUM_CPUS', value, 0))                          "cpu count",
       max(decode(stat_name, 'NUM_CPU_CORES', value, 0))                     "NUM_CPU_CORES",
       max(decode(stat_name, 'NUM_CPU_SOCKETS', value, 0))                   "NUM_CPU_SOCKETS",
       max(decode(stat_name, 'LOAD', delta, 0))            "Load",
       decode(max(decode(stat_name, 'USER_TIME', delta, 0))+max(decode(stat_name, 'SYS_TIME', delta, 0))+max(decode(stat_name, 'IDLE_TIME', delta, 0)),0,0,
            max(decode(stat_name, 'USER_TIME', delta, 0))
           /(max(decode(stat_name, 'USER_TIME', delta, 0))+max(decode(stat_name, 'SYS_TIME', delta, 0))+max(decode(stat_name, 'IDLE_TIME', delta, 0)))) "usr(%)",
       decode(max(decode(stat_name, 'USER_TIME', delta, 0))+max(decode(stat_name, 'SYS_TIME', delta, 0))+max(decode(stat_name, 'IDLE_TIME', delta, 0)),0,0,
            max(decode(stat_name, 'SYS_TIME', delta, 0))
           /(max(decode(stat_name, 'USER_TIME', delta, 0))+max(decode(stat_name, 'SYS_TIME', delta, 0))+max(decode(stat_name, 'IDLE_TIME', delta, 0)))) "sys(%)",
       decode(max(decode(stat_name, 'USER_TIME', delta, 0))+max(decode(stat_name, 'SYS_TIME', delta, 0))+max(decode(stat_name, 'IDLE_TIME', delta, 0)),0,0,
            max(decode(stat_name, 'IOWAIT_TIME', delta, 0))
           /(max(decode(stat_name, 'USER_TIME', delta, 0))+max(decode(stat_name, 'SYS_TIME', delta, 0))+max(decode(stat_name, 'IDLE_TIME', delta, 0)))) "wio(%)",
       decode(max(decode(stat_name, 'USER_TIME', delta, 0))+max(decode(stat_name, 'SYS_TIME', delta, 0))+max(decode(stat_name, 'IDLE_TIME', delta, 0)),0,0,
            (max(decode(stat_name, 'IDLE_TIME', delta, 0))-max(decode(stat_name, 'IOWAIT_TIME', delta, 0)))
           /(max(decode(stat_name, 'USER_TIME', delta, 0))+max(decode(stat_name, 'SYS_TIME', delta, 0))+max(decode(stat_name, 'IDLE_TIME', delta, 0)))) "idle(%)"
  from (
         select sn.snap_id,
                sn.end_interval_time,
                os.stat_name,
                (nvl(value - lag(value)
                  over ( partition by stat_name order by os.snap_id), 0)) delta,
                os.value value
          from dba_hist_osstat os,
               dba_hist_snapshot sn
         where os.snap_id = sn.snap_id
           and os.instance_number = sn.instance_number
           and os.dbid = sn.dbid
           and sn.snap_id between :snap_start and :snap_end
           and sn.instance_number = :instno
           and sn.dbid = :dbid
       )
where snap_id > :snap_start
group by snap_id, to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss')
order by snap_id
"""

time_model = """
select  snap_id,
        to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss') "Dates",
        max(to_char(end_interval_time,'dd hh24:mi'))       "Dates2",
        sum(decode(stat_name, 'DB time',                                          delta/1000000, 0)) "DB time",
        sum(decode(stat_name, 'DB CPU',                                           delta/1000000, 0)) "DB CPU",
        sum(decode(stat_name, 'background elapsed time',                          delta/1000000, 0)) "background elapsed",
        sum(decode(stat_name, 'background cpu time',                              delta/1000000, 0)) "background cpu",
        sum(decode(stat_name, 'sequence load elapsed time',                       delta/1000000, 0)) "sequence load",
        sum(decode(stat_name, 'parse time elapsed',                               delta/1000000, 0)) "parse time elapsed",
        sum(decode(stat_name, 'hard parse elapsed time',                          delta/1000000, 0)) "hard parse elasped",
        sum(decode(stat_name, 'sql execute elapsed time',                         delta/1000000, 0)) "sql execute elapsed",
        sum(decode(stat_name, 'connection management call elapsed time',          delta/1000000, 0)) "CM call elapsed",
        sum(decode(stat_name, 'failed parse elapsed time',                        delta/1000000, 0)) "failed parse",
        sum(decode(stat_name, 'failed parse (out of shared memory) elapsed time', delta/1000000, 0)) "failed parse(OutOfSM)",
        sum(decode(stat_name, 'hard parse (sharing criteria) elapsed time',       delta/1000000, 0)) "hard parse(SC)",
        sum(decode(stat_name, 'hard parse (bind mismatch) elapsed time',          delta/1000000, 0)) "hard parse(BM)",
        sum(decode(stat_name, 'PL/SQL execution elapsed time',                    delta/1000000, 0)) "PL/SQL execution",
        sum(decode(stat_name, 'inbound PL/SQL rpc elapsed time',                  delta/1000000, 0)) "inbound PL/SQL rpc",
        sum(decode(stat_name, 'PL/SQL compilation elapsed time',                  delta/1000000, 0)) "PL/SQL compilation",
        sum(decode(stat_name, 'Java execution elapsed time',                      delta/1000000, 0)) "Java execution",
        sum(decode(stat_name, 'DB CPU',                                           pct, 0)) "%DB CPU",
        sum(decode(stat_name, 'sequence load elapsed time',                       pct, 0)) "%sequence load",
        sum(decode(stat_name, 'parse time elapsed',                               pct, 0)) "%parse",
        sum(decode(stat_name, 'hard parse elapsed time',                          pct, 0)) "%hard parse",
        sum(decode(stat_name, 'sql execute elapsed time',                         pct, 0)) "%sql execute",
        sum(decode(stat_name, 'connection management call elapsed time',          pct, 0)) "%CM call",
        sum(decode(stat_name, 'failed parse elapsed time',                        pct, 0)) "%failed parse",
        sum(decode(stat_name, 'failed parse (out of shared memory) elapsed time', pct, 0)) "%failed parse(OutOfSM)",
        sum(decode(stat_name, 'hard parse (sharing criteria) elapsed time',       pct, 0)) "%hard parse(SC)",
        sum(decode(stat_name, 'hard parse (bind mismatch) elapsed time',          pct, 0)) "%hard parse(BM)",
        sum(decode(stat_name, 'PL/SQL execution elapsed time',                    pct, 0)) "%PL/SQL execution",
        sum(decode(stat_name, 'inbound PL/SQL rpc elapsed time',                  pct, 0)) "%inbound PL/SQL rpc",
        sum(decode(stat_name, 'PL/SQL compilation elapsed time',                  pct, 0)) "%PL/SQL compilation",
        sum(decode(stat_name, 'Java execution elapsed time',                      pct, 0)) "%Java execution"
  from  (
         select snap_id,
                end_interval_time,
                stat_name,
                delta,
                decode(sum(decode(stat_name, 'DB time', delta, 0)) over ( partition by snap_id ), 0, 0,
                       delta/sum(decode(stat_name, 'DB time', delta, 0)) over ( partition by snap_id )) pct
          from
               (
                select sn.snap_id,
                       sn.end_interval_time,
                       stm.stat_name,
                      (nvl(value - lag(value)
                         over ( partition by stm.stat_name order by stm.snap_id), 0)) delta
                  from dba_hist_sys_time_model stm,
                       dba_hist_snapshot sn
                 where stm.snap_id = sn.snap_id
                   and stm.instance_number = sn.instance_number
                   and stm.dbid = sn.dbid
                   and sn.snap_id between :snap_start and :snap_end
                   and sn.instance_number = :instno
                   and sn.dbid = :dbid
               )
        )
where snap_id > :snap_start
group by snap_id, to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss')
order by snap_id
"""

wait5 = """
select /*+ no_merge(v1) */
       sn.snap_id,
       to_char(sn.end_interval_time,'yyyy-mm-dd hh24:mi:ss') "Dates",
       max(to_char(sn.end_interval_time,'dd hh24:mi'))       "Dates2",
       v_dbtime.delta/1000000 "DB time",
       decode(max(v_call.calls),0,0,(v_dbtime.delta/1000)/max(v_call.calls)) "db time per call",
       decode(max(v_call.exec),0,0,(v_dbtime.delta/1000)/max(v_call.exec)) "db time per execute",
       max(decode(rank, 1, v1.delta_wait, 0)) wait_1,
       max(decode(rank, 2, v1.delta_wait, 0)) wait_2,
       max(decode(rank, 3, v1.delta_wait, 0)) wait_3,
       max(decode(rank, 4, v1.delta_wait, 0)) wait_4,
       max(decode(rank, 5, v1.delta_wait, 0)) wait_5,
       max(decode(rank, 1, v1.delta_time/1000000, 0)) time_1,
       max(decode(rank, 2, v1.delta_time/1000000, 0)) time_2,
       max(decode(rank, 3, v1.delta_time/1000000, 0)) time_3,
       max(decode(rank, 4, v1.delta_time/1000000, 0)) time_4,
       max(decode(rank, 5, v1.delta_time/1000000, 0)) time_5,
       max(decode(rank, 1, v1.avg_wait, 0)) avg_wait_1,
       max(decode(rank, 2, v1.avg_wait, 0)) avg_wait_2,
       max(decode(rank, 3, v1.avg_wait, 0)) avg_wait_3,
       max(decode(rank, 4, v1.avg_wait, 0)) avg_wait_4,
       max(decode(rank, 5, v1.avg_wait, 0)) avg_wait_5,
       max(decode(rank, 1, decode(v_dbtime.delta, 0, 0, v1.delta_time/v_dbtime.delta), 0)) pctwait_1,
       max(decode(rank, 2, decode(v_dbtime.delta, 0, 0, v1.delta_time/v_dbtime.delta), 0)) pctwait_2,
       max(decode(rank, 3, decode(v_dbtime.delta, 0, 0, v1.delta_time/v_dbtime.delta), 0)) pctwait_3,
       max(decode(rank, 4, decode(v_dbtime.delta, 0, 0, v1.delta_time/v_dbtime.delta), 0)) pctwait_4,
       max(decode(rank, 5, decode(v_dbtime.delta, 0, 0, v1.delta_time/v_dbtime.delta), 0)) pctwait_5
  from (  select v_wait.snap_id,
                 v_rank.rank,
                 v_wait.event_name,
                 v_wait.delta_wait,
                 v_wait.delta_time,
                 decode(nvl(v_wait.delta_wait,0), 0, 0, v_wait.delta_time/v_wait.delta_wait/1000) avg_wait
            from ( select event_name, rownum rank
                     from
                        ( select event_name, sum(nvl(delta,0))
                            from
                               ( select snap_id,
                                        event_name,
                                        nvl(time_waited_micro -
                                            lag(time_waited_micro) over ( partition by event_name order by snap_id), 0) delta
                                   from dba_hist_system_event
                                  where wait_class != 'Idle'
                                    and snap_id between :snap_start and :snap_end
                                    and instance_number = :instno
                                    and dbid = :dbid
                                 union all
                                 select snap_id,
                                        decode(stat_name, 'DB CPU', 'CPU time', stat_name) event_name,
                                        nvl(value - lag(value) over ( partition by stat_name order by snap_id ), 0)  delta
                                   from dba_hist_sys_time_model
                                  where stat_name = 'DB CPU'
                                    and snap_id between :snap_start and :snap_end
                                    and instance_number = :instno
                                    and dbid = :dbid
                               )
                          group by event_name order by sum(delta) desc
                        )
                    where rownum <= 5
                 ) v_rank,
                 ( select snap_id,
                          event_name,
                          nvl(total_waits - lag(total_waits) over ( partition by event_name order by snap_id), 0) delta_wait,
                          nvl(time_waited_micro - lag(time_waited_micro) over ( partition by event_name order by snap_id), 0) delta_time
                     from dba_hist_system_event
                    where wait_class != 'Idle'
                      and snap_id between :snap_start and :snap_end
                      and instance_number = :instno
                      and dbid = :dbid
                   union all
                   select snap_id,
                          decode(stat_name, 'DB CPU', 'CPU time', stat_name) event_name,
                          NULL,
                          nvl(value - lag(value) over ( partition by stat_name order by snap_id ), 0)  delta
                     from dba_hist_sys_time_model
                    where stat_name = 'DB CPU'
                      and snap_id between :snap_start and :snap_end
                      and instance_number = :instno
                      and dbid = :dbid
                 ) v_wait
           where v_wait.event_name = v_rank.event_name
          ) v1,
          ( select snap_id,
                   nvl(value - lag(value) over ( partition by stat_name order by snap_id ), 0)  delta
              from dba_hist_sys_time_model
             where stat_name = 'DB time'
               and snap_id between :snap_start and :snap_end
               and instance_number = :instno
               and dbid = :dbid
          ) v_dbtime,
          (
          select snap_id,
                 sum(case when stat_name in ('user calls', 'recursive calls') then delta else 0 end) calls,
                 sum(case when stat_name = 'execute count' then delta else 0 end) exec
            from (
                  select   snap_id,
                           stat_name,
                           nvl(value - lag(value) over ( partition by stat_name order by snap_id ), 0)  delta
                      from dba_hist_sysstat
                     where stat_name in ( 'user calls', 'recursive calls', 'execute count' )
                       and snap_id between :snap_start and :snap_end
                       and instance_number = :instno
                       and dbid = :dbid
                       )
          group by snap_id
          ) v_call,
          ( select snap_id,
                   end_interval_time
              from dba_hist_snapshot
             where snap_id between 1+ :snap_start and :snap_end
               and instance_number = :instno
               and dbid = :dbid
          ) sn
 where v1.snap_id = sn.snap_id
   and v_call.snap_id = sn.snap_id
   and v_dbtime.snap_id = sn.snap_id
 group by sn.snap_id,
       to_char(sn.end_interval_time,'yyyy-mm-dd hh24:mi:ss'),
       v_dbtime.delta
 order by sn.snap_id
"""

wait52 = """
select event_name||' ('||nvl(wait_class,'N/A')||')'
  from ( select 1 num from dual
          union all
         select 2 from dual
          union all
         select 3 from dual
          union all
         select 4 from dual
       ) v1,
       (
         select event_name, wait_class, rownum rank
           from
              ( select event_name, wait_class, sum(nvl(delta,0))
                  from
                    ( select snap_id,
                             event_name,
                             wait_class,
                             nvl(time_waited_micro -
                              lag(time_waited_micro) over ( partition by event_name order by snap_id), 0) delta
                        from dba_hist_system_event
                       where wait_class != 'Idle'
                         and snap_id between :snap_start and :snap_end
                         and instance_number = :instno
                         and dbid = :dbid
                       union all
                      select snap_id,
                             decode(stat_name, 'DB CPU', 'CPU time', stat_name) event_name,
                             null,
                             nvl(value - lag(value) over ( partition by stat_name order by snap_id ), 0)  delta
                        from dba_hist_sys_time_model
                       where stat_name = 'DB CPU'
                         and snap_id between :snap_start and :snap_end
                         and instance_number = :instno
                         and dbid = :dbid
                    )
              group by event_name, wait_class order by sum(delta) desc
              )
          where rownum <= 5
      ) v2
 order by num, rank
"""

wait20 = """
select sn.snap_id,
       to_char(sn.end_interval_time,'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time,'dd hh24:mi'))       dates2,
       v_dbtime.delta/1000000 "DB time",
       decode(max(v_call.calls),0,0,(v_dbtime.delta/1000)/max(v_call.calls)) "db time per call",
       decode(max(v_call.exec),0,0,(v_dbtime.delta/1000)/max(v_call.exec)) "db time per execute",
       max(decode(rank, 1, v1.delta_wait, 0)) wait_1,
       max(decode(rank, 2, v1.delta_wait, 0)) wait_2,
       max(decode(rank, 3, v1.delta_wait, 0)) wait_3,
       max(decode(rank, 4, v1.delta_wait, 0)) wait_4,
       max(decode(rank, 5, v1.delta_wait, 0)) wait_5,
       max(decode(rank, 6, v1.delta_wait, 0)) wait_6,
       max(decode(rank, 7, v1.delta_wait, 0)) wait_7,
       max(decode(rank, 8, v1.delta_wait, 0)) wait_8,
       max(decode(rank, 9, v1.delta_wait, 0)) wait_9,
       max(decode(rank,10, v1.delta_wait, 0)) wait_10,
       max(decode(rank,11, v1.delta_wait, 0)) wait_11,
       max(decode(rank,12, v1.delta_wait, 0)) wait_12,
       max(decode(rank,13, v1.delta_wait, 0)) wait_13,
       max(decode(rank,14, v1.delta_wait, 0)) wait_14,
       max(decode(rank,15, v1.delta_wait, 0)) wait_15,
       max(decode(rank,16, v1.delta_wait, 0)) wait_16,
       max(decode(rank,17, v1.delta_wait, 0)) wait_17,
       max(decode(rank,18, v1.delta_wait, 0)) wait_18,
       max(decode(rank,19, v1.delta_wait, 0)) wait_19,
       max(decode(rank,20, v1.delta_wait, 0)) wait_20,
       max(decode(rank, 1, v1.delta_time/1000000,0)) time_1,
       max(decode(rank, 2, v1.delta_time/1000000,0)) time_2,
       max(decode(rank, 3, v1.delta_time/1000000,0)) time_3,
       max(decode(rank, 4, v1.delta_time/1000000,0)) time_4,
       max(decode(rank, 5, v1.delta_time/1000000,0)) time_5,
       max(decode(rank, 6, v1.delta_time/1000000,0)) time_6,
       max(decode(rank, 7, v1.delta_time/1000000,0)) time_7,
       max(decode(rank, 8, v1.delta_time/1000000,0)) time_8,
       max(decode(rank, 9, v1.delta_time/1000000,0)) time_9,
       max(decode(rank,10, v1.delta_time/1000000,0)) time_10,
       max(decode(rank,11, v1.delta_time/1000000,0)) time_11,
       max(decode(rank,12, v1.delta_time/1000000,0)) time_12,
       max(decode(rank,13, v1.delta_time/1000000,0)) time_13,
       max(decode(rank,14, v1.delta_time/1000000,0)) time_14,
       max(decode(rank,15, v1.delta_time/1000000,0)) time_15,
       max(decode(rank,16, v1.delta_time/1000000,0)) time_16,
       max(decode(rank,17, v1.delta_time/1000000,0)) time_17,
       max(decode(rank,18, v1.delta_time/1000000,0)) time_18,
       max(decode(rank,19, v1.delta_time/1000000,0)) time_19,
       max(decode(rank,20, v1.delta_time/1000000,0)) time_20,
       max(decode(rank, 1, v1.avg_wait, 0)) avg_wait_1,
       max(decode(rank, 2, v1.avg_wait, 0)) avg_wait_2,
       max(decode(rank, 3, v1.avg_wait, 0)) avg_wait_3,
       max(decode(rank, 4, v1.avg_wait, 0)) avg_wait_4,
       max(decode(rank, 5, v1.avg_wait, 0)) avg_wait_5,
       max(decode(rank, 6, v1.avg_wait, 0)) avg_wait_6,
       max(decode(rank, 7, v1.avg_wait, 0)) avg_wait_7,
       max(decode(rank, 8, v1.avg_wait, 0)) avg_wait_8,
       max(decode(rank, 9, v1.avg_wait, 0)) avg_wait_9,
       max(decode(rank,10, v1.avg_wait, 0)) avg_wait_10,
       max(decode(rank,11, v1.avg_wait, 0)) avg_wait_11,
       max(decode(rank,12, v1.avg_wait, 0)) avg_wait_12,
       max(decode(rank,13, v1.avg_wait, 0)) avg_wait_13,
       max(decode(rank,14, v1.avg_wait, 0)) avg_wait_14,
       max(decode(rank,15, v1.avg_wait, 0)) avg_wait_15,
       max(decode(rank,16, v1.avg_wait, 0)) avg_wait_16,
       max(decode(rank,17, v1.avg_wait, 0)) avg_wait_17,
       max(decode(rank,18, v1.avg_wait, 0)) avg_wait_18,
       max(decode(rank,19, v1.avg_wait, 0)) avg_wait_19,
       max(decode(rank,20, v1.avg_wait, 0)) avg_wait_20,
       max(decode(rank, 1, decode(v_dbtime.delta, 0, 0, 100*(nvl(v1.delta_time,0)/v_dbtime.delta)), 0)) pctwait_1,
       max(decode(rank, 2, decode(v_dbtime.delta, 0, 0, 100*(nvl(v1.delta_time,0)/v_dbtime.delta)), 0)) pctwait_2,
       max(decode(rank, 3, decode(v_dbtime.delta, 0, 0, 100*(nvl(v1.delta_time,0)/v_dbtime.delta)), 0)) pctwait_3,
       max(decode(rank, 4, decode(v_dbtime.delta, 0, 0, 100*(nvl(v1.delta_time,0)/v_dbtime.delta)), 0)) pctwait_4,
       max(decode(rank, 5, decode(v_dbtime.delta, 0, 0, 100*(nvl(v1.delta_time,0)/v_dbtime.delta)), 0)) pctwait_5,
       max(decode(rank, 6, decode(v_dbtime.delta, 0, 0, 100*(nvl(v1.delta_time,0)/v_dbtime.delta)), 0)) pctwait_6,
       max(decode(rank, 7, decode(v_dbtime.delta, 0, 0, 100*(nvl(v1.delta_time,0)/v_dbtime.delta)), 0)) pctwait_7,
       max(decode(rank, 8, decode(v_dbtime.delta, 0, 0, 100*(nvl(v1.delta_time,0)/v_dbtime.delta)), 0)) pctwait_8,
       max(decode(rank, 9, decode(v_dbtime.delta, 0, 0, 100*(nvl(v1.delta_time,0)/v_dbtime.delta)), 0)) pctwait_9,
       max(decode(rank,10, decode(v_dbtime.delta, 0, 0, 100*(nvl(v1.delta_time,0)/v_dbtime.delta)), 0)) pctwait_10,
       max(decode(rank,11, decode(v_dbtime.delta, 0, 0, 100*(nvl(v1.delta_time,0)/v_dbtime.delta)), 0)) pctwait_11,
       max(decode(rank,12, decode(v_dbtime.delta, 0, 0, 100*(nvl(v1.delta_time,0)/v_dbtime.delta)), 0)) pctwait_12,
       max(decode(rank,13, decode(v_dbtime.delta, 0, 0, 100*(nvl(v1.delta_time,0)/v_dbtime.delta)), 0)) pctwait_13,
       max(decode(rank,14, decode(v_dbtime.delta, 0, 0, 100*(nvl(v1.delta_time,0)/v_dbtime.delta)), 0)) pctwait_14,
       max(decode(rank,15, decode(v_dbtime.delta, 0, 0, 100*(nvl(v1.delta_time,0)/v_dbtime.delta)), 0)) pctwait_15,
       max(decode(rank,16, decode(v_dbtime.delta, 0, 0, 100*(nvl(v1.delta_time,0)/v_dbtime.delta)), 0)) pctwait_16,
       max(decode(rank,17, decode(v_dbtime.delta, 0, 0, 100*(nvl(v1.delta_time,0)/v_dbtime.delta)), 0)) pctwait_17,
       max(decode(rank,18, decode(v_dbtime.delta, 0, 0, 100*(nvl(v1.delta_time,0)/v_dbtime.delta)), 0)) pctwait_18,
       max(decode(rank,19, decode(v_dbtime.delta, 0, 0, 100*(nvl(v1.delta_time,0)/v_dbtime.delta)), 0)) pctwait_19,
       max(decode(rank,20, decode(v_dbtime.delta, 0, 0, 100*(nvl(v1.delta_time,0)/v_dbtime.delta)), 0)) pctwait_20
  from (  select v_wait.snap_id,
                 v_rank.rank,
                 v_wait.event_name,
                 v_wait.delta_wait,
                 v_wait.delta_time,
                 decode(nvl(v_wait.delta_wait,0), 0, 0, v_wait.delta_time/v_wait.delta_wait/1000) avg_wait
            from ( select event_name, rownum rank
                     from
                        ( select event_name, sum(nvl(delta,0))
                            from
                               ( select snap_id,
                                        event_name,
                                        nvl(time_waited_micro -
                                            lag(time_waited_micro) over ( partition by event_name order by snap_id), 0) delta
                                   from dba_hist_system_event
                                  where wait_class != 'Idle'
                                    and snap_id between :snap_start and :snap_end
                                    and instance_number = :instno
                                    and dbid = :dbid
                                 union all
                                 select snap_id,
                                        decode(stat_name, 'DB CPU', 'CPU time', stat_name) event_name,
                                        nvl(value - lag(value) over ( partition by stat_name order by snap_id ), 0)  delta
                                   from dba_hist_sys_time_model
                                  where stat_name = 'DB CPU'
                                    and snap_id between :snap_start and :snap_end
                                    and instance_number = :instno
                                    and dbid = :dbid
                               )
                          group by event_name order by sum(delta) desc
                        )
                    where rownum <= 20
                 ) v_rank,
                 ( select snap_id,
                          event_name,
                          nvl(total_waits - lag(total_waits) over ( partition by event_name order by snap_id), 0) delta_wait,
                          nvl(time_waited_micro - lag(time_waited_micro) over ( partition by event_name order by snap_id), 0) delta_time
                     from dba_hist_system_event
                    where wait_class != 'Idle'
                      and snap_id between :snap_start and :snap_end
                      and instance_number = :instno
                      and dbid = :dbid
                   union all
                   select snap_id,
                          decode(stat_name, 'DB CPU', 'CPU time', stat_name) event_name,
                          NULL,
                          nvl(value - lag(value) over ( partition by stat_name order by snap_id ), 0)  delta
                     from dba_hist_sys_time_model
                    where stat_name = 'DB CPU'
                      and snap_id between :snap_start and :snap_end
                      and instance_number = :instno
                      and dbid = :dbid
                 ) v_wait
           where v_wait.event_name = v_rank.event_name
          ) v1,
          ( select snap_id,
                   nvl(value - lag(value) over ( partition by stat_name order by snap_id ), 0)  delta
              from dba_hist_sys_time_model
             where stat_name = 'DB time'
               and snap_id between :snap_start and :snap_end
               and instance_number = :instno
               and dbid = :dbid
          ) v_dbtime,
          (
          select snap_id,
                 sum(case when stat_name in ('user calls', 'recursive calls') then delta else 0 end) calls,
                 sum(case when stat_name = 'execute count' then delta else 0 end) exec
            from (
                  select   snap_id,
                           stat_name,
                           nvl(value - lag(value) over ( partition by stat_name order by snap_id ), 0)  delta
                      from dba_hist_sysstat
                     where stat_name in ( 'user calls', 'recursive calls', 'execute count' )
                       and snap_id between :snap_start and :snap_end
                       and instance_number = :instno
                       and dbid = :dbid
                       )
          group by snap_id
          ) v_call,
          ( select snap_id,
                   end_interval_time
              from dba_hist_snapshot
             where snap_id between 1+ :snap_start and :snap_end
               and instance_number = :instno
               and dbid = :dbid
          ) sn
 where v1.snap_id = sn.snap_id
   and v_call.snap_id = sn.snap_id
   and v_dbtime.snap_id = sn.snap_id
 group by sn.snap_id,
       to_char(sn.end_interval_time,'yyyy-mm-dd hh24:mi:ss'),
       v_dbtime.delta
 order by sn.snap_id
"""

wait202 = """
select event_name||' ('||nvl(wait_class,'N/A')||')'
  from ( select 1 num from dual
          union all
         select 2 from dual
          union all
         select 3 from dual
          union all
         select 4 from dual
       ) v1,
       (
         select event_name, wait_class, rownum rank
           from
              ( select event_name, wait_class, sum(nvl(delta,0))
                  from
                    ( select snap_id,
                             event_name,
                             wait_class,
                             nvl(time_waited_micro -
                              lag(time_waited_micro) over ( partition by event_name order by snap_id), 0) delta
                        from dba_hist_system_event
                       where wait_class != 'Idle'
                         and snap_id between :snap_start and :snap_end
                         and instance_number = :instno
                         and dbid = :dbid
                       union all
                      select snap_id,
                             decode(stat_name, 'DB CPU', 'CPU time', stat_name) event_name,
                             null,
                             nvl(value - lag(value) over ( partition by stat_name order by snap_id ), 0)  delta
                        from dba_hist_sys_time_model
                       where stat_name = 'DB CPU'
                         and snap_id between :snap_start and :snap_end
                         and instance_number = :instno
                         and dbid = :dbid
                    )
              group by event_name, wait_class order by sum(delta) desc
              )
          where rownum <= 20
      ) v2
 order by num, rank
"""

wait_disk = """
select snap_id,
       to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss')      "dates",
       max(to_char(end_interval_time,'dd hh24:mi'))            "dates2",
       sum(decode(event_name, 'db file sequential read', delta_waits/interval, 0)) "db file sequential read",
       sum(decode(event_name, 'db file scattered read',  delta_waits/interval, 0)) "db file scattered read",
       sum(decode(event_name, 'direct path read',        delta_waits/interval, 0)) "direct path read",
       sum(decode(event_name, 'direct path write',       delta_waits/interval, 0)) "direct path write",
       sum(decode(event_name, 'db file sequential read', delta_avgt, 0))/1000 "db file sequential read(ms)",
       sum(decode(event_name, 'db file scattered read',  delta_avgt, 0))/1000 "db file scattered read(ms)",
       sum(decode(event_name, 'direct path read',        delta_avgt, 0))/1000 "direct path read(ms)",
       sum(decode(event_name, 'direct path write',       delta_avgt, 0))/1000 "direct path write(ms)"
  from (
         select sn.snap_id,
                sn.end_interval_time,
                to_number(substr((sn.end_interval_time-sn.begin_interval_time)*86400,2,9)) interval,
                event_name,
                (nvl(total_waits - lag(total_waits) over ( partition by event_name order by se.snap_id), 0)) delta_waits,
               case when nvl(total_waits - lag(total_waits) over ( partition by event_name order by se.snap_id), 0) > 0
                  then
                nvl(time_waited_micro - lag(time_waited_micro) over ( partition by event_name order by se.snap_id), 0) /
                nvl(total_waits - lag(total_waits) over ( partition by event_name order by se.snap_id), 0)
                  else
                  0
                  end delta_avgt
         from dba_hist_system_event se,
              dba_hist_snapshot sn
         where se.snap_id = sn.snap_id
           and se.instance_number = sn.instance_number
           and se.dbid = sn.dbid
           and event_name in ('db file sequential read',
                              'db file scattered read',
                              'direct path read',
                              'direct path write')
           and sn.snap_id between :snap_start and :snap_end
           and sn.instance_number = :instno
           and sn.dbid = :dbid
       )
 where snap_id > :snap_start
 group by snap_id, to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss')
 order by snap_id
"""

enqueue5 = """
select snap_id,
       to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(end_interval_time,'dd hh24:mi'))       dates2,
       max(decode(rank, 1,wait_tm, 0))/1000               rank1_wait_tm,
       max(decode(rank, 2,wait_tm, 0))/1000               rank2_wait_tm,
       max(decode(rank, 3,wait_tm, 0))/1000               rank3_wait_tm,
       max(decode(rank, 4,wait_tm, 0))/1000               rank4_wait_tm,
       max(decode(rank, 5,wait_tm, 0))/1000               rank5_wait_tm
  from (
         select e.snap_id,
                s.end_interval_time,
                to_number(substr((s.end_interval_time-s.begin_interval_time)*86400,2,9)) interval,
                rank,
                (e.cum_wait_time - nvl(b.cum_wait_time,0))  wait_tm
           from DBA_HIST_ENQUEUE_STAT e,
                DBA_HIST_ENQUEUE_STAT b,
                DBA_HIST_SNAPSHOT S,
                (select ety,req_reason,wttm, rownum rank
                   from ( select e.eq_type  ety,
                                 e.req_reason,
                                 e.cum_wait_time - b.cum_wait_time  wttm
                            from DBA_HIST_ENQUEUE_STAT e,
                                 DBA_HIST_ENQUEUE_STAT b
                           where b.snap_id         = :snap_start
                             and e.snap_id         = :snap_end
                             and b.dbid(+)            = :dbid
                             and e.dbid               = :dbid
                             and b.dbid(+)            = e.dbid
                             and b.instance_number(+) = :instno
                             and e.instance_number    = :instno
                             and b.instance_number(+) = e.instance_number
                             and b.eq_type(+)         = e.eq_type
                             and b.req_reason(+)      = e.req_reason
                             and e.total_wait# - nvl(b.total_wait#,0) > 0
                           order by wttm desc )  a
                  where rownum <= 5  ) top_5
          where b.snap_id(+)         = e.snap_id - 1
            and e.snap_id    between :snap_start and :snap_end
            and b.dbid(+)            = :dbid
            and e.dbid               = :dbid
            and b.dbid(+)            = e.dbid
            and b.instance_number(+) = :instno
            and e.instance_number    = :instno
            and b.eq_type            = e.eq_type
            and b.req_reason         = e.req_reason
            and e.eq_type            = top_5.ety
            and b.eq_type            = top_5.ety
            and e.req_reason         = top_5.req_reason
            and b.req_reason         = top_5.req_reason
            and e.snap_id            = s.snap_id
            and s.instance_number    = :instno
            and s.dbid               = :dbid
            and s.snap_id between  :snap_start and :snap_end
          order by e.snap_id )
 group by snap_id ,to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss')
 order by snap_id
"""

enqueue52 = """
select rank||':'||ety || '-' || to_char(nvl(l.name,' '))
            || decode( upper(req_reason)
               , 'CONTENTION', null
               , '-',          null
               , ' ('||req_reason||')')             Enqueu_type
from v$lock_type l,
(select ety,req_reason,wttm,rownum rank
  from  (select e.eq_type  ety,e.req_reason req_reason
               ,e.cum_wait_time - b.cum_wait_time  wttm
          from DBA_HIST_ENQUEUE_STAT e
              ,DBA_HIST_ENQUEUE_STAT b
         where b.snap_id         = :snap_start
           and e.snap_id         = :snap_end
              and b.dbid(+)            = :dbid
                 and e.dbid               = :dbid
                 and b.dbid(+)            = e.dbid
                 and b.instance_number(+) = :instno
                 and e.instance_number    = :instno
                 and b.instance_number(+) = e.instance_number
                 and b.eq_type(+)         = e.eq_type
                 and b.req_reason(+)      = e.req_reason
                 and e.total_wait# - nvl(b.total_wait#,0) > 0
               order by wttm desc )  a
      where rownum <= 5  ) top_5
where l.type(+) = top_5.ety
order by rank
"""

enq_raw_sql = """
select e.snap_id snapid,
       to_char(s.end_interval_time,'yyyy-mm-dd hh24:mi:ss') dates,
       e.eq_type || '-' || to_char(nvl(l.name,' ')) || decode(upper(e.req_reason),'CONTENTION',null,'-',null,' ('||e.req_reason||')')   enq_type,
       e.total_req#    - nvl(b.total_req#,0)            requests,
       e.succ_req#     - nvl(b.succ_req#,0)             succ_gets,
       e.failed_req#   - nvl(b.failed_req#,0)           fail_gets,
       e.total_wait#   - nvl(b.total_wait#,0)           waits,
       (e.cum_wait_time - nvl(b.cum_wait_time,0))/1000  "Wait time(s)",
       decode((e.total_wait#   - nvl(b.total_wait#,0)), 0,0,
              (e.cum_wait_time - nvl(b.cum_wait_time,0))
             /(e.total_wait#   - nvl(b.total_wait#,0))) "avg wait time(ms)"
  from DBA_HIST_ENQUEUE_STAT e,
       DBA_HIST_ENQUEUE_STAT b,
       v$lock_type           l,
       DBA_HIST_SNAPSHOT     s
 where b.snap_id(+)         = e.snap_id - 1
   and e.snap_id    between :snap_start and :snap_end
   and b.dbid(+)            = :dbid
   and e.dbid               = :dbid
   and b.dbid(+)            = e.dbid
   and b.instance_number(+) = :instno
   and e.instance_number    = :instno
   and b.instance_number(+) = e.instance_number
   and b.eq_type(+)         = e.eq_type
   and b.req_reason(+)      = e.req_reason
   and e.total_wait# - nvl(b.total_wait#,0) > 0
   and l.type(+)            = e.eq_type
   and e.snap_id = s.snap_id
   and s.INSTANCE_NUMBER = :instno
   and s.dbid = :dbid
   and s.snap_id between  :snap_start and :snap_end
 order by e.snap_id, 8 desc, 7 desc
"""

latch = """
select e.snap_id  snapid,
       to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(end_interval_time,'dd hh24:mi'))       dates2,
       max(decode(rank, 1, (e.wait_time - b.wait_time)/interval, 0))/1000 rank1_wait_t,
       max(decode(rank, 2, (e.wait_time - b.wait_time)/interval, 0))/1000 rank2_wait_t,
       max(decode(rank, 3, (e.wait_time - b.wait_time)/interval, 0))/1000 rank3_wait_t,
       max(decode(rank, 4, (e.wait_time - b.wait_time)/interval, 0))/1000 rank4_wait_t,
       max(decode(rank, 5, (e.wait_time - b.wait_time)/interval, 0))/1000 rank5_wait_t,
       max(decode(rank, 1, decode((e.gets-b.gets),0,0,(e.misses-b.misses)/(e.gets-b.gets)), 0)) rank1_missratio,
       max(decode(rank, 2, decode((e.gets-b.gets),0,0,(e.misses-b.misses)/(e.gets-b.gets)), 0)) rank2_missratio,
       max(decode(rank, 3, decode((e.gets-b.gets),0,0,(e.misses-b.misses)/(e.gets-b.gets)), 0)) rank3_missratio,
       max(decode(rank, 4, decode((e.gets-b.gets),0,0,(e.misses-b.misses)/(e.gets-b.gets)), 0)) rank4_missratio,
       max(decode(rank, 5, decode((e.gets-b.gets),0,0,(e.misses-b.misses)/(e.gets-b.gets)), 0)) rank5_missratio,
       max(decode(rank, 1, (e.misses-b.misses), 0)) rank1_misses,
       max(decode(rank, 2, (e.misses-b.misses), 0)) rank2_misses,
       max(decode(rank, 3, (e.misses-b.misses), 0)) rank3_misses,
       max(decode(rank, 4, (e.misses-b.misses), 0)) rank4_misses,
       max(decode(rank, 5, (e.misses-b.misses), 0)) rank5_misses
  from dba_hist_latch b,
       dba_hist_latch e,
       dba_hist_latch_name n,
       (select instance_number,
               snap_id,
               end_interval_time,
               to_number(substr((end_interval_time-begin_interval_time)*86400,2,9)) interval
          from dba_hist_snapshot
         where snap_id between :snap_start and :snap_end
           and instance_number = :instno
           and dbid = :dbid
       ) s,
       (select dbid, latch_hash, rownum rank
          from ( select e.dbid, 
                        e.latch_hash latch_hash,
                        e.wait_time - b.wait_time  wait_time
                 from dba_hist_latch b,
                      dba_hist_latch e
                where b.snap_id         = :snap_start
                  and e.snap_id         = :snap_end
                  and b.instance_number = :instno
                  and e.instance_number = :instno
                  and b.dbid            = :dbid
                  and e.dbid            = :dbid
                  and b.latch_hash      = e.latch_hash
                order by wait_time desc ) a
         where rownum <= 5
       ) top_5
 where b.snap_id(+)         = e.snap_id - 1
    and e.snap_id    between :snap_start and :snap_end
    and b.dbid(+)            = :dbid
    and e.dbid               = :dbid
    and b.dbid(+)            = e.dbid
    and b.instance_number(+) = :instno
    and e.instance_number    = :instno
    and b.latch_hash         = e.latch_hash
    and e.latch_hash = top_5.latch_hash
    and e.dbid = top_5.dbid
    and top_5.latch_hash = n.latch_hash
    and top_5.dbid = n.dbid
    and b.latch_hash = top_5.latch_hash
    and e.snap_id = s.snap_id
 group by e.snap_id, to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss')
 order by e.snap_id
"""

latch2 = """
select aa.lable || n.latch_name title
from dba_hist_latch_name n,
     (select dbid, latch_hash ,rownum rank
       from ( select e.dbid, e.latch_hash latch_hash
                   , e.wait_time - b.wait_time  wait_tim
                from dba_hist_latch b
                   , dba_hist_latch e
               where b.snap_id         = :snap_start
                 and e.snap_id         = :snap_end
                 and b.instance_number = :instno
                 and e.instance_number = :instno
                 and b.dbid(+)         = :dbid
                 and e.dbid            = :dbid
                 and b.latch_hash      = e.latch_hash
            order by wait_tim desc
            ) a
       where rownum <= 5
     ) top_5,
     ( select 1 no,'' lable from dual union all
       select 2,   ''     from dual union all
       select 3,   'misses: '     from dual
     ) aa
where top_5.latch_hash = n.latch_hash
and   top_5.dbid       = n.dbid
order by aa.no, top_5.rank
"""

latch_sleep = """
select b.latch_name                        latchname,
       e.gets        - b.gets              gets,
       e.misses      - b.misses            misses,
       e.sleeps      - b.sleeps            sleeps,
       e.spin_gets   - b.spin_gets         spin_gets
  from DBA_HIST_LATCH b,
       DBA_HIST_LATCH e,
       DBA_HIST_SNAPSHOT S
 where b.snap_id         = :snap_start
   and e.snap_id         = :snap_end
   and b.dbid            = :dbid
   and e.dbid            = :dbid
   and b.dbid            = e.dbid
   and b.instance_number = :instno
   and e.instance_number = :instno
   and b.instance_number = e.instance_number
   and b.latch_name      = e.latch_name
   and e.sleeps - b.sleeps > 0
   and e.snap_id = s.snap_id
   and s.INSTANCE_NUMBER = :instno
   and s.dbid = :dbid
   and s.snap_id between  :snap_start and :snap_end
 order by 4 desc,1
"""

latch_miss = """
select e.parent_name                              parent,
       e.where_in_code                            "where from",
       e.nwfail_count  - nvl(b.nwfail_count,0)    nowait_misses,
       e.sleep_count   - nvl(b.sleep_count,0)     sleeps,
       e.wtr_slp_count - nvl(b.wtr_slp_count,0)   waiter_sleeps
  from dba_hist_latch_misses_summary b,
       dba_hist_latch_misses_summary e,
       dba_hist_snapshot s
 where b.snap_id(+)         = :snap_start
   and e.snap_id            = :snap_end
   and b.dbid(+)            = :dbid
   and e.dbid               = :dbid
   and b.dbid(+)            = e.dbid
   and b.instance_number(+) = :instno
   and e.instance_number    = :instno
   and b.instance_number(+) = e.instance_number
   and b.parent_name(+)     = e.parent_name
   and b.where_in_code(+)   = e.where_in_code
   and e.sleep_count        > nvl(b.sleep_count,0)
   and e.snap_id = s.snap_id
   and s.instance_number = :instno
   and s.dbid = :dbid
   and s.snap_id between  :snap_start and :snap_end
 order by e.snap_id,e.parent_name, sleeps desc
"""

activity = """
select snap_id,
       to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss')             "dates",
       max(to_char(end_interval_time,'dd hh24:mi'))                   "dates2",
       max(decode(stat_name, 'logons current', value, 0))             "logons current",
       max(decode(stat_name, 'opened cursors current', value, 0))     "opened cursors current",
       max(decode(stat_name, 'workarea memory allocated', value, 0))  "workarea memory allocated",
       max(decode(stat_name, 'session cursor cache count', value, 0)) "session cursor cache count",
       max(decode(stat_name, 'open threads (derived)', value, 0))     "open threads"
  from (
         select sn.snap_id,
                sn.end_interval_time,
                v1.stat_name,
                v1.value
          from (select snap_id,
                       instance_number,
                       dbid,
                       stat_name,
                       value
                  from dba_hist_sysstat sy
                 where sy.stat_name in ( 'logons current',
                                         'opened cursors current',
                                         'workarea memory allocated',
                                         'session cursor cache count' )
                union all
                select snap_id,
                       instance_number,
                       dbid,
                       'open threads (derived)' stat_name,
                       count(thread#) value
                  from dba_hist_thread
                 where status = 'OPEN'
                 group by snap_id, instance_number, dbid
               ) v1,
               dba_hist_snapshot sn
         where v1.snap_id = sn.snap_id
           and v1.instance_number = sn.instance_number
           and v1.dbid = sn.dbid
           and sn.snap_id between 1+ :snap_start and :snap_end
           and sn.instance_number = :instno
           and sn.dbid = :dbid
       )
group by snap_id, to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss')
order by snap_id
"""

sqlsum = """
select sn.snap_id,
       to_char(sn.end_interval_time,'yyyy-mm-dd hh24:mi:ss')            "dates",
       to_char(sn.end_interval_time,'dd hh24:mi')                       "dates2",
       total_sql - single_use_sql                                       "#SQL(exec>1)",
       single_use_sql                                                   "#SQL(exec=1)",
       (total_sql_mem - single_use_sql_mem)/1048576                     "SQL Mem(Mb) (exec>1)",
       single_use_sql_mem/1048576                                       "SQL Mem(Mb) (exec=1)",
       decode(total_sql, 0, 0,(1-single_use_sql/total_sql))             "SQL:exec>1(%)",
       decode(total_sql, 0, 0,single_use_sql/total_sql)                 "SQL:exec=1(%)",
       decode(total_sql_mem, 0, 0,(1-single_use_sql_mem/total_sql_mem)) "SQL Mem:exec>1(%)",
       decode(total_sql_mem, 0, 0,single_use_sql_mem/total_sql_mem)     "SQL Mem:exec=1(%)"
  from dba_hist_sql_summary sq,
       dba_hist_snapshot sn
 where sq.snap_id = sn.snap_id
   and sq.instance_number = sn.instance_number
   and sq.dbid = sn.dbid
   and sq.snap_id between :snap_start + 1 and :snap_end
   and sn.instance_number = :instno
   and sn.dbid = :dbid
 order by sn.snap_id
"""

sql_elaps = """
select sn.snap_id,
       to_char(sn.end_interval_time,'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time,'dd hh24:mi'))       dates2,
       max(decode(rank, 1, elap/1000000, 0)) rank1_elap,
       max(decode(rank, 2, elap/1000000, 0)) rank2_elap,
       max(decode(rank, 3, elap/1000000, 0)) rank3_elap,
       max(decode(rank, 4, elap/1000000, 0)) rank4_elap,
       max(decode(rank, 5, elap/1000000, 0)) rank5_elap,
       max(decode(rank, 1, cput/1000000, 0)) rank1_cputime,
       max(decode(rank, 2, cput/1000000, 0)) rank2_cputime,
       max(decode(rank, 3, cput/1000000, 0)) rank3_cputime,
       max(decode(rank, 4, cput/1000000, 0)) rank4_cputime,
       max(decode(rank, 5, cput/1000000, 0)) rank5_cputime,
       max(decode(rank, 1, exec, 0)) rank1_exec,
       max(decode(rank, 2, exec, 0)) rank2_exec,
       max(decode(rank, 3, exec, 0)) rank3_exec,
       max(decode(rank, 4, exec, 0)) rank4_exec,
       max(decode(rank, 5, exec, 0)) rank5_exec,
       decode(max(decode(rank, 1, exec, 0)), 0, 0,max(decode(rank, 1, elap/1000000, 0))/max(decode(rank, 1, exec, 0))) rank1_elap_per_exec,
       decode(max(decode(rank, 2, exec, 0)), 0, 0,max(decode(rank, 2, elap/1000000, 0))/max(decode(rank, 2, exec, 0))) rank2_elap_per_exec,
       decode(max(decode(rank, 3, exec, 0)), 0, 0,max(decode(rank, 3, elap/1000000, 0))/max(decode(rank, 3, exec, 0))) rank3_elap_per_exec,
       decode(max(decode(rank, 4, exec, 0)), 0, 0,max(decode(rank, 4, elap/1000000, 0))/max(decode(rank, 4, exec, 0))) rank4_elap_per_exec,
       decode(max(decode(rank, 5, exec, 0)), 0, 0,max(decode(rank, 5, elap/1000000, 0))/max(decode(rank, 5, exec, 0))) rank5_elap_per_exec,
       decode(max(delta_dbtime), 0, 0, max(decode(rank, 1, elap, 0))/max(delta_dbtime)) rank1_elap_per_dbtime,
       decode(max(delta_dbtime), 0, 0, max(decode(rank, 2, elap, 0))/max(delta_dbtime)) rank2_elap_per_dbtime,
       decode(max(delta_dbtime), 0, 0, max(decode(rank, 3, elap, 0))/max(delta_dbtime)) rank3_elap_per_dbtime,
       decode(max(delta_dbtime), 0, 0, max(decode(rank, 4, elap, 0))/max(delta_dbtime)) rank4_elap_per_dbtime,
       decode(max(delta_dbtime), 0, 0, max(decode(rank, 5, elap, 0))/max(delta_dbtime)) rank5_elap_per_dbtime,
       max(decode(rank, 1, sql_id, null)) rank1,
       max(decode(rank, 2, sql_id, null)) rank2,
       max(decode(rank, 3, sql_id, null)) rank3,
       max(decode(rank, 4, sql_id, null)) rank4,
       max(decode(rank, 5, sql_id, null)) rank5
  from (  
         select v_sqlstat.snap_id,
                v_rank.rank,
                v_sqlstat.sql_id,
                v_sqlstat.module,
                v_sqlstat.elap,
                v_sqlstat.cput,
                v_sqlstat.exec
           from ( select sql_id, rownum rank
                     from
                        ( select sql_id, sum(nvl(elapsed_time_delta,0))
                            from dba_hist_sqlstat
                           where snap_id between 1+ :snap_start and :snap_end
                             and instance_number = :instno
                             and dbid = :dbid
                           group by sql_id order by sum(nvl(elapsed_time_delta,0)) desc
                        )
                    where rownum <= 5
                ) v_rank,
                ( select snap_id,
                         sql_id,
                         module,
                         elapsed_time_delta elap,
                         cpu_time_delta cput,
                         executions_delta exec
                    from dba_hist_sqlstat
                   where snap_id between 1+ :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                ) v_sqlstat
           where v_sqlstat.sql_id = v_rank.sql_id
       ) v1,
       ( 
         select sn.snap_id,
                sn.end_interval_time,
                nvl(value - lag(value) over (partition by stm.stat_name order by stm.snap_id), 0)  delta_dbtime
           from dba_hist_snapshot sn,
                dba_hist_sys_time_model stm
          where sn.snap_id = stm.snap_id
            and sn.instance_number = stm.instance_number
            and sn.dbid = stm.dbid
            and stm.stat_name = 'DB time'
            and stm.snap_id  between :snap_start and :snap_end
            and stm.instance_number = :instno
            and stm.dbid = :dbid
       ) sn
 where v1.snap_id(+) = sn.snap_id
   and sn.snap_id > :snap_start
 group by sn.snap_id,
       to_char(sn.end_interval_time,'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

sql_elaps2 = """
select lable||sql_id title
  from ( select 1 no,'' lable from dual union all
         select 2   ,''       from dual union all
         select 3   ,''       from dual union all
         select 4   ,''       from dual union all
         select 5   ,''       from dual
       ) a,
       (
          select sql_id, rownum rank
            from ( select sql_id, sum(nvl(elapsed_time_delta,0))
                     from dba_hist_sqlstat
                    where snap_id between 1+ :snap_start and :snap_end
                      and instance_number = :instno
                      and dbid = :dbid
                    group by sql_id
                    order by sum(nvl(elapsed_time_delta,0)) desc
                 )
           where rownum <= 5
       ) b
 order by no,rank
"""

sql_elaps_sql = """
select rank,
       v1.sql_id,
       v1.module,
       nvl(replace(replace(replace(to_char(substr(st.sql_text,1,2500)),chr(9),' '),chr(10),' '),chr(13),' '), '** Not Found **') "SQL Text"
  from ( 
         select sql_id, dbid, module, rownum rank
           from ( select sql_id, dbid, module, sum(nvl(elapsed_time_delta,0))
                    from dba_hist_sqlstat
                   where snap_id between 1+ :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                   group by sql_id, dbid, module order by sum(nvl(elapsed_time_delta,0)) desc
                )
          where rownum <= 5
       ) v1,
       dba_hist_sqltext st
 where st.sql_id(+) = v1.sql_id
   and st.dbid(+) = v1.dbid
"""

sql_elaps_raw = """
select snap_id,
       to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss') "Timestamp",
       elap/1000000 "Elapsed(s)",
       cput/1000000 "CPU(s)",
       exec,
       decode(nvl(exec,0), 0, 0, elap/exec/1000000) "Elap/exec(s)",
       decode(nvl(delta_dbtime,0), 0, 0, elap/delta_dbtime) "%Total DB time",
       sql_id,
       nvl(replace(replace(replace(to_char(substr(sql_text,1,2500)),chr(9),' '),chr(10),' '),chr(13),' '), '** Not Found **') "SQL Text",
       module,
       rank
  from (
         select sn.snap_id,
                sn.end_interval_time,
                sq.sql_id,
                st.sql_text,
                sq.module,
                sq.elapsed_time_delta elap,
                sq.cpu_time_delta cput,
                sq.executions_delta exec,
                rank() over ( partition by sq.snap_id order by sq.elapsed_time_delta desc ) rank,
                stm.delta_dbtime
           from dba_hist_sqlstat sq,
                ( select snap_id,
                         instance_number,
                         dbid,
                         value,
                         nvl(value - lag(value) over (partition by stat_name order by snap_id), 0) delta_dbtime
                    from dba_hist_sys_time_model
                   where stat_name = 'DB time'
                     and snap_id between :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                ) stm,
                dba_hist_sqltext st,
                dba_hist_snapshot sn
          where sq.snap_id = sn.snap_id
            and stm.snap_id = sn.snap_id
            and sq.instance_number = sn.instance_number
            and sq.dbid = sn.dbid
            and st.sql_id(+) = sq.sql_id
            and st.dbid(+) = sq.dbid
            and sn.snap_id between :snap_start and :snap_end
            and sn.instance_number = :instno
            and sn.dbid = :dbid
        )
 where rank <= 5
   and snap_id > :snap_start
 order by snap_id
"""

sql_cpu = """
select sn.snap_id,
       to_char(sn.end_interval_time,'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time,'dd hh24:mi'))       dates2,
       max(decode(rank, 1, cput/1000000, 0)) rank1_cputime,
       max(decode(rank, 2, cput/1000000, 0)) rank2_cputime,
       max(decode(rank, 3, cput/1000000, 0)) rank3_cputime,
       max(decode(rank, 4, cput/1000000, 0)) rank4_cputime,
       max(decode(rank, 5, cput/1000000, 0)) rank5_cputime,
       max(decode(rank, 1, elap/1000000, 0)) rank1_elapsed,
       max(decode(rank, 2, elap/1000000, 0)) rank2_elapsed,
       max(decode(rank, 3, elap/1000000, 0)) rank3_elapsed,
       max(decode(rank, 4, elap/1000000, 0)) rank4_elapsed,
       max(decode(rank, 5, elap/1000000, 0)) rank5_elapsed,
       max(decode(rank, 1, exec, 0)) rank1_exec,
       max(decode(rank, 2, exec, 0)) rank2_exec,
       max(decode(rank, 3, exec, 0)) rank3_exec,
       max(decode(rank, 4, exec, 0)) rank4_exec,
       max(decode(rank, 5, exec, 0)) rank5_exec,
       decode(max(decode(rank, 1, exec, 0)), 0, 0,max(decode(rank, 1, cput/1000000, 0))/max(decode(rank, 1, exec, 0))) rank1_cput_per_exec,
       decode(max(decode(rank, 2, exec, 0)), 0, 0,max(decode(rank, 2, cput/1000000, 0))/max(decode(rank, 2, exec, 0))) rank2_cput_per_exec,
       decode(max(decode(rank, 3, exec, 0)), 0, 0,max(decode(rank, 3, cput/1000000, 0))/max(decode(rank, 3, exec, 0))) rank3_cput_per_exec,
       decode(max(decode(rank, 4, exec, 0)), 0, 0,max(decode(rank, 4, cput/1000000, 0))/max(decode(rank, 4, exec, 0))) rank4_cput_per_exec,
       decode(max(decode(rank, 5, exec, 0)), 0, 0,max(decode(rank, 5, cput/1000000, 0))/max(decode(rank, 5, exec, 0))) rank5_cput_per_exec,
       decode(max(delta_dbtime), 0, 0, max(decode(rank, 1, elap, 0))/max(delta_dbtime)) rank1_elap_per_dbtime,
       decode(max(delta_dbtime), 0, 0, max(decode(rank, 2, elap, 0))/max(delta_dbtime)) rank2_elap_per_dbtime,
       decode(max(delta_dbtime), 0, 0, max(decode(rank, 3, elap, 0))/max(delta_dbtime)) rank3_elap_per_dbtime,
       decode(max(delta_dbtime), 0, 0, max(decode(rank, 4, elap, 0))/max(delta_dbtime)) rank4_elap_per_dbtime,
       decode(max(delta_dbtime), 0, 0, max(decode(rank, 5, elap, 0))/max(delta_dbtime)) rank5_elap_per_dbtime,
       max(decode(rank, 1, sql_id, null)) rank1,
       max(decode(rank, 2, sql_id, null)) rank2,
       max(decode(rank, 3, sql_id, null)) rank3,
       max(decode(rank, 4, sql_id, null)) rank4,
       max(decode(rank, 5, sql_id, null)) rank5
  from (  select v_sqlstat.snap_id,
                 v_rank.rank,
                 v_sqlstat.sql_id,
                 v_sqlstat.module,
                 v_sqlstat.elap,
                 v_sqlstat.cput,
                 v_sqlstat.exec
            from ( select sql_id, rownum rank
                     from ( select sql_id, sum(nvl(cpu_time_delta,0))
                              from dba_hist_sqlstat
                             where snap_id between 1+ :snap_start and :snap_end
                               and instance_number = :instno
                               and dbid = :dbid
                             group by sql_id order by sum(nvl(cpu_time_delta,0)) desc
                          )
                    where rownum <= 5
                 ) v_rank,
                 ( select snap_id,
                          sql_id,
                          module,
                          elapsed_time_delta elap,
                          cpu_time_delta cput,
                          executions_delta exec
                     from dba_hist_sqlstat
                    where snap_id between 1+ :snap_start and :snap_end
                      and instance_number = :instno
                      and dbid = :dbid
                 ) v_sqlstat
           where v_sqlstat.sql_id = v_rank.sql_id
       ) v1,
       ( select sn.snap_id,
                sn.end_interval_time,
                nvl(value - lag(value) over (partition by stm.stat_name order by stm.snap_id), 0)  delta_dbtime
           from dba_hist_snapshot sn,
                dba_hist_sys_time_model stm
          where sn.snap_id = stm.snap_id
            and sn.instance_number = stm.instance_number
            and sn.dbid = stm.dbid
            and stm.stat_name = 'DB time'
            and stm.snap_id  between :snap_start and :snap_end
            and stm.instance_number = :instno
            and stm.dbid = :dbid
       ) sn
 where v1.snap_id(+) = sn.snap_id
   and sn.snap_id > :snap_start
 group by sn.snap_id,
       to_char(sn.end_interval_time,'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

sql_cpu2 = """
select lable||sql_id title
  from ( select 1 no,'' lable from dual union all
         select 2   ,''       from dual union all
         select 3   ,''       from dual union all
         select 4   ,''       from dual union all
         select 5   ,''       from dual
       ) a,
       (
          select sql_id, rownum rank
            from ( select sql_id, sum(nvl(cpu_time_delta,0))
                     from dba_hist_sqlstat
                    where snap_id between 1+ :snap_start and :snap_end
                      and instance_number = :instno
                      and dbid = :dbid
                    group by sql_id order by sum(nvl(cpu_time_delta,0)) desc
                 )
           where rownum <= 5
       ) b
 order by no,rank
"""

sql_cpu_sql = """
select rank,
       v1.sql_id,
       v1.module,
       nvl(replace(replace(replace(to_char(substr(st.sql_text,1,2500)),chr(9),' '),chr(10),' '),chr(13),' '), '** Not Found **') "SQL Text"
  from ( select sql_id, dbid, module, rownum rank
           from ( select sql_id, dbid, module, sum(nvl(cpu_time_delta,0))
                   from dba_hist_sqlstat
                  where snap_id between 1+ :snap_start and :snap_end
                    and instance_number = :instno
                    and dbid = :dbid
                  group by sql_id, dbid, module order by sum(nvl(cpu_time_delta,0)) desc
                )
          where rownum <= 5
       ) v1,
       dba_hist_sqltext st
 where st.sql_id(+) = v1.sql_id
   and st.dbid(+) = v1.dbid
"""

sql_cpu_raw = """
select snap_id,
       to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss') dates,
       to_char(end_interval_time,'dd hh24:mi')            dates2,
       cput/1000000 "CPU(s)",
       elap/1000000 "Elapsed(s)",
       exec,
       decode(nvl(exec,0), 0, 0, cput/exec/1000000) "CPU/exec(s)",
       decode(nvl(delta_dbtime,0), 0, 0, elap/delta_dbtime) "%Total DB time",
       sql_id,
       nvl(replace(replace(replace(to_char(substr(sql_text,1,2500)),chr(9),' '),chr(10),' '),chr(13),' '), '** Not Found **') "SQL Text",
       module,
       rank
  from (
         select sn.snap_id,
                sn.end_interval_time,
                sq.sql_id,
                st.sql_text,
                sq.module,
                sq.elapsed_time_delta elap,
                sq.cpu_time_delta cput,
                sq.executions_delta exec,
                rank() over ( partition by sq.snap_id order by sq.cpu_time_delta desc ) rank,
                stm.delta_dbtime
           from dba_hist_sqlstat sq,
                ( select snap_id,
                         instance_number,
                         dbid,
                         value,
                         nvl(value - lag(value) over (partition by stat_name order by snap_id), 0) delta_dbtime
                    from dba_hist_sys_time_model
                   where stat_name = 'DB time'
                     and snap_id between :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                ) stm,
                dba_hist_sqltext st,
                dba_hist_snapshot sn
          where sq.snap_id = sn.snap_id
            and stm.snap_id = sn.snap_id
            and sq.instance_number = sn.instance_number
            and sq.dbid = sn.dbid
            and st.sql_id(+) = sq.sql_id
            and st.dbid(+) = sq.dbid
            and sn.snap_id between :snap_start and :snap_end
            and sn.instance_number = :instno
            and sn.dbid = :dbid
       )
 where rank <= 5
   and snap_id > :snap_start
 order by snap_id
"""

sql_get = """
select sn.snap_id,
       to_char(sn.end_interval_time,'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time,'dd hh24:mi'))       dates2,
       max(decode(rank, 1, bufget, 0)) rank1_bufget,
       max(decode(rank, 2, bufget, 0)) rank2_bufget,
       max(decode(rank, 3, bufget, 0)) rank3_bufget,
       max(decode(rank, 4, bufget, 0)) rank4_bufget,
       max(decode(rank, 5, bufget, 0)) rank5_bufget,
       max(decode(rank, 1, cput/1000000, 0)) rank1_cputime,
       max(decode(rank, 2, cput/1000000, 0)) rank2_cputime,
       max(decode(rank, 3, cput/1000000, 0)) rank3_cputime,
       max(decode(rank, 4, cput/1000000, 0)) rank4_cputime,
       max(decode(rank, 5, cput/1000000, 0)) rank5_cputime,
       max(decode(rank, 1, elap/1000000, 0)) rank1_elapsed,
       max(decode(rank, 2, elap/1000000, 0)) rank2_elapsed,
       max(decode(rank, 3, elap/1000000, 0)) rank3_elapsed,
       max(decode(rank, 4, elap/1000000, 0)) rank4_elapsed,
       max(decode(rank, 5, elap/1000000, 0)) rank5_elapsed,
       max(decode(rank, 1, exec, 0)) rank1_exec,
       max(decode(rank, 2, exec, 0)) rank2_exec,
       max(decode(rank, 3, exec, 0)) rank3_exec,
       max(decode(rank, 4, exec, 0)) rank4_exec,
       max(decode(rank, 5, exec, 0)) rank5_exec,
       decode(max(decode(rank, 1, exec, 0)), 0, 0,max(decode(rank, 1, bufget, 0))/max(decode(rank, 1, exec, 0))) rank1_bufget_per_exec,
       decode(max(decode(rank, 2, exec, 0)), 0, 0,max(decode(rank, 2, bufget, 0))/max(decode(rank, 2, exec, 0))) rank2_bufget_per_exec,
       decode(max(decode(rank, 3, exec, 0)), 0, 0,max(decode(rank, 3, bufget, 0))/max(decode(rank, 3, exec, 0))) rank3_bufget_per_exec,
       decode(max(decode(rank, 4, exec, 0)), 0, 0,max(decode(rank, 4, bufget, 0))/max(decode(rank, 4, exec, 0))) rank4_bufget_per_exec,
       decode(max(decode(rank, 5, exec, 0)), 0, 0,max(decode(rank, 5, bufget, 0))/max(decode(rank, 5, exec, 0))) rank5_bufget_per_exec,
       decode(max(delta_bufget_tot), 0, 0,max(decode(rank, 1, bufget, 0))/max(delta_bufget_tot)) rank1_bufget_per_tot,
       decode(max(delta_bufget_tot), 0, 0,max(decode(rank, 2, bufget, 0))/max(delta_bufget_tot)) rank2_bufget_per_tot,
       decode(max(delta_bufget_tot), 0, 0,max(decode(rank, 3, bufget, 0))/max(delta_bufget_tot)) rank3_bufget_per_tot,
       decode(max(delta_bufget_tot), 0, 0,max(decode(rank, 4, bufget, 0))/max(delta_bufget_tot)) rank4_bufget_per_tot,
       decode(max(delta_bufget_tot), 0, 0,max(decode(rank, 5, bufget, 0))/max(delta_bufget_tot)) rank5_bufget_per_tot,
       max(decode(rank, 1, sql_id, null)) rank1,
       max(decode(rank, 2, sql_id, null)) rank2,
       max(decode(rank, 3, sql_id, null)) rank3,
       max(decode(rank, 4, sql_id, null)) rank4,
       max(decode(rank, 5, sql_id, null)) rank5
  from ( select v_sqlstat.snap_id,
                v_rank.rank,
                v_sqlstat.sql_id,
                v_sqlstat.module,
                v_sqlstat.bufget,
                v_sqlstat.elap,
                v_sqlstat.cput,
                v_sqlstat.exec
           from ( select sql_id, rownum rank
                    from ( select sql_id, sum(nvl(buffer_gets_delta,0))
                            from dba_hist_sqlstat
                           where snap_id between 1+ :snap_start and :snap_end
                             and instance_number = :instno
                             and dbid = :dbid
                           group by sql_id order by sum(nvl(buffer_gets_delta,0)) desc
                         )
                   where rownum <= 5
                ) v_rank,
                ( select snap_id,
                         sql_id,
                         module,
                         buffer_gets_delta bufget,
                         elapsed_time_delta elap,
                         cpu_time_delta cput,
                         executions_delta exec
                    from dba_hist_sqlstat
                   where snap_id between 1+ :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                ) v_sqlstat
          where v_sqlstat.sql_id = v_rank.sql_id
       ) v1,
       ( select sn.snap_id,
                sn.end_interval_time,
                nvl(value - lag(value) over (partition by sy.stat_name order by sy.snap_id), 0) delta_bufget_tot
           from dba_hist_snapshot sn,
                dba_hist_sysstat sy
          where sn.snap_id = sy.snap_id
            and sn.instance_number = sy.instance_number
            and sn.dbid = sy.dbid
            and sy.stat_name = 'session logical reads'
            and sy.snap_id  between :snap_start and :snap_end
            and sy.instance_number = :instno
            and sy.dbid = :dbid
       ) sn
 where v1.snap_id(+) = sn.snap_id
   and sn.snap_id > :snap_start
 group by sn.snap_id,
       to_char(sn.end_interval_time,'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

sql_get2 = """
select lable||sql_id title
  from ( select 1 no,'' lable from dual union all
         select 2   ,''       from dual union all
         select 3   ,''       from dual union all
         select 4   ,''       from dual union all
         select 5   ,''       from dual union all
         select 6   ,''       from dual
       ) a,
       (
         select sql_id, rownum rank
           from ( select sql_id, sum(nvl(buffer_gets_delta,0))
                    from dba_hist_sqlstat
                   where snap_id between 1+ :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                   group by sql_id order by sum(nvl(buffer_gets_delta,0)) desc
                )
          where rownum <= 5
       ) b
 order by no,rank
"""

sql_get_sql = """
select rank,
       v1.sql_id,
       v1.module,
       nvl(replace(replace(replace(to_char(substr(st.sql_text,1,2500)),chr(9),' '),chr(10),' '),chr(13),' '), '** Not Found **') "SQL Text"
  from ( select sql_id, dbid, module, rownum rank
           from ( select sql_id, dbid, module, sum(nvl(buffer_gets_delta,0))
                    from dba_hist_sqlstat
                   where snap_id between 1+ :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                   group by sql_id, dbid, module order by sum(nvl(buffer_gets_delta,0)) desc
                )
          where rownum <= 5
       ) v1,
       dba_hist_sqltext st
 where st.sql_id(+) = v1.sql_id
   and st.dbid(+) = v1.dbid
"""

sql_get_raw = """
select snap_id,
       to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss') dates,
       bufget buffer_gets,
       exec,
       decode(nvl(exec,0), 0, 0, bufget/exec) "gets/exec",
       decode(nvl(delta_bufget_tot,0), 0, 0, bufget/delta_bufget_tot) "Total(%)",
       cput/1000000 "CPU(s)",
       elap/1000000 "Elapsed(s)",
       sql_id,
       nvl(replace(replace(replace(to_char(substr(sql_text,1,2500)),chr(9),' '),chr(10),' '),chr(13),' '), '** Not Found **') "SQL Text",
       module,
       rank
  from (
         select sn.snap_id,
                sn.end_interval_time,
                sq.sql_id,
                st.sql_text,
                sq.module,
                sq.buffer_gets_delta bufget,
                sq.executions_delta exec,
                sq.elapsed_time_delta elap,
                sq.cpu_time_delta cput,
                rank() over ( partition by sq.snap_id order by sq.buffer_gets_delta desc ) rank,
                sy.delta_bufget_tot
           from dba_hist_sqlstat sq,
                ( select snap_id,
                         value,
                         nvl(value - lag(value) over (partition by stat_name order by snap_id), 0) delta_bufget_tot
                    from dba_hist_sysstat
                   where stat_name = 'session logical reads'
                     and snap_id between :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                ) sy,
                dba_hist_sqltext st,
                dba_hist_snapshot sn
          where sq.snap_id = sn.snap_id
            and sy.snap_id = sn.snap_id
            and sq.instance_number = sn.instance_number
            and sq.dbid = sn.dbid
            and st.sql_id(+) = sq.sql_id
            and st.dbid(+) = sq.dbid
            and sn.snap_id between :snap_start and :snap_end
            and sn.instance_number = :instno
            and sn.dbid = :dbid
       )
 where rank <= 5
   and snap_id > :snap_start
 order by snap_id
"""

sql_io = """
select sn.snap_id,
       to_char(sn.end_interval_time,'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time,'dd hh24:mi'))       dates2,
       max(decode(rank, 1, phyrds, 0)) rank1_phyrds,
       max(decode(rank, 2, phyrds, 0)) rank2_phyrds,
       max(decode(rank, 3, phyrds, 0)) rank3_phyrds,
       max(decode(rank, 4, phyrds, 0)) rank4_phyrds,
       max(decode(rank, 5, phyrds, 0)) rank5_phyrds,
       max(decode(rank, 1, cput/1000000, 0)) rank1_cputime,
       max(decode(rank, 2, cput/1000000, 0)) rank2_cputime,
       max(decode(rank, 3, cput/1000000, 0)) rank3_cputime,
       max(decode(rank, 4, cput/1000000, 0)) rank4_cputime,
       max(decode(rank, 5, cput/1000000, 0)) rank5_cputime,
       max(decode(rank, 1, elap/1000000, 0)) rank1_elapsed,
       max(decode(rank, 2, elap/1000000, 0)) rank2_elapsed,
       max(decode(rank, 3, elap/1000000, 0)) rank3_elapsed,
       max(decode(rank, 4, elap/1000000, 0)) rank4_elapsed,
       max(decode(rank, 5, elap/1000000, 0)) rank5_elapsed,
       max(decode(rank, 1, exec, 0)) rank1_exec,
       max(decode(rank, 2, exec, 0)) rank2_exec,
       max(decode(rank, 3, exec, 0)) rank3_exec,
       max(decode(rank, 4, exec, 0)) rank4_exec,
       max(decode(rank, 5, exec, 0)) rank5_exec,
       decode(max(decode(rank, 1, exec, 0)), 0, 0,max(decode(rank, 1, phyrds, 0))/max(decode(rank, 1, exec, 0))) rank1_phyrds_per_exec,
       decode(max(decode(rank, 2, exec, 0)), 0, 0,max(decode(rank, 2, phyrds, 0))/max(decode(rank, 2, exec, 0))) rank2_phyrds_per_exec,
       decode(max(decode(rank, 3, exec, 0)), 0, 0,max(decode(rank, 3, phyrds, 0))/max(decode(rank, 3, exec, 0))) rank3_phyrds_per_exec,
       decode(max(decode(rank, 4, exec, 0)), 0, 0,max(decode(rank, 4, phyrds, 0))/max(decode(rank, 4, exec, 0))) rank4_phyrds_per_exec,
       decode(max(decode(rank, 5, exec, 0)), 0, 0,max(decode(rank, 5, phyrds, 0))/max(decode(rank, 5, exec, 0))) rank5_phyrds_per_exec,
       decode(max(delta_phyrds_tot), 0, 0,max(decode(rank, 1, phyrds, 0))/max(delta_phyrds_tot)) rank1_phyrds_per_tot,
       decode(max(delta_phyrds_tot), 0, 0,max(decode(rank, 2, phyrds, 0))/max(delta_phyrds_tot)) rank2_phyrds_per_tot,
       decode(max(delta_phyrds_tot), 0, 0,max(decode(rank, 3, phyrds, 0))/max(delta_phyrds_tot)) rank3_phyrds_per_tot,
       decode(max(delta_phyrds_tot), 0, 0,max(decode(rank, 4, phyrds, 0))/max(delta_phyrds_tot)) rank4_phyrds_per_tot,
       decode(max(delta_phyrds_tot), 0, 0,max(decode(rank, 5, phyrds, 0))/max(delta_phyrds_tot)) rank5_phyrds_per_tot,
       max(decode(rank, 1, sql_id, null)) rank1,
       max(decode(rank, 2, sql_id, null)) rank2,
       max(decode(rank, 3, sql_id, null)) rank3,
       max(decode(rank, 4, sql_id, null)) rank4,
       max(decode(rank, 5, sql_id, null)) rank5
  from (  select v_sqlstat.snap_id,
                 v_rank.rank,
                 v_sqlstat.sql_id,
                 v_sqlstat.module,
                 v_sqlstat.phyrds,
                 v_sqlstat.elap,
                 v_sqlstat.cput,
                 v_sqlstat.exec
            from ( select sql_id, rownum rank
                     from
                        ( select sql_id, sum(nvl(disk_reads_delta,0))
                            from dba_hist_sqlstat
                           where snap_id between 1+ :snap_start and :snap_end
                             and instance_number = :instno
                             and dbid = :dbid
                           group by sql_id order by sum(nvl(disk_reads_delta,0)) desc
                        )
                    where rownum <= 5
                 ) v_rank,
                 ( select snap_id,
                          sql_id,
                          module,
                          disk_reads_delta phyrds,
                          elapsed_time_delta elap,
                          cpu_time_delta cput,
                          executions_delta exec
                     from dba_hist_sqlstat
                    where snap_id between 1+ :snap_start and :snap_end
                      and instance_number = :instno
                      and dbid = :dbid
                 ) v_sqlstat
           where v_sqlstat.sql_id = v_rank.sql_id
          ) v1,
          ( select sn.snap_id,
                   sn.end_interval_time,
                   nvl(value - lag(value) over (partition by sy.stat_name order by sy.snap_id), 0) delta_phyrds_tot
              from dba_hist_snapshot sn,
                   dba_hist_sysstat sy
             where sn.snap_id = sy.snap_id
               and sn.instance_number = sy.instance_number
               and sn.dbid = sy.dbid
               and sy.stat_name = 'physical reads'
               and sy.snap_id  between :snap_start and :snap_end
               and sy.instance_number = :instno
               and sy.dbid = :dbid
          ) sn
 where v1.snap_id(+)  = sn.snap_id
   and sn.snap_id > :snap_start
 group by sn.snap_id,
       to_char(sn.end_interval_time,'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

sql_io2 = """
select lable||sql_id title
  from ( select 1 no,'' lable from dual union all
         select 2   ,''       from dual union all
         select 3   ,''       from dual union all
         select 4   ,''       from dual union all
         select 5   ,''       from dual union all
         select 6   ,''       from dual
       ) a,
       ( select sql_id, rownum rank
           from ( select sql_id, sum(nvl(disk_reads_delta,0))
                    from dba_hist_sqlstat
                   where snap_id between 1+ :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                   group by sql_id order by sum(nvl(disk_reads_delta,0)) desc
                )
          where rownum <= 5
       ) b
 order by no,rank
"""

sql_io_sql = """
select rank,
       v1.sql_id,
       v1.module,
       nvl(replace(replace(replace(to_char(substr(st.sql_text,1,2500)),chr(9),' '),chr(10),' '),chr(13),' '), '** Not Found **') "SQL Text"
  from ( select sql_id, dbid, module, rownum rank
           from ( select sql_id, dbid, module, sum(nvl(disk_reads_delta,0))
                    from dba_hist_sqlstat
                   where snap_id between 1+ :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                   group by sql_id, dbid, module order by sum(nvl(disk_reads_delta,0)) desc
                )
          where rownum <= 5
       ) v1,
       dba_hist_sqltext st
 where st.sql_id(+) = v1.sql_id
   and st.dbid(+) = v1.dbid
"""

sql_io_raw = """
select snap_id,
       to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss') dates,
       phyrds "Physical Reads",
       exec,
       decode(nvl(exec,0), 0, 0, phyrds/exec) "Reads/exec",
       decode(nvl(delta_phyrds_tot,0), 0, 0, phyrds/delta_phyrds_tot) "%Total",
       cput/1000000 "CPU(s)",
       elap/1000000 "Elapsed(s)",
       sql_id,
       nvl(replace(replace(replace(to_char(substr(sql_text,1,2500)),chr(9),' '),chr(10),' '),chr(13),' '), 'Not Found') "SQL Text",
       module,
       rank
  from ( select sn.snap_id,
                sn.end_interval_time,
                sq.sql_id,
                st.sql_text,
                sq.module,
                sq.disk_reads_delta phyrds,
                sq.executions_delta exec,
                sq.elapsed_time_delta elap,
                sq.cpu_time_delta cput,
                rank() over ( partition by sq.snap_id order by sq.disk_reads_delta desc ) rank,
                sy.delta_phyrds_tot
           from dba_hist_sqlstat sq,
                ( select snap_id,
                         value,
                         nvl(value - lag(value) over (partition by stat_name order by snap_id), 0) delta_phyrds_tot
                    from dba_hist_sysstat
                   where stat_name = 'physical reads'
                     and snap_id between :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                ) sy,
                dba_hist_sqltext st,
                dba_hist_snapshot sn
          where sq.snap_id = sn.snap_id
            and sy.snap_id = sn.snap_id
            and sq.instance_number = sn.instance_number
            and sq.dbid = sn.dbid
            and st.sql_id(+) = sq.sql_id
            and st.dbid(+) = sq.dbid
            and sn.snap_id between :snap_start and :snap_end
            and sn.instance_number = :instno
            and sn.dbid = :dbid
       )
 where rank <= 5
   and snap_id > :snap_start
 order by snap_id
"""

sql_exec = """
select sn.snap_id,
       to_char(sn.end_interval_time,'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time,'dd hh24:mi'))       dates2,
       max(decode(rank, 1, exec, 0)) rank1_exec,
       max(decode(rank, 2, exec, 0)) rank2_exec,
       max(decode(rank, 3, exec, 0)) rank3_exec,
       max(decode(rank, 4, exec, 0)) rank4_exec,
       max(decode(rank, 5, exec, 0)) rank5_exec,
       max(decode(rank, 1, rows_processed, 0)) rank1_rows_processed,
       max(decode(rank, 2, rows_processed, 0)) rank2_rows_processed,
       max(decode(rank, 3, rows_processed, 0)) rank3_rows_processed,
       max(decode(rank, 4, rows_processed, 0)) rank4_rows_processed,
       max(decode(rank, 5, rows_processed, 0)) rank5_rows_processed,
       decode(max(decode(rank, 1, exec, 0)), 0, 0,max(decode(rank, 1, rows_processed, 0))/max(decode(rank, 1, exec, 0))) rank1_rows_per_exec,
       decode(max(decode(rank, 2, exec, 0)), 0, 0,max(decode(rank, 2, rows_processed, 0))/max(decode(rank, 2, exec, 0))) rank2_rows_per_exec,
       decode(max(decode(rank, 3, exec, 0)), 0, 0,max(decode(rank, 3, rows_processed, 0))/max(decode(rank, 3, exec, 0))) rank3_rows_per_exec,
       decode(max(decode(rank, 4, exec, 0)), 0, 0,max(decode(rank, 4, rows_processed, 0))/max(decode(rank, 4, exec, 0))) rank4_rows_per_exec,
       decode(max(decode(rank, 5, exec, 0)), 0, 0,max(decode(rank, 5, rows_processed, 0))/max(decode(rank, 5, exec, 0))) rank5_rows_per_exec,
       decode(max(decode(rank, 1, exec, 0)), 0, 0,max(decode(rank, 1, cput/1000000, 0))/max(decode(rank, 1, exec, 0))) rank1_cpu_per_exec,
       decode(max(decode(rank, 2, exec, 0)), 0, 0,max(decode(rank, 2, cput/1000000, 0))/max(decode(rank, 2, exec, 0))) rank2_cpu_per_exec,
       decode(max(decode(rank, 3, exec, 0)), 0, 0,max(decode(rank, 3, cput/1000000, 0))/max(decode(rank, 3, exec, 0))) rank3_cpu_per_exec,
       decode(max(decode(rank, 4, exec, 0)), 0, 0,max(decode(rank, 4, cput/1000000, 0))/max(decode(rank, 4, exec, 0))) rank4_cpu_per_exec,
       decode(max(decode(rank, 5, exec, 0)), 0, 0,max(decode(rank, 5, cput/1000000, 0))/max(decode(rank, 5, exec, 0))) rank5_cpu_per_exec,
       decode(max(decode(rank, 1, exec, 0)), 0, 0,max(decode(rank, 1, elap/1000000, 0))/max(decode(rank, 1, exec, 0))) rank1_elap_per_exec,
       decode(max(decode(rank, 2, exec, 0)), 0, 0,max(decode(rank, 2, elap/1000000, 0))/max(decode(rank, 2, exec, 0))) rank2_elap_per_exec,
       decode(max(decode(rank, 3, exec, 0)), 0, 0,max(decode(rank, 3, elap/1000000, 0))/max(decode(rank, 3, exec, 0))) rank3_elap_per_exec,
       decode(max(decode(rank, 4, exec, 0)), 0, 0,max(decode(rank, 4, elap/1000000, 0))/max(decode(rank, 4, exec, 0))) rank4_elap_per_exec,
       decode(max(decode(rank, 5, exec, 0)), 0, 0,max(decode(rank, 5, elap/1000000, 0))/max(decode(rank, 5, exec, 0))) rank5_elap_per_exec,
       max(decode(rank, 1, sql_id, null)) rank1,
       max(decode(rank, 2, sql_id, null)) rank2,
       max(decode(rank, 3, sql_id, null)) rank3,
       max(decode(rank, 4, sql_id, null)) rank4,
       max(decode(rank, 5, sql_id, null)) rank5
  from (  select v_sqlstat.snap_id,
                 v_rank.rank,
                 v_sqlstat.sql_id,
                 v_sqlstat.module,
                 v_sqlstat.rows_processed,
                 v_sqlstat.elap,
                 v_sqlstat.cput,
                 v_sqlstat.exec
            from ( select sql_id, rownum rank
                     from ( select sql_id, sum(nvl(executions_delta,0))
                              from dba_hist_sqlstat
                             where snap_id between 1+ :snap_start and :snap_end
                               and instance_number = :instno
                               and dbid = :dbid
                             group by sql_id order by sum(nvl(executions_delta,0)) desc
                          )
                    where rownum <= 5
                 ) v_rank,
                 ( select snap_id,
                          sql_id,
                          module,
                          elapsed_time_delta elap,
                          cpu_time_delta cput,
                          executions_delta exec,
                          rows_processed_delta rows_processed
                     from dba_hist_sqlstat
                    where snap_id between 1+ :snap_start and :snap_end
                      and instance_number = :instno
                      and dbid = :dbid
                 ) v_sqlstat
           where v_sqlstat.sql_id = v_rank.sql_id
       ) v1,
       ( select sn.snap_id,
                sn.end_interval_time
           from dba_hist_snapshot sn
          where sn.snap_id  between :snap_start and :snap_end
            and sn.instance_number = :instno
            and sn.dbid = :dbid
       ) sn
 where v1.snap_id(+) -1  = sn.snap_id
   and sn.snap_id > :snap_start
 group by sn.snap_id,
       to_char(sn.end_interval_time,'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

sql_exec2 = """
select lable||sql_id title
  from ( select 1 no,'' lable from dual union all
         select 2   ,''       from dual union all
         select 3   ,''       from dual union all
         select 4   ,''       from dual union all
         select 5   ,''       from dual
       ) a,
       ( select sql_id, rownum rank
           from ( select sql_id, sum(nvl(executions_delta,0))
                    from dba_hist_sqlstat
                   where snap_id between 1+ :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                   group by sql_id order by sum(nvl(executions_delta,0)) desc
                )
          where rownum <= 5
       ) b
 order by no,rank
"""

sql_exec_sql = """
select rank,
       v1.sql_id,
       v1.module,
       nvl(replace(replace(replace(to_char(substr(st.sql_text,1,2500)),chr(9),' '),chr(10),' '),chr(13),' '), '** Not Found **') "SQL Text"
  from ( select sql_id, dbid, module, rownum rank
           from ( select sql_id, dbid, module, sum(nvl(executions_delta,0))
                    from dba_hist_sqlstat
                   where snap_id between 1+ :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                   group by sql_id, dbid, module order by sum(nvl(executions_delta,0)) desc
                )
          where rownum <= 5
       ) v1,
       dba_hist_sqltext st
 where st.sql_id(+) = v1.sql_id
   and st.dbid(+) = v1.dbid
"""

sql_exec_raw = """
select snap_id,
       to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss') dates,
       exec,
       rows_processed,
       decode(nvl(exec,0), 0, 0, rows_processed/exec) "Rows/exec",
       decode(nvl(exec,0), 0, 0, cput/exec/1000000) "CPU(s)/exec",
       decode(nvl(exec,0), 0, 0, elap/exec/1000000) "Elapsed(s)/exec",
       sql_id,
       nvl(replace(replace(replace(to_char(substr(sql_text,1,2500)),chr(9),' '),chr(10),' '),chr(13),' '), '** Not Found **') "SQL Text",
       module,
       rank
  from ( select sn.snap_id,
                sn.end_interval_time,
                sq.sql_id,
                st.sql_text,
                sq.module,
                sq.executions_delta exec,
                sq.rows_processed_delta rows_processed,
                sq.elapsed_time_delta elap,
                sq.cpu_time_delta cput,
                rank() over ( partition by sq.snap_id order by sq.executions_delta desc ) rank
           from dba_hist_sqlstat sq,
                dba_hist_sqltext st,
                dba_hist_snapshot sn
          where sq.snap_id = sn.snap_id
            and sq.instance_number = sn.instance_number
            and sq.dbid = sn.dbid
            and st.sql_id(+) = sq.sql_id
            and st.dbid(+) = sq.dbid
            and sn.snap_id between :snap_start and :snap_end
            and sn.instance_number = :instno
            and sn.dbid = :dbid
       )
 where rank <= 5
   and snap_id > :snap_start
 order by snap_id
"""
sql_clwait = """
select sn.snap_id,
       to_char(sn.end_interval_time,'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time,'dd hh24:mi'))       dates2,
       max(decode(rank, 1, clwait/1000000, 0)) rank1_clwait,
       max(decode(rank, 2, clwait/1000000, 0)) rank2_clwait,
       max(decode(rank, 3, clwait/1000000, 0)) rank3_clwait,
       max(decode(rank, 4, clwait/1000000, 0)) rank4_clwait,
       max(decode(rank, 5, clwait/1000000, 0)) rank5_clwait,
       max(decode(rank, 1, cput/1000000, 0)) rank1_cputime,
       max(decode(rank, 2, cput/1000000, 0)) rank2_cputime,
       max(decode(rank, 3, cput/1000000, 0)) rank3_cputime,
       max(decode(rank, 4, cput/1000000, 0)) rank4_cputime,
       max(decode(rank, 5, cput/1000000, 0)) rank5_cputime,
       max(decode(rank, 1, elap/1000000, 0)) rank1_elapsed,
       max(decode(rank, 2, elap/1000000, 0)) rank2_elapsed,
       max(decode(rank, 3, elap/1000000, 0)) rank3_elapsed,
       max(decode(rank, 4, elap/1000000, 0)) rank4_elapsed,
       max(decode(rank, 5, elap/1000000, 0)) rank5_elapsed,
       max(decode(rank, 1, exec, 0)) rank1_exec,
       max(decode(rank, 2, exec, 0)) rank2_exec,
       max(decode(rank, 3, exec, 0)) rank3_exec,
       max(decode(rank, 4, exec, 0)) rank4_exec,
       max(decode(rank, 5, exec, 0)) rank5_exec,
       decode(max(decode(rank, 1, elap, 0)), 0, 0,max(decode(rank, 1, clwait, 0))/max(decode(rank, 1, elap, 0))) rank1_clwait_per_elapsed,
       decode(max(decode(rank, 2, elap, 0)), 0, 0,max(decode(rank, 2, clwait, 0))/max(decode(rank, 2, elap, 0))) rank2_clwait_per_elapsed,
       decode(max(decode(rank, 3, elap, 0)), 0, 0,max(decode(rank, 3, clwait, 0))/max(decode(rank, 3, elap, 0))) rank3_clwait_per_elapsed,
       decode(max(decode(rank, 4, elap, 0)), 0, 0,max(decode(rank, 4, clwait, 0))/max(decode(rank, 4, elap, 0))) rank4_clwait_per_elapsed,
       decode(max(decode(rank, 5, elap, 0)), 0, 0,max(decode(rank, 5, clwait, 0))/max(decode(rank, 5, elap, 0))) rank5_clwait_per_elapsed,
       max(decode(rank, 1, sql_id, null)) rank1,
       max(decode(rank, 2, sql_id, null)) rank2,
       max(decode(rank, 3, sql_id, null)) rank3,
       max(decode(rank, 4, sql_id, null)) rank4,
       max(decode(rank, 5, sql_id, null)) rank5
  from ( select v_sqlstat.snap_id,
                v_rank.rank,
                v_sqlstat.sql_id,
                v_sqlstat.module,
                v_sqlstat.clwait,
                v_sqlstat.elap,
                v_sqlstat.cput,
                v_sqlstat.exec
           from ( select sql_id, rownum rank
                    from ( select sql_id, sum(nvl(clwait_delta,0))
                             from dba_hist_sqlstat
                            where snap_id between 1+ :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                            group by sql_id order by sum(nvl(clwait_delta,0)) desc
                         )
                   where rownum <= 5
                ) v_rank,
                ( select snap_id,
                         sql_id,
                         module,
                         elapsed_time_delta elap,
                         cpu_time_delta cput,
                         executions_delta exec,
                         clwait_delta clwait
                    from dba_hist_sqlstat
                   where snap_id between 1+ :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                ) v_sqlstat
          where v_sqlstat.sql_id = v_rank.sql_id
       ) v1,
       ( select sn.snap_id,
                sn.end_interval_time
           from dba_hist_snapshot sn
          where sn.snap_id  between :snap_start and :snap_end
            and sn.instance_number = :instno
            and sn.dbid = :dbid
       ) sn
 where v1.snap_id(+) = sn.snap_id
   and sn.snap_id > :snap_start
 group by sn.snap_id, to_char(sn.end_interval_time,'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

sql_clwait2 = """
select lable||sql_id title
  from ( select 1 no,'' lable from dual union all
         select 2   ,''       from dual union all
         select 3   ,''       from dual union all
         select 4   ,''       from dual union all
         select 5   ,''       from dual
       ) a,
       ( select sql_id, rownum rank
           from ( select sql_id, sum(nvl(clwait_delta,0))
                    from dba_hist_sqlstat
                   where snap_id between 1+ :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                   group by sql_id order by sum(nvl(clwait_delta,0)) desc
                )
          where rownum <= 5
       ) b
 order by no,rank
"""

sql_clwait_sql = """
select rank,
       v1.sql_id,
       v1.module,
       nvl(replace(replace(replace(to_char(substr(st.sql_text,1,2500)),chr(9),' '),chr(10),' '),chr(13),' '), '** Not Found **') "SQL Text"
  from ( select sql_id, dbid, module, rownum rank
           from ( select sql_id, dbid, module, sum(nvl(clwait_delta,0))
                    from dba_hist_sqlstat
                   where snap_id between 1+ :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                   group by sql_id, dbid, module order by sum(nvl(clwait_delta,0)) desc
                )
          where rownum <= 5
       ) v1,
       dba_hist_sqltext st
 where st.sql_id(+) = v1.sql_id
   and st.dbid(+) = v1.dbid
"""

sql_clwait_raw = """
select snap_id,
       to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss') dates,
       clwait/1000000 "Cluster Wait Time(s)",
       decode(nvl(elap,0), 0, 0, clwait/elap) "%CWT/Elap",
       elap/1000000 "Elapsed(s)",
       cput/1000000 "CPU(s)",
       exec,
       sql_id,
       nvl(replace(replace(replace(to_char(substr(sql_text,1,2500)),chr(9),' '),chr(10),' '),chr(13),' '), '** Not Found **') "SQL Text",
       module,
       rank
  from ( select sn.snap_id,
                sn.end_interval_time,
                sq.sql_id,
                st.sql_text,
                sq.module,
                sq.clwait_delta clwait,
                sq.executions_delta exec,
                sq.elapsed_time_delta elap,
                sq.cpu_time_delta cput,
                rank() over ( partition by sq.snap_id order by sq.clwait_delta desc ) rank
           from dba_hist_sqlstat sq,
                dba_hist_sqltext st,
                dba_hist_snapshot sn
          where sq.snap_id = sn.snap_id
            and sq.instance_number = sn.instance_number
            and sq.dbid = sn.dbid
            and st.sql_id(+) = sq.sql_id
            and st.dbid(+) = sq.dbid
            and sn.snap_id between :snap_start and :snap_end
            and sn.instance_number = :instno
            and sn.dbid = :dbid
       )
 where rank <= 5
   and snap_id > :snap_start
 order by snap_id
"""

seg_logical = """
select sn.snap_id SnapID,
       to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time, 'dd hh24:mi'))       dates2,
       max(decode(rank, 1, logical_reads_delta, 0)) rank1_value,
       max(decode(rank, 2, logical_reads_delta, 0)) rank2_value,
       max(decode(rank, 3, logical_reads_delta, 0)) rank3_value,
       max(decode(rank, 4, logical_reads_delta, 0)) rank4_value,
       max(decode(rank, 5, logical_reads_delta, 0)) rank5_value,
       max(decode(rank, 1, ratio, 0))  rank1_ratio,
       max(decode(rank, 2, ratio, 0))  rank2_ratio,
       max(decode(rank, 3, ratio, 0))  rank3_ratio,
       max(decode(rank, 4, ratio, 0))  rank4_ratio,
       max(decode(rank, 5, ratio, 0))  rank5_ratio,
       max(decode(rank, 1, logical_reads_delta/interval, 0)) rank1_valueps,
       max(decode(rank, 2, logical_reads_delta/interval, 0)) rank2_valueps,
       max(decode(rank, 3, logical_reads_delta/interval, 0)) rank3_valueps,
       max(decode(rank, 4, logical_reads_delta/interval, 0)) rank4_valueps,
       max(decode(rank, 5, logical_reads_delta/interval, 0)) rank5_valueps
  from ( select a.snap_id,
                 rank,
                 a.dataobj#,
                 a.obj#,
                 logical_reads_delta,
                 nvl(ratio_to_report(logical_reads_delta) over (partition by snap_id),0) ratio
            from dba_hist_seg_stat a,
                 ( select dataobj#, obj#, rownum rank
                     from ( select dataobj#, obj#, sum(logical_reads_delta)
                              from dba_hist_seg_stat
                             where snap_id between 1+ :snap_start and :snap_end
                               and instance_number = :instno
                               and dbid = :dbid
                             group by dataobj#, obj#
                             order by sum(logical_reads_delta) desc
                          )
                    where rownum <= 5
                 ) b
           where a.dataobj# = b.dataobj#
             and a.obj# = b.obj#
             and snap_id between 1+ :snap_start and :snap_end
             and instance_number = :instno
             and dbid = :dbid
       ) v1,
       ( select snap_id,
                end_interval_time,
                to_number(substr((end_interval_time-begin_interval_time)*86400,2,9)) interval
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) sn
 where v1.snap_id(+) = sn.snap_id
 group by sn.snap_id, sn.end_interval_time
 order by sn.snap_id
"""

seg_logical2 = """
select lable||name title
  from ( select 1 no,'' lable from dual union all
         select 2   ,''       from dual union all
         select 3   ,''       from dual
       ) a,
       ( select v_rank.rank,
                case when obj.subobject_name is null then obj.owner||':'||obj.object_name
                     else obj.owner||':'||obj.object_name||':'||obj.subobject_name
                end name
          from ( select dbid, dataobj#, obj#, rownum rank
                   from ( select dbid, dataobj#, obj#, sum(logical_reads_delta)
                            from dba_hist_seg_stat
                           where snap_id between 1+ :snap_start and :snap_end
                             and instance_number = :instno
                             and dbid = :dbid
                           group by dbid, dataobj#, obj#
                           order by sum(logical_reads_delta) desc
                        )
                where rownum <= 5
               ) v_rank,
               dba_hist_seg_stat_obj obj
         where obj.dataobj# = v_rank.dataobj#
           and obj.obj# = v_rank.obj#
           and obj.dbid = v_rank.dbid
       ) b
 order by no,rank
"""

seg_physical = """
select sn.snap_id snapid,
       to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time, 'dd hh24:mi'))       dates2,
       max(decode(rank, 1, physical_reads_delta, 0)) rank1_value,
       max(decode(rank, 2, physical_reads_delta, 0)) rank2_value,
       max(decode(rank, 3, physical_reads_delta, 0)) rank3_value,
       max(decode(rank, 4, physical_reads_delta, 0)) rank4_value,
       max(decode(rank, 5, physical_reads_delta, 0)) rank5_value,
       max(decode(rank, 1, ratio, 0))  rank1_ratio,
       max(decode(rank, 2, ratio, 0))  rank2_ratio,
       max(decode(rank, 3, ratio, 0))  rank3_ratio,
       max(decode(rank, 4, ratio, 0))  rank4_ratio,
       max(decode(rank, 5, ratio, 0))  rank5_ratio,
       max(decode(rank, 1, physical_reads_delta/interval, 0)) rank1_valueps,
       max(decode(rank, 2, physical_reads_delta/interval, 0)) rank2_valueps,
       max(decode(rank, 3, physical_reads_delta/interval, 0)) rank3_valueps,
       max(decode(rank, 4, physical_reads_delta/interval, 0)) rank4_valueps,
       max(decode(rank, 5, physical_reads_delta/interval, 0)) rank5_valueps
  from ( select a.snap_id,
                rank,
                a.dataobj#,
                a.obj#,
                physical_reads_delta,
                nvl(ratio_to_report(physical_reads_delta) over (partition by snap_id),0) ratio
           from dba_hist_seg_stat a,
                ( select dataobj#, obj#, rownum rank
                    from ( select dataobj#, obj#, sum(physical_reads_delta)
                             from dba_hist_seg_stat
                            where snap_id between 1+ :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                            group by dataobj#, obj#
                            order by sum(physical_reads_delta) desc
                         )
                   where rownum <= 5
                ) b
          where a.dataobj# = b.dataobj#
            and a.obj# = b.obj#
            and snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) v1,
       ( select snap_id,
                end_interval_time,
                to_number(substr((end_interval_time-begin_interval_time)*86400,2,9)) interval
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) sn
 where v1.snap_id(+) = sn.snap_id
 group by sn.snap_id, to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

seg_physical2 = """
select lable||name title
  from ( select 1 no,'' lable from dual union all
         select 2   ,''       from dual union all
         select 3   ,''       from dual
       ) a,
       ( select v_rank.rank,
                case when obj.subobject_name is null then obj.owner||':'||obj.object_name
                     else obj.owner||':'||obj.object_name||':'||obj.subobject_name
                end name
           from ( select dbid, dataobj#, obj#, rownum rank
                    from ( select dbid, dataobj#, obj#, sum(physical_reads_delta)
                             from dba_hist_seg_stat
                            where snap_id between 1+ :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                            group by dbid, dataobj#, obj#
                            order by sum(physical_reads_delta) desc
                         )
                   where rownum <= 5
                ) v_rank,
                dba_hist_seg_stat_obj obj
          where obj.dataobj# = v_rank.dataobj#
            and obj.obj# = v_rank.obj#
            and obj.dbid = v_rank.dbid
       ) b
 order by no,rank
"""

seg_buffbusy = """
select sn.snap_id snapid,
       to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time, 'dd hh24:mi'))       dates2,
       max(decode(rank, 1, buffer_busy_waits_delta, 0)) rank1_value,
       max(decode(rank, 2, buffer_busy_waits_delta, 0)) rank2_value,
       max(decode(rank, 3, buffer_busy_waits_delta, 0)) rank3_value,
       max(decode(rank, 4, buffer_busy_waits_delta, 0)) rank4_value,
       max(decode(rank, 5, buffer_busy_waits_delta, 0)) rank5_value,
       max(decode(rank, 1, ratio, 0))  rank1_ratio,
       max(decode(rank, 2, ratio, 0))  rank2_ratio,
       max(decode(rank, 3, ratio, 0))  rank3_ratio,
       max(decode(rank, 4, ratio, 0))  rank4_ratio,
       max(decode(rank, 5, ratio, 0))  rank5_ratio,
       max(decode(rank, 1, buffer_busy_waits_delta/interval, 0)) rank1_valueps,
       max(decode(rank, 2, buffer_busy_waits_delta/interval, 0)) rank2_valueps,
       max(decode(rank, 3, buffer_busy_waits_delta/interval, 0)) rank3_valueps,
       max(decode(rank, 4, buffer_busy_waits_delta/interval, 0)) rank4_valueps,
       max(decode(rank, 5, buffer_busy_waits_delta/interval, 0)) rank5_valueps
  from ( select a.snap_id,
                rank,
                a.dataobj#,
                a.obj#,
                buffer_busy_waits_delta,
                nvl(ratio_to_report(buffer_busy_waits_delta) over (partition by snap_id),0) ratio
           from dba_hist_seg_stat a,
                ( select dataobj#, obj#, rownum rank
                    from ( select dataobj#, obj#, sum(buffer_busy_waits_delta)
                             from dba_hist_seg_stat
                            where snap_id between 1+ :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                            group by dataobj#, obj#
                            order by sum(buffer_busy_waits_delta) desc
                         )
                   where rownum <= 5
                ) b
          where a.dataobj# = b.dataobj#
            and a.obj# = b.obj#
            and snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) v1,
       ( select snap_id,
                end_interval_time,
                to_number(substr((end_interval_time-begin_interval_time)*86400,2,9)) interval
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) sn
 where v1.snap_id(+) = sn.snap_id
 group by sn.snap_id, to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

seg_buffbusy2 = """
select lable||name title
  from ( select 1 no,'' lable from dual union all
         select 2   ,''       from dual union all
         select 3   ,''       from dual
       ) a,
       ( select v_rank.rank,
                case when obj.subobject_name is null then obj.owner||':'||obj.object_name
                     else obj.owner||':'||obj.object_name||':'||obj.subobject_name
                end name
           from ( select dbid, dataobj#, obj#, rownum rank
                    from ( select dbid, dataobj#, obj#, sum(buffer_busy_waits_delta)
                             from dba_hist_seg_stat
                            where snap_id between 1+ :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                            group by dbid, dataobj#, obj#
                            order by sum(buffer_busy_waits_delta) desc
                         )
                   where rownum <= 5
                ) v_rank,
                dba_hist_seg_stat_obj obj
          where obj.dataobj# = v_rank.dataobj#
            and obj.obj# = v_rank.obj#
            and obj.dbid = v_rank.dbid
       ) b
 order by no,rank
"""

seg_rowlock = """
select sn.snap_id snapid,
       to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time, 'dd hh24:mi'))       dates2,
       max(decode(rank, 1, row_lock_waits_delta, 0)) rank1_value,
       max(decode(rank, 2, row_lock_waits_delta, 0)) rank2_value,
       max(decode(rank, 3, row_lock_waits_delta, 0)) rank3_value,
       max(decode(rank, 4, row_lock_waits_delta, 0)) rank4_value,
       max(decode(rank, 5, row_lock_waits_delta, 0)) rank5_value,
       max(decode(rank, 1, ratio, 0))  rank1_ratio,
       max(decode(rank, 2, ratio, 0))  rank2_ratio,
       max(decode(rank, 3, ratio, 0))  rank3_ratio,
       max(decode(rank, 4, ratio, 0))  rank4_ratio,
       max(decode(rank, 5, ratio, 0))  rank5_ratio,
       max(decode(rank, 1, row_lock_waits_delta/interval, 0)) rank1_valueps,
       max(decode(rank, 2, row_lock_waits_delta/interval, 0)) rank2_valueps,
       max(decode(rank, 3, row_lock_waits_delta/interval, 0)) rank3_valueps,
       max(decode(rank, 4, row_lock_waits_delta/interval, 0)) rank4_valueps,
       max(decode(rank, 5, row_lock_waits_delta/interval, 0)) rank5_valueps
  from ( select a.snap_id,
                rank,
                a.dataobj#,
                a.obj#,
                row_lock_waits_delta,
                nvl(ratio_to_report(row_lock_waits_delta) over (partition by snap_id),0) ratio
           from dba_hist_seg_stat a,
                ( select dataobj#, obj#, rownum rank
                    from ( select dataobj#, obj#, sum(row_lock_waits_delta)
                             from dba_hist_seg_stat
                            where snap_id between 1+ :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                            group by dataobj#, obj#
                            order by sum(row_lock_waits_delta) desc
                         )
                   where rownum <= 5
                ) b
          where a.dataobj# = b.dataobj#
            and a.obj# = b.obj#
            and snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) v1,
       ( select snap_id,
                end_interval_time,
                to_number(substr((end_interval_time-begin_interval_time)*86400,2,9)) interval
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) sn
 where v1.snap_id(+) = sn.snap_id
 group by sn.snap_id, to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

seg_rowlock2 = """
select lable||name title
  from ( select 1 no,'' lable from dual union all
         select 2   ,''       from dual union all
         select 3   ,''       from dual
       ) a,
       ( select v_rank.rank,
                case when obj.subobject_name is null then obj.owner||':'||obj.object_name
                     else obj.owner||':'||obj.object_name||':'||obj.subobject_name
                end name
           from ( select dbid, dataobj#, obj#, rownum rank
                    from ( select dbid, dataobj#, obj#, sum(row_lock_waits_delta)
                             from dba_hist_seg_stat
                            where snap_id between 1+ :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                            group by dbid, dataobj#, obj#
                            order by sum(row_lock_waits_delta) desc
                         )
                   where rownum <= 5
                ) v_rank,
                dba_hist_seg_stat_obj obj
          where obj.dataobj# = v_rank.dataobj#
            and obj.obj# = v_rank.obj#
            and obj.dbid = v_rank.dbid
       ) b
 order by no,rank
"""

seg_itl = """
select sn.snap_id snapid,
       to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time, 'dd hh24:mi'))       dates2,
       max(decode(rank, 1, itl_waits_delta, 0)) rank1_value,
       max(decode(rank, 2, itl_waits_delta, 0)) rank2_value,
       max(decode(rank, 3, itl_waits_delta, 0)) rank3_value,
       max(decode(rank, 4, itl_waits_delta, 0)) rank4_value,
       max(decode(rank, 5, itl_waits_delta, 0)) rank5_value,
       max(decode(rank, 1, ratio, 0))  rank1_ratio,
       max(decode(rank, 2, ratio, 0))  rank2_ratio,
       max(decode(rank, 3, ratio, 0))  rank3_ratio,
       max(decode(rank, 4, ratio, 0))  rank4_ratio,
       max(decode(rank, 5, ratio, 0))  rank5_ratio,
       max(decode(rank, 1, itl_waits_delta/interval, 0)) rank1_valueps,
       max(decode(rank, 2, itl_waits_delta/interval, 0)) rank2_valueps,
       max(decode(rank, 3, itl_waits_delta/interval, 0)) rank3_valueps,
       max(decode(rank, 4, itl_waits_delta/interval, 0)) rank4_valueps,
       max(decode(rank, 5, itl_waits_delta/interval, 0)) rank5_valueps
  from ( select a.snap_id,
                rank,
                a.dataobj#,
                a.obj#,
                itl_waits_delta,
                nvl(ratio_to_report(itl_waits_delta) over (partition by snap_id),0) ratio
           from dba_hist_seg_stat a,
                ( select dataobj#, obj#, rownum rank
                    from ( select dataobj#, obj#, sum(itl_waits_delta)
                             from dba_hist_seg_stat
                            where snap_id between 1+ :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                            group by dataobj#, obj#
                            order by sum(itl_waits_delta) desc
                         )
                   where rownum <= 5
                ) b
          where a.dataobj# = b.dataobj#
            and a.obj# = b.obj#
            and snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) v1,
       ( select snap_id,
                end_interval_time,
                to_number(substr((end_interval_time-begin_interval_time)*86400,2,9)) interval
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) sn
 where v1.snap_id(+) = sn.snap_id
 group by sn.snap_id, to_char(end_interval_time, 'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

seg_itl2 = """
select lable||name title
  from ( select 1 no,'' lable from dual union all
         select 2   ,''       from dual union all
         select 3   ,''       from dual
       ) a,
       ( select v_rank.rank,
                case when obj.subobject_name is null then obj.owner||':'||obj.object_name
                     else obj.owner||':'||obj.object_name||':'||obj.subobject_name
                end name
           from ( select dbid, dataobj#, obj#, rownum rank
                    from ( select dbid, dataobj#, obj#, sum(itl_waits_delta)
                             from dba_hist_seg_stat
                            where snap_id between 1+ :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                            group by dbid, dataobj#, obj#
                            order by sum(itl_waits_delta) desc
                         )
                   where rownum <= 5
                ) v_rank,
                dba_hist_seg_stat_obj obj
          where obj.dataobj# = v_rank.dataobj#
            and obj.obj# = v_rank.obj#
            and obj.dbid = v_rank.dbid
       ) b
 order by no,rank
"""

seg_spaceused = """
select sn.snap_id snapid,
       to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time, 'dd hh24:mi:ss'))    dates2,
       max(decode(rank, 1, space_used_delta, 0)) rank1_value,
       max(decode(rank, 2, space_used_delta, 0)) rank2_value,
       max(decode(rank, 3, space_used_delta, 0)) rank3_value,
       max(decode(rank, 4, space_used_delta, 0)) rank4_value,
       max(decode(rank, 5, space_used_delta, 0)) rank5_value,
       max(decode(rank, 1, ratio, 0))  rank1_ratio,
       max(decode(rank, 2, ratio, 0))  rank2_ratio,
       max(decode(rank, 3, ratio, 0))  rank3_ratio,
       max(decode(rank, 4, ratio, 0))  rank4_ratio,
       max(decode(rank, 5, ratio, 0))  rank5_ratio,
       max(decode(rank, 1, space_used_delta/interval, 0)) rank1_valueps,
       max(decode(rank, 2, space_used_delta/interval, 0)) rank2_valueps,
       max(decode(rank, 3, space_used_delta/interval, 0)) rank3_valueps,
       max(decode(rank, 4, space_used_delta/interval, 0)) rank4_valueps,
       max(decode(rank, 5, space_used_delta/interval, 0)) rank5_valueps
  from ( select a.snap_id,
                rank,
                a.dataobj#,
                a.obj#,
                space_used_delta,
                nvl(ratio_to_report(space_used_delta) over (partition by snap_id),0) ratio
           from dba_hist_seg_stat a,
                ( select dataobj#, obj#, rownum rank
                    from ( select dataobj#, obj#, sum(space_used_delta)
                             from dba_hist_seg_stat
                            where snap_id between 1+ :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                            group by dataobj#, obj#
                            order by sum(space_used_delta) desc
                         )
                  where rownum <= 5
                ) b
          where a.dataobj# = b.dataobj#
            and a.obj# = b.obj#
            and snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) v1,
       ( select snap_id,
                end_interval_time,
                to_number(substr((end_interval_time-begin_interval_time)*86400,2,9)) interval
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) sn
 where v1.snap_id(+) = sn.snap_id
 group by sn.snap_id, to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

seg_spaceused2 = """
select lable||name title
  from ( select 1 no,'' lable from dual union all
         select 2   ,''       from dual union all
         select 3   ,''       from dual
       ) a,
       ( select v_rank.rank,
                case when obj.subobject_name is null then obj.owner||':'||obj.object_name
                     else obj.owner||':'||obj.object_name||':'||obj.subobject_name
                end name
           from ( select dbid, dataobj#, obj#, rownum rank
                    from ( select dbid, dataobj#, obj#, sum(space_used_delta)
                             from dba_hist_seg_stat
                            where snap_id between 1+ :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                            group by dbid, dataobj#, obj#
                            order by sum(space_used_delta) desc
                         )
                   where rownum <= 5
              ) v_rank,
              dba_hist_seg_stat_obj obj
        where obj.dataobj# = v_rank.dataobj#
          and obj.obj# = v_rank.obj#
          and obj.dbid = v_rank.dbid
       ) b
 order by no,rank
"""

seg_gccrserv = """
select sn.snap_id snapid,
       to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time, 'dd hh24:mi'))       dates2,
       max(decode(rank, 1, gc_cr_blocks_served_delta, 0)) rank1_value,
       max(decode(rank, 2, gc_cr_blocks_served_delta, 0)) rank2_value,
       max(decode(rank, 3, gc_cr_blocks_served_delta, 0)) rank3_value,
       max(decode(rank, 4, gc_cr_blocks_served_delta, 0)) rank4_value,
       max(decode(rank, 5, gc_cr_blocks_served_delta, 0)) rank5_value,
       max(decode(rank, 1, ratio, 0))  rank1_ratio,
       max(decode(rank, 2, ratio, 0))  rank2_ratio,
       max(decode(rank, 3, ratio, 0))  rank3_ratio,
       max(decode(rank, 4, ratio, 0))  rank4_ratio,
       max(decode(rank, 5, ratio, 0))  rank5_ratio,
       max(decode(rank, 1, gc_cr_blocks_served_delta/interval, 0)) rank1_valueps,
       max(decode(rank, 2, gc_cr_blocks_served_delta/interval, 0)) rank2_valueps,
       max(decode(rank, 3, gc_cr_blocks_served_delta/interval, 0)) rank3_valueps,
       max(decode(rank, 4, gc_cr_blocks_served_delta/interval, 0)) rank4_valueps,
       max(decode(rank, 5, gc_cr_blocks_served_delta/interval, 0)) rank5_valueps
  from  ( select a.snap_id,
                 rank,
                 a.dataobj#,
                 a.obj#,
                 gc_cr_blocks_served_delta,
                 nvl(ratio_to_report(gc_cr_blocks_served_delta) over (partition by snap_id),0) ratio
            from dba_hist_seg_stat a,
                 ( select dataobj#, obj#, rownum rank
                     from ( select dataobj#, obj#, sum(gc_cr_blocks_served_delta)
                              from dba_hist_seg_stat
                             where snap_id between 1+ :snap_start and :snap_end
                               and instance_number = :instno
                               and dbid = :dbid
                             group by dataobj#, obj#
                             order by sum(gc_cr_blocks_served_delta) desc
                           )
                   where rownum <= 5
                 ) b
           where a.dataobj# = b.dataobj#
             and a.obj# = b.obj#
             and snap_id between 1+ :snap_start and :snap_end
             and instance_number = :instno
             and dbid = :dbid
          ) v1,
       (
       select snap_id,
              end_interval_time,
              to_number(substr((end_interval_time-begin_interval_time)*86400,2,9)) interval
         from dba_hist_snapshot
        where snap_id between 1+ :snap_start and :snap_end
          and instance_number = :instno
          and dbid = :dbid
       ) sn
 where v1.snap_id(+) = sn.snap_id
 group by sn.snap_id, to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

seg_gccrserv2 = """
select lable||name title
  from ( select 1 no,'' lable from dual union all
         select 2   ,''       from dual union all
         select 3   ,''       from dual
       ) a,
       ( select v_rank.rank,
                case when obj.subobject_name is null then obj.owner||':'||obj.object_name
                     else obj.owner||':'||obj.object_name||':'||obj.subobject_name
                end name
           from ( select dbid, dataobj#, obj#, rownum rank
                    from ( select dbid, dataobj#, obj#, sum(gc_cr_blocks_served_delta)
                             from dba_hist_seg_stat
                            where snap_id between 1+ :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                            group by dbid, dataobj#, obj#
                            order by sum(gc_cr_blocks_served_delta) desc
                         )
                   where rownum <= 5
                ) v_rank,
                dba_hist_seg_stat_obj obj
          where obj.dataobj# = v_rank.dataobj#
            and obj.obj# = v_rank.obj#
            and obj.dbid = v_rank.dbid
       ) b
 order by no,rank
"""

seg_gccuserv = """
select sn.snap_id snapid,
       to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time, 'dd hh24:mi'))       dates2,
       max(decode(rank, 1, gc_cu_blocks_served_delta, 0)) rank1_value,
       max(decode(rank, 2, gc_cu_blocks_served_delta, 0)) rank2_value,
       max(decode(rank, 3, gc_cu_blocks_served_delta, 0)) rank3_value,
       max(decode(rank, 4, gc_cu_blocks_served_delta, 0)) rank4_value,
       max(decode(rank, 5, gc_cu_blocks_served_delta, 0)) rank5_value,
       max(decode(rank, 1, ratio, 0))  rank1_ratio,
       max(decode(rank, 2, ratio, 0))  rank2_ratio,
       max(decode(rank, 3, ratio, 0))  rank3_ratio,
       max(decode(rank, 4, ratio, 0))  rank4_ratio,
       max(decode(rank, 5, ratio, 0))  rank5_ratio,
       max(decode(rank, 1, gc_cu_blocks_served_delta/interval, 0)) rank1_valueps,
       max(decode(rank, 2, gc_cu_blocks_served_delta/interval, 0)) rank2_valueps,
       max(decode(rank, 3, gc_cu_blocks_served_delta/interval, 0)) rank3_valueps,
       max(decode(rank, 4, gc_cu_blocks_served_delta/interval, 0)) rank4_valueps,
       max(decode(rank, 5, gc_cu_blocks_served_delta/interval, 0)) rank5_valueps
  from ( select a.snap_id,
                rank,
                a.dataobj#,
                a.obj#,
                gc_cu_blocks_served_delta,
                nvl(ratio_to_report(gc_cu_blocks_served_delta) over (partition by snap_id),0) ratio
           from dba_hist_seg_stat a,
                ( select dataobj#, obj#, rownum rank
                    from ( select dataobj#, obj#, sum(gc_cu_blocks_served_delta)
                             from dba_hist_seg_stat
                            where snap_id between 1+ :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                            group by dataobj#, obj#
                            order by sum(gc_cu_blocks_served_delta) desc
                          )
                  where rownum <= 5
                ) b
          where a.dataobj# = b.dataobj#
            and a.obj# = b.obj#
            and snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) v1,
       ( select snap_id,
                end_interval_time,
                to_number(substr((end_interval_time-begin_interval_time)*86400,2,9)) interval
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) sn
 where v1.snap_id(+) = sn.snap_id
 group by sn.snap_id, to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

seg_gccuserv2 = """
select lable||name title
  from ( select 1 no,'' lable from dual union all
         select 2   ,''       from dual union all
         select 3   ,''       from dual
       ) a,
       ( select v_rank.rank,
                case when obj.subobject_name is null then obj.owner||':'||obj.object_name
                     else obj.owner||':'||obj.object_name||':'||obj.subobject_name
                end name
           from ( select dbid, dataobj#, obj#, rownum rank
                    from ( select dbid, dataobj#, obj#, sum(gc_cu_blocks_served_delta)
                             from dba_hist_seg_stat
                            where snap_id between 1+ :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                            group by dbid, dataobj#, obj#
                            order by sum(gc_cu_blocks_served_delta) desc
                         )
                   where rownum <= 5
                ) v_rank,
                dba_hist_seg_stat_obj obj
          where obj.dataobj# = v_rank.dataobj#
            and obj.obj# = v_rank.obj#
            and obj.dbid = v_rank.dbid
       ) b
 order by no,rank
"""

seg_gccrrcvd = """
select sn.snap_id snapid,
       to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time, 'dd hh24:mi'))       dates2,
       max(decode(rank, 1, gc_cr_blocks_received_delta, 0)) rank1_value,
       max(decode(rank, 2, gc_cr_blocks_received_delta, 0)) rank2_value,
       max(decode(rank, 3, gc_cr_blocks_received_delta, 0)) rank3_value,
       max(decode(rank, 4, gc_cr_blocks_received_delta, 0)) rank4_value,
       max(decode(rank, 5, gc_cr_blocks_received_delta, 0)) rank5_value,
       max(decode(rank, 1, ratio, 0))  rank1_ratio,
       max(decode(rank, 2, ratio, 0))  rank2_ratio,
       max(decode(rank, 3, ratio, 0))  rank3_ratio,
       max(decode(rank, 4, ratio, 0))  rank4_ratio,
       max(decode(rank, 5, ratio, 0))  rank5_ratio,
       max(decode(rank, 1, gc_cr_blocks_received_delta/interval, 0)) rank1_valueps,
       max(decode(rank, 2, gc_cr_blocks_received_delta/interval, 0)) rank2_valueps,
       max(decode(rank, 3, gc_cr_blocks_received_delta/interval, 0)) rank3_valueps,
       max(decode(rank, 4, gc_cr_blocks_received_delta/interval, 0)) rank4_valueps,
       max(decode(rank, 5, gc_cr_blocks_received_delta/interval, 0)) rank5_valueps
  from ( select a.snap_id,
                rank,
                a.dataobj#,
                a.obj#,
                gc_cr_blocks_received_delta,
                nvl(ratio_to_report(gc_cr_blocks_received_delta) over (partition by snap_id),0) ratio
           from dba_hist_seg_stat a,
                ( select dataobj#, obj#, rownum rank
                    from ( select dataobj#, obj#, sum(gc_cr_blocks_received_delta)
                             from dba_hist_seg_stat
                            where snap_id between 1+ :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                            group by dataobj#, obj#
                            order by sum(gc_cr_blocks_received_delta) desc
                         )
                  where rownum <= 5
                ) b
          where a.dataobj# = b.dataobj#
            and a.obj# = b.obj#
            and snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) v1,
       ( select snap_id,
                end_interval_time,
                to_number(substr((end_interval_time-begin_interval_time)*86400,2,9)) interval
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) sn
 where v1.snap_id(+) = sn.snap_id
 group by sn.snap_id, to_char(end_interval_time, 'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

seg_gccrrcvd2 = """
select lable||name title
  from ( select 1 no,'' lable from dual union all
         select 2   ,''       from dual union all
         select 3   ,''       from dual
       ) a,
       ( select v_rank.rank,
                case when obj.subobject_name is null then obj.owner||':'||obj.object_name
                     else obj.owner||':'||obj.object_name||':'||obj.subobject_name
                end name
           from ( select dbid, dataobj#, obj#, rownum rank
                    from ( select dbid, dataobj#, obj#, sum(gc_cr_blocks_received_delta)
                             from dba_hist_seg_stat
                            where snap_id between 1+ :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                            group by dbid, dataobj#, obj#
                            order by sum(gc_cr_blocks_received_delta) desc
                          )
                    where rownum <= 5
                ) v_rank,
                dba_hist_seg_stat_obj obj
          where obj.dataobj# = v_rank.dataobj#
            and obj.obj# = v_rank.obj#
            and obj.dbid = v_rank.dbid
       ) b
 order by no,rank
"""

seg_gccurcvd = """
select sn.snap_id snapid,
       to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time, 'dd hh24:mi:ss'))    dates2,
       max(decode(rank, 1, gc_cu_blocks_received_delta, 0)) rank1_value,
       max(decode(rank, 2, gc_cu_blocks_received_delta, 0)) rank2_value,
       max(decode(rank, 3, gc_cu_blocks_received_delta, 0)) rank3_value,
       max(decode(rank, 4, gc_cu_blocks_received_delta, 0)) rank4_value,
       max(decode(rank, 5, gc_cu_blocks_received_delta, 0)) rank5_value,
       max(decode(rank, 1, ratio, 0))  rank1_ratio,
       max(decode(rank, 2, ratio, 0))  rank2_ratio,
       max(decode(rank, 3, ratio, 0))  rank3_ratio,
       max(decode(rank, 4, ratio, 0))  rank4_ratio,
       max(decode(rank, 5, ratio, 0))  rank5_ratio,
       max(decode(rank, 1, gc_cu_blocks_received_delta/interval, 0)) rank1_valueps,
       max(decode(rank, 2, gc_cu_blocks_received_delta/interval, 0)) rank2_valueps,
       max(decode(rank, 3, gc_cu_blocks_received_delta/interval, 0)) rank3_valueps,
       max(decode(rank, 4, gc_cu_blocks_received_delta/interval, 0)) rank4_valueps,
       max(decode(rank, 5, gc_cu_blocks_received_delta/interval, 0)) rank5_valueps
  from ( select a.snap_id,
                rank,
                a.dataobj#,
                a.obj#,
                gc_cu_blocks_received_delta,
                nvl(ratio_to_report(gc_cu_blocks_received_delta) over (partition by snap_id),0) ratio
           from dba_hist_seg_stat a,
                ( select dataobj#, obj#, rownum rank
                    from ( select dataobj#, obj#, sum(gc_cu_blocks_received_delta)
                             from dba_hist_seg_stat
                            where snap_id between 1+ :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                            group by dataobj#, obj#
                            order by sum(gc_cu_blocks_received_delta) desc
                          )
                  where rownum <= 5
                ) b
          where a.dataobj# = b.dataobj#
            and a.obj# = b.obj#
            and snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) v1,
       ( select snap_id,
                end_interval_time,
                to_number(substr((end_interval_time-begin_interval_time)*86400,2,9)) interval
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) sn
 where v1.snap_id(+) = sn.snap_id
 group by sn.snap_id, to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

seg_gccurcvd2 = """
select lable||name title
  from ( select 1 no,'' lable from dual union all
         select 2   ,''       from dual union all
         select 3   ,''       from dual
       ) a,
       ( select v_rank.rank,
                case when obj.subobject_name is null then obj.owner||':'||obj.object_name
                     else obj.owner||':'||obj.object_name||':'||obj.subobject_name
                end name
           from ( select dbid, dataobj#, obj#, rownum rank
                    from ( select dbid, dataobj#, obj#, sum(gc_cu_blocks_received_delta)
                             from dba_hist_seg_stat
                            where snap_id between 1+ :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                            group by dbid, dataobj#, obj#
                            order by sum(gc_cu_blocks_received_delta) desc
                         )
                   where rownum <= 5
                ) v_rank,
                dba_hist_seg_stat_obj obj
          where obj.dataobj# = v_rank.dataobj#
            and obj.obj# = v_rank.obj#
            and obj.dbid = v_rank.dbid
       ) b
 order by no,rank
"""

seg_gcbuffbusy = """
select sn.snap_id snapid,
       to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time, 'dd hh24:mi:ss'))    dates2,
       max(decode(rank, 1, gc_buffer_busy_delta, 0)) rank1_value,
       max(decode(rank, 2, gc_buffer_busy_delta, 0)) rank2_value,
       max(decode(rank, 3, gc_buffer_busy_delta, 0)) rank3_value,
       max(decode(rank, 4, gc_buffer_busy_delta, 0)) rank4_value,
       max(decode(rank, 5, gc_buffer_busy_delta, 0)) rank5_value,
       max(decode(rank, 1, ratio, 0))  rank1_ratio,
       max(decode(rank, 2, ratio, 0))  rank2_ratio,
       max(decode(rank, 3, ratio, 0))  rank3_ratio,
       max(decode(rank, 4, ratio, 0))  rank4_ratio,
       max(decode(rank, 5, ratio, 0))  rank5_ratio,
       max(decode(rank, 1, gc_buffer_busy_delta/interval, 0)) rank1_valueps,
       max(decode(rank, 2, gc_buffer_busy_delta/interval, 0)) rank2_valueps,
       max(decode(rank, 3, gc_buffer_busy_delta/interval, 0)) rank3_valueps,
       max(decode(rank, 4, gc_buffer_busy_delta/interval, 0)) rank4_valueps,
       max(decode(rank, 5, gc_buffer_busy_delta/interval, 0)) rank5_valueps
  from ( select a.snap_id,
                 rank,
                 a.dataobj#,
                 a.obj#,
                 gc_buffer_busy_delta,
                 nvl(ratio_to_report(gc_buffer_busy_delta) over (partition by snap_id),0) ratio
            from dba_hist_seg_stat a,
                 ( select dataobj#, obj#, rownum rank
                     from ( select dataobj#, obj#, sum(gc_buffer_busy_delta)
                              from dba_hist_seg_stat
                             where snap_id between 1+ :snap_start and :snap_end
                               and instance_number = :instno
                               and dbid = :dbid
                             group by dataobj#, obj#
                             order by sum(gc_buffer_busy_delta) desc
                           )
                   where rownum <= 5
                 ) b
           where a.dataobj# = b.dataobj#
             and a.obj# = b.obj#
             and snap_id between 1+ :snap_start and :snap_end
             and instance_number = :instno
             and dbid = :dbid
       ) v1,
       (
       select snap_id,
              end_interval_time,
              to_number(substr((end_interval_time-begin_interval_time)*86400,2,9)) interval
         from dba_hist_snapshot
        where snap_id between 1+ :snap_start and :snap_end
          and instance_number = :instno
          and dbid = :dbid
       ) sn
 where v1.snap_id(+) = sn.snap_id
 group by sn.snap_id, to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

seg_gcbuffbusy2 = """
select lable||name title
  from ( select 1 no,'' lable from dual union all
         select 2   ,''       from dual union all
         select 3   ,''       from dual
       ) a,
       ( select v_rank.rank,
                case when obj.subobject_name is null then obj.owner||':'||obj.object_name
                     else obj.owner||':'||obj.object_name||':'||obj.subobject_name
                end name
           from ( select dbid, dataobj#, obj#, rownum rank
                    from ( select dbid, dataobj#, obj#, sum(gc_buffer_busy_delta)
                             from dba_hist_seg_stat
                            where snap_id between 1+ :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                            group by dbid, dataobj#, obj#
                            order by sum(gc_buffer_busy_delta) desc
                            )
                    where rownum <= 5
                ) v_rank,
                dba_hist_seg_stat_obj obj
          where obj.dataobj# = v_rank.dataobj#
            and obj.obj# = v_rank.obj#
            and obj.dbid = v_rank.dbid
       ) b
 order by no,rank
"""

blk_waitstat_ms = """
select snap_id "SnapID",
       to_char(end_interval_time, 'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(end_interval_time, 'dd hh24:mi'))       dates2,
       max(decode(class, '1st level bmb'        , delta, 0)) "1st level bmb",
       max(decode(class, '2nd level bmb'        , delta, 0)) "2nd level bmb",
       max(decode(class, '3rd level bmb'        , delta, 0)) "3rd level bmb",
       max(decode(class, 'bitmap block'         , delta, 0)) "bitmap block",
       max(decode(class, 'bitmap index block'   , delta, 0)) "bitmap index block",
       max(decode(class, 'data block'           , delta, 0)) "data block",
       max(decode(class, 'extent map'           , delta, 0)) "extent map",
       max(decode(class, 'file header block'    , delta, 0)) "file header block",
       max(decode(class, 'free list'            , delta, 0)) "free list",
       max(decode(class, 'save undo block'      , delta, 0)) "save undo block",
       max(decode(class, 'save undo header'     , delta, 0)) "save undo header",
       max(decode(class, 'segment header'       , delta, 0)) "segment header",
       max(decode(class, 'sort block'           , delta, 0)) "sort block",
       max(decode(class, 'system undo block'    , delta, 0)) "system undo block",
       max(decode(class, 'system undo header'   , delta, 0)) "system undo header",
       max(decode(class, 'undo block'           , delta, 0)) "undo block",
       max(decode(class, 'undo header'          , delta, 0)) "undo header",
       max(decode(class, '1st level bmb'        , delta_cnt, 0)) "1st level bmb",
       max(decode(class, '2nd level bmb'        , delta_cnt, 0)) "2nd level bmb",
       max(decode(class, '3rd level bmb'        , delta_cnt, 0)) "3rd level bmb",
       max(decode(class, 'bitmap block'         , delta_cnt, 0)) "bitmap block",
       max(decode(class, 'bitmap index block'   , delta_cnt, 0)) "bitmap index block",
       max(decode(class, 'data block'           , delta_cnt, 0)) "data block",
       max(decode(class, 'extent map'           , delta_cnt, 0)) "extent map",
       max(decode(class, 'file header block'    , delta_cnt, 0)) "file header block",
       max(decode(class, 'free list'            , delta_cnt, 0)) "free list",
       max(decode(class, 'save undo block'      , delta_cnt, 0)) "save undo block",
       max(decode(class, 'save undo header'     , delta_cnt, 0)) "save undo header",
       max(decode(class, 'segment header'       , delta_cnt, 0)) "segment header",
       max(decode(class, 'sort block'           , delta_cnt, 0)) "sort block",
       max(decode(class, 'system undo block'    , delta_cnt, 0)) "system undo block",
       max(decode(class, 'system undo header'   , delta_cnt, 0)) "system undo header",
       max(decode(class, 'undo block'           , delta_cnt, 0)) "undo block",
       max(decode(class, 'undo header'          , delta_cnt, 0)) "undo header"
 from (
      select class,snap_id,end_interval_time, delta_cnt,delta_time,decode(delta_cnt,0,0,delta_time/delta_cnt)*10 delta
        from (
             select class,
                    ws.snap_id,
                    end_interval_time,
                    time-lag(time) over(partition by class order by ws.snap_id) delta_time,
                    wait_count-lag(wait_count) over (partition by class order by ws.snap_id) delta_cnt
               from dba_hist_waitstat ws,
                    dba_hist_snapshot sn
              where ws.snap_id = sn.snap_id
                and ws.instance_number = sn.instance_number
                and ws.dbid = sn.dbid
                and ws.snap_id between :snap_start and :snap_end
                and ws.instance_number = :instno
                and ws.dbid = :dbid
                and class in (
                       '1st level bmb',
                       '2nd level bmb',
                       '3rd level bmb',
                       'bitmap block',
                       'bitmap index block',
                       'data block',
                       'extent map',
                       'file header block',
                       'free list',
                       'save undo block',
                       'save undo header',
                       'segment header',
                       'sort block',
                       'system undo block',
                       'system undo header',
                       'undo block',
                       'undo header'
                      )
            )
      where snap_id > :snap_start
      )
group by snap_id, to_char(end_interval_time, 'yyyy-mm-dd hh24:mi:ss')
order by snap_id
"""

tbsstat_total = """
select tsname                                      "Tablespace",
       nvl(ratio_to_report(wait_time) over (),0)   "WaitTime%",
       wait_time/100                               "WaitTime(s)",
       nvl(ratio_to_report(wait_count) over (),0)  "WaitCount%",
       wait_count                                  "WaitCount",
       nvl(ratio_to_report(phyblkrd) over (),0)    "PhyBlock Read%",
       nvl(ratio_to_report(phyblkwrt) over (),0)   "PhyBlock Write%",
       decode(phyrds,0,0,phyblkrd/phyrds)          "FullScan Ratio",
       phyblkrd                                    "PhyBlkRead",
       phyrds                                      "PhyReads",
       phyblkwrt                                   "PhyBlkWrite",
       phywrts                                     "PhyWrites",
       decode(phywrts,0,0,phyblkwrt/phywrts)       "Blocks/write",
       singleblkrds                                "SingleBlkRead",
       10*singleblkrdtim                           "SingleBlkReadTime(ms)",
       10*decode(singleblkrds,0,0,singleblkrdtim/singleblkrds) "avg.singleReadTime(ms)",
       10*decode(phyblkrd,0,0,readtim/phyblkrd)    "avg.Blk Read(ms)",
       10*decode(phyblkwrt,0,0,writetim/phyblkwrt) "avg.Blk Write(ms)",
       phyblkrd*block_size/1048576                 "PhyRd(Mb)",
       phyblkwrt*block_size/1048576                "PhyWrt(Mb)",
       block_size                                  "BlkSiz"
from   ( select tsname,block_size,
                sum(wait_time)      wait_time,
                sum(wait_count)     wait_count,
                sum(phyblkrd)       phyblkrd,
                sum(phyrds)         phyrds,
                sum(phyblkwrt)      phyblkwrt,
                sum(phywrts)        phywrts,
                sum(singleblkrds)   singleblkrds,
                sum(singleblkrdtim) singleblkrdtim,
                sum(readtim)        readtim,
                sum(writetim)       writetim
           from ( select  a.tsname,a.filename,a.block_size,
                          case when ( nvl(b.time,0)           - nvl(a.time,0)           ) < 0 then 0 else  ( nvl(b.time,0)           - nvl(a.time,0)           )  end wait_time,
                          case when ( nvl(b.wait_count,0)     - nvl(a.wait_count,0)     ) < 0 then 0 else  ( nvl(b.wait_count,0)     - nvl(a.wait_count,0)     )  end wait_count,
                          case when ( nvl(b.phyblkrd,0)       - nvl(a.phyblkrd,0)       ) < 0 then 0 else  ( nvl(b.phyblkrd,0)       - nvl(a.phyblkrd,0)       )  end phyblkrd,
                          case when ( nvl(b.phyrds,0)         - nvl(a.phyrds,0)         ) < 0 then 0 else  ( nvl(b.phyrds,0)         - nvl(a.phyrds,0)         )  end phyrds,
                          case when ( nvl(b.phyblkwrt,0)      - nvl(a.phyblkwrt,0)      ) < 0 then 0 else  ( nvl(b.phyblkwrt,0)      - nvl(a.phyblkwrt,0)      )  end phyblkwrt,
                          case when ( nvl(b.phywrts,0)        - nvl(a.phywrts,0)        ) < 0 then 0 else  ( nvl(b.phywrts,0)        - nvl(a.phywrts,0)        )  end phywrts,
                          case when ( nvl(b.singleblkrds,0)   - nvl(a.singleblkrds,0)   ) < 0 then 0 else  ( nvl(b.singleblkrds,0)   - nvl(a.singleblkrds,0)   )  end singleblkrds,
                          case when ( nvl(b.singleblkrdtim,0) - nvl(a.singleblkrdtim,0) ) < 0 then 0 else  ( nvl(b.singleblkrdtim,0) - nvl(a.singleblkrdtim,0) )  end singleblkrdtim,
                          case when ( nvl(b.readtim,0)        - nvl(a.readtim,0)        ) < 0 then 0 else  ( nvl(b.readtim,0)        - nvl(a.readtim,0)        )  end readtim,
                          case when ( nvl(b.writetim,0)       - nvl(a.writetim,0)       ) < 0 then 0 else  ( nvl(b.writetim,0)       - nvl(a.writetim,0)       )  end writetim
                    from dba_hist_filestatxs a, dba_hist_filestatxs b
                   where a.snap_id = :snap_start
                     and b.snap_id = :snap_end
                     and a.instance_number = :instno
                     and b.instance_number = :instno
                     and a.dbid = :dbid
                     and b.dbid = :dbid
                     and a.filename = b.filename
                )
          group by tsname,block_size
       )
 order by wait_time desc,wait_count desc,phyblkrd desc
"""

filestat_total = """
select tsname                                      "Tablespace",
       filename                                    "File",
       nvl(ratio_to_report(wait_time) over (),0)   "WaitTime%",
       wait_time/100                               "WaitTime(s)",
       nvl(ratio_to_report(wait_count) over (),0)  "WaitCount%",
       wait_count                                  "WaitCount",
       nvl(ratio_to_report(phyblkrd) over (),0)    "PhyBlock Read%",
       nvl(ratio_to_report(phyblkwrt) over (),0)   "PhyBlock Write%",
       decode(phyrds,0,0,phyblkrd/phyrds)          "FullScan Ratio",
       phyblkrd                                    "PhyBlkRead",
       phyrds                                      "PhyReads",
       phyblkwrt                                   "PhyBlkWrite",
       phywrts                                     "PhyWrites",
       decode(phywrts,0,0,phyblkwrt/phywrts)       "Blocks/write",
       singleblkrds                                "SingleBlkRead",
       10*singleblkrdtim                           "SingleBlkReadTime(ms)",
       10*decode(singleblkrds,0,0,singleblkrdtim/singleblkrds) "avg.singleReadTime(ms)",
       10*decode(phyblkrd,0,0,readtim/phyblkrd)    "avg.Blk Read(ms)",
       10*decode(phyblkwrt,0,0,writetim/phyblkwrt) "avg.Blk Write(ms)",
       phyblkrd*block_size/1048576                 "PhyRd(Mb)",
       phyblkwrt*block_size/1048576                "PhyWrt(Mb)",
       block_size                                  "BlkSiz"
  from ( select a.tsname,a.filename,a.block_size,
                case when ( nvl(b.time,0)           - nvl(a.time,0)           ) < 0 then 0 else  ( nvl(b.time,0)           - nvl(a.time,0)           )  end wait_time,
                case when ( nvl(b.wait_count,0)     - nvl(a.wait_count,0)     ) < 0 then 0 else  ( nvl(b.wait_count,0)     - nvl(a.wait_count,0)     )  end wait_count,
                case when ( nvl(b.phyblkrd,0)       - nvl(a.phyblkrd,0)       ) < 0 then 0 else  ( nvl(b.phyblkrd,0)       - nvl(a.phyblkrd,0)       )  end phyblkrd,
                case when ( nvl(b.phyrds,0)         - nvl(a.phyrds,0)         ) < 0 then 0 else  ( nvl(b.phyrds,0)         - nvl(a.phyrds,0)         )  end phyrds,
                case when ( nvl(b.phyblkwrt,0)      - nvl(a.phyblkwrt,0)      ) < 0 then 0 else  ( nvl(b.phyblkwrt,0)      - nvl(a.phyblkwrt,0)      )  end phyblkwrt,
                case when ( nvl(b.phywrts,0)        - nvl(a.phywrts,0)        ) < 0 then 0 else  ( nvl(b.phywrts,0)        - nvl(a.phywrts,0)        )  end phywrts,
                case when ( nvl(b.singleblkrds,0)   - nvl(a.singleblkrds,0)   ) < 0 then 0 else  ( nvl(b.singleblkrds,0)   - nvl(a.singleblkrds,0)   )  end singleblkrds,
                case when ( nvl(b.singleblkrdtim,0) - nvl(a.singleblkrdtim,0) ) < 0 then 0 else  ( nvl(b.singleblkrdtim,0) - nvl(a.singleblkrdtim,0) )  end singleblkrdtim,
                case when ( nvl(b.readtim,0)        - nvl(a.readtim,0)        ) < 0 then 0 else  ( nvl(b.readtim,0)        - nvl(a.readtim,0)        )  end readtim,
                case when ( nvl(b.writetim,0)       - nvl(a.writetim,0)       ) < 0 then 0 else  ( nvl(b.writetim,0)       - nvl(a.writetim,0)       )  end writetim
           from dba_hist_filestatxs a, dba_hist_filestatxs b
          where a.snap_id = :snap_start
            and b.snap_id = :snap_end
            and a.instance_number = :instno
            and b.instance_number = :instno
            and a.dbid = :dbid
            and b.dbid = :dbid
            and a.filename = b.filename
       )
 order by wait_time desc,wait_count desc,phyblkrd desc
"""

tbs_blkrw = """
select sn.snap_id snapid,
       to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time, 'dd hh24:mi'))       dates2,
       max(decode(rank, 1, v2.delta/interval, 0)) rank_1,
       max(decode(rank, 2, v2.delta/interval, 0)) rank_2,
       max(decode(rank, 3, v2.delta/interval, 0)) rank_3,
       max(decode(rank, 4, v2.delta/interval, 0)) rank_4,
       max(decode(rank, 5, v2.delta/interval, 0)) rank_5
  from (
         select tsname, rownum rank
           from (
                  select tsname, sum(delta)
                    from (
                           select snap_id,
                                  tsname,
                                  s-lag(s) over(partition by tsname order by snap_id) delta
                             from (
                                    select snap_id,
                                           tsname,
                                           sum(phyblkrd+phyblkwrt) s
                                      from dba_hist_filestatxs
                                     where snap_id between :snap_start and :snap_end
                                       and instance_number = :instno
                                       and dbid = :dbid
                                     group by snap_id, tsname
                                  )
                         )
                   group by tsname
                   order by sum(delta) desc
                )
          where rownum <=5
       ) v1,
       (
         select snap_id,
                tsname,
                s-lag(s) over(partition by tsname order by snap_id) delta
           from (
                  select snap_id,
                         tsname,
                         sum(phyblkrd+phyblkwrt) s
                    from dba_hist_filestatxs
                   where snap_id between :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                   group by snap_id, tsname
                )
       ) v2,
       (
         select snap_id,
                end_interval_time,
                to_number(substr((end_interval_time-begin_interval_time)*86400,2,9)) interval
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) sn
 where v1.tsname = v2.tsname
   and v2.snap_id = sn.snap_id
 group by sn.snap_id, to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

tbs_blkrw2 = """
select tsname
  from (
         select tsname, sum(delta)
           from (
                  select snap_id,
                         tsname,
                         s-lag(s) over(partition by tsname order by snap_id) delta
                    from (
                           select snap_id,
                                  tsname,
                                  sum(phyblkrd+phyblkwrt) s
                             from dba_hist_filestatxs
                            where snap_id between :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                            group by snap_id, tsname
                         )
                )
          group by tsname
          order by sum(delta) desc
       )
 where rownum <=5
"""

files_blkrw = """
select sn.snap_id snapid,
       to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time, 'dd hh24:mi'))       dates2,
       max(decode(rank, 1, v2.delta/interval, 0)) rank_1,
       max(decode(rank, 2, v2.delta/interval, 0)) rank_2,
       max(decode(rank, 3, v2.delta/interval, 0)) rank_3,
       max(decode(rank, 4, v2.delta/interval, 0)) rank_4,
       max(decode(rank, 5, v2.delta/interval, 0)) rank_5
  from (
         select filename, rownum rank
           from (
                  select filename, sum(nvl(delta,0))
                    from (
                           select snap_id,
                                  filename,
                                  (phyblkrd+phyblkwrt)-lag(phyblkrd+phyblkwrt) over (partition by filename order by snap_id) delta
                             from dba_hist_filestatxs
                            where snap_id between :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                         )
                   group by filename
                   order by sum(delta) desc
                 )
           where rownum <=5
       ) v1,
       (
         select snap_id,
                filename,
                (phyblkrd+phyblkwrt)-lag(phyblkrd+phyblkwrt) over (partition by filename order by snap_id) delta
           from dba_hist_filestatxs
          where snap_id between :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) v2,
       (
         select snap_id,
                end_interval_time,
                to_number(substr((end_interval_time-begin_interval_time)*86400,2,9)) interval
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) sn
 where v1.filename = v2.filename
   and v2.snap_id  = sn.snap_id
 group by sn.snap_id, to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

files_blkrw2 = """
select filename
  from ( select filename, sum(nvl(delta,0))
           from (
                  select snap_id,
                         filename,
                         (phyblkrd+phyblkwrt)-lag(phyblkrd+phyblkwrt) over (partition by filename order by snap_id) delta
                    from dba_hist_filestatxs
                   where snap_id between :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                )
          group by filename
          order by sum(delta) desc
       )
 where rownum <=5
"""

files_avg_blks_read = """
select sn.snap_id snapid,
       to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time, 'dd hh24:mi'))       dates2,
       max(decode(rank, 1, v2.delta, 0)) rank_1,
       max(decode(rank, 2, v2.delta, 0)) rank_2,
       max(decode(rank, 3, v2.delta, 0)) rank_3,
       max(decode(rank, 4, v2.delta, 0)) rank_4,
       max(decode(rank, 5, v2.delta, 0)) rank_5
  from (
         select filename, rownum rank
           from (
                  select filename, sum(delta)
                    from (
                           select snap_id,filename,phyblkrd,phyrds,decode(phyrds,0,0,phyblkrd/phyrds) delta
                             from (
                                    select snap_id,
                                           filename,
                                           phyrds-lag(phyrds) over (partition by filename order by snap_id) phyrds,
                                           phyblkrd-lag(phyblkrd) over (partition by filename order by snap_id) phyblkrd
                                      from dba_hist_filestatxs
                                     where snap_id between :snap_start and :snap_end
                                       and instance_number = :instno
                                       and dbid = :dbid
                                  )
                         )
                   group by filename
                   order by sum(delta) desc
                 )
           where rownum <=5
       ) v1,
       (
         select snap_id,filename,phyblkrd,phyrds,decode(phyrds,0,0,phyblkrd/phyrds) delta
           from (
                  select snap_id,
                         filename,
                         phyrds-lag(phyrds) over (partition by filename order by snap_id) phyrds,
                         phyblkrd-lag(phyblkrd) over (partition by filename order by snap_id) phyblkrd
                    from dba_hist_filestatxs
                   where snap_id between :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                )
       ) v2,
       (
         select snap_id,
                end_interval_time,
                to_number(substr((end_interval_time-begin_interval_time)*86400,2,9)) interval
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) sn
 where v1.filename = v2.filename
   and v2.snap_id  = sn.snap_id
 group by sn.snap_id, to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

files_avg_blks_read2 = """
select filename
  from (
         select filename, sum(delta)
           from (
                  select snap_id,filename,phyblkrd,phyrds,decode(phyrds,0,0,phyblkrd/phyrds) delta
                    from (
                           select snap_id,
                                  filename,
                                  phyrds-lag(phyrds) over (partition by filename order by snap_id) phyrds,
                                  phyblkrd-lag(phyblkrd) over (partition by filename order by snap_id) phyblkrd
                             from dba_hist_filestatxs
                            where snap_id between :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                         )
                )
          group by filename
          order by sum(delta) desc
       )
 where rownum <=5
"""

files_avg_time_read = """
select sn.snap_id snapid,
       to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time, 'dd hh24:mi'))       dates2,
       max(decode(rank, 1, v2.delta, 0)) rank_1,
       max(decode(rank, 2, v2.delta, 0)) rank_2,
       max(decode(rank, 3, v2.delta, 0)) rank_3,
       max(decode(rank, 4, v2.delta, 0)) rank_4,
       max(decode(rank, 5, v2.delta, 0)) rank_5
  from (
         select filename, rownum rank
           from (
                  select filename, sum(delta)
                    from (
                           select snap_id,filename,readtim,phyblkrd,decode(phyblkrd,0,0,readtim/phyblkrd) delta
                             from (
                                    select snap_id,
                                           filename,
                                           phyblkrd-lag(phyblkrd) over (partition by filename order by snap_id) phyblkrd,
                                           readtim-lag(readtim) over (partition by filename order by snap_id) readtim
                                      from dba_hist_filestatxs
                                     where snap_id between :snap_start and :snap_end
                                       and instance_number = :instno
                                       and dbid = :dbid
                                  )
                         )
                   group by filename
                   order by sum(delta) desc
                 )
           where rownum <=5
       ) v1,
       (
         select snap_id,filename,readtim,phyblkrd,decode(phyblkrd,0,0,readtim/phyblkrd)*10 delta
           from (
                  select snap_id,
                         filename,
                         phyblkrd-lag(phyblkrd) over (partition by filename order by snap_id) phyblkrd,
                         readtim-lag(readtim) over (partition by filename order by snap_id) readtim
                    from dba_hist_filestatxs
                   where snap_id between :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                )
       ) v2,
       (
         select snap_id,
                end_interval_time,
                to_number(substr((end_interval_time-begin_interval_time)*86400,2,9)) interval
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) sn
 where v1.filename = v2.filename
   and v2.snap_id  = sn.snap_id
 group by sn.snap_id, to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

files_avg_time_read2 = """
select filename
  from (
         select filename, sum(delta)
           from (
                  select snap_id,filename,readtim,phyblkrd,decode(phyblkrd,0,0,readtim/phyblkrd) delta
                    from (
                           select snap_id,
                                  filename,
                                  phyblkrd-lag(phyblkrd) over (partition by filename order by snap_id) phyblkrd,
                                  readtim-lag(readtim) over (partition by filename order by snap_id) readtim
                             from dba_hist_filestatxs
                            where snap_id between :snap_start and :snap_end
                              and instance_number = :instno
                              and dbid = :dbid
                         )
                )
          group by filename
          order by sum(delta) desc
       )
 where rownum <=5
"""

files_wait_time = """
select sn.snap_id snapid,
       to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time, 'dd hh24:mi'))       dates2,
       max(decode(rank, 1, v2.delta, 0)) rank_1,
       max(decode(rank, 2, v2.delta, 0)) rank_2,
       max(decode(rank, 3, v2.delta, 0)) rank_3,
       max(decode(rank, 4, v2.delta, 0)) rank_4,
       max(decode(rank, 5, v2.delta, 0)) rank_5
  from (
         select filename, rownum rank
           from (
                  select filename, sum(NVL(delta,0))
                    from (
                           select snap_id, filename,wait_count,wait_time,decode(wait_count,0,0,wait_time/wait_count) delta
                             from ( select snap_id,
                                           filename,
                                           wait_count-lag(wait_count) over (partition by filename order by snap_id) wait_count,
                                           time-lag(time) over (partition by filename order by snap_id) wait_time
                                      from dba_hist_filestatxs
                                     where snap_id between :snap_start and :snap_end
                                       and instance_number = :instno
                                       and dbid = :dbid
                                  )
                         )
                   group by filename
                   order by sum(NVL(delta,0)) desc
                )
          where rownum <=5
       ) v1,
       (
         select snap_id,filename,wait_count,wait_time,decode(wait_count,0,0,wait_time/wait_count)*10 delta
           from (
                  select snap_id,
                         filename,
                         wait_count-lag(wait_count) over (partition by filename order by snap_id) wait_count,
                         time-lag(time) over (partition by filename order by snap_id) wait_time
                    from dba_hist_filestatxs
                   where snap_id between :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                )
       ) v2,
       (
         select snap_id,
                end_interval_time,
                to_number(substr((end_interval_time-begin_interval_time)*86400,2,9)) interval
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) sn
 where v1.filename = v2.filename
   and v2.snap_id = sn.snap_id
 group by sn.snap_id, to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

files_wait_time2 = """
select filename
  from (
         select filename, sum(NVL(delta,0))
           from (
                  select snap_id, filename,wait_count,wait_time,decode(wait_count,0,0,wait_time/wait_count) delta
                    from (select snap_id,
                                 filename,
                                 wait_count-lag(wait_count) over (partition by filename order by snap_id) wait_count,
                                 time-lag(time) over (partition by filename order by snap_id) wait_time
                            from dba_hist_filestatxs
                           where snap_id between :snap_start and :snap_end
                             and instance_number = :instno
                             and dbid = :dbid
                         )
                )
          group by filename
          order by sum(NVL(delta,0)) desc
       )
 where rownum <=5
"""

v_pga = """
select name, 
       round(decode(unit, 'bytes', value/1024/1024, value),2) value2,
       decode(unit, 'bytes', 'MB', unit)                      unit
  from v$pgastat
"""

pga = """
select sn.snap_id snapid,
       to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time, 'dd hh24:mi'))       dates2,
       max(decode(pga.name, 'aggregate PGA target parameter', value/1024/1024, 0))            "pga aggr target",
       max(decode(pga.name, 'aggregate PGA auto target', value/1024/1024, 0))                 "pga auto target",
       max(decode(pga.name, 'total PGA allocated', value/1024/1024, 0))                       "total pga allocated",
       max(decode(pga.name, 'maximum PGA allocated', value/1024/1024, 0))                     "max pga allocated",
       max(decode(pga.name, 'total PGA used for auto workareas', value/1024/1024, 0))         "auto workarea",
       max(decode(pga.name, 'total PGA used for manual workareas', value/1024/1024, 0))       "manual workarea",
       decode(max(decode(pga.name, 'total PGA allocated', value, 0)),0,0,
             (max(decode(pga.name, 'total PGA used for auto workareas', value, 0))
             +max(decode(pga.name, 'total PGA used for manual workareas', value, 0))
             )/max(decode(pga.name, 'total PGA allocated', value, 0)))                        "%PGA W/A Mem",
       decode(max(decode(pga.name, 'total PGA used for auto workareas',value, 0))
             +max(decode(pga.name, 'total PGA used for manual workareas',value, 0)), 0, 0,
              max(decode(pga.name, 'total PGA used for auto workareas',value, 0))
              /(max(decode(pga.name, 'total PGA used for auto workareas',value, 0))
               +max(decode(pga.name, 'total PGA used for manual workareas',value, 0))))       "%Auto W/A Mem",
       decode(max(decode(pga.name, 'total PGA used for auto workareas',value, 0))
             +max(decode(pga.name, 'total PGA used for manual workareas',value, 0)), 0, 0,
              max(decode(pga.name, 'total PGA used for manual workareas',value, 0))
              /(max(decode(pga.name, 'total PGA used for auto workareas',value, 0))
               +max(decode(pga.name, 'total PGA used for manual workareas',value, 0))))       "%Manual W/A Mem",
       max(decode(pga.name, 'global memory bound',value/1024, 0))                             "Global Mem Bound(K)",
       max(case when pga.name='maximum PGA used for auto workareas' then value else 0 end)/1048576   "max. auto W/A(M)",
       max(case when pga.name='maximum PGA used for manual workareas' then value else 0 end)/1048576 "max. manual W/A(M)"
 from (
       select dbid,instance_number,snap_id, end_interval_time
         from dba_hist_snapshot
        where snap_id between 1+ :snap_start and :snap_end
          and instance_number = :instno
          and dbid = :dbid
       ) sn,
      dba_hist_pgastat pga
where pga.snap_id = sn.snap_id
  and pga.instance_number = sn.instance_number
  and pga.dbid = sn.dbid
 group by sn.snap_id, to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

pga_cachehit = """
select sn.snap_id snapid,
       to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(sn.end_interval_time, 'dd hh24:mi'))       dates2,
       sum(decode(name, 'bytes processed', delta, 0))
            / ( sum(decode(name, 'bytes processed', delta, 0))
               +sum(decode(name, 'extra bytes read/written', delta, 0))
              ) "cache hit(%)",
        sum(decode(name, 'bytes processed', delta/1024/1024, 0)) "byte processed(MB)",
        sum(decode(name, 'extra bytes read/written', delta/1024/1024, 0)) "extra byte read/write(MB)"
  from (
         select snap_id,
                name,
                value-lag(value) over (partition by name order by snap_id) delta
           from dba_hist_pgastat
          where name in ('bytes processed','extra bytes read/written')
            and snap_id between :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) v1,
       (
       select snap_id, 
              end_interval_time
         from dba_hist_snapshot
        where snap_id between 1+ :snap_start and :snap_end
          and instance_number = :instno
          and dbid = :dbid
       ) sn
 where v1.snap_id  = sn.snap_id
group by sn.snap_id, to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss')
order by sn.snap_id
"""

workarea = """
select sn.snap_id "SnapID"
      ,snap_time  "Timestamp"
      ,case when low_optimal_size >= 1024*1024*1024*1024
            then lpad(round(low_optimal_size/1024/1024/1024/1024) || 'T',7)
            when low_optimal_size >= 1024*1024*1024
            then lpad(round(low_optimal_size/1024/1024/1024) || 'G' ,7)
            when low_optimal_size >= 1024*1024
            then lpad(round(low_optimal_size/1024/1024) || 'M',7)
            when low_optimal_size >= 1024
            then lpad(round(low_optimal_size/1024) || 'K',7)
            else lpad(low_optimal_size || 'B',7)
       end                    "Low Optimal"
      ,case when high_optimal_size >= 1024*1024*1024*1024
            then lpad(round(high_optimal_size/1024/1024/1024/1024) || 'T',7)
            when high_optimal_size >= 1024*1024*1024
            then lpad(round(high_optimal_size/1024/1024/1024) || 'G',7)
            when high_optimal_size >= 1024*1024
            then lpad(round(high_optimal_size/1024/1024) || 'M',7)
            when high_optimal_size >= 1024
            then lpad(round(high_optimal_size/1024) || 'K',7)
            else high_optimal_size || 'B'
       end                           "High Optimal"
      ,nvl(total_executions,0)       "tot_exe"
      ,nvl(optimal_executions,0)     "opt_exe"
      ,nvl(onepass_executions,0)     "one_exe"
      ,nvl(multipasses_executions,0) "mul_exe"
  from (select  snap_id,low_optimal_size,high_optimal_size,
            total_executions   - lag(total_executions) over (partition by low_optimal_size,high_optimal_size order by snap_id) total_executions,
            optimal_executions - lag(optimal_executions) over (partition by low_optimal_size,high_optimal_size order by snap_id) optimal_executions,
            onepass_executions - lag(onepass_executions) over (partition by low_optimal_size,high_optimal_size order by snap_id) onepass_executions,
            multipasses_executions - lag(multipasses_executions) over (partition by low_optimal_size,high_optimal_size order by snap_id) multipasses_executions
         from DBA_HIST_SQL_WORKAREA_HSTGRM
        where snap_id between :snap_start and :snap_end
          and instance_number = :instno
          and dbid = :dbid
       ) v1,
       (
       select snap_id,
              to_char(end_interval_time, 'yyyy-mm-dd hh24:mi:ss') snap_time,
              to_number(substr((end_interval_time-begin_interval_time)*86400,2,9)) interval
         from dba_hist_snapshot
        where snap_id between 1+ :snap_start and :snap_end
          and instance_number = :instno
          and dbid = :dbid
       ) sn
 where v1.snap_id = sn.snap_id
 order by sn.snap_id,low_optimal_size
"""

optimal = """
select e.snap_id "SnapID"
      , to_char(s.end_interval_time,'yyyy-mm-dd hh24:mi:ss') "Timestamp"
      ,case when e.low_optimal_size >= 1024*1024*1024*1024
            then lpad(round(e.low_optimal_size/1024/1024/1024/1024) || 'T',7)
            when e.low_optimal_size >= 1024*1024*1024
            then lpad(round(e.low_optimal_size/1024/1024/1024) || 'G' ,7)
            when e.low_optimal_size >= 1024*1024
            then lpad(round(e.low_optimal_size/1024/1024) || 'M',7)
            when e.low_optimal_size >= 1024
            then lpad(round(e.low_optimal_size/1024) || 'K',7)
            else lpad(e.low_optimal_size || 'B',7)
       end                                             "Low Optimal"
     , case when e.high_optimal_size >= 1024*1024*1024*1024
            then lpad(round(e.high_optimal_size/1024/1024/1024/1024) || 'T',7)
            when e.high_optimal_size >= 1024*1024*1024
            then lpad(round(e.high_optimal_size/1024/1024/1024) || 'G',7)
            when e.high_optimal_size >= 1024*1024
            then lpad(round(e.high_optimal_size/1024/1024) || 'M',7)
            when e.high_optimal_size >= 1024
            then lpad(round(e.high_optimal_size/1024) || 'K',7)
            else e.high_optimal_size || 'B'
       end                                              "High Optimal"
     , e.total_executions       - nvl(b.total_executions,0)        "tot_exe"
     , e.optimal_executions     - nvl(b.optimal_executions,0)      "opt_exe"
     , e.onepass_executions     - nvl(b.onepass_executions,0)      "one_exe"
     , e.multipasses_executions - nvl(b.multipasses_executions,0)  "mul_exe"
  from DBA_HIST_SQL_WORKAREA_HSTGRM e
     , DBA_HIST_SQL_WORKAREA_HSTGRM b
     , DBA_HIST_SNAPSHOT S
 where e.snap_id              = :snap_end
   and e.dbid                 = :dbid
   and e.instance_number      = :instno
   and b.snap_id(+)           = :snap_start
   and b.dbid(+)              = e.dbid
   and b.instance_number(+)   = e.instance_number
   and b.low_optimal_size(+)  = e.low_optimal_size
   and b.high_optimal_size(+) = e.high_optimal_size
   and e.snap_id = s.snap_id
   and s.INSTANCE_NUMBER = :instno
   and s.dbid = :dbid
   and s.snap_id between  :snap_start and :snap_end
 order by e.low_optimal_size
"""

resource_limit = """
select sn.snap_id snapid,
       to_char(end_interval_time, 'yyyy-mm-dd hh24:mi:ss') dates,
       max(to_char(end_interval_time, 'dd hh24:mi'))       dates2,
       max(decode(r.resource_name, 'processes', current_utilization, 0))                       "current processes",
       max(decode(r.resource_name, 'processes', max_utilization, 0))                           "max processes",
       max(decode(r.resource_name, 'processes', to_number(trim(limit_value)), 0))              "processes limit",
       max(decode(r.resource_name, 'sessions',  current_utilization, 0))                       "current sessions",
       max(decode(r.resource_name, 'sessions',  max_utilization, 0))                           "max sessions",
       max(decode(r.resource_name, 'sessions',  to_number(trim(limit_value)), 0))              "sessions limit",
       max(nvl(active_sess.cnt,0))                                                             "active session count",
       max(decode(r.resource_name, 'enqueue_locks',  current_utilization, 0))                  "current enqueue",       
       max(decode(r.resource_name, 'enqueue_locks',  max_utilization, 0))                      "max enqueue",
       max(decode(r.resource_name, 'enqueue_locks',  to_number(trim(limit_value)), 0))         "enqueue limit",   
       max(decode(r.resource_name, 'max_rollback_segments',  current_utilization, 0))          "current max rollback segments",
       max(decode(r.resource_name, 'max_rollback_segments',  max_utilization, 0))              "max max rollback segments",
       max(decode(r.resource_name, 'max_rollback_segments',  to_number(trim(limit_value)), 0)) "max rollback segments limit",
       max(decode(r.resource_name, 'parallel_max_servers',  current_utilization, 0))           "current parallel max servers",       
       max(decode(r.resource_name, 'parallel_max_servers',  max_utilization, 0))               "max parallel max servers",
       max(decode(r.resource_name, 'parallel_max_servers',  to_number(trim(limit_value)), 0))  "parallel max servers limit",
       max(decode(r.resource_name, 'gcs_resources',  current_utilization, 0))                  "current gcs resources",
       max(decode(r.resource_name, 'gcs_resources',  max_utilization, 0))                      "max gcs_resources",
       max(decode(r.resource_name, 'gcs_resources',  trim(limit_value)))                       "gcs resources limit",
       max(decode(r.resource_name, 'gcs_shadows',  current_utilization, 0))                    "current gcs shadows",
       max(decode(r.resource_name, 'ges_procs',  to_number(trim(limit_value)), 0))             "ges procs limit"
  from dba_hist_snapshot sn,
       dba_hist_resource_limit  r,
       ( select snap_id, max(session_count) cnt
           from (
                  select snap_id, sample_id,
                         count(distinct session_id) session_count
                    from dba_hist_active_sess_history ash
                   where snap_id between 1+ :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                     and event is not null
                   group by snap_id,sample_id
                )
          group by snap_id
          order by snap_id) active_sess
 where sn.snap_id = r.snap_id
   and sn.instance_number = r.instance_number
   and sn.dbid = r.dbid
   and sn.snap_id = active_sess.snap_id(+)
   and r.resource_name in ('processes',
                           'sessions',
                           'enqueue_locks',
                           'max_rollback_segments',
                           'parallel_max_servers',
                           'gcs_resources',
                           'gcs_shadows',
                           'ges_procs' )
   and r.snap_id between 1+ :snap_start and :snap_end
   and r.instance_number = :instno
   and r.dbid = :dbid
 group by sn.snap_id, to_char(sn.end_interval_time, 'yyyy-mm-dd hh24:mi:ss')
 order by sn.snap_id
"""

mttr_sql = """
select r.snap_id snapid,
       to_char(sn.end_interval_time,'yyyy-mm-dd hh24:mi:ss') dates,
       to_char(sn.end_interval_time,'dd hh24:mi')            dates2,
       estimated_mttr                 "estimated MTTR",
       recovery_estimated_ios         "recovery estimated IO",
       actual_redo_blks               "actual redo blocks",
       target_redo_blks               "target redo blocks",
       log_file_size_redo_blks        "log file size redo blocks",
       log_chkpt_timeout_redo_blks    "chkpt timeout redo blocks",
       log_chkpt_interval_redo_blks   "chkpt interval redo blocks"
  from dba_hist_instance_recovery r,
       dba_hist_snapshot sn
 where r.instance_number = :instno
   and r.snap_id between  :snap_start and :snap_end
   and r.dbid = :dbid
   and r.snap_id = sn.snap_id
   and sn.instance_number = :instno
   and sn.dbid = :dbid
   and sn.snap_id between  :snap_start and :snap_end
   order by r.snap_id
"""

sga = """
select b.snap_id     snapid,
       to_char(b.end_interval_time, 'yyyy-mm-dd hh24:mi:ss')   dates,
       max(to_char(b.end_interval_time, 'dd hh24:mi'))         dates2,
       sum(case when pool='null' and name='buffer_cache' then bytes/1048576 else 0 end) "buffer cache",
       sum(case when pool='null' and name='log_buffer'   then bytes/1048576 else 0 end) "log buffer",
       sum(case when pool='shared pool'                  then bytes/1048576 else 0 end) "shared pool",
       sum(case when pool='java pool'                    then bytes/1048576 else 0 end) "java pool",
       sum(case when pool='large pool'                   then bytes/1048576 else 0 end) "large pool",
       sum(case when pool='streams pool'                 then bytes/1048576 else 0 end) "streams pool",
       sum(case when pool='shared pool'  and name='SQLA' then bytes/1048576 else 0 end) "sqlarea",
       sum(case when pool='shared pool'  and name in ('library cache','KGLH0','KGLHD','KGLNA','KGLSG','KGLDA','KGLA','KGLS') then bytes/1048576 else 0 end) "library cache",
       sum(case when pool='shared pool'  and name not in ('SQLA','library cache','KGLH0','KGLHD','KGLNA','KGLSG','KGLDA','KGLA','KGLS','free memory') then bytes/1048576 else 0 end) "others",
       sum(case when pool='shared pool'  and name='free memory' then bytes/1048576 else 0 end)  "free",
       sum(case when pool='large pool'   and name<>'free memory' then bytes/1048576 else 0 end) "largepool-used",
       sum(case when pool='large pool'   and name='free memory' then bytes/1048576 else 0 end)  "largepool-free",
       sum(case when pool='java pool'    and name<>'free memory' then bytes/1048576 else 0 end) "javapool-used",
       sum(case when pool='java pool'    and name='free memory' then bytes/1048576 else 0 end)  "javapool-free",
       sum(case when pool='streams pool' and name<>'free memory' then bytes/1048576 else 0 end) "streams-used",
       sum(case when pool='streams pool' and name='free memory' then bytes/1048576 else 0 end)  "streams-free"
  from ( select snap_id,
                nvl(pool,'null') pool,
                name,
                bytes
           from dba_hist_sgastat
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) a,
       ( select snap_id,
                end_interval_time
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) b
 where a.snap_id = b.snap_id
 group by b.snap_id, to_char(b.end_interval_time, 'yyyy-mm-dd hh24:mi:ss')
 order by snapid
"""

sga_raw = """
select b.snap_id     snapid,
       b.snap_time   dates,
       pool          "Pool",
       name          "Name",
       bytes/1048576 "Size(M)",
       (bytes-pre_bytes)/1048576 "variation Size(M)"
  from ( select snap_id,
                nvl(pool,'null') pool,
                name,
                bytes,
                nvl(lag(bytes,1) over (partition by pool,name order by pool,name,snap_id),0) pre_bytes
           from dba_hist_sgastat
          where snap_id between :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
            and (nvl(pool,'null')<>'shared pool' or (pool='shared pool' and name <'h'))
       ) a,
       ( select snap_id,
                to_char(end_interval_time, 'yyyy-mm-dd hh24:mi:ss') snap_time
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) b
 where a.snap_id=b.snap_id
 order by pool,name,snap_time
"""

bufpool = """
select sn.snap_id snapid,
       to_char(end_interval_time, 'yyyy-mm-dd hh24:mi:ss') dates,
       to_char(end_interval_time, 'dd hh24:mi')            dates2,
       block_size/1024||'k'||substr(name,1,1)              pools,
       set_msize/1000                                      "Number of Buffers(K)",
       decode(db_block_gets+consistent_gets,0,0,1-(physical_reads/(db_block_gets+consistent_gets))) "buffer hit(%)",
       db_block_gets+consistent_gets                       "buffer gets",
       physical_reads                                      "physical reads",
       physical_writes                                     "physical writes",
       free_buffer_wait                                    "free buffer wait",
       write_complete_wait                                 "write complete wait",
       buffer_busy_wait                                    "buffer busy wait"
  from ( select snap_id, block_size, name, set_msize,
                db_block_gets    -lag(db_block_gets)    over (partition by block_size order  by snap_id) db_block_gets,
                consistent_gets  -lag(consistent_gets)  over (partition by block_size order  by snap_id) consistent_gets,
                physical_reads   -lag(physical_reads)   over (partition by block_size order  by snap_id) physical_reads,
                physical_writes  -lag(physical_writes)  over (partition by block_size order  by snap_id) physical_writes,
                free_buffer_wait -lag(free_buffer_wait) over (partition by block_size order  by snap_id) free_buffer_wait,
                write_complete_wait -lag(write_complete_wait) over (partition by block_size order  by snap_id) write_complete_wait,
                buffer_busy_wait -lag(buffer_busy_wait) over (partition by block_size order  by snap_id) buffer_busy_wait
           from DBA_HIST_BUFFER_POOL_STAT
          where snap_id between :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
            and name = 'DEFAULT'
       ) v1,
       ( select snap_id,
                end_interval_time,
                to_number(substr((end_interval_time-begin_interval_time)*86400,2,9)) interval
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) sn
 where v1.snap_id = sn.snap_id
 order by sn.snap_id
"""

rowcache = """
select b.snap_id     snapid,
       to_char(b.end_interval_time, 'yyyy-mm-dd hh24:mi:ss') dates,
       to_char(b.end_interval_time, 'dd hh24:mi')            dates2,
       parameter                                             "Dictionary",
       decode(gets,0,0,getmisses/gets)                       "get miss/gets(%)",
       decode(scans,0,0,scanmisses/scans)                    "scan miss(%)",
       gets                                                  gets,
       getmisses                                             "get misses",
       scans                                                 scans,
       scanmisses                                            "scan misses",
       modifications modifications,
       flushes       flushes,
       usage         usage,
       dlm_requests  "GES Request",
       dlm_conflicts "GES Conflict",
       dlm_releases  "GES Release"
  from (
         select snap_id,parameter,
                usage,
                gets-lag(gets) over (partition by parameter order by parameter,snap_id) gets,
                getmisses-lag(getmisses) over (partition by parameter order by parameter,snap_id) getmisses,
                scans-lag(scans) over (partition by parameter order by parameter,snap_id) scans,
                scanmisses-lag(scanmisses) over (partition by parameter order by parameter,snap_id) scanmisses,
                modifications-lag(modifications) over (partition by parameter order by parameter,snap_id) modifications,
                flushes-lag(flushes) over (partition by parameter order by parameter,snap_id) flushes,
                dlm_requests-lag(dlm_requests) over (partition by parameter order by parameter,snap_id) dlm_requests,
                dlm_conflicts-lag(dlm_conflicts) over (partition by parameter order by parameter,snap_id) dlm_conflicts,
                dlm_releases-lag(dlm_releases) over (partition by parameter order by parameter,snap_id) dlm_releases
           from dba_hist_rowcache_summary
          where snap_id between :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) a,
       ( select snap_id,
                end_interval_time
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) b
 where a.snap_id=b.snap_id
 order by 1, 4 desc, 5 desc
"""

librarycache = """
select b.snap_id                                                snapid,
       to_char(b.end_interval_time, 'yyyy-mm-dd hh24:mi:ss')    dates,
       max(to_char(b.end_interval_time, 'dd hh24:mi:ss'))       dates2,
       max(decode(namespace,'SQL AREA',reloads,0))              "reload: sql",
       max(decode(namespace,'TABLE/PROCEDURE',reloads,0))       "reload: tab/proc",
       max(decode(namespace,'TRIGGER',reloads,0))               "reload: trg",
       max(decode(namespace,'BODY',reloads,0))                  "reload: body",
       max(decode(namespace,'SQL AREA',invalidations,0))        "invalid: sql",
       max(decode(namespace,'TABLE/PROCEDURE',invalidations,0)) "invalid: tab/proc",
       max(decode(namespace,'TRIGGER',invalidations,0))         "invalid: trg",
       max(decode(namespace,'BODY',invalidations,0))            "invalid: body"
  from ( select snap_id,
                namespace,
                gets-lag(gets,1) over (partition by namespace order by namespace,snap_id) gets,
                gethits-lag(gethits,1) over (partition by namespace order by namespace,snap_id) gethits,
                pins-lag(pins,1) over (partition by namespace order by namespace,snap_id) pins,
                pinhits-lag(pinhits,1) over (partition by namespace order by namespace,snap_id) pinhits,
                reloads-lag(reloads,1) over (partition by namespace order by namespace,snap_id) reloads,
                invalidations-lag(invalidations,1) over (partition by namespace order by namespace,snap_id) invalidations
           from dba_hist_librarycache
          where snap_id between :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
            and namespace in ('SQL AREA','TABLE/PROCEDURE','TRIGGER','BODY')
       ) a,
       ( select snap_id,
                end_interval_time
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) b
 where a.snap_id = b.snap_id 
 group by b.snap_id, to_char(b.end_interval_time, 'yyyy-mm-dd hh24:mi:ss')
 order by b.snap_id
"""

libcache_raw = """
select b.snap_id     "SnapID",
       b.snap_time   "Timestamp",
       namespace     "Library",
       1-decode(gets,0,1,gethits/gets) "getMiss%",
       1-decode(pins,0,1,pinhits/pins) "pinMiss%",
       gets        "Get",
       gethits     "getHit",
       pins        "Pin",
       pinhits     "pinHit",
       reloads     "Reload",
       invalidations   "Invalid",
       dlm_lock_requests   "GES lock req.",
       dlm_pin_requests    "GES pin req.",
       dlm_pin_releases    "GES pin release",
       dlm_invalidation_requests "GES invalid req.",
       dlm_invalidations   "GES invalid"
  from ( select snap_id,namespace,
                gets-lag(gets,1) over (partition by namespace order by namespace,snap_id) gets,
                gethits-lag(gethits,1) over (partition by namespace order by namespace,snap_id) gethits,
                pins-lag(pins,1) over (partition by namespace order by namespace,snap_id) pins,
                pinhits-lag(pinhits,1) over (partition by namespace order by namespace,snap_id) pinhits,
                reloads-lag(reloads,1) over (partition by namespace order by namespace,snap_id) reloads,
                invalidations-lag(invalidations,1) over (partition by namespace order by namespace,snap_id) invalidations,
                dlm_lock_requests-lag(dlm_lock_requests,1) over (partition by namespace order by namespace,snap_id) dlm_lock_requests,
                dlm_pin_requests-lag(dlm_pin_requests,1) over (partition by namespace order by namespace,snap_id) dlm_pin_requests,
                dlm_pin_releases-lag(dlm_pin_releases,1) over (partition by namespace order by namespace,snap_id) dlm_pin_releases,
                dlm_invalidation_requests-lag(dlm_invalidation_requests,1) over (partition by namespace order by namespace,snap_id) dlm_invalidation_requests,
                dlm_invalidations-lag(dlm_invalidations,1) over (partition by namespace order by namespace,snap_id) dlm_invalidations
           from dba_hist_librarycache
          where snap_id between :snap_start and :snap_end
                and instance_number = :instno
                and dbid = :dbid
       ) a,
       ( select snap_id,
                to_char(end_interval_time, 'yyyy-mm-dd hh24:mi:ss') snap_time
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) b
 where a.snap_id=b.snap_id
 order by 1, 4 desc, 5 desc
"""

sess_cache = """
select sn.snap_id                                            snapid,
       to_char(sn.end_interval_time,'yyyy-mm-dd hh24:mi:ss') dates,
       to_char(sn.end_interval_time, 'dd hh24:mi')           dates2,
       parse_req_total                                       "parse requests",
       cursor_cache_hits                                     "cursor cache hits",
       parse_req_total-cursor_cache_hits                     "Reparsed requests",
       decode(parse_req_total,0,0,cursor_cache_hits/parse_req_total) "cursor cache hit(%)"
  from ( select snap_id,
                case when max(decode(stat_name,'session cursor cache hits',value)) < 0 then 0
                     else max(decode(stat_name,'session cursor cache hits',value)) end   cursor_cache_hits,
                case when max(decode(stat_name,'parse count (total)',value)) < 0 then 0
                     else max(decode(stat_name,'parse count (total)',value)) end     parse_req_total
           from ( select snap_id,
                         stat_name,
                         nvl(value - lag(value) over ( partition by stat_name order by snap_id ), 0)  value
                    from dba_hist_sysstat
                   where snap_id between :snap_start and :snap_end
                     and instance_number = :instno
                     and dbid = :dbid
                     and stat_name in ('session cursor cache hits','parse count (total)')
                )
          group by snap_id
       ) v1,
       ( select snap_id,
                end_interval_time
           from dba_hist_snapshot
          where snap_id between 1+ :snap_start and :snap_end
            and instance_number = :instno
            and dbid = :dbid
       ) sn
 where v1.snap_id = sn.snap_id
 order by sn.snap_id
"""

undo = """
select e.snap_id snapid,
       to_char(s.end_interval_time,'yyyy-mm-dd hh24:mi:ss') dates,
       undotsn                                              "Undo TS#",
       max(to_char(s.end_interval_time,'dd hh24:mi'))       dates2,
       sum(undoblks)/1000                                   "undoblks(K)",
       sum(txncount)                                        "tx cnt",
       max(maxquerylen)                                     "Max query len(s)",
       max(maxconcurrency)                                  "Max concurrency",
       min(tuned_undoretention)                             "min tuned undo retention(s)" ,
       max(tuned_undoretention)                             "max tuned undo retention(s)",
       sum(ssolderrcnt)                                     "snapshot too old count",
       sum(nospaceerrcnt)                                   "out of space count",
       max(maxquerysqlid)                                   "sql_id(long query)"
  from dba_hist_undostat e,
       dba_hist_snapshot s
 where e.dbid            = :dbid
   and e.instance_number = :instno
   and e.snap_id    between :snap_start and :snap_end
   and e.snap_id = s.snap_id
   and s.instance_number = :instno
   and s.dbid = :dbid
   and s.snap_id between  :snap_start and :snap_end
 group by e.snap_id,to_char(s.end_interval_time,'yyyy-mm-dd hh24:mi:ss'),undotsn
 order by e.snap_id
"""

parameter = """
select parameter_name "Parameter",
       nvl(value,'n/a') value,
       isdefault       isdefault,
       ismodified      ismodified
  from dba_hist_parameter
 where snap_id = :snap_end
   and instance_number = :instno
   and dbid = :dbid
 order by parameter_name
 """

sga_param = """
select parameter_name "SGA Parameter",
       nvl(value,'n/a') value
  from dba_hist_parameter
 where parameter_name in (
             'db_block_buffers','db_block_size','db_cache_size','db_2k_cache_size','db_4k_cache_size',
             'db_8k_cache_size','db_16k_cache_size','db_32k_cache_size','db_keep_cache_size','db_recycle_cache_size',
             'java_pool_size','large_pool_size','log_buffer',
             'pga_aggregate_target',
             'shared_pool_size','sga_max_size','sga_target',
             'statistics_level','streams_pool_size','workarea_size_policy'
                )
   and snap_id = :snap_end
   and instance_number = :instno
   and dbid = :dbid
 order by parameter_name
"""

top_sql_by_qio = """
select round(a1.buffer_gets_delta*8/(1024*(case when executions = 0 then 1 else executions end)),0) as per_io , a1.*
from
(
	select  dbid , instance_number, sql_id , plan_hash_value , parsing_schema_name , sum(cpu_time_delta)/1000000 as cpu_time , round(sum(elapsed_time_delta)/1000000,2) as elapsed_time 
    , sum(executions_delta) as executions
    , round((sum(elapsed_time_delta)/1000000)/sum(case when executions_delta = 0 then 1 else executions_delta end),3) as per_elapsed_time  
    , sum(buffer_gets_delta) as buffer_gets_delta
    , sum(disk_reads_delta) as disk_reads_delta
    , sum(iowait_delta) as iowait_delta
    , sum(io_interconnect_bytes_delta) as io_interconnect_bytes_delta
    , sum(loads_delta) as loads_delta
    , sum(optimized_physical_reads_delta) as optimized_physical_reads_delta
    , sum(parse_calls_delta) as parse_calls_delta
    , sum(physical_read_bytes_delta) as physical_read_bytes_delta
    , sum(rows_processed_delta) as rows_processed_delta
	, module
	from dba_hist_sqlstat
	where snap_id between :snap_start and :snap_end
    and instance_number = :instno
    and dbid = :dbid
    and parsing_schema_name not in ('SYS', 'MA_TUN' , 'DBSNMP' , 'MA_SEL' , 'SYSMAN','MA_MON','SYSTEM','MIG_ST','MIG_TB','CONV_ST','CONV_TB','MASP','MA_DBA','MAAS_SEL')
    and parsing_schema_name not like '%DBA'
    and parsing_schema_name not like '%MON'    
	group by  dbid , instance_number , sql_id , plan_hash_value , parsing_schema_name , module
) a1, dba_hist_sqltext a2
where a1.dbid = a2.dbid
  and a1.sql_id = a2.sql_id
order by 8 desc
"""

gen_rpt2 = """
select 'generate report end time : ' || to_char(sysdate, 'yyyy/mm/dd hh24:mi:ss') "End Gen!" from dual
"""