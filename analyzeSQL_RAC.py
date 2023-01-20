load_2node = """
select snap_id,
       max(to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss'))              "Dates",
       max(to_char(end_interval_time,'dd hh24:mi'))                         "Dates2",
       max(case when a.inst=b.inst and stat_name='user commits'          then delta else 0 end)
       +max(case when a.inst=b.inst and stat_name='user rollbacks'       then delta else 0 end) "Transactions1",
       max(case when a.inst=c.inst and stat_name='user commits'          then delta else 0 end)
       +max(case when a.inst=c.inst and stat_name='user rollbacks'       then delta else 0 end) "Transactions2",
       max(case when a.inst=b.inst and stat_name='redo size' then delta else 0 end) "Redo size1",
       max(case when a.inst=c.inst and stat_name='redo size' then delta else 0 end) "Redo size2",
       decode(max(case when a.inst=b.inst and stat_name='redo entries' then delta_val else 0 end), 0, 0,
                   (1 -
                              (max(case when a.inst=b.inst and stat_name='redo log space requests' then delta_val else 0 end)
                               /max(case when a.inst=b.inst and stat_name='redo entries' then delta_val else 0 end)
                              ))
             ) "Redo NoWait1(%)",
       decode(max(case when a.inst=c.inst and stat_name='redo entries' then delta_val else 0 end), 0, 0,
                   (1 -
                              (max(case when a.inst=c.inst and stat_name='redo log space requests' then delta_val else 0 end)
                               /max(case when a.inst=c.inst and stat_name='redo entries' then delta_val else 0 end)
                              ))
             ) "Redo NoWait2(%)",
       max(case when a.inst=b.inst and stat_name='logons cumulative'     then delta else 0 end) "Logons1",
       max(case when a.inst=c.inst and stat_name='logons cumulative'     then delta else 0 end) "Logons2",
       max(case when a.inst=b.inst and stat_name='user calls'            then delta else 0 end) "User calls1",
       max(case when a.inst=c.inst and stat_name='user calls'            then delta else 0 end) "User calls2",
       max(case when a.inst=b.inst and stat_name='execute count'         then delta else 0 end) "Executes1",
       max(case when a.inst=c.inst and stat_name='execute count'         then delta else 0 end) "Executes2",
       max(case when a.inst=b.inst and stat_name='db block changes'      then delta else 0 end) "Block changes1",
       max(case when a.inst=c.inst and stat_name='db block changes'      then delta else 0 end) "Block changes2",
       decode(max(case when a.inst=b.inst and stat_name='consistent gets from cache' then delta_val else 0 end)
             +max(case when a.inst=b.inst and stat_name='db block gets from cache' then delta_val else 0 end), 0, 0,
             1-(max(case when a.inst=b.inst and stat_name='physical reads cache' then delta_val else 0 end)
              /(max(case when a.inst=b.inst and stat_name='consistent gets from cache' then delta_val else 0 end)
               +max(case when a.inst=b.inst and stat_name='db block gets from cache' then delta_val else 0 end))))  "Buffer Hits1(%)",
       decode(max(case when a.inst=c.inst and stat_name='consistent gets from cache' then delta_val else 0 end)
             +max(case when a.inst=c.inst and stat_name='db block gets from cache' then delta_val else 0 end), 0, 0,
             1-(max(case when a.inst=c.inst and stat_name='physical reads cache' then delta_val else 0 end)
              /(max(case when a.inst=c.inst and stat_name='consistent gets from cache' then delta_val else 0 end)
               +max(case when a.inst=c.inst and stat_name='db block gets from cache' then delta_val else 0 end))))  "Buffer Hits2(%)",
       decode(max(case when a.inst=b.inst and stat_name='consistent gets from cache' then delta_val else 0 end)
             +max(case when a.inst=b.inst and stat_name='db block gets from cache'   then delta_val else 0 end),0,0,
           1-(max(case when a.inst=b.inst then delta_wa else 0 end)
            /(max(case when a.inst=b.inst and stat_name='consistent gets from cache' then delta_val else 0 end)
             +max(case when a.inst=b.inst and stat_name='db block gets from cache'   then delta_val else 0 end)))) "Buffer Nowait1(%)",
       decode(max(case when a.inst=c.inst and stat_name='consistent gets from cache' then delta_val else 0 end)
             +max(case when a.inst=c.inst and stat_name='db block gets from cache'   then delta_val else 0 end),0,0,
           1-(max(case when a.inst=c.inst then delta_wa else 0 end)
            /(max(case when a.inst=c.inst and stat_name='consistent gets from cache' then delta_val else 0 end)
             +max(case when a.inst=c.inst and stat_name='db block gets from cache'   then delta_val else 0 end)))) "Buffer Nowait2(%)",
       max(case when a.inst=b.inst and delta_pins <> 0 then delta_pinhits/delta_pins else 0 end)     "Library Hit1(%)",
       max(case when a.inst=c.inst and delta_pins <> 0 then delta_pinhits/delta_pins else 0 end)     "Library Hit2(%)",
       max(case when a.inst=b.inst and delta_gets <> 0 then 1-(delta_misses/delta_gets) else 0 end) "Latch Hit1(%)",
       max(case when a.inst=c.inst and delta_gets <> 0 then 1-(delta_misses/delta_gets) else 0 end) "Latch Hit2(%)",
       max(case when a.inst=b.inst and stat_name='sorts (memory)'        then delta else 0 end) "Sorts1 (memory)",
       max(case when a.inst=c.inst and stat_name='sorts (memory)'        then delta else 0 end) "Sorts2 (memory)",
       max(case when a.inst=b.inst and stat_name='sorts (disk)'          then delta else 0 end) "Sorts1 (disk)",
       max(case when a.inst=c.inst and stat_name='sorts (disk)'          then delta else 0 end) "Sorts2 (disk)",
       decode(max(case when a.inst=b.inst and stat_name='sorts (memory)' then delta_val else 0 end)
                 +max(case when a.inst=b.inst and stat_name='sorts (disk)' then delta_val else 0 end), 0, 0,
                  max(case when a.inst=b.inst and stat_name='sorts (memory)' then delta_val else 0 end)
                               /(max(case when a.inst=b.inst and stat_name='sorts (memory)' then delta_val else 0 end)
                                +max(case when a.inst=b.inst and stat_name='sorts (disk)' then delta_val else 0 end)))      "In-memory Sort1(%)",
       decode(max(case when a.inst=c.inst and stat_name='sorts (memory)' then delta_val else 0 end)
                 +max(case when a.inst=c.inst and stat_name='sorts (disk)' then delta_val else 0 end), 0, 0,
                  max(case when a.inst=c.inst and stat_name='sorts (memory)' then delta_val else 0 end)
                               /(max(case when a.inst=c.inst and stat_name='sorts (memory)' then delta_val else 0 end)
                                +max(case when a.inst=c.inst and stat_name='sorts (disk)' then delta_val else 0 end)))      "In-memory Sort2(%)",
       max(case when a.inst=b.inst and stat_name='session logical reads' then delta else 0 end) "Logical reads1",
       max(case when a.inst=c.inst and stat_name='session logical reads' then delta else 0 end) "Logical reads2",
       max(case when a.inst=b.inst and stat_name='physical reads'        then delta else 0 end) "Physical reads1",
       max(case when a.inst=c.inst and stat_name='physical reads'        then delta else 0 end) "Physical reads2",
       max(case when a.inst=b.inst and stat_name='physical reads direct' then delta else 0 end) "Physical reads direct1",
       max(case when a.inst=c.inst and stat_name='physical reads direct' then delta else 0 end) "Physical reads direct2",
       max(case when a.inst=b.inst and stat_name='physical writes'       then delta else 0 end) "Physical writes1",
       max(case when a.inst=c.inst and stat_name='physical writes'       then delta else 0 end) "Physical writes2",
       max(case when a.inst=b.inst and stat_name='physical writes direct'then delta else 0 end) "Physical writes direct1",
       max(case when a.inst=c.inst and stat_name='physical writes direct'then delta else 0 end) "Physical writes direct2",
       max(case when a.inst=b.inst and stat_name='parse count (total)'   then delta else 0 end) "Parses1(total)",
       max(case when a.inst=c.inst and stat_name='parse count (total)'   then delta else 0 end) "Parses2(total)",
       max(case when a.inst=b.inst and stat_name='parse count (hard)'    then delta else 0 end) "Parses1(hard)",
       max(case when a.inst=c.inst and stat_name='parse count (hard)'    then delta else 0 end) "Parses2(hard)",
       decode(max(case when a.inst=b.inst and stat_name = 'parse count (total)' then delta_val else 0 end), 0, 0,
                    (max(case when a.inst=b.inst and stat_name='parse count (hard)' then delta_val else 0 end)
                      /max(case when a.inst=b.inst and stat_name = 'parse count (total)' then delta_val else 0 end)
                              )
             )  "Hard Parse1(%)",
       decode(max(case when a.inst=c.inst and stat_name = 'parse count (total)' then delta_val else 0 end), 0, 0,
                    (max(case when a.inst=c.inst and stat_name='parse count (hard)' then delta_val else 0 end)
                      /max(case when a.inst=c.inst and stat_name = 'parse count (total)' then delta_val else 0 end)
                              )
             )  "Hard Parse2(%)",
       decode(max(case when a.inst=b.inst and stat_name='parse count (total)' then delta_val else 0 end), 0, 0,
                  1-(max(case when a.inst=b.inst and stat_name='parse count (hard)' then delta_val else 0 end)
                    /max(case when a.inst=b.inst and stat_name='parse count (total)' then delta_val else 0 end)))  "Soft Parse1(%)",
       decode(max(case when a.inst=c.inst and stat_name='parse count (total)' then delta_val else 0 end), 0, 0,
                  1-(max(case when a.inst=c.inst and stat_name='parse count (hard)' then delta_val else 0 end)
                    /max(case when a.inst=c.inst and stat_name='parse count (total)' then delta_val else 0 end)))  "Soft Parse2(%)",
       decode(max(case when a.inst=b.inst and stat_name='execute count' then delta_val else 0 end), 0, 0,
                  1-(max(case when a.inst=b.inst and stat_name='parse count (total)' then delta_val else 0 end)
                    /max(case when a.inst=b.inst and stat_name='execute count' then delta_val else 0 end))) "Exec to Parse1(%)",
       decode(max(case when a.inst=c.inst and stat_name='execute count' then delta_val else 0 end), 0, 0,
                  1-(max(case when a.inst=c.inst and stat_name='parse count (total)' then delta_val else 0 end)
                    /max(case when a.inst=c.inst and stat_name='execute count' then delta_val else 0 end))) "Exec to Parse2(%)",
       decode(max(case when a.inst=b.inst and stat_name='parse time elapsed' then delta_val else 0 end), 0, 0,
                  max(case when a.inst=b.inst and stat_name='parse time cpu' then delta_val else 0 end)
                 /max(case when a.inst=b.inst and stat_name='parse time elapsed' then delta_val else 0 end)) "Parse CPU to Parse Elapsd1(%)",
       decode(max(case when a.inst=c.inst and stat_name='parse time elapsed' then delta_val else 0 end), 0, 0,
                  max(case when a.inst=c.inst and stat_name='parse time cpu' then delta_val else 0 end)
                 /max(case when a.inst=c.inst and stat_name='parse time elapsed' then delta_val else 0 end)) "Parse CPU to Parse Elapsd2(%)",
       decode(max(case when a.inst=b.inst and stat_name='CPU used by this session' then delta_val else 0 end), 0, 0,
                  1-(max(case when a.inst=b.inst and stat_name='parse time cpu' then delta_val else 0 end)
                    /max(case when a.inst=b.inst and stat_name='CPU used by this session' then delta_val else 0 end))) "Non-Parse CPU1(%)",
       decode(max(case when a.inst=c.inst and stat_name='CPU used by this session' then delta_val else 0 end), 0, 0,
                  1-(max(case when a.inst=c.inst and stat_name='parse time cpu' then delta_val else 0 end)
                    /max(case when a.inst=c.inst and stat_name='CPU used by this session' then delta_val else 0 end))) "Non-Parse CPU2(%)",
       max(case when a.inst=b.inst and stat_name='parse time cpu'       then delta else 0 end)/100 "parse cpu1(s)",
       max(case when a.inst=c.inst and stat_name='parse time cpu'       then delta else 0 end)/100 "parse cpu2(s)",
       max(case when a.inst=b.inst and stat_name='parse time elapsed'   then delta else 0 end)/100 "Parse ela1(s)",
       max(case when a.inst=c.inst and stat_name='parse time elapsed'   then delta else 0 end)/100 "Parse ela2(s)",
       max(case when a.inst=b.inst and stat_name='user commits'         then delta else 0 end) "user commits1",
       max(case when a.inst=c.inst and stat_name='user commits'         then delta else 0 end) "user commits2",
       max(case when a.inst=b.inst and stat_name='user rollbacks'       then delta else 0 end) "user rollbacks1",
       max(case when a.inst=c.inst and stat_name='user rollbacks'       then delta else 0 end) "user rollbacks2"
 from (
       select sy.stat_name,
              sy.snap_id,
              sy.instance_number inst,
              sn.end_interval_time,
              nvl(sy.value,0)  - nvl(lag(sy.value) over( partition by sy.instance_number,sy.stat_name order by sy.snap_id ),0) delta_val,
              case when
                (extract (day    from sn.end_interval_time - sn.begin_interval_time)* 86400 +
                 extract (hour   from sn.end_interval_time - sn.begin_interval_time)*  3600 +
                 extract (minute from sn.end_interval_time - sn.begin_interval_time)*    60 +
                 extract (second from sn.end_interval_time - sn.begin_interval_time)) > 0
                then
                  (nvl(sy.value,0)  - nvl(lag(sy.value) over( partition by sy.instance_number,sy.stat_name order by sy.snap_id ),0))
                  /(extract (day    from sn.end_interval_time - begin_interval_time)* 86400 +
                    extract (hour   from sn.end_interval_time - begin_interval_time)*  3600 +
                    extract (minute from sn.end_interval_time - begin_interval_time)*    60 +
                    extract (second from sn.end_interval_time - begin_interval_time)
                   )
                else null end delta,
              nvl(wa.wait_cnt,0)       - nvl(lag(wa.wait_cnt)      over( partition by wa.instance_number       order by wa.snap_id ),0)       delta_wa,
              nvl(libcache.pins,0)     - nvl(lag(libcache.pins)    over( partition by libcache.instance_number order by libcache.snap_id ),0) delta_pins,
              nvl(libcache.pinhits,0)  - nvl(lag(libcache.pinhits) over( partition by libcache.instance_number order by libcache.snap_id ),0) delta_pinhits,
              nvl(latch.gets,0)        - nvl(lag(latch.gets)       over( partition by latch.instance_number    order by latch.snap_id ),0)    delta_gets,
              nvl(latch.misses,0)      - nvl(lag(latch.misses)     over( partition by latch.instance_number    order by latch.snap_id ),0)    delta_misses
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
          and sn.instance_number in (select instance_number from dba_hist_parameter where parameter_name = 'instance_name' and snap_id = :snap_end and dbid = :dbid)
          and sn.dbid = :dbid
      ) a,
      (
        select instance_number inst, instance_name
          from (
                 select rownum rn, instance_number,value instance_name
                   from dba_hist_parameter
                  where parameter_name = 'instance_name'
                    and snap_id = :snap_end
                    and dbid = :dbid
                  order by instance_number
               )
         where rn = 1
      ) b,
      (
        select instance_number inst, instance_name
          from (
                 select rownum rn, instance_number,value instance_name
                   from dba_hist_parameter
                  where parameter_name = 'instance_name'
                    and snap_id = :snap_end
                    and dbid = :dbid
                  order by instance_number
               )
         where rn = 2
      ) c
 where snap_id > :snap_start
group by snap_id
order by snap_id
"""

load_2node2 = """
select b.value||'-'||a.title title
  from (
         select 1  no, 'Transactions'                  title from dual union all
         select 2  no, 'Redo Size'                     title from dual union all
         select 3  no, 'Redo NoWait(%)'                title from dual union all
         select 4  no, 'Logons'                        title from dual union all
         select 5  no, 'User calls'                    title from dual union all
         select 6  no, 'Executes'                      title from dual union all
         select 7  no, 'Block changes'                 title from dual union all
         select 8  no, 'Buffer Hits(%)'                title from dual union all
         select 9  no, 'Buffer Nowait(%)'              title from dual union all
         select 10 no, 'Library Hit(%)'                title from dual union all
         select 11 no, 'Latch Hit(%)'                  title from dual union all
         select 12 no, 'Sorts (memory)'                title from dual union all
         select 13 no, 'Sorts (disk)'                  title from dual union all
         select 14 no, 'In-memory Sort(%)'             title from dual union all
         select 15 no, 'Logical reads'                 title from dual union all
         select 16 no, 'Physical reads'                title from dual union all
         select 17 no, 'Physical reads direct'         title from dual union all
         select 18 no, 'Physical writes'               title from dual union all
         select 19 no, 'Physical writes direct'        title from dual union all
         select 20 no, 'Parses(total)'                 title from dual union all
         select 21 no, 'Parses(hard)'                  title from dual union all
         select 22 no, 'Hard Parse(%)'                 title from dual union all
         select 23 no, 'Soft Parse(%)'                 title from dual union all
         select 24 no, 'Exec to Parse(%)'              title from dual union all
         select 25 no, 'Parse CPU to Parse Elapsd(%)'  title from dual union all
         select 26 no, 'Non-Parse CPU(%)'              title from dual union all
         select 27 no, 'parse cpu(s)'                  title from dual union all
         select 28 no, 'Parse ela(s)'                  title from dual union all
         select 29 no, 'user commits'                  title from dual union all
         select 30 no, 'user rollbacks'                title from dual 
         
       ) a,
       (
         select instance_number,value
           from dba_hist_parameter
          where parameter_name = 'instance_name'
            and snap_id = :snap_end
            and dbid = :dbid
       ) b
order by a.no, b.instance_number
"""

rac_load_2node = """
select snap_id "SnapID",
       max(to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss'))              "Dates",
       max(to_char(end_interval_time,'dd hh24:mi'))                         "Dates2",
       ((max(case when a.inst=b.inst and stat_name='gc cr blocks received' then delta else 0 end)
        +max(case when a.inst=b.inst and stat_name='gc current blocks received' then delta else 0 end)
        +max(case when a.inst=b.inst and stat_name='gc cr blocks served' then delta else 0 end)
        +max(case when a.inst=b.inst and stat_name='gc current blocks served' then delta else 0 end)
        ) * max ((select to_number(value) from dba_hist_parameter
              where parameter_name='db_block_size' and dbid=:dbid and instance_number=b.inst and snap_id=:snap_end))
       +(max(case when a.inst=b.inst and stat_name='gcs messages sent' then delta else 0 end)
        +max(case when a.inst=b.inst and stat_name='ges messages sent' then delta else 0 end)
        +max(case when a.inst=b.inst and      name='gcs msgs received' then delta_dm else 0 end)
        +max(case when a.inst=b.inst and      name='ges msgs received' then delta_dm else 0 end)
        ) * 200 ) / 1048576 "Interconnect traffic1(Mb)",
       ((max(case when a.inst=c.inst and stat_name='gc cr blocks received' then delta else 0 end)
        +max(case when a.inst=c.inst and stat_name='gc current blocks received' then delta else 0 end)
        +max(case when a.inst=c.inst and stat_name='gc cr blocks served' then delta else 0 end)
        +max(case when a.inst=c.inst and stat_name='gc current blocks served' then delta else 0 end)
        ) * max ((select to_number(value) from dba_hist_parameter
              where parameter_name='db_block_size' and dbid=:dbid and instance_number=c.inst and snap_id=:snap_end))
       +(max(case when a.inst=c.inst and stat_name='gcs messages sent' then delta else 0 end)
        +max(case when a.inst=c.inst and stat_name='ges messages sent' then delta else 0 end)
        +max(case when a.inst=c.inst and      name='gcs msgs received' then delta_dm else 0 end)
        +max(case when a.inst=c.inst and      name='ges msgs received' then delta_dm else 0 end)
        ) * 200 ) / 1048576 "Interconnect traffic2(Mb)",
       max(case when a.inst=b.inst and stat_name='gc cr blocks received' then delta else 0 end)
       +max(case when a.inst=b.inst and stat_name='gc current blocks received' then delta else 0 end) "GC blocks received1",
       max(case when a.inst=c.inst and stat_name='gc cr blocks received' then delta else 0 end)
       +max(case when a.inst=c.inst and stat_name='gc current blocks received' then delta else 0 end) "GC blocks received2",
       max(case when a.inst=b.inst and stat_name='gc cr blocks received' then delta else 0 end) "GC CR block received1",
       max(case when a.inst=c.inst and stat_name='gc cr blocks received' then delta else 0 end) "GC CR block received2",
       max(case when a.inst=b.inst and stat_name='gc current blocks received' then delta else 0 end) "GC Current block received1",
       max(case when a.inst=c.inst and stat_name='gc current blocks received' then delta else 0 end) "GC Current block received2",
       max(case when a.inst=b.inst and stat_name='gc cr blocks served' then delta else 0 end)
       +max(case when a.inst=b.inst and stat_name='gc current blocks served' then delta else 0 end) "GC blocks served1",
       max(case when a.inst=c.inst and stat_name='gc cr blocks served' then delta else 0 end)
       +max(case when a.inst=c.inst and stat_name='gc current blocks served' then delta else 0 end) "GC blocks served2",
       max(case when a.inst=b.inst and stat_name='gc cr blocks served' then delta else 0 end)       "GC CR block served1",
       max(case when a.inst=c.inst and stat_name='gc cr blocks served' then delta else 0 end)       "GC CR block served2",
       max(case when a.inst=b.inst and stat_name='gc current blocks served' then delta else 0 end)  "GC Current blocks served1",
       max(case when a.inst=c.inst and stat_name='gc current blocks served' then delta else 0 end)  "GC Current blocks served2",
       max(case when a.inst=b.inst and      name='gcs msgs received' then delta_dm else 0 end)
       +max(case when a.inst=b.inst and      name='ges msgs received' then delta_dm else 0 end) "GCS/GES msg received1",
       max(case when a.inst=c.inst and      name='gcs msgs received' then delta_dm else 0 end)
       +max(case when a.inst=c.inst and      name='ges msgs received' then delta_dm else 0 end) "GCS/GES msg received2",
       max(case when a.inst=b.inst and stat_name='gcs messages sent' then delta else 0 end)
       +max(case when a.inst=b.inst and stat_name='ges messages sent' then delta else 0 end) "GCS/GES msg sent1",
       max(case when a.inst=c.inst and stat_name='gcs messages sent' then delta else 0 end)
       +max(case when a.inst=c.inst and stat_name='ges messages sent' then delta else 0 end) "GCS/GES msg sent2",
       max(case when a.inst=b.inst and stat_name='DBWR fusion writes' then delta else 0 end) "DBWR fusion writes1",
       max(case when a.inst=c.inst and stat_name='DBWR fusion writes' then delta else 0 end) "DBWR fusion writes2",
       max(case when a.inst=b.inst and stat_name='gc blocks lost' then delta_val else 0 end) "gc block lost1",
       max(case when a.inst=c.inst and stat_name='gc blocks lost' then delta_val else 0 end) "gc block lost2",
       max(case when a.inst=b.inst and stat_name='gc blocks corrupt' then delta_val else 0 end) "gc block corrupt1",
       max(case when a.inst=c.inst and stat_name='gc blocks corrupt' then delta_val else 0 end) "gc block corrupt2",
       decode(max(case when a.inst=b.inst and stat_name='consistent gets from cache' then delta_val else 0 end)
              +max(case when a.inst=b.inst and stat_name='db block gets from cache' then delta_val else 0 end), 0, 0,
                 (1 -
                     ((max(case when a.inst=b.inst and stat_name='physical reads cache' then delta_val else 0 end)
                       +max(case when a.inst=b.inst and stat_name='gc cr blocks received' then delta_val else 0 end)
                       +max(case when a.inst=b.inst and stat_name='gc current blocks received' then delta_val else 0 end ))
                       /(max(case when a.inst=b.inst and stat_name='consistent gets from cache' then delta_val else 0 end)
                         +max(case when a.inst=b.inst and stat_name='db block gets from cache' then delta_val else 0 end))))
             ) "local1(%)",
       decode(max(case when a.inst=c.inst and stat_name='consistent gets from cache' then delta_val else 0 end)
              +max(case when a.inst=c.inst and stat_name='db block gets from cache' then delta_val else 0 end), 0, 0,
                 (1 -
                     ((max(case when a.inst=c.inst and stat_name='physical reads cache' then delta_val else 0 end)
                       +max(case when a.inst=c.inst and stat_name='gc cr blocks received' then delta_val else 0 end)
                       +max(case when a.inst=c.inst and stat_name='gc current blocks received' then delta_val else 0 end ))
                       /(max(case when a.inst=c.inst and stat_name='consistent gets from cache' then delta_val else 0 end)
                         +max(case when a.inst=c.inst and stat_name='db block gets from cache' then delta_val else 0 end))))
             ) "local2(%)",
       decode(max(case when a.inst=b.inst and stat_name='consistent gets from cache' then delta_val else 0 end)
                 +max(case when a.inst=b.inst and stat_name='db block gets from cache'then delta_val else 0 end), 0, 0,
                   ((max(case when a.inst=b.inst and stat_name='gc cr blocks received' then delta_val else 0 end)
                               +max(case when a.inst=b.inst and stat_name='gc current blocks received' then delta_val else 0 end))
                   /(max(case when a.inst=b.inst and stat_name='consistent gets from cache' then delta_val else 0 end)
                     +max(case when a.inst=b.inst and stat_name='db block gets from cache' then delta_val else 0 end)))
             ) "remote1(%)",
       decode(max(case when a.inst=c.inst and stat_name='consistent gets from cache' then delta_val else 0 end)
                 +max(case when a.inst=c.inst and stat_name='db block gets from cache'then delta_val else 0 end), 0, 0,
                   ((max(case when a.inst=c.inst and stat_name='gc cr blocks received' then delta_val else 0 end)
                               +max(case when a.inst=c.inst and stat_name='gc current blocks received' then delta_val else 0 end))
                   /(max(case when a.inst=c.inst and stat_name='consistent gets from cache' then delta_val else 0 end)
                     +max(case when a.inst=c.inst and stat_name='db block gets from cache' then delta_val else 0 end)))
             ) "remote2(%)",
       decode(max(case when a.inst=b.inst and stat_name='consistent gets from cache' then delta_val else 0 end)
                 +max(case when a.inst=b.inst and stat_name='db block gets from cache' then delta_val else 0 end), 0, 0,
                   ( max(case when a.inst=b.inst and stat_name='physical reads cache' then delta_val else 0 end)
                               /(max(case when a.inst=b.inst and stat_name='consistent gets from cache' then delta_val else 0 end)
                                +max(case when a.inst=b.inst and stat_name='db block gets from cache' then delta_val else 0 end)))
             ) "disk1(%)",
       decode(max(case when a.inst=c.inst and stat_name='consistent gets from cache' then delta_val else 0 end)
                 +max(case when a.inst=c.inst and stat_name='db block gets from cache' then delta_val else 0 end), 0, 0,
                   ( max(case when a.inst=c.inst and stat_name='physical reads cache' then delta_val else 0 end)
                               /(max(case when a.inst=c.inst and stat_name='consistent gets from cache' then delta_val else 0 end)
                                +max(case when a.inst=c.inst and stat_name='db block gets from cache' then delta_val else 0 end)))
             ) "disk2(%)"
  from (
       select sy.stat_name,
              sy.snap_id,
              sy.instance_number inst,
              sn.end_interval_time,
              nvl(sy.value,0) - nvl(lag(sy.value) over( partition by sy.instance_number,sy.stat_name order by sy.snap_id ),0) delta_val,
              case when
                (extract (day    from sn.end_interval_time - sn.begin_interval_time) * 86400 +
                 extract (hour   from sn.end_interval_time - sn.begin_interval_time) *  3600 +
                 extract (minute from sn.end_interval_time - sn.begin_interval_time) *    60 +
                 extract (second from sn.end_interval_time - sn.begin_interval_time) ) > 0
                then
                   (nvl(sy.value,0) - nvl(lag(sy.value) over( partition by sy.instance_number,sy.stat_name order by sy.snap_id ),0))
                  /(extract (day    from sn.end_interval_time - sn.begin_interval_time) * 86400 +
                    extract (hour   from sn.end_interval_time - sn.begin_interval_time) *  3600 +
                    extract (minute from sn.end_interval_time - sn.begin_interval_time) *    60 +
                    extract (second from sn.end_interval_time - sn.begin_interval_time)
                   ) else null end delta,
              dm.name,
              nvl(dm.value-lag(dm.value) over( partition by dm.instance_number,dm.name order by dm.snap_id ),0) delta_dm_val,
              case when
                (extract (day    from sn.end_interval_time - sn.begin_interval_time) * 86400 +
                 extract (hour   from sn.end_interval_time - sn.begin_interval_time) *  3600 +
                 extract (minute from sn.end_interval_time - sn.begin_interval_time) *    60 +
                 extract (second from sn.end_interval_time - sn.begin_interval_time) ) > 0
                then
                   (nvl(dm.value,0) - nvl(lag(dm.value) over( partition by dm.instance_number,dm.name order by dm.snap_id ),0))
                  /(extract (day    from sn.end_interval_time - sn.begin_interval_time) * 86400 +
                    extract (hour   from sn.end_interval_time - sn.begin_interval_time) *  3600 +
                    extract (minute from sn.end_interval_time - sn.begin_interval_time) *    60 +
                    extract (second from sn.end_interval_time - sn.begin_interval_time) )
                else null end delta_dm,
              nvl(cbs.flushes,0)    - nvl(lag(cbs.flushes)    over( partition by cbs.instance_number order by cbs.snap_id ),0) delta_cbs,
              nvl(cus.flush1,0)     - nvl(lag(cus.flush1)     over( partition by cus.instance_number order by cus.snap_id),0) delta_cusf1,
              nvl(cus.flush10,0)    - nvl(lag(cus.flush10)    over( partition by cus.instance_number order by cus.snap_id),0) delta_cusf10,
              nvl(cus.flush100,0)   - nvl(lag(cus.flush100)   over( partition by cus.instance_number order by cus.snap_id),0) delta_cusf100,
              nvl(cus.flush1000,0)  - nvl(lag(cus.flush1000)  over( partition by cus.instance_number order by cus.snap_id),0) delta_cusf1000,
              nvl(cus.flush10000,0) - nvl(lag(cus.flush10000) over( partition by cus.instance_number order by cus.snap_id),0) delta_cusf10000
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
          and sn.instance_number in (select instance_number from dba_hist_parameter where parameter_name = 'instance_name' and snap_id = :snap_end and dbid = :dbid)
          and sn.dbid = :dbid
      ) a,
      (
        select instance_number inst, instance_name
          from (
                 select rownum rn, instance_number,value instance_name
                   from dba_hist_parameter
                  where parameter_name = 'instance_name'
                    and snap_id = :snap_end
                    and dbid = :dbid
                  order by instance_number
               )
         where rn = 1
      ) b,
      (
        select instance_number inst, instance_name
          from (
                 select rownum rn, instance_number,value instance_name
                   from dba_hist_parameter
                  where parameter_name = 'instance_name'
                    and snap_id = :snap_end
                    and dbid = :dbid
                  order by instance_number
               )
         where rn = 2
      ) c
 where snap_id > :snap_start
group by snap_id
order by snap_id
"""

rac_load_2node2 = """
select b.value||'-'||a.title title
  from (
         select 1  no, 'Interconnect traffic(Mb)'      title from dual union all
         select 2  no, 'GC blocks received'            title from dual union all
         select 3  no, 'GC CR block received'          title from dual union all
         select 4  no, 'GC current block received'     title from dual union all
         select 5  no, 'GC blocks served'              title from dual union all
         select 6  no, 'GC CR blocks served'           title from dual union all
         select 7  no, 'GC current blocks served'      title from dual union all
         select 8  no, 'GCS/GES msg received'          title from dual union all
         select 9  no, 'GCS/GES msg sent'              title from dual union all
         select 10 no, 'DBWR fusion writes'            title from dual union all
         select 11 no, 'gc block lost'                 title from dual union all
         select 12 no, 'gc block corrupt'              title from dual union all
         select 13 no, 'local(%)'                      title from dual union all
         select 14 no, 'remote(%)'                     title from dual union all
         select 15 no, 'disk(%)'                       title from dual 
       ) a,
       (
         select instance_number,value
           from dba_hist_parameter
          where parameter_name = 'instance_name'
            and snap_id = :snap_end
            and dbid = :dbid
       ) b
order by a.no, b.instance_number
"""

rac_load2_2node = """
select snap_id   snapid,
       max(to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss')) dates,
       max(to_char(end_interval_time,'dd hh24:mi'))            dates2,
       decode(max(case when a.inst=b.inst and stat_name='gc cr blocks received'     then delta_val else 0 end), 0, 0,
             (max(case when a.inst=b.inst and stat_name='gc cr block receive time'  then delta_val else 0 end)
             /max(case when a.inst=b.inst and stat_name='gc cr blocks received'     then delta_val else 0 end)) * 10
             ) "avg gc cr blk rcv time1(ms)" ,
       decode(max(case when a.inst=c.inst and stat_name='gc cr blocks received'     then delta_val else 0 end), 0, 0,
             (max(case when a.inst=c.inst and stat_name='gc cr block receive time'  then delta_val else 0 end)
             /max(case when a.inst=c.inst and stat_name='gc cr blocks received'     then delta_val else 0 end)) * 10
             ) "avg gc cr blk rcv time2(ms)" ,
       decode(max(case when a.inst=b.inst and stat_name='gc current blocks received'    then delta_val else 0 end), 0, 0,
             (max(case when a.inst=b.inst and stat_name='gc current block receive time' then delta_val else 0 end)
             /max(case when a.inst=b.inst and stat_name='gc current blocks received'    then delta_val else 0 end)) * 10
             ) "avg gc cur blk rcv time1(ms)",
       decode(max(case when a.inst=c.inst and stat_name='gc current blocks received'    then delta_val else 0 end), 0, 0,
             (max(case when a.inst=c.inst and stat_name='gc current block receive time' then delta_val else 0 end)
             /max(case when a.inst=c.inst and stat_name='gc current blocks received'    then delta_val else 0 end)) * 10
             ) "avg gc cur blk rcv time2(ms)",
       decode(max(case when a.inst=b.inst and stat_name='gc cr blocks served' then delta_val else 0 end), 0, 0,
                  (max(case when a.inst=b.inst and stat_name='gc cr block build time' then delta_val else 0 end)
                    /max(case when a.inst=b.inst and stat_name='gc cr blocks served' then delta_val else 0 end)) * 10
             ) "Avg GC cr blk bld time1(ms)",
       decode(max(case when a.inst=c.inst and stat_name='gc cr blocks served' then delta_val else 0 end), 0, 0,
                  (max(case when a.inst=c.inst and stat_name='gc cr block build time' then delta_val else 0 end)
                    /max(case when a.inst=c.inst and stat_name='gc cr blocks served' then delta_val else 0 end)) * 10
             ) "Avg GC cr blk bld time2(ms)",
       decode(max(decode(a.inst, b.inst, delta_cbs,0)), 0, 0,
                  (max(case when a.inst=b.inst and stat_name='gc cr block flush time' then delta_val else 0 end)
                    /max(decode(a.inst, b.inst, delta_cbs,0))) * 10
             ) "Avg GC cr blk flush time1(ms)",
       decode(max(decode(a.inst, c.inst, delta_cbs,0)), 0, 0,
                  (max(case when a.inst=c.inst and stat_name='gc cr block flush time' then delta_val else 0 end)
                    /max(decode(a.inst, c.inst, delta_cbs,0))) * 10
             ) "Avg GC cr blk flush time2(ms)",
       decode(max(case when a.inst=b.inst and stat_name='gc cr blocks served' then delta_val else 0 end), 0, 0,
                  (max(case when a.inst=b.inst and stat_name='gc cr block send time' then delta_val else 0 end)
                    /max(case when a.inst=b.inst and stat_name='gc cr blocks served' then delta_val else 0 end)) * 10
             ) "Avg GC cr blk snd time1(ms)",
       decode(max(case when a.inst=c.inst and stat_name='gc cr blocks served' then delta_val else 0 end), 0, 0,
                  (max(case when a.inst=c.inst and stat_name='gc cr block send time' then delta_val else 0 end)
                    /max(case when a.inst=c.inst and stat_name='gc cr blocks served' then delta_val else 0 end)) * 10
             ) "Avg GC cr blk snd time2(ms)",
       decode(max(case when a.inst=b.inst and stat_name='gc current blocks served' then delta_val else 0 end), 0, 0,
                  (max(case when a.inst=b.inst and stat_name='gc current block pin time' then delta_val else 0 end)
                    /max(case when a.inst=b.inst and stat_name='gc current blocks served' then delta_val else 0 end)) * 10
             ) "Avg GC cu blk pin time1(ms)",
       decode(max(case when a.inst=c.inst and stat_name='gc current blocks served' then delta_val else 0 end), 0, 0,
                  (max(case when a.inst=c.inst and stat_name='gc current block pin time' then delta_val else 0 end)
                    /max(case when a.inst=c.inst and stat_name='gc current blocks served' then delta_val else 0 end)) * 10
             ) "Avg GC cu blk pin time2(ms)",
       decode(max(case when a.inst=b.inst and stat_name='gc current blocks served' then delta_val else 0 end), 0, 0,
                  (max(case when a.inst=b.inst and stat_name='gc current block send time' then delta_val else 0 end)
                    /max(case when a.inst=b.inst and stat_name='gc current blocks served' then delta_val else 0 end)) * 10
             ) "Avg GC cu blk snd time1(ms)",
       decode(max(case when a.inst=c.inst and stat_name='gc current blocks served' then delta_val else 0 end), 0, 0,
                  (max(case when a.inst=c.inst and stat_name='gc current block send time' then delta_val else 0 end)
                    /max(case when a.inst=c.inst and stat_name='gc current blocks served' then delta_val else 0 end)) * 10
             ) "Avg GC cu blk snd time2(ms)",
       decode( max(decode(a.inst,b.inst,delta_cusf1,0))+max(decode(a.inst,b.inst,delta_cusf10,0))+max(decode(a.inst,b.inst,delta_cusf100,0))
               +max(decode(a.inst,b.inst,delta_cusf1000,0))+max(decode(a.inst,b.inst,delta_cusf1000,0)), 0, 0,
                  (max(case when a.inst=b.inst and stat_name='gc current block flush time' then delta_val else 0 end)
                    /(max(decode(a.inst,b.inst,delta_cusf1,0))+max(decode(a.inst,b.inst,delta_cusf10,0))+max(decode(a.inst,b.inst,delta_cusf100,0))
               +max(decode(a.inst,b.inst,delta_cusf1000,0))+max(decode(a.inst,b.inst,delta_cusf1000,0)))) * 10
             ) "Avg GC cu blk flush time1(ms)",
       decode( max(decode(a.inst,c.inst,delta_cusf1,0))+max(decode(a.inst,c.inst,delta_cusf10,0))+max(decode(a.inst,c.inst,delta_cusf100,0))
               +max(decode(a.inst,c.inst,delta_cusf1000,0))+max(decode(a.inst,c.inst,delta_cusf1000,0)), 0, 0,
                  (max(case when a.inst=c.inst and stat_name='gc current block flush time' then delta_val else 0 end)
                    /(max(decode(a.inst,c.inst,delta_cusf1,0))+max(decode(a.inst,c.inst,delta_cusf10,0))+max(decode(a.inst,c.inst,delta_cusf100,0))
               +max(decode(a.inst,c.inst,delta_cusf1000,0))+max(decode(a.inst,c.inst,delta_cusf1000,0)))) * 10
             ) "Avg GC cu blk flush time2(ms)",
       decode(max(case when a.inst=b.inst and stat_name='global enqueue gets async' then delta_val else 0 end)
             +max(case when a.inst=b.inst and stat_name='global enqueue gets sync'  then delta_val else 0 end), 0, 0,
             (max(case when a.inst=b.inst and stat_name='global enqueue get time'   then delta_val else 0 end)
            /(max(case when a.inst=b.inst and stat_name='global enqueue gets async' then delta_val else 0 end)
             +max(case when a.inst=b.inst and stat_name='global enqueue gets sync'  then delta_val else 0 end))) * 10
             ) "avg GE get time1(ms)" ,
       decode(max(case when a.inst=c.inst and stat_name='global enqueue gets async' then delta_val else 0 end)
             +max(case when a.inst=c.inst and stat_name='global enqueue gets sync'  then delta_val else 0 end), 0, 0,
             (max(case when a.inst=c.inst and stat_name='global enqueue get time'   then delta_val else 0 end)
            /(max(case when a.inst=c.inst and stat_name='global enqueue gets async' then delta_val else 0 end)
             +max(case when a.inst=c.inst and stat_name='global enqueue gets sync'  then delta_val else 0 end))) * 10
             ) "avg GE get time2(ms)"
 from (
       select sy.stat_name,
              sy.snap_id,
              sy.instance_number inst,
              sn.end_interval_time,
              nvl(sy.value,0) - nvl(lag(sy.value) over( partition by sy.instance_number,sy.stat_name order by sy.snap_id ),0) delta_val,
              case when
                (extract (day    from sn.end_interval_time - sn.begin_interval_time) * 86400 +
                 extract (hour   from sn.end_interval_time - sn.begin_interval_time) *  3600 +
                 extract (minute from sn.end_interval_time - sn.begin_interval_time) *    60 +
                 extract (second from sn.end_interval_time - sn.begin_interval_time) ) > 0
                then
                   (nvl(sy.value,0) - nvl(lag(sy.value) over( partition by sy.instance_number,sy.stat_name order by sy.snap_id ),0))
                  /(extract (day    from sn.end_interval_time - sn.begin_interval_time) * 86400 +
                    extract (hour   from sn.end_interval_time - sn.begin_interval_time) *  3600 +
                    extract (minute from sn.end_interval_time - sn.begin_interval_time) *    60 +
                    extract (second from sn.end_interval_time - sn.begin_interval_time)
                   ) else null end delta,
              dm.name,
              nvl(dm.value-lag(dm.value) over( partition by dm.instance_number,dm.name order by dm.snap_id ),0) delta_dm_val,
              case when
                (extract (day    from sn.end_interval_time - sn.begin_interval_time) * 86400 +
                 extract (hour   from sn.end_interval_time - sn.begin_interval_time) *  3600 +
                 extract (minute from sn.end_interval_time - sn.begin_interval_time) *    60 +
                 extract (second from sn.end_interval_time - sn.begin_interval_time) ) > 0
                then
                   (nvl(dm.value,0) - nvl(lag(dm.value) over( partition by dm.instance_number,dm.name order by dm.snap_id ),0))
                  /(extract (day    from sn.end_interval_time - sn.begin_interval_time) * 86400 +
                    extract (hour   from sn.end_interval_time - sn.begin_interval_time) *  3600 +
                    extract (minute from sn.end_interval_time - sn.begin_interval_time) *    60 +
                    extract (second from sn.end_interval_time - sn.begin_interval_time) )
                else null end delta_dm,
              nvl(cbs.flushes,0)    - nvl(lag(cbs.flushes)    over( partition by cbs.instance_number order by cbs.snap_id ),0) delta_cbs,
              nvl(cus.flush1,0)     - nvl(lag(cus.flush1)     over( partition by cus.instance_number order by cus.snap_id),0) delta_cusf1,
              nvl(cus.flush10,0)    - nvl(lag(cus.flush10)    over( partition by cus.instance_number order by cus.snap_id),0) delta_cusf10,
              nvl(cus.flush100,0)   - nvl(lag(cus.flush100)   over( partition by cus.instance_number order by cus.snap_id),0) delta_cusf100,
              nvl(cus.flush1000,0)  - nvl(lag(cus.flush1000)  over( partition by cus.instance_number order by cus.snap_id),0) delta_cusf1000,
              nvl(cus.flush10000,0) - nvl(lag(cus.flush10000) over( partition by cus.instance_number order by cus.snap_id),0) delta_cusf10000
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
          and sn.instance_number in  (select instance_number from dba_hist_parameter where parameter_name = 'instance_name' and snap_id = :snap_end and dbid = :dbid)
          and sn.dbid = :dbid
      ) a,
      (
        select instance_number inst, instance_name
          from (
                 select rownum rn, instance_number,value instance_name
                   from dba_hist_parameter
                  where parameter_name = 'instance_name'
                    and snap_id = :snap_end
                    and dbid = :dbid
                  order by instance_number
               )
         where rn = 1
      ) b,
      (
        select instance_number inst, instance_name
          from (
                 select rownum rn, instance_number,value instance_name
                   from dba_hist_parameter
                  where parameter_name = 'instance_name'
                    and snap_id = :snap_end
                    and dbid = :dbid
                  order by instance_number
               )
         where rn = 2
      ) c
 where snap_id > :snap_start
group by snap_id
order by snap_id
"""

rac_load2_2node2 = """
select b.value||'-'||a.title title
  from (
         select 1  no, 'avg gc cr blk rcv time(ms)'    title from dual union all
         select 2  no, 'avg gc cur blk rcv time(ms)'   title from dual union all
         select 3  no, 'avg GC cr blk bld time(ms)'    title from dual union all
         select 4  no, 'avg GC cr blk flush time(ms)'  title from dual union all
         select 5  no, 'avg GC cr blk send time(ms)'   title from dual union all
         select 6  no, 'avg GC cu blk pin time(ms)'    title from dual union all
         select 7  no, 'avg GC cu blk send time(ms)'   title from dual union all
         select 8  no, 'avg GC cu blk flush time(ms)'  title from dual union all
         select 9  no, 'avg GE get time1(ms)'          title from dual 
       ) a,
       (
         select instance_number,value
           from dba_hist_parameter
          where parameter_name = 'instance_name'
            and snap_id = :snap_end
            and dbid = :dbid
       ) b
order by a.no, b.instance_number
"""

rac_message_2node = """
select snap_id,
       max(to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss'))  dates,
       max(to_char(end_interval_time,'dd hh24:mi'))             dates2,
       decode(max(case when a.inst=b.inst and name='msgs sent queued' then delta_dm_val else 0 end), 0, 0,
             (max(case when a.inst=b.inst and name='msgs sent queue time (ms)' then delta_dm_val else 0 end)
             /max(case when a.inst=b.inst and name='msgs sent queued' then delta_dm_val else 0 end)))           "msg sent queue time1(ms)",
       decode(max(case when a.inst=c.inst and name='msgs sent queued' then delta_dm_val else 0 end), 0, 0,
             (max(case when a.inst=c.inst and name='msgs sent queue time (ms)' then delta_dm_val else 0 end)
             /max(case when a.inst=c.inst and name='msgs sent queued' then delta_dm_val else 0 end)))           "msg sent queue time2(ms)",
       decode(max(case when a.inst=b.inst and name='msgs sent queued on ksxp' then delta_dm_val else 0 end), 0, 0,
             (max(case when a.inst=b.inst and name='msgs sent queue time on ksxp (ms)' then delta_dm_val else 0 end)
             /max(case when a.inst=b.inst and name='msgs sent queued on ksxp' then delta_dm_val else 0 end)))   "msg snt q time on ksxp1(ms)",
       decode(max(case when a.inst=c.inst and name='msgs sent queued on ksxp' then delta_dm_val else 0 end), 0, 0,
             (max(case when a.inst=c.inst and name='msgs sent queue time on ksxp (ms)' then delta_dm_val else 0 end)
             /max(case when a.inst=c.inst and name='msgs sent queued on ksxp' then delta_dm_val else 0 end)))   "msg snt q time on ksxp2(ms)",
       decode(max(case when a.inst=b.inst and name='msgs received queued' then delta_dm_val else 0 end), 0, 0,
             (max(case when a.inst=b.inst and name='msgs received queue time (ms)' then delta_dm_val else 0 end)
             /max(case when a.inst=b.inst and name='msgs received queued' then delta_dm_val else 0 end)))       "msg rcvd queue time1(ms)",
       decode(max(case when a.inst=c.inst and name='msgs received queued' then delta_dm_val else 0 end), 0, 0,
             (max(case when a.inst=c.inst and name='msgs received queue time (ms)' then delta_dm_val else 0 end)
             /max(case when a.inst=c.inst and name='msgs received queued' then delta_dm_val else 0 end)))       "msg rcvd queue time2(ms)",
       decode(max(case when a.inst=b.inst and name='gcs msgs received' then delta_dm_val else 0 end), 0, 0,
             (max(case when a.inst=b.inst and name='gcs msgs process time(ms)' then delta_dm_val else 0 end)
             /max(case when a.inst=b.inst and name='gcs msgs received' then delta_dm_val else 0 end)))          "GCS msg process time1(ms)",
       decode(max(case when a.inst=c.inst and name='gcs msgs received' then delta_dm_val else 0 end), 0, 0,
             (max(case when a.inst=c.inst and name='gcs msgs process time(ms)' then delta_dm_val else 0 end)
             /max(case when a.inst=c.inst and name='gcs msgs received' then delta_dm_val else 0 end)))          "GCS msg process time2(ms)",
       decode(max(case when a.inst=b.inst and name='ges msgs received' then delta_dm_val else 0 end), 0, 0,
             (max(case when a.inst=b.inst and name='ges msgs process time(ms)' then delta_dm_val else 0 end)
             /max(case when a.inst=b.inst and name='ges msgs received' then delta_dm_val else 0 end)))          "GES msg process time1(ms)",
       decode(max(case when a.inst=c.inst and name='ges msgs received' then delta_dm_val else 0 end), 0, 0,
             (max(case when a.inst=c.inst and name='ges msgs process time(ms)' then delta_dm_val else 0 end)
             /max(case when a.inst=c.inst and name='ges msgs received' then delta_dm_val else 0 end)))          "GES msg process time2(ms)",
       decode(max(case when a.inst=b.inst and name='messages sent directly' then delta_dm_val else 0 end) +
              max(case when a.inst=b.inst and name='messages sent indirectly' then delta_dm_val else 0 end) +
              max(case when a.inst=b.inst and name='messages flow controlled' then delta_dm_val else 0 end), 0, 0,
             (max(case when a.inst=b.inst and name='messages sent directly'   then delta_dm_val else 0 end)
            /(max(case when a.inst=b.inst and name='messages sent directly' then delta_dm_val else 0 end) +
              max(case when a.inst=b.inst and name='messages sent indirectly' then delta_dm_val else 0 end) +
              max(case when a.inst=b.inst and name='messages flow controlled' then delta_dm_val else 0 end)))
             ) "direct sent msg1(%)",
       decode(max(case when a.inst=c.inst and name='messages sent directly' then delta_dm_val else 0 end) +
              max(case when a.inst=c.inst and name='messages sent indirectly' then delta_dm_val else 0 end) +
              max(case when a.inst=c.inst and name='messages flow controlled' then delta_dm_val else 0 end), 0, 0,
             (max(case when a.inst=c.inst and name='messages sent directly'   then delta_dm_val else 0 end)
            /(max(case when a.inst=c.inst and name='messages sent directly' then delta_dm_val else 0 end) +
              max(case when a.inst=c.inst and name='messages sent indirectly' then delta_dm_val else 0 end) +
              max(case when a.inst=c.inst and name='messages flow controlled' then delta_dm_val else 0 end)))
             ) "direct sent msg2(%)",
       decode(max(case when a.inst=b.inst and name='messages sent directly' then delta_dm_val else 0 end) +
              max(case when a.inst=b.inst and name='messages sent indirectly' then delta_dm_val else 0 end) +
              max(case when a.inst=b.inst and name='messages flow controlled' then delta_dm_val else 0 end), 0, 0,
             (max(case when a.inst=b.inst and name='messages sent indirectly' then delta_dm_val else 0 end)
            /(max(case when a.inst=b.inst and name='messages sent directly' then delta_dm_val else 0 end) +
              max(case when a.inst=b.inst and name='messages sent indirectly' then delta_dm_val else 0 end) +
              max(case when a.inst=b.inst and name='messages flow controlled' then delta_dm_val else 0 end)))
             ) "indirect sent msg1(%)",
       decode(max(case when a.inst=c.inst and name='messages sent directly' then delta_dm_val else 0 end) +
              max(case when a.inst=c.inst and name='messages sent indirectly' then delta_dm_val else 0 end) +
              max(case when a.inst=c.inst and name='messages flow controlled' then delta_dm_val else 0 end), 0, 0,
             (max(case when a.inst=c.inst and name='messages sent indirectly' then delta_dm_val else 0 end)
            /(max(case when a.inst=c.inst and name='messages sent directly' then delta_dm_val else 0 end) +
              max(case when a.inst=c.inst and name='messages sent indirectly' then delta_dm_val else 0 end) +
              max(case when a.inst=c.inst and name='messages flow controlled' then delta_dm_val else 0 end)))
             ) "indirect sent msg2(%)",
       decode(max(case when a.inst=b.inst and name='messages sent directly' then delta_dm_val else 0 end) +
              max(case when a.inst=b.inst and name='messages sent indirectly' then delta_dm_val else 0 end) +
              max(case when a.inst=b.inst and name='messages flow controlled' then delta_dm_val else 0 end), 0, 0,
             (max(case when a.inst=b.inst and name='messages flow controlled' then delta_dm_val else 0 end)
            /(max(case when a.inst=b.inst and name='messages sent directly' then delta_dm_val else 0 end) +
              max(case when a.inst=b.inst and name='messages sent indirectly' then delta_dm_val else 0 end) +
              max(case when a.inst=b.inst and name='messages flow controlled' then delta_dm_val else 0 end)))
             ) "flow controlled msg1(%)",
       decode(max(case when a.inst=c.inst and name='messages sent directly' then delta_dm_val else 0 end) +
              max(case when a.inst=c.inst and name='messages sent indirectly' then delta_dm_val else 0 end) +
              max(case when a.inst=c.inst and name='messages flow controlled' then delta_dm_val else 0 end), 0, 0,
             (max(case when a.inst=c.inst and name='messages flow controlled' then delta_dm_val else 0 end)
            /(max(case when a.inst=c.inst and name='messages sent directly' then delta_dm_val else 0 end) +
              max(case when a.inst=c.inst and name='messages sent indirectly' then delta_dm_val else 0 end) +
              max(case when a.inst=c.inst and name='messages flow controlled' then delta_dm_val else 0 end)))
             ) "flow controlled msg2(%)",
       max(case when a.inst=b.inst and name='messages sent directly'   then delta_dm_val else 0 end) "msg(direct)1",
       max(case when a.inst=c.inst and name='messages sent directly'   then delta_dm_val else 0 end) "msg(direct)2",
       max(case when a.inst=b.inst and name='messages sent indirectly' then delta_dm_val else 0 end) "msg(indirect)1",
       max(case when a.inst=c.inst and name='messages sent indirectly' then delta_dm_val else 0 end) "msg(indirect)2",
       max(case when a.inst=b.inst and name='messages flow controlled' then delta_dm_val else 0 end) "msg(controlled)1",
       max(case when a.inst=c.inst and name='messages flow controlled' then delta_dm_val else 0 end) "msg(controlled)2"
 from (
       select sy.stat_name,
              sy.snap_id,
              sy.instance_number inst,
              sn.end_interval_time,
              nvl(sy.value,0) - nvl(lag(sy.value) over( partition by sy.instance_number,sy.stat_name order by sy.snap_id ),0) delta_val,
              case when
                (extract (day    from sn.end_interval_time - sn.begin_interval_time) * 86400 +
                 extract (hour   from sn.end_interval_time - sn.begin_interval_time) *  3600 +
                 extract (minute from sn.end_interval_time - sn.begin_interval_time) *    60 +
                 extract (second from sn.end_interval_time - sn.begin_interval_time) ) > 0
                then
                   (nvl(sy.value,0) - nvl(lag(sy.value) over( partition by sy.instance_number,sy.stat_name order by sy.snap_id ),0))
                  /(extract (day    from sn.end_interval_time - sn.begin_interval_time) * 86400 +
                    extract (hour   from sn.end_interval_time - sn.begin_interval_time) *  3600 +
                    extract (minute from sn.end_interval_time - sn.begin_interval_time) *    60 +
                    extract (second from sn.end_interval_time - sn.begin_interval_time)
                   ) else null end delta,
              dm.name,
              nvl(dm.value-lag(dm.value) over( partition by dm.instance_number,dm.name order by dm.snap_id ),0) delta_dm_val,
              case when
                (extract (day    from sn.end_interval_time - sn.begin_interval_time) * 86400 +
                 extract (hour   from sn.end_interval_time - sn.begin_interval_time) *  3600 +
                 extract (minute from sn.end_interval_time - sn.begin_interval_time) *    60 +
                 extract (second from sn.end_interval_time - sn.begin_interval_time) ) > 0
                then
                   (nvl(dm.value,0) - nvl(lag(dm.value) over( partition by dm.instance_number,dm.name order by dm.snap_id ),0))
                  /(extract (day    from sn.end_interval_time - sn.begin_interval_time) * 86400 +
                    extract (hour   from sn.end_interval_time - sn.begin_interval_time) *  3600 +
                    extract (minute from sn.end_interval_time - sn.begin_interval_time) *    60 +
                    extract (second from sn.end_interval_time - sn.begin_interval_time) )
                else null end delta_dm,
              nvl(cbs.flushes,0)    - nvl(lag(cbs.flushes)    over( partition by cbs.instance_number order by cbs.snap_id ),0) delta_cbs,
              nvl(cus.flush1,0)     - nvl(lag(cus.flush1)     over( partition by cus.instance_number order by cus.snap_id),0) delta_cusf1,
              nvl(cus.flush10,0)    - nvl(lag(cus.flush10)    over( partition by cus.instance_number order by cus.snap_id),0) delta_cusf10,
              nvl(cus.flush100,0)   - nvl(lag(cus.flush100)   over( partition by cus.instance_number order by cus.snap_id),0) delta_cusf100,
              nvl(cus.flush1000,0)  - nvl(lag(cus.flush1000)  over( partition by cus.instance_number order by cus.snap_id),0) delta_cusf1000,
              nvl(cus.flush10000,0) - nvl(lag(cus.flush10000) over( partition by cus.instance_number order by cus.snap_id),0) delta_cusf10000
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
          and sn.instance_number in (select instance_number from dba_hist_parameter where parameter_name = 'instance_name' and snap_id = :snap_end and dbid = :dbid)
          and sn.dbid = :dbid
      ) a,
      (
        select instance_number inst, instance_name
          from (
                 select rownum rn, instance_number,value instance_name
                   from dba_hist_parameter
                  where parameter_name = 'instance_name'
                    and snap_id = :snap_end
                    and dbid = :dbid
                  order by instance_number
               )
         where rn = 1
      ) b,
      (
        select instance_number inst, instance_name
          from (
                 select rownum rn, instance_number,value instance_name
                   from dba_hist_parameter
                  where parameter_name = 'instance_name'
                    and snap_id = :snap_end
                    and dbid = :dbid
                  order by instance_number
               )
         where rn = 2
      ) c
 where snap_id > :snap_start
group by snap_id
order by snap_id
"""

rac_message_2node2 = """
select b.value||'-'||a.title title
  from (
         select 1  no, 'avg msg sent queue time(ms)'    title from dual union all
         select 2  no, 'avg msg snt q time on ksxp(ms)' title from dual union all
         select 3  no, 'avg msg rcvd queue time(ms)'    title from dual union all
         select 4  no, 'avg GCS msg process time(ms)'   title from dual union all
         select 5  no, 'avg GES msg process time(ms)'   title from dual union all
         select 6  no, 'direct sent msg(%)'             title from dual union all
         select 7  no, 'indirect sent msg(%)'           title from dual union all
         select 8  no, 'flow controlled msg(%)'         title from dual union all
         select 9  no, 'msg(direct)'                    title from dual union all
         select 10 no, 'msg(indirect)'                  title from dual union all
         select 11 no, 'msg(controlled)'                title from dual
       ) a,
       (
         select instance_number,value
           from dba_hist_parameter
          where parameter_name = 'instance_name'
            and snap_id = :snap_end
            and dbid = :dbid
       ) b
order by a.no, b.instance_number
"""

osstat_2node = """
select snap_id,
       max(to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss'))  dates,
       max(to_char(end_interval_time,'dd hh24:mi'))             dates2,
       max(case when a.inst=b.inst and stat_name='USER_TIME' then delta else 0 end)/100        "usr1(s)",
       max(case when a.inst=c.inst and stat_name='USER_TIME' then delta else 0 end)/100        "usr2(s)",
       max(case when a.inst=b.inst and stat_name='SYS_TIME'  then delta else 0 end)/100        "sys1(s)",
       max(case when a.inst=c.inst and stat_name='SYS_TIME'  then delta else 0 end)/100        "sys2(s)",
       max(case when a.inst=b.inst and stat_name='IOWAIT_TIME' then delta else 0 end)/100      "wio1(s)",
       max(case when a.inst=c.inst and stat_name='IOWAIT_TIME' then delta else 0 end)/100      "wio2(s)",
       max(case when a.inst=b.inst and stat_name='IDLE_TIME' then delta else 0 end)/100 - max(case when a.inst=b.inst and stat_name='IOWAIT_TIME' then delta else 0 end)/100 "idle1(s)",
       max(case when a.inst=c.inst and stat_name='IDLE_TIME' then delta else 0 end)/100 - max(case when a.inst=c.inst and stat_name='IOWAIT_TIME' then delta else 0 end)/100 "idle2(s)",
       decode(max(case when a.inst=b.inst and stat_name='USER_TIME' then delta else 0 end)
             +max(case when a.inst=b.inst and stat_name='SYS_TIME' then delta else 0 end)
             +max(case when a.inst=b.inst and stat_name='IDLE_TIME' then delta else 0 end),0,0,
            max(case when a.inst=b.inst and stat_name='USER_TIME' then delta else 0 end)
           /(max(case when a.inst=b.inst and stat_name='USER_TIME' then delta else 0 end)
            +max(case when a.inst=b.inst and stat_name='SYS_TIME' then delta else 0 end)
            +max(case when a.inst=b.inst and stat_name='IDLE_TIME' then delta else 0 end))) "usr1(%)",
       decode(max(case when a.inst=c.inst and stat_name='USER_TIME' then delta else 0 end)
             +max(case when a.inst=c.inst and stat_name='SYS_TIME' then delta else 0 end)
             +max(case when a.inst=c.inst and stat_name='IDLE_TIME' then delta else 0 end),0,0,
            max(case when a.inst=c.inst and stat_name='USER_TIME' then delta else 0 end)
           /(max(case when a.inst=c.inst and stat_name='USER_TIME' then delta else 0 end)
            +max(case when a.inst=c.inst and stat_name='SYS_TIME' then delta else 0 end)
            +max(case when a.inst=c.inst and stat_name='IDLE_TIME' then delta else 0 end))) "usr2(%)",
       decode(max(case when a.inst=b.inst and stat_name='USER_TIME' then delta else 0 end)
             +max(case when a.inst=b.inst and stat_name='SYS_TIME' then delta else 0 end)
             +max(case when a.inst=b.inst and stat_name='IDLE_TIME' then delta else 0 end),0,0,
            max(case when a.inst=b.inst and stat_name='SYS_TIME' then delta else 0 end)
           /(max(case when a.inst=b.inst and stat_name='USER_TIME' then delta else 0 end)
            +max(case when a.inst=b.inst and stat_name='SYS_TIME' then delta else 0 end)
            +max(case when a.inst=b.inst and stat_name='IDLE_TIME' then delta else 0 end))) "sys1(%)",
       decode(max(case when a.inst=c.inst and stat_name='USER_TIME' then delta else 0 end)
             +max(case when a.inst=c.inst and stat_name='SYS_TIME' then delta else 0 end)
             +max(case when a.inst=c.inst and stat_name='IDLE_TIME' then delta else 0 end),0,0,
            max(case when a.inst=c.inst and stat_name='SYS_TIME' then delta else 0 end)
           /(max(case when a.inst=c.inst and stat_name='USER_TIME' then delta else 0 end)
            +max(case when a.inst=c.inst and stat_name='SYS_TIME' then delta else 0 end)
            +max(case when a.inst=c.inst and stat_name='IDLE_TIME' then delta else 0 end))) "sys2(%)",
       decode(max(case when a.inst=b.inst and stat_name='USER_TIME' then delta else 0 end)
             +max(case when a.inst=b.inst and stat_name='SYS_TIME' then delta else 0 end)
             +max(case when a.inst=b.inst and stat_name='IDLE_TIME' then delta else 0 end),0,0,
            max(case when a.inst=b.inst and stat_name='IOWAIT_TIME' then delta else 0 end)
           /(max(case when a.inst=b.inst and stat_name='USER_TIME' then delta else 0 end)
            +max(case when a.inst=b.inst and stat_name='SYS_TIME' then delta else 0 end)
            +max(case when a.inst=b.inst and stat_name='IDLE_TIME' then delta else 0 end))) "wio1(%)",
       decode(max(case when a.inst=c.inst and stat_name='USER_TIME' then delta else 0 end)
             +max(case when a.inst=c.inst and stat_name='SYS_TIME' then delta else 0 end)
             +max(case when a.inst=c.inst and stat_name='IDLE_TIME' then delta else 0 end),0,0,
            max(case when a.inst=c.inst and stat_name='IOWAIT_TIME' then delta else 0 end)
           /(max(case when a.inst=c.inst and stat_name='USER_TIME' then delta else 0 end)
            +max(case when a.inst=c.inst and stat_name='SYS_TIME' then delta else 0 end)
            +max(case when a.inst=c.inst and stat_name='IDLE_TIME' then delta else 0 end))) "wio1(%)",
       decode(max(case when a.inst=b.inst and stat_name='USER_TIME' then delta else 0 end)
             +max(case when a.inst=b.inst and stat_name='SYS_TIME' then delta else 0 end)
             +max(case when a.inst=b.inst and stat_name='IDLE_TIME' then delta else 0 end),0,0,
            (max(case when a.inst=b.inst and stat_name='IDLE_TIME' then delta else 0 end)-max(case when a.inst=b.inst and stat_name='IOWAIT_TIME' then delta else 0 end))
           /(max(case when a.inst=b.inst and stat_name='USER_TIME' then delta else 0 end)
            +max(case when a.inst=b.inst and stat_name='SYS_TIME' then delta else 0 end)
            +max(case when a.inst=b.inst and stat_name='IDLE_TIME' then delta else 0 end))) "idle1(%)",
       decode(max(case when a.inst=c.inst and stat_name='USER_TIME' then delta else 0 end)
             +max(case when a.inst=c.inst and stat_name='SYS_TIME' then delta else 0 end)
             +max(case when a.inst=c.inst and stat_name='IDLE_TIME' then delta else 0 end),0,0,
            (max(case when a.inst=c.inst and stat_name='IDLE_TIME' then delta else 0 end)-max(case when a.inst=c.inst and stat_name='IOWAIT_TIME' then delta else 0 end))
           /(max(case when a.inst=c.inst and stat_name='USER_TIME' then delta else 0 end)
            +max(case when a.inst=c.inst and stat_name='SYS_TIME' then delta else 0 end)
            +max(case when a.inst=c.inst and stat_name='IDLE_TIME' then delta else 0 end))) "idle2(%)",
       decode(max(case when a.inst=b.inst and stat_name='USER_TIME' then delta else 0 end)
             +max(case when a.inst=b.inst and stat_name='SYS_TIME' then delta else 0 end)
             +max(case when a.inst=b.inst and stat_name='IDLE_TIME' then delta else 0 end),0,0,
            (1 -(max(case when a.inst=b.inst and stat_name='IDLE_TIME' then delta else 0 end)-max(case when a.inst=b.inst and stat_name='IOWAIT_TIME' then delta else 0 end))
           /(max(case when a.inst=b.inst and stat_name='USER_TIME' then delta else 0 end)
            +max(case when a.inst=b.inst and stat_name='SYS_TIME' then delta else 0 end)
            +max(case when a.inst=b.inst and stat_name='IDLE_TIME' then delta else 0 end)))) "cpu1(%)",
       decode(max(case when a.inst=c.inst and stat_name='USER_TIME' then delta else 0 end)
             +max(case when a.inst=c.inst and stat_name='SYS_TIME' then delta else 0 end)
             +max(case when a.inst=c.inst and stat_name='IDLE_TIME' then delta else 0 end),0,0,
            (1 -(max(case when a.inst=c.inst and stat_name='IDLE_TIME' then delta else 0 end)-max(case when a.inst=c.inst and stat_name='IOWAIT_TIME' then delta else 0 end))
           /(max(case when a.inst=c.inst and stat_name='USER_TIME' then delta else 0 end)
            +max(case when a.inst=c.inst and stat_name='SYS_TIME' then delta else 0 end)
            +max(case when a.inst=c.inst and stat_name='IDLE_TIME' then delta else 0 end)))) "cpu2(%)"
  from (
        select sn.snap_id,
               os.instance_number inst,
               sn.end_interval_time,
               os.stat_name,
               (nvl(value - lag(value) over ( partition by os.instance_number, stat_name order by os.snap_id), 0)) delta,
               os.value value
          from dba_hist_osstat os,
               dba_hist_snapshot sn
         where os.snap_id = sn.snap_id
           and os.instance_number = sn.instance_number
           and os.dbid = sn.dbid
           and sn.snap_id between :snap_start and :snap_end
           and sn.instance_number in (select instance_number from dba_hist_parameter where parameter_name = 'instance_name' and snap_id = :snap_end and dbid = :dbid)
           and sn.dbid = :dbid
       ) a,
       (
        select instance_number inst, instance_name
          from (
                 select rownum rn, instance_number,value instance_name
                   from dba_hist_parameter
                  where parameter_name = 'instance_name'
                    and snap_id = :snap_end
                    and dbid = :dbid
                  order by instance_number
               )
         where rn = 1
       ) b,
       (
        select instance_number inst, instance_name
          from (
                 select rownum rn, instance_number,value instance_name
                   from dba_hist_parameter
                  where parameter_name = 'instance_name'
                    and snap_id = :snap_end
                    and dbid = :dbid
                  order by instance_number
               )
         where rn = 2
       ) c
 where snap_id > :snap_start
 group by snap_id
 order by snap_id
"""

osstat_2node2 = """
select b.value||'-'||a.title title
  from (
         select 1  no, 'usr(s)'    title from dual union all
         select 2  no, 'sys(s)'    title from dual union all
         select 3  no, 'wio(s)'    title from dual union all
         select 4  no, 'idle(s)'   title from dual union all
         select 5  no, 'usr(%)'    title from dual union all
         select 6  no, 'sys(%)'    title from dual union all
         select 7  no, 'wio(%)'    title from dual union all
         select 8  no, 'idle(%)'   title from dual union all
         select 9  no, 'cpu(%)'    title from dual
       ) a,
       (
         select instance_number,value
           from dba_hist_parameter
          where parameter_name = 'instance_name'
            and snap_id = :snap_end
            and dbid = :dbid
       ) b
order by a.no, b.instance_number
"""

time_model_2node = """
select snap_id,
       max(to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss'))  dates,
       max(to_char(end_interval_time,'dd hh24:mi'))             dates2,
       sum(case when a.inst=b.inst and stat_name='DB time' then delta/1000000 else 0 end) "DB time1",
       sum(case when a.inst=c.inst and stat_name='DB time' then delta/1000000 else 0 end) "DB time2",
       sum(case when a.inst=b.inst and stat_name='DB CPU'  then delta/1000000 else 0 end) "DB CPU1",
       sum(case when a.inst=c.inst and stat_name='DB CPU'  then delta/1000000 else 0 end) "DB CPU2",
       sum(case when a.inst=b.inst and stat_name='sql execute elapsed time' then delta/1000000 else 0 end) "sql execute elapsed1",
       sum(case when a.inst=c.inst and stat_name='sql execute elapsed time' then delta/1000000 else 0 end) "sql execute elapsed2",
       sum(case when a.inst=b.inst and stat_name='DB CPU'then pct else 0 end)             "DB CPU1(%)",
       sum(case when a.inst=c.inst and stat_name='DB CPU'then pct else 0 end)             "DB CPU2(%)",
       sum(case when a.inst=b.inst and stat_name='sql execute elapsed time' then pct else 0 end)           "sql execute1(%)",
       sum(case when a.inst=c.inst and stat_name='sql execute elapsed time' then pct else 0 end)           "sql execute2(%)"
  from (
         select snap_id,
                end_interval_time,
                inst,
                stat_name,
                delta,
                decode(sum(decode(stat_name, 'DB time', delta, 0)) over ( partition by snap_id, inst ), 0, 0,
                       delta/sum(decode(stat_name, 'DB time', delta, 0)) over ( partition by snap_id, inst )) pct
          from (
                select sn.snap_id,
                       sn.end_interval_time,
                       st.instance_number inst,
                       st.stat_name,
                      (nvl(value - lag(value) over ( partition by st.instance_number, st.stat_name order by st.snap_id), 0)) delta
                  from dba_hist_sys_time_model st,
                       dba_hist_snapshot sn
                 where st.snap_id         = sn.snap_id
                   and st.instance_number = sn.instance_number
                   and st.dbid            = sn.dbid
                   and sn.snap_id between :snap_start and :snap_end
                   and sn.instance_number in (select instance_number from dba_hist_parameter where parameter_name = 'instance_name' and snap_id = :snap_end and dbid = :dbid)
                   and sn.dbid = :dbid
               )
       ) a,
       (
        select instance_number inst, instance_name
          from (
                 select rownum rn, instance_number,value instance_name
                   from dba_hist_parameter
                  where parameter_name = 'instance_name'
                    and snap_id = :snap_end
                    and dbid = :dbid
                  order by instance_number
               )
         where rn = 1
       ) b,
       (
        select instance_number inst, instance_name
          from (
                 select rownum rn, instance_number,value instance_name
                   from dba_hist_parameter
                  where parameter_name = 'instance_name'
                    and snap_id = :snap_end
                    and dbid = :dbid
                  order by instance_number
               )
         where rn = 2
       ) c
 where snap_id > :snap_start
 group by snap_id
 order by snap_id
"""

time_model_2node2 = """
select b.value||'-'||a.title title
  from (
         select 1  no, 'DB time'             title from dual union all
         select 2  no, 'DB CPU'              title from dual union all
         select 3  no, 'sql execute elapsed' title from dual union all
         select 4  no, 'DB CPU(%)'           title from dual union all
         select 5  no, 'sql execute(%)'      title from dual
       ) a,
       (
         select instance_number,value
           from dba_hist_parameter
          where parameter_name = 'instance_name'
            and snap_id = :snap_end
            and dbid = :dbid
       ) b
order by a.no, b.instance_number
"""