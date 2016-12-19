--views
CREATE VIEW adbmgr.host AS
  SELECT
    hostname AS name,
    hostuser AS user,
    hostport AS port,
    CASE hostproto
      WHEN 's' THEN 'ssh'::text
      WHEN 't' THEN 'telnet'::text
    END AS protocol,
    hostagentport  AS agentport,
    hostaddr AS address,
    hostpghome AS pghome
  FROM pg_catalog.mgr_host order by 1;

CREATE VIEW adbmgr.parm AS
SELECT
	parmtype		AS	type,
	parmname		AS	name,
	parmvalue		AS	value,
	parmcontext		AS	context,
	parmvartype		AS	vartype,
	parmunit		AS	unit,
	parmminval		AS	minval,
	parmmaxval		AS	maxval,
	parmenumval		AS	enumval
FROM pg_catalog.mgr_parm;

CREATE VIEW adbmgr.updateparm AS
SELECT
	updateparmnodename		AS	nodename,
	CASE updateparmnodetype
		WHEN 'c' THEN 'coordinator'::text
		WHEN 'd' THEN 'datanode master'::text
		WHEN 'b' THEN 'datanode slave'::text
		WHEN 'n' THEN 'datanode extra'::text
		WHEN 'g' THEN 'gtm master'::text
		WHEN 'p' THEN 'gtm slave'::text
		WHEN 'e' THEN 'gtm extra'::text
		WHEN 'G' THEN 'gtm master|slave|extra'::text
		WHEN 'D' THEN 'datanode master|slave|extra'::text
	END AS nodetype,
	updateparmkey			AS	key,
	updateparmvalue			AS	value
FROM pg_catalog.mgr_updateparm order by 1,2;

CREATE VIEW adbmgr.node AS
  SELECT
    mgrnode.nodename    AS  name,
    hostname   AS  host,
    CASE mgrnode.nodetype
      WHEN 'g' THEN 'gtm master'::text
      WHEN 'p' THEN 'gtm slave'::text
      WHEN 'e' THEN 'gtm extra'::text
      WHEN 'c' THEN 'coordinator'::text
      WHEN 's' THEN 'coordinator slave'::text
      WHEN 'd' THEN 'datanode master'::text
      WHEN 'b' THEN 'datanode slave'::text
      WHEN 'n' THEN 'datanode extra'::text
    END AS type,
    node_alise.nodename AS mastername,
    mgrnode.nodeport    AS  port,
    mgrnode.nodesync    AS  sync,
    mgrnode.nodepath    AS  path,
    mgrnode.nodeinited  AS  initialized,
    mgrnode.nodeincluster AS incluster
  FROM pg_catalog.mgr_node AS mgrnode LEFT JOIN pg_catalog.mgr_host ON mgrnode.nodehost = pg_catalog.mgr_host.oid LEFT JOIN pg_catalog.mgr_node AS node_alise
  ON node_alise.oid = mgrnode.nodemasternameoid order by 1,3;

--monitor all
CREATE VIEW adbmgr.monitor_all AS
        select * from mgr_monitor_all() order by 1,2;

--init all
CREATE VIEW adbmgr.initall AS
	SELECT 'init gtm master' AS "operation type",* FROM mgr_init_gtm_master(NULL)
	UNION ALL
	SELECT 'start gtm master' AS "operation type", * FROM mgr_start_gtm_master(NULL)
	UNION ALL
	SELECT 'init gtm slave' AS "operation type",* FROM mgr_init_gtm_slave(NULL)
	UNION ALL
	SELECT 'start gtm slave' AS "operation type", * FROM mgr_start_gtm_slave(NULL)
	UNION ALL
	SELECT 'init gtm extra' AS "operation type",* FROM mgr_init_gtm_extra(NULL)
	UNION ALL
	SELECT 'start gtm extra' AS "operation type", * FROM mgr_start_gtm_extra(NULL)
	UNION ALL
	SELECT 'init coordinator' AS "operation type",* FROM mgr_init_cn_master(NULL)
	UNION ALL
	SELECT 'start coordinator' AS "operation type", * FROM mgr_start_cn_master(NULL)
	UNION ALL
	SELECT 'init datanode master' AS "operation type", * FROM mgr_init_dn_master(NULL)
	UNION ALL
	SELECT 'start datanode master' AS "operation type", * FROM mgr_start_dn_master(NULL)
	UNION ALL
	SELECT 'init datanode slave' AS "operation type", * FROM mgr_init_dn_slave_all()
	UNION ALL
	SELECT 'start datanode slave' AS "operation type", * FROM mgr_start_dn_slave(NULL)
	UNION ALL
	SELECT 'init datanode extra' AS "operation type", * FROM mgr_init_dn_extra_all()
	UNION ALL
	SELECT 'start datanode extra' AS "operation type", * FROM mgr_start_dn_extra(NULL)
	UNION ALL
	SELECT 'config coordinator' AS "operation type", * FROM mgr_configure_nodes_all(NULL);

--start gtm all
CREATE VIEW adbmgr.start_gtm_all AS
	SELECT 'start gtm master' AS "operation type", * FROM mgr_start_gtm_master(NULL)
	UNION all
	SELECT 'start gtm slave' AS "operation type", * FROM mgr_start_gtm_slave(NULL)
	UNION all
	SELECT 'start gtm extra' AS "operation type", * FROM mgr_start_gtm_extra(NULL);
--stop gtm all
CREATE VIEW adbmgr.stop_gtm_all AS
	SELECT 'stop gtm extra' AS "operation type", * FROM mgr_stop_gtm_extra(NULL)
	UNION all
	SELECT 'stop gtm slave' AS "operation type", * FROM mgr_stop_gtm_slave(NULL)
	UNION all
	SELECT 'stop gtm master' AS "operation type", * FROM mgr_stop_gtm_master(NULL);
--stop gtm all -m f
CREATE VIEW adbmgr.stop_gtm_all_f AS
	SELECT 'stop gtm extra' AS "operation type", * FROM mgr_stop_gtm_extra_f(NULL)
	UNION all
	SELECT 'stop gtm slave' AS "operation type", * FROM mgr_stop_gtm_slave_f(NULL)
	UNION all
	SELECT 'stop gtm master' AS "operation type", * FROM mgr_stop_gtm_master_f(NULL);
--stop gtm all -m i
CREATE VIEW adbmgr.stop_gtm_all_i AS
	SELECT 'stop gtm extra' AS "operation type", * FROM mgr_stop_gtm_extra_i(NULL)
	UNION all
	SELECT 'stop gtm slave' AS "operation type", * FROM mgr_stop_gtm_slave_i(NULL)
	UNION all
	SELECT 'stop gtm master' AS "operation type", * FROM mgr_stop_gtm_master_i(NULL);
--init datanode all
CREATE VIEW adbmgr.initdatanodeall AS
    SELECT 'init datanode master' AS "operation type",* FROM mgr_init_dn_master(NULL)
    UNION all
    SELECT 'init datanode slave' AS "operation type", * FROM mgr_init_dn_slave_all()
    UNION all
    SELECT 'init datanode extra' AS "operation type", * FROM mgr_init_dn_extra_all();
--start datanode all
CREATE VIEW adbmgr.start_datanode_all AS
    SELECT 'start datanode master' AS "operation type", * FROM mgr_start_dn_master(NULL)
    UNION all
    SELECT 'start datanode slave' AS "operation type", * FROM mgr_start_dn_slave(NULL)
    UNION all
    SELECT 'start datanode extra' AS "operation type", * FROM mgr_start_dn_extra(NULL);
--start all
CREATE VIEW adbmgr.startall AS
    SELECT 'start gtm master' AS "operation type", * FROM mgr_start_gtm_master(NULL)
    UNION all
    SELECT 'start gtm slave' AS "operation type", * FROM mgr_start_gtm_slave(NULL)
    UNION all
    SELECT 'start gtm extra' AS "operation type", * FROM mgr_start_gtm_extra(NULL)
    UNION all
    SELECT 'start coordinator' AS "operation type", * FROM mgr_start_cn_master(NULL)
    UNION all
    SELECT 'start datanode master' AS "operation type", * FROM mgr_start_dn_master(NULL)
    UNION all
    SELECT 'start datanode slave' AS "operation type", * FROM mgr_start_dn_slave(NULL)
    UNION all
    SELECT 'start datanode extra' AS "operation type", * FROM mgr_start_dn_extra(NULL);
--stop datanode all
CREATE VIEW adbmgr.stop_datanode_all AS
    SELECT 'stop datanode extra' AS "operation type", * FROM mgr_stop_dn_extra(NULL)
    UNION all
    SELECT 'stop datanode slave' AS "operation type", * FROM mgr_stop_dn_slave(NULL)
    UNION all
    SELECT 'stop datanode master' AS "operation type", * FROM mgr_stop_dn_master(NULL);

CREATE VIEW adbmgr.stop_datanode_all_f AS
    SELECT 'stop datanode extra' AS "operation type", * FROM mgr_stop_dn_extra_f(NULL)
    UNION all
    SELECT 'stop datanode slave' AS "operation type", * FROM mgr_stop_dn_slave_f(NULL)
    UNION all
    SELECT 'stop datanode master' AS "operation type", * FROM mgr_stop_dn_master_f(NULL);

CREATE VIEW adbmgr.stop_datanode_all_i AS
    SELECT 'stop datanode extra' AS "operation type", * FROM mgr_stop_dn_extra_i(NULL)
    UNION all
    SELECT 'stop datanode slave' AS "operation type", * FROM mgr_stop_dn_slave_i(NULL)
    UNION all
    SELECT 'stop datanode master' AS "operation type", * FROM mgr_stop_dn_master_i(NULL);

--stop all
CREATE VIEW adbmgr.stopall AS
    SELECT 'stop datanode extra' AS "operation type", * FROM mgr_stop_dn_extra(NULL)
    UNION all
    SELECT 'stop datanode slave' AS "operation type", * FROM mgr_stop_dn_slave(NULL)
    UNION all
    SELECT 'stop datanode master' AS "operation type", * FROM mgr_stop_dn_master(NULL)
    UNION all
    SELECT 'stop coordinator' AS "operation type", * FROM mgr_stop_cn_master(NULL)
    UNION all
    SELECT 'stop gtm extra' AS "operation type", * FROM mgr_stop_gtm_extra(NULL)
    UNION all
    SELECT 'stop gtm slave' AS "operation type", * FROM mgr_stop_gtm_slave(NULL)
    UNION all
    SELECT 'stop gtm master' AS "operation type", * FROM mgr_stop_gtm_master(NULL);

CREATE VIEW adbmgr.stopall_f AS
    SELECT 'stop datanode extra' AS "operation type", * FROM mgr_stop_dn_extra_f(NULL)
    UNION all
    SELECT 'stop datanode slave' AS "operation type", * FROM mgr_stop_dn_slave_f(NULL)
    UNION all
    SELECT 'stop datanode master' AS "operation type", * FROM mgr_stop_dn_master_f(NULL)
    UNION all
    SELECT 'stop coordinator' AS "operation type", * FROM mgr_stop_cn_master_f(NULL)
    UNION all
    SELECT 'stop gtm extra' AS "operation type", * FROM mgr_stop_gtm_extra_f(NULL)
    UNION all
    SELECT 'stop gtm slave' AS "operation type", * FROM mgr_stop_gtm_slave_f(NULL)
    UNION all
    SELECT 'stop gtm master' AS "operation type", * FROM mgr_stop_gtm_master_f(NULL);

CREATE VIEW adbmgr.stopall_i AS
    SELECT 'stop datanode extra' AS "operation type", * FROM mgr_stop_dn_extra_i(NULL)
    UNION all
    SELECT 'stop datanode slave' AS "operation type", * FROM mgr_stop_dn_slave_i(NULL)
    UNION all
    SELECT 'stop datanode master' AS "operation type", * FROM mgr_stop_dn_master_i(NULL)
    UNION all
    SELECT 'stop coordinator' AS "operation type", * FROM mgr_stop_cn_master_i(NULL)
    UNION all
    SELECT 'stop gtm extra' AS "operation type", * FROM mgr_stop_gtm_extra_i(NULL)
    UNION all
    SELECT 'stop gtm slave' AS "operation type", * FROM mgr_stop_gtm_slave_i(NULL)
    UNION all
    SELECT 'stop gtm master' AS "operation type", * FROM mgr_stop_gtm_master_i(NULL);

-- for ADB monitor host page: get all host various parameters.
CREATE VIEW adbmgr.get_all_host_parm AS
    select 
        mgh.hostname AS hostName, 
        mgh.hostaddr AS ip,
        nh.mh_cpu_core_available AS cpu,
        round(c.mc_cpu_usage::numeric, 1) AS cpuRate,
        round((m.mm_total/1024.0/1024.0/1024.0)::numeric, 1) AS mem,
        round(m.mm_usage::numeric, 1) AS memRate,
        round((d.md_total/1024.0/1024.0/1024.0)::numeric, 1) AS disk,
        round(((d.md_used/d.md_total::float) * 100)::numeric, 1) AS diskRate,
        mtc.mt_emergency_threshold AS cpu_threshold,
        mtm.mt_emergency_threshold AS mem_threshold,
        mtd.mt_emergency_threshold AS disk_threshold
    from 
        mgr_host mgh,
        
        (
            select * from (
                            select *, (ROW_NUMBER()OVER(PARTITION BY T.host_oid ORDER BY T.mc_timestamptz desc)) as rm
                            from monitor_cpu t
                           ) tt where tt.rm = 1
        ) c,
        
        (
            select * from (
                            select *,(ROW_NUMBER()OVER(PARTITION BY T.host_oid ORDER BY T.mm_timestamptz desc)) as rm
                            from monitor_mem t
                           ) tt where tt.rm =1
        ) m,
        
        (
            select * from (
                            select *, (ROW_NUMBER()OVER(PARTITION BY T.host_oid ORDER BY T.md_timestamptz desc)) as rm
                            from monitor_disk t
                           ) tt where tt.rm = 1
        ) d,
        
        (
            select * from (
                            select *, (ROW_NUMBER()OVER(PARTITION BY T.host_oid ORDER BY T.mh_current_time desc)) as rm
                            from monitor_host t
                           ) tt where tt.rm = 1
        ) nh,
        
        (
            select * from monitor_host_threshold where mt_type = 1
        ) mtc,
        
        (
            select * from monitor_host_threshold where mt_type = 2
        ) mtm,
        
        (
            select * from monitor_host_threshold where mt_type = 3
        )mtd
    where mgh.oid = c.host_oid and
        c.host_oid = m.host_oid and
        m.host_oid = d.host_oid and
        d.host_oid = nh.host_oid;

-- for ADB monitor host page: get specific host various parameters.
CREATE VIEW adbmgr.get_spec_host_parm AS
    select mgh.hostname as hostname,
       mgh.hostaddr as ip,
       mh.mh_system as os,
       mh.mh_cpu_core_available as cpu,
       round((mm.mm_total/1024.0/1024.0/1024.0)::numeric, 1) as mem,
       round((md.md_total/1024.0/1024.0/1024.0)::numeric, 1) as disk,
       mh.mh_current_time - (mh.mh_seconds_since_boot || 'sec')::interval as createtime,
       mh_run_state as state,
       round(mc.mc_cpu_usage::numeric, 1) as cpurate,
       round(mm.mm_usage::numeric, 1) as memrate,
       round(((md.md_used/md.md_total::float) * 100)::numeric, 1) as diskRate,
       round((md.md_io_read_bytes/1024.0/1024.0)/(md.md_io_read_time/1000.0), 1) as ioreadps,
       round((md.md_io_write_bytes/1024.0/1024.0)/(md.md_io_write_time/1000.0), 1) as iowriteps,
       round(mn.mn_recv/1024.0,1) as netinps,
       round(mn.mn_sent/1024.0,1) as netoutps,
       mh.mh_seconds_since_boot as runtime
    from mgr_host mgh,
    
        (
            select * from (
                            select *, (ROW_NUMBER()OVER(PARTITION BY t.host_oid ORDER BY t.mh_current_time desc)) as rm
                            from monitor_host t
                           ) tt where tt.rm = 1
        ) mh,
        
        (
            select * from (
                            select *,(ROW_NUMBER()OVER(PARTITION BY t.host_oid ORDER BY t.mc_timestamptz desc)) as rm
                            from monitor_cpu t
                           ) tt where tt.rm = 1
        ) mc,
        
        (
            select * from (
                            select *,(ROW_NUMBER()OVER(PARTITION BY t.host_oid ORDER BY t.mm_timestamptz desc)) as rm
                            from monitor_mem t
                           ) tt where tt.rm =1
        ) mm,
        
        (
            select * from (
                            select *, (ROW_NUMBER()OVER(PARTITION BY t.host_oid ORDER BY t.md_timestamptz desc)) as rm
                            from monitor_disk t
                           ) tt where tt.rm = 1
        ) md,
        
        (
            select * from (
                            select *, (ROW_NUMBER()OVER(PARTITION BY t.host_oid ORDER BY t.mn_timestamptz desc)) as rm
                            from monitor_net t
                           ) tt where tt.rm = 1
        ) mn
    where mgh.oid = mh.host_oid and
        mh.host_oid = mc.host_oid and
        mc.host_oid = mm.host_oid and
        mm.host_oid = md.host_oid and
        md.host_oid = mn.host_oid;

-- for ADB monitor host page: get cpu, memory, i/o and net info for specific time period.
CREATE OR REPLACE FUNCTION pg_catalog.get_host_history_usage(hostname text, i int)
    RETURNS table 
    (
        recordtimes timestamptz,
        cpuuseds numeric,
        memuseds numeric,
        ioreadps numeric,
        iowriteps numeric,
        netinps numeric,
        netoutps numeric
    )
    AS 
    $$
    
    select c.mc_timestamptz as recordtimes,
           round(c.mc_cpu_usage::numeric, 1) as cpuuseds,
           round(m.mm_usage::numeric, 1) as memuseds,
           round((d.md_io_read_bytes/1024.0/1024.0)/(d.md_io_read_time/1000.0), 1) as ioreadps,
           round((d.md_io_write_bytes/1024.0/1024.0)/(d.md_io_write_time/1000.0), 1) as iowriteps,
           round(n.mn_recv/1024.0,1) as netinps,
           round(n.mn_sent/1024.0,1) as netoutps
    from monitor_cpu c left join monitor_mem  m on(c.host_oid = m.host_oid and c.mc_timestamptz = m.mm_timestamptz)
                       left join monitor_disk d on(c.host_oid = d.host_oid and c.mc_timestamptz = d.md_timestamptz)
                       left join monitor_net  n on(c.host_oid = n.host_oid and c.mc_timestamptz = n.mn_timestamptz)
                       left join monitor_host h on(c.host_oid = h.host_oid and c.mc_timestamptz = h.mh_current_time)
                       left join mgr_host   mgr on(c.host_oid = mgr.oid),

                       (select mh.host_oid, mh.mh_current_time 
                        from monitor_host mh 
                        order by mh.mh_current_time desc 
                        limit 1) as temp
    where c.mc_timestamptz  >  temp.mh_current_time - case $2 
                                                      when 0 then interval '1 hour'
                                                      when 1 then interval '1 day'
                                                      when 2 then interval '7 day'
                                                      end and
          mgr.hostname = $1;
    $$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

-- for ADB monitor host page: The names of all the nodes on a host
CREATE OR REPLACE FUNCTION pg_catalog.get_all_nodename_in_spec_host(hostname text)
    RETURNS table (all_nodename name)
    AS 
    $$

    select nodename as all_node_name
    from mgr_node
    where nodehost = (select oid from mgr_host where hostname = $1);

    $$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

--make view for 12 hours data for  tps, qps, connectnum, dbsize, indexsize
CREATE VIEW adbmgr.monitor_cluster_fouritem_v AS 
SELECT a.monitor_databasetps_time::timestamptz(0) AS time , a.tps, a.qps, b.connectnum, b.dbsize, b.indexsize
FROM
	(SELECT monitor_databasetps_time, sum(monitor_databasetps_tps) AS tps , 
	sum(monitor_databasetps_qps) AS qps  , row_number() over (PARTITION BY 1) FROM (SELECT 
	distinct(monitor_databasetps_dbname),  monitor_databasetps_time, monitor_databasetps_tps,
	monitor_databasetps_qps, monitor_databasetps_runtime FROM monitor_databasetps WHERE 
	monitor_databasetps_time > (SELECT max(monitor_databasetps_time) - interval '12 hour' FROM 
	monitor_databasetps ) ORDER BY 2 ASC)AS a  GROUP BY monitor_databasetps_time) AS a
JOIN 
	(SELECT  monitor_databaseitem_time, sum(monitor_databaseitem_connectnum) AS connectnum, 
	(sum(monitor_databaseitem_dbsize)/1024.0)::numeric(18,2) AS dbsize , (sum(monitor_databaseitem_indexsize)/1024.0)::numeric(18,2) AS indexsize, row_number() over (PARTITION BY 1) FROM (SELECT 
	distinct(monitor_databaseitem_dbname),  monitor_databaseitem_time, monitor_databaseitem_connectnum, 
	monitor_databaseitem_dbsize, monitor_databaseitem_indexsize FROM monitor_databaseitem WHERE monitor_databaseitem_time > (SELECT 
	max(monitor_databaseitem_time) - interval '12 hour' FROM monitor_databaseitem ) ORDER BY 2 ASC) AS b 
GROUP BY monitor_databaseitem_time ) AS b on a.row_number = b.row_number;


--make function for 12 hours data for tps qps connectnum dbsize indexsize
CREATE OR REPLACE FUNCTION pg_catalog.monitor_cluster_fouritem_func()
	RETURNS TABLE
	(
		recordtime timestamptz(0),
		tps  bigint,
		qps  bigint,
		connectnum bigint,
		dbsize numeric(18,2),
		indexsize numeric(18,2)
	)
	AS 
	$$
	SELECT time as recordtime, tps, qps, connectnum, dbsize, indexsize
	FROM 
		adbmgr.monitor_cluster_fouritem_v
	$$
	LANGUAGE SQL
	IMMUTABLE
	RETURNS NULL ON NULL INPUT;	
		
--get first page first line values
CREATE VIEW adbmgr.monitor_cluster_firstline_v
AS
	SELECT * from 
	(SELECT monitor_databasetps_time::timestamptz(0) AS time, sum(monitor_databasetps_tps) AS tps , sum(monitor_databasetps_qps) AS qps, 
		sum(monitor_databaseitem_connectnum) AS connectnum , (sum(monitor_databaseitem_dbsize)/1024.0)::numeric(18,2) AS dbsize, (sum(monitor_databaseitem_indexsize)/1024.0)::numeric(18,2) AS indexsize, max(monitor_databasetps_runtime) as runtime
	FROM 
		(SELECT  tps.monitor_databasetps_time, tps.monitor_databasetps_tps,  tps.monitor_databasetps_qps, tps.monitor_databasetps_runtime,
					b.monitor_databaseitem_connectnum , b.monitor_databaseitem_dbsize, b.monitor_databaseitem_indexsize
				FROM (SELECT * ,(ROW_NUMBER()OVER(PARTITION BY monitor_databasetps_dbname ORDER BY 
									monitor_databasetps_time desc ))AS tc  
							FROM monitor_databasetps
							)AS tps
			JOIN 
			(SELECT monitor_databaseitem_dbname,
					monitor_databaseitem_connectnum,  monitor_databaseitem_dbsize, monitor_databaseitem_indexsize
			 FROM (SELECT * ,(ROW_NUMBER()OVER(PARTITION BY monitor_databaseitem_dbname ORDER BY 
									monitor_databaseitem_time desc ))AS tc  FROM monitor_databaseitem)AS item  WHERE item.tc =1
			) AS b
			on tps.monitor_databasetps_dbname = b.monitor_databaseitem_dbname 
			WHERE   tps.tc =1
		) AS c 
		GROUP BY  monitor_databasetps_time
	) AS d
	join 
	( SELECT (sum(md_total/1024.0/1024)/1024)::numeric(18,2) AS md_total FROM (SELECT  host_oid,md_timestamptz, md_total, (ROW_NUMBER()OVER(PARTITION BY host_oid  ORDER BY  md_timestamptz desc ))AS tc   from monitor_disk) AS d WHERE tc =1
	) AS e
	on 1=1;


--make function to get tps and qps for given dbname time and time interval
CREATE OR REPLACE FUNCTION pg_catalog.monitor_databasetps_func(in text, in timestamptz, in int)
		RETURNS TABLE
	(
		recordtime timestamptz(0),
		tps int,
		qps int
	)
	AS 
		$$
	SELECT monitor_databasetps_time::timestamptz(0) AS recordtime,
				monitor_databasetps_tps   AS tps,
				monitor_databasetps_qps   AS qps
	FROM 
				monitor_databasetps
	WHERE monitor_databasetps_dbname = $1 and monitor_databasetps_time >= $2 and monitor_databasetps_time <= $2 + case $3
				when 0 then interval '1 hour'
				when 1 then interval '24 hour'
				end;
	$$
		LANGUAGE SQL
	IMMUTABLE
	RETURNS NULL ON NULL INPUT;	
--to show all database tps, qps, runtime at current_time
 CREATE VIEW adbmgr.monitor_all_dbname_tps_qps_runtime_v
 AS 
 SELECT distinct(monitor_databasetps_dbname) as databasename, monitor_databasetps_tps as tps, monitor_databasetps_qps as qps, monitor_databasetps_runtime as runtime FROM monitor_databasetps WHERE monitor_databasetps_time=(SELECT max(monitor_databasetps_time) FROM monitor_databasetps) ORDER BY 1 ASC;
--show cluster summary at current_time
CREATE VIEW adbmgr.monitor_cluster_summary_v
AS
SELECT (SUM(monitor_databaseitem_dbsize)/1024.0)::numeric(18,2) AS dbsize,
	CASE AVG(monitor_databaseitem_archivemode::int)
	 WHEN 0 THEN false
	 ELSE true
	 END AS archivemode,
	CASE AVG(monitor_databaseitem_autovacuum::int)
	 WHEN 0 THEN false
	 ELSE true
	 END AS autovacuum,
	AVG(monitor_databaseitem_heaphitrate)::numeric(18,2) AS heaphitrate,
	AVG(monitor_databaseitem_commitrate)::numeric(18,2) AS commitrate,
	AVG(monitor_databaseitem_dbage)::int AS dbage,
	SUM(monitor_databaseitem_connectnum) AS connectnum,
	AVG(monitor_databaseitem_standbydelay)::int AS standbydelay,
	SUM(monitor_databaseitem_locksnum) AS locksnum,
	SUM(monitor_databaseitem_longtransnum) AS longtransnum,
	SUM(monitor_databaseitem_idletransnum) AS idletransnum,
	SUM(monitor_databaseitem_preparenum) AS preparenum,
	SUM(monitor_databaseitem_unusedindexnum) AS unusedindexnum
from (SELECT * from monitor_databaseitem where monitor_databaseitem_time=(SELECT MAX(monitor_databaseitem_time) from monitor_databaseitem)) AS a;
--show database summary at current_time for given name
CREATE OR REPLACE FUNCTION pg_catalog.monitor_databasesummary_func(in name)
	RETURNS TABLE
	(
		dbsize 			numeric(18,2),
		archivemode		bool,
		autovacuum		bool,
		heaphitrate		numeric(18,2),
		commitrate		numeric(18,2),
		dbage			int,
		connectnum		int,
		standbydelay	int,
		locksnum		int,
		longtransnum	int,
		idletransnum	int,
		preparenum		int,
		unusedindexnum	int
	)
	AS 
		$$
	SELECT (monitor_databaseitem_dbsize/1024.0)::numeric(18,2) AS dbsize,
			 monitor_databaseitem_archivemode AS archivemode,
			 monitor_databaseitem_autovacuum  AS autovacuum,
			 monitor_databaseitem_heaphitrate::numeric(18,2) AS heaphitrate,
			 monitor_databaseitem_commitrate::numeric(18,2)  AS commitrate,
			 monitor_databaseitem_dbage       AS dbage,
			 monitor_databaseitem_connectnum  AS connectnum,
			 monitor_databaseitem_standbydelay AS standbydelay,
			 monitor_databaseitem_locksnum     AS locksnum,
			 monitor_databaseitem_longtransnum  AS longtransum,
			 monitor_databaseitem_idletransnum  AS idletransum,
			 monitor_databaseitem_preparenum    AS  preparenum,
			 monitor_databaseitem_unusedindexnum  AS unusedindexnum
	from (SELECT * from monitor_databaseitem where monitor_databaseitem_time=(SELECT 
		MAX(monitor_databaseitem_time) from monitor_databaseitem) and monitor_databaseitem_dbname = $1) AS a
	$$
		LANGUAGE SQL
	IMMUTABLE
	RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION pg_catalog.monitor_slowlog_count_func(in name, in timestamptz, in timestamptz)
		RETURNS bigint
	AS 
		$$
	SELECT count(*)
	FROM 
				monitor_slowlog
	WHERE slowlogdbname = $1 and slowlogtime >= $2 and slowlogtime < $3;
	$$
		LANGUAGE SQL
	IMMUTABLE
	RETURNS NULL ON NULL INPUT;
	
CREATE OR REPLACE FUNCTION pg_catalog.monitor_slowlog_func_page(in name, in timestamptz, in timestamptz, in int, in int)
		RETURNS TABLE
		(
			query text,
			dbuser name,
			singletime float4,
			totalnum int,
			queryplan text
		)
	AS 
		$$
	SELECT slowlogquery AS query,
				slowloguser   AS dbuser,
				slowlogsingletime AS singletime,
				slowlogtotalnum   AS totalnum,
				slowlogqueryplan  AS queryplan
	FROM 
				monitor_slowlog
	WHERE slowlogdbname = $1 and slowlogtime >= $2 and slowlogtime < $3 order by slowlogtime desc limit $4 offset $5;
	$$
		LANGUAGE SQL
	IMMUTABLE
	RETURNS NULL ON NULL INPUT;
--for cluster warning, monitor_dbthreshold systbl, see "typedef enum DbthresholdObject" 
--  in monitor_dbthreshold.c
--heaphitrate
insert into pg_catalog.monitor_host_threshold values(11, 0, 98, 95, 90);
insert into pg_catalog.monitor_host_threshold values(21, 0, 98, 95, 90);
--commitrate
insert into pg_catalog.monitor_host_threshold values(12, 0, 95, 90, 85);
insert into pg_catalog.monitor_host_threshold values(22, 0, 95, 90, 85);
--standbydelay
insert into pg_catalog.monitor_host_threshold values(13, 1, 200, 250, 300);
insert into pg_catalog.monitor_host_threshold values(23, 1, 1000, 2000, 3000);
--locks num
insert into pg_catalog.monitor_host_threshold values(14, 1, 40, 50, 60);
insert into pg_catalog.monitor_host_threshold values(24, 1, 50, 80, 120);
--connect num
insert into pg_catalog.monitor_host_threshold values(15, 1, 400, 500, 800);
insert into pg_catalog.monitor_host_threshold values(25, 1, 500, 800, 1500);
--long transaction, idle transaction
insert into pg_catalog.monitor_host_threshold values(16, 1, 25, 30, 50);
insert into pg_catalog.monitor_host_threshold values(26, 1, 30, 60, 80);
--unused index num
insert into pg_catalog.monitor_host_threshold values(17, 1, 0, 0, 0);
insert into pg_catalog.monitor_host_threshold values(27, 1, 100, 150, 200);
--tps timeinterval
insert into pg_catalog.monitor_host_threshold values(31, 1, 3, 0, 0);
--long transactions min time
insert into pg_catalog.monitor_host_threshold values(32, 1, 100, 0, 0);
--slow query min time
insert into pg_catalog.monitor_host_threshold values(33, 1, 2, 0, 0);
--get limit num slowlog from database cluster once time
insert into pg_catalog.monitor_host_threshold values(34, 1, 5, 0, 0);

-- for ADB monitor the topology in home page : get datanode node topology
CREATE VIEW adbmgr.get_datanode_node_topology AS
    select '{'|| ARRAY_TO_STRING || '}' as datanode_result
    from (
    select ARRAY_TO_STRING(
                            array(
                                    select case f.nodetype
                                           when 'd' then '"master"'
                                           when 'b' then '"slave"'
                                           when 'n' then '"extra"'
                                           end 
                                           || ':' || '{' || '"node_name"' || ':' || '"' || f.nodename || '"' || ',' 
                                                         || '"node_port"' || ':' ||        f.nodeport        || ','
                                                         || '"node_ip"'   || ':' || '"' || f.hostaddr || '"' ||
                                                     '}'
                                    from(
                                            select n.nodename,n.oid,n.nodetype,n.nodeport,n.nodemasternameoid, h.hostaddr
                                            from mgr_node n, mgr_host h
                                            where n.nodemasternameoid = '0' and 
                                                  n.nodename = x.nodename  and 
                                                  n.nodetype = 'd' and
                                                  h.oid = n.nodehost and
                                                  n.nodeincluster = true and
                                                  n.nodeinited = true
                                            
                                            union all
                                            
                                            select t2.nodename,t2.oid,t2.nodetype,t2.nodeport,t2.nodemasternameoid,t2.hostaddr
                                            from (
                                                    select n.nodename,n.oid,n.nodetype,n.nodeport,n.nodemasternameoid,h.hostaddr
                                                    from mgr_node n,mgr_host h
                                                    where n.nodemasternameoid = '0' and 
                                                          n.nodename = x.nodename and 
                                                          n.nodetype = 'd' and
                                                          h.oid = n.nodehost and
                                                          n.nodeincluster = true and
                                                          n.nodeinited = true
                                                ) t1
                                                left join 
                                                (
                                                    select n.nodename,n.oid,n.nodetype,n.nodeport,n.nodemasternameoid,h.hostaddr
                                                    from mgr_node n,mgr_host h
                                                    where h.oid = n.nodehost and
                                                          n.nodeincluster = true and
                                                          n.nodeinited = true
                                                ) t2
                                                on t1.oid = t2.nodemasternameoid and t2.nodetype in ('b','n')
                                        ) as f
                                ), ','
                        ) from (select nodename from mgr_node where nodetype = 'd') x
        ) r;

-- for ADB monitor the topology in home page : get coordinator node topology 
CREATE VIEW adbmgr.get_coordinator_node_topology AS
    select n.nodename AS node_name,
        n.nodeport AS node_port,
        h.hostaddr AS node_ip
    from mgr_node n, mgr_host h
    where n.nodeincluster = true and
        n.nodeinited = true and
        n.nodehost = h.oid and
        n.nodetype = 'c';

-- for ADB monitor the topology in home page : get agtm node topology 
CREATE VIEW adbmgr.get_agtm_node_topology AS
    select '{'|| ARRAY_TO_STRING || '}' as agtm_result
    from(
    select ARRAY_TO_STRING(
                            array(
                                    select case f.nodetype
                                        when 'g' then '"master"'
                                        when 'p' then '"slave"'
                                        when 'e' then '"extra"'
                                        end 
                                        || ':' || '{' || '"node_name"' || ':' || '"' || f.nodename || '"' || ',' 
                                                      || '"node_port"' || ':' ||        f.nodeport        || ','
                                                      || '"node_ip"'   || ':' || '"' || f.hostaddr || '"' ||
                                                '}'
                                    from(
                                        select n.nodename,n.oid,n.nodetype,n.nodeport,n.nodemasternameoid, h.hostaddr
                                        from mgr_node n, mgr_host h
                                        where n.nodeincluster = true and
                                              n.nodeinited = true and
                                              n.nodehost = h.oid and
                                              n.nodetype in ('g', 'p', 'e')
                                        ) f
                                ) -- end array
                            , ','
                        ) -- end ARRAY_TO_STRING
        ) r;

-- insert default values into monitor_host_threthold.
insert into pg_catalog.monitor_host_threshold values (1, 1, 90, 95, 99);
insert into pg_catalog.monitor_host_threshold values (2, 1, 85, 90, 95);
insert into pg_catalog.monitor_host_threshold values (3, 1, 80, 85, 90);
insert into pg_catalog.monitor_host_threshold values (4, 1, 2000, 3000, 4000);
insert into pg_catalog.monitor_host_threshold values (5, 1, 2000, 3000, 4000);
insert into pg_catalog.monitor_host_threshold values (6, 1, 3000, 4000, 5000);

-- update threshold value by type
create or replace function pg_catalog.update_threshold_value(type int, value1 int, value2 int, value3 int)
returns  void
as $$
update pg_catalog.monitor_host_threshold
set mt_warning_threshold = $2, mt_critical_threshold = $3, mt_emergency_threshold = $4
where mt_type = $1;
$$ language sql
VOLATILE
returns null on null input;

--get the threshold for specific type
create or replace function pg_catalog.get_threshold_type(type int)
returns text
as $$
    select '{' || row_string || '}' as show_threshold
    from (
           select case $1
                  when 1 then '"cpu_usage"'
                  when 2 then '"mem_usage"'
                  when 3 then '"disk_usage"'
                  when 4 then '"sent_net"'
                  when 5 then '"recv_net"'
                  when 6 then '"IOPS"'
                  when 11 then '"node_heaphit_rate"'
                  when 21 then '"cluster_heaphit_rate"'
                  when 12 then '"node_commit/rollback_rate"'
                  when 22 then '"cluster_commit/rollback_rate"'
                  when 13 then '"node_standy_delay"'
                  when 23 then '"cluster_standy_delay"'
                  when 14 then '"node_wait_locks"'
                  when 24 then '"cluster_wait_locks"'
                  when 15 then '"node_connect"'
                  when 25 then '"cluster_connect"'
                  when 16 then '"node_longtrnas/idletrans"'
                  when 26 then '"cluster_longtrnas/idletrans"'
                  when 27 then '"cluster_unused_indexs"'
                  END
                  || ':' || row_to_json as row_string
           from (
           
                   select row_to_json(t)
                   from (
                           select 
                               mt_warning_threshold AS "warning",
                               mt_critical_threshold AS "critical",
                               mt_emergency_threshold AS "emergency"
                           from monitor_host_threshold
                           where mt_type = $1
                        ) t
               )y
         )s;
    $$
language sql
IMMUTABLE
RETURNS NULL ON NULL INPUT;

--get the threshlod for all type
CREATE VIEW adbmgr.get_threshold_all_type AS
select mt_type as type, mt_direction as direction, mt_warning_threshold as warning, mt_critical_threshold as critical, mt_emergency_threshold as emergency
  from pg_catalog.monitor_host_threshold 
	where mt_type in (1,2,3,4,5,6) 
	   order by 1 asc;

--get the threshlod for all type
CREATE VIEW adbmgr.get_db_threshold_all_type
as 
 select tt1.mt_type as type, tt1.mt_direction as direction, tt1.mt_warning_threshold as node_warning, tt1.mt_critical_threshold as node_critical, 
			  tt1.mt_emergency_threshold as node_emergency,tt2.mt_warning_threshold as cluster_warning, 
				tt2.mt_critical_threshold as cluster_critical, tt2.mt_emergency_threshold as cluster_emergency 
from (select * from pg_catalog.monitor_host_threshold where mt_type in (11,12,13,14,15,16,17))as tt1  
   join  (select * from pg_catalog.monitor_host_threshold where mt_type in (21,22,23,24,25,26,27)) tt2 on tt1.mt_type +10 =tt2.mt_type 
	 order by 1 asc;

-- get all alarm info (host and DB)
create or replace FUNCTION pg_catalog.get_alarm_info_asc(starttime timestamp ,
                                                        endtime timestamp ,
                                                        search_text text default '',
                                                        alarm_level int default 0,
                                                        alarm_type int default 0,
                                                        alarm_status int default 0,
                                                        alarm_limit int default 20,
                                                        alarm_offset int default 0)
returns TABLE (
                alarm_level smallint,
                alarm_text text,
                alarm_type smallint,
                alarm_id oid,
                alarm_source text,
                alarm_time timestamptz,
                alarm_status smallint,
                alarm_resolve_timetz timestamptz,
                alarm_solution text)
as $$
select a.ma_alarm_level AS alarm_level,
       a.ma_alarm_text AS alarm_text,
       a.ma_alarm_type AS alarm_type,
       a.oid AS alarm_id,
       a.ma_alarm_source AS alarm_source,
       a.ma_alarm_timetz AS alarm_time,
       a.ma_alarm_status AS alarm_status,
       r.mr_resolve_timetz AS alarm_resolve_timetz,
       r.mr_solution AS alarm_solution
from monitor_alarm a left join monitor_resolve r on(a.oid = r.mr_alarm_oid)
where case $4 when 0 then 1::boolean else a.ma_alarm_level = $4 end and
      case $5 when 0 then 1::boolean else a.ma_alarm_type = $5 end and
      case $6 when 0 then 1::boolean else a.ma_alarm_status = $6 end and
      a.ma_alarm_text ~ $3 and
      a.ma_alarm_timetz between $1 and $2
    order by a.ma_alarm_timetz asc limit $7 offset $8

$$ LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;

-- get all alarm info (host and DB)
create or replace FUNCTION pg_catalog.get_alarm_info_desc(starttime timestamp ,
                                                        endtime timestamp ,
                                                        search_text text default '',
                                                        alarm_level int default 0,
                                                        alarm_type int default 0,
                                                        alarm_status int default 0,
                                                        alarm_limit int default 20,
                                                        alarm_offset int default 0)
returns TABLE (
                alarm_level smallint,
                alarm_text text,
                alarm_type smallint,
                alarm_id oid,
                alarm_source text,
                alarm_time timestamptz,
                alarm_status smallint,
                alarm_resolve_timetz timestamptz,
                alarm_solution text)
as $$
select a.ma_alarm_level AS alarm_level,
       a.ma_alarm_text AS alarm_text,
       a.ma_alarm_type AS alarm_type,
       a.oid AS alarm_id,
       a.ma_alarm_source AS alarm_source,
       a.ma_alarm_timetz AS alarm_time,
       a.ma_alarm_status AS alarm_status,
       r.mr_resolve_timetz AS alarm_resolve_timetz,
       r.mr_solution AS alarm_solution
from monitor_alarm a left join monitor_resolve r on(a.oid = r.mr_alarm_oid)
where case $4 when 0 then 1::boolean else a.ma_alarm_level = $4 end and
      case $5 when 0 then 1::boolean else a.ma_alarm_type = $5 end and
      case $6 when 0 then 1::boolean else a.ma_alarm_status = $6 end and
      a.ma_alarm_text ~ $3 and
      a.ma_alarm_timetz between $1 and $2
    order by a.ma_alarm_timetz desc limit $7 offset $8

$$ LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;

create or replace FUNCTION pg_catalog.get_alarm_info_count(starttime timestamp ,
                                                        endtime timestamp ,
                                                        search_text text default '',
                                                        alarm_level int default 0,
                                                        alarm_type int default 0,
                                                        alarm_status int default 0)
returns bigint
as $$
select count(*)
from monitor_alarm a left join monitor_resolve r on(a.oid = r.mr_alarm_oid)
where case $4 when 0 then 1::boolean else a.ma_alarm_level = $4 end and
      case $5 when 0 then 1::boolean else a.ma_alarm_type = $5 end and
      case $6 when 0 then 1::boolean else a.ma_alarm_status = $6 end and
      a.ma_alarm_text ~ $3 and
      a.ma_alarm_timetz between $1 and $2
$$ LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;

-- save resolve alarm log
create or replace function pg_catalog.resolve_alarm(alarm_id int, resolve_time timestamp, resolve_text text)
returns void as
$$
    insert into monitor_resolve (mr_alarm_oid, mr_resolve_timetz, mr_solution)
    values ($1, $2, $3);
    update monitor_alarm set ma_alarm_status = 2 where oideq(oid,$1)
$$
LANGUAGE SQL
VOLATILE
RETURNS NULL ON NULL INPUT;

--insert into monitor_user, as default value: 1: ordinary users, 2: db manager
insert into monitor_user values('数据库DBA', 2, '2016-01-01','2050-01-01', '12345678901', 
'userdba@asiainfo.com', '亚信', '数据库', '数据库研发工程师', '21232f297a57a5a743894a0e4a801fc3','系统管理员');
--check user name/password
CREATE OR REPLACE  FUNCTION  pg_catalog.monitor_checkuser_func(in Name, in Name)
RETURNS oid AS $$
  select oid from pg_catalog.monitor_user where username=$1 and userpassword=$2;

$$  LANGUAGE sql IMMUTABLE STRICT;
	
--show user info
 create or replace function pg_catalog.monitor_getuserinfo_func(in int)
returns TABLE 
	(
		username			Name,				/*the user name*/
		usertype			int,
		userstarttime		timestamptz,
		userendtime			timestamptz,
		usertel				Name,
		useremail			Name,
		usercompany			Name,
		userdepart			Name,
		usertitle			Name,
		userdesc			text
	)
as
$$
    select username, usertype, userstarttime, userendtime, usertel, useremail
		, usercompany, userdepart, usertitle, userdesc from pg_catalog.monitor_user where oid=$1;
$$
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;

--update user info
create or replace function pg_catalog.monitor_updateuserinfo_func(in int, in Name, in Name, in Name, in Name, in Name, in text)
returns int
as
$$
    update pg_catalog.monitor_user set username=$2, usertel=$3, useremail=$4, usertitle=$5, usercompany=$6, userdesc=$7 where oid=$1 returning 0;
 
$$
LANGUAGE SQL
VOLATILE
RETURNS NULL ON NULL INPUT;

--check user oldpassword
create or replace function pg_catalog.monitor_checkuserpassword_func(in int, in Name)
returns bigint
as
$$
  select count(*) -1 from pg_catalog.monitor_user where oid=$1 and userpassword=$2;
$$
LANGUAGE SQL
VOLATILE
RETURNS NULL ON NULL INPUT;

--update user password
create or replace function  pg_catalog.monitor_updateuserpassword_func(in int, in Name)
returns int
as
$$
    update pg_catalog.monitor_user set userpassword=$2 where oid=$1  returning 0;
$$
LANGUAGE SQL
VOLATILE
RETURNS NULL ON NULL INPUT;


--get the content "INSERT INTO adbmgr.parm VALUES..."
--create table parm(id1 char,name text,setting text,context text,vartype text,unit text, min_val text,max_val text,enumvals text[]) distribute by replication;
--insert into parm select '*', name, setting, unit, context, vartype,  min_val, max_val, enumvals from pg_settings order by 2;
--update parm set id1='#' where name in ('adb_ha_param_delimiter', 'agtm_host', 'agtm_port', 'enable_adb_ha_sync', 'enable_adb_ha_sync_select', 'enable_fast_query_shipping', 'enable_remotegroup', 'enable_remotejoin', 'enable_remotelimit', 'enable_remotesort', 'enforce_two_phase_commit', 'grammar', 'max_coordinators', 'max_datanodes', 'max_pool_size', 'min_pool_size', 'nls_date_format', 'nls_timestamp_format', 'nls_timestamp_tz_format', 'persistent_datanode_connections', 'pgxc_node_name', 'pgxcnode_cancel_delay', 'pool_remote_cmd_timeout', 'remotetype', 'require_replicated_table_pkey', 'snapshot_level', 'xc_enable_node_tcp_log', 'xc_maintenance_mode');
--update parm set setting='minimal' where name = 'wal_level';
--update parm set setting='localhost' where name = 'agtm_host';
--pg_dump -p xxxx -d postgres -t parm -f cn1.txt --inserts -a
--add these
--INSERT INTO adbmgr.parm VALUES ('*', 'pg_stat_statements.max', '1000', 'postmaster', 'integer', '', '100', '2147483647', NULL);
--INSERT INTO adbmgr.parm VALUES ('*', 'pg_stat_statements.track', 'top', 'superuser', 'enum', NULL, NULL, NULL, '{none,top,all}');
--INSERT INTO adbmgr.parm VALUES ('*', 'pg_stat_statements.save', 'on', 'sighup', 'bool', NULL, NULL, NULL, NULL);
--INSERT INTO adbmgr.parm VALUES ('*', 'pg_stat_statements.track_utility', 'on', 'superuser', 'bool', NULL, NULL, NULL, NULL);

--'*' stands for gtm/coordinator/datanode, '#' stands for coordinator/datanode

INSERT INTO adbmgr.parm VALUES ('*', 'DateStyle', 'ISO, MDY', 'user', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'IntervalStyle', 'postgres', 'user', 'enum', NULL, NULL, NULL, '{postgres,postgres_verbose,sql_standard,iso_8601}');
INSERT INTO adbmgr.parm VALUES ('*', 'TimeZone', 'Hongkong', 'user', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'allow_system_table_mods', 'off', 'postmaster', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'application_name', 'psql', 'user', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'archive_command', '(disabled)', 'sighup', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'archive_mode', 'off', 'postmaster', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'archive_timeout', '0', 'sighup', 'integer', 's', '0', '1073741823', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'array_nulls', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'authentication_timeout', '60', 'sighup', 'integer', 's', '1', '600', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'autovacuum', 'on', 'sighup', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'autovacuum_analyze_scale_factor', '0.1', 'sighup', 'real', NULL, '0', '100', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'autovacuum_analyze_threshold', '50', 'sighup', 'integer', '', '0', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'autovacuum_freeze_max_age', '200000000', 'postmaster', 'integer', '', '100000', '2000000000', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'autovacuum_max_workers', '3', 'postmaster', 'integer', '', '1', '8388607', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'autovacuum_multixact_freeze_max_age', '400000000', 'postmaster', 'integer', '', '10000', '2000000000', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'autovacuum_naptime', '60', 'sighup', 'integer', 's', '1', '2147483', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'autovacuum_vacuum_cost_delay', '20', 'sighup', 'integer', 'ms', '-1', '100', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'autovacuum_vacuum_cost_limit', '-1', 'sighup', 'integer', '', '-1', '10000', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'autovacuum_vacuum_scale_factor', '0.2', 'sighup', 'real', NULL, '0', '100', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'autovacuum_vacuum_threshold', '50', 'sighup', 'integer', '', '0', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'backslash_quote', 'safe_encoding', 'user', 'enum', NULL, NULL, NULL, '{safe_encoding,on,off}');
INSERT INTO adbmgr.parm VALUES ('*', 'bgwriter_delay', '200', 'sighup', 'integer', 'ms', '10', '10000', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'bgwriter_lru_maxpages', '100', 'sighup', 'integer', '', '0', '1000', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'bgwriter_lru_multiplier', '2', 'sighup', 'real', NULL, '0', '10', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'block_size', '8192', 'internal', 'integer', '', '8192', '8192', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'bonjour', 'off', 'postmaster', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'bonjour_name', '', 'postmaster', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'bytea_output', 'hex', 'user', 'enum', NULL, NULL, NULL, '{escape,hex}');
INSERT INTO adbmgr.parm VALUES ('*', 'check_function_bodies', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'checkpoint_completion_target', '0.5', 'sighup', 'real', NULL, '0', '1', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'checkpoint_segments', '3', 'sighup', 'integer', '', '1', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'checkpoint_timeout', '300', 'sighup', 'integer', 's', '30', '3600', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'checkpoint_warning', '30', 'sighup', 'integer', 's', '0', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'client_encoding', 'UTF8', 'user', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'client_min_messages', 'notice', 'user', 'enum', NULL, NULL, NULL, '{debug5,debug4,debug3,debug2,debug1,log,notice,warning,error}');
INSERT INTO adbmgr.parm VALUES ('*', 'commit_delay', '0', 'superuser', 'integer', '', '0', '100000', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'commit_siblings', '5', 'user', 'integer', '', '0', '1000', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'config_file', '/tmp/test/cn1/postgresql.conf', 'postmaster', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'constraint_exclusion', 'partition', 'user', 'enum', NULL, NULL, NULL, '{partition,on,off}');
INSERT INTO adbmgr.parm VALUES ('*', 'copy_batch_rows', '1000', 'user', 'integer', '', '1', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'copy_batch_size', '64', 'user', 'integer', 'kB', '1', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'cpu_index_tuple_cost', '0.005', 'user', 'real', NULL, '0', '1.79769e+308', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'cpu_operator_cost', '0.0025', 'user', 'real', NULL, '0', '1.79769e+308', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'cpu_tuple_cost', '0.01', 'user', 'real', NULL, '0', '1.79769e+308', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'cursor_tuple_fraction', '0.1', 'user', 'real', NULL, '0', '1', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'data_checksums', 'off', 'internal', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'data_directory', '/tmp/test/cn1', 'postmaster', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'db_user_namespace', 'off', 'sighup', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'deadlock_timeout', '1000', 'superuser', 'integer', 'ms', '1', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'debug_assertions', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'debug_pretty_print', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'debug_print_grammar', 'off', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'debug_print_parse', 'off', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'debug_print_plan', 'off', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'debug_print_rewritten', 'off', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'default_statistics_target', '100', 'user', 'integer', '', '1', '10000', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'default_tablespace', '', 'user', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'default_text_search_config', 'pg_catalog.english', 'user', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'default_transaction_deferrable', 'off', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'default_transaction_isolation', 'read committed', 'user', 'enum', NULL, NULL, NULL, '{serializable,"repeatable read","read committed","read uncommitted"}');
INSERT INTO adbmgr.parm VALUES ('*', 'default_transaction_read_only', 'off', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'default_with_oids', 'off', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'dynamic_library_path', '$libdir', 'superuser', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'effective_cache_size', '16384', 'user', 'integer', '8kB', '1', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'effective_io_concurrency', '1', 'user', 'integer', '', '0', '1000', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'enable_bitmapscan', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'enable_hashagg', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'enable_hashjoin', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'enable_indexonlyscan', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'enable_indexscan', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'enable_material', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'enable_mergejoin', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'enable_nestloop', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'enable_seqscan', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'enable_sort', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'enable_tidscan', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'escape_string_warning', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'event_source', 'PostgreSQL', 'postmaster', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'exit_on_error', 'off', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'external_pid_file', '', 'postmaster', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'extra_float_digits', '0', 'user', 'integer', '', '-15', '3', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'from_collapse_limit', '8', 'user', 'integer', '', '1', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'fsync', 'on', 'sighup', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'full_page_writes', 'on', 'sighup', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'geqo', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'geqo_effort', '5', 'user', 'integer', '', '1', '10', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'geqo_generations', '0', 'user', 'integer', '', '0', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'geqo_pool_size', '0', 'user', 'integer', '', '0', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'geqo_seed', '0', 'user', 'real', NULL, '0', '1', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'geqo_selection_bias', '2', 'user', 'real', NULL, '1.5', '2', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'geqo_threshold', '12', 'user', 'integer', '', '2', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'gin_fuzzy_search_limit', '0', 'user', 'integer', '', '0', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'hba_file', '/tmp/test/cn1/pg_hba.conf', 'postmaster', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'hot_standby', 'off', 'postmaster', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'hot_standby_feedback', 'off', 'sighup', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'ident_file', '/tmp/test/cn1/pg_ident.conf', 'postmaster', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'ignore_checksum_failure', 'off', 'superuser', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'ignore_system_indexes', 'off', 'backend', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'integer_datetimes', 'on', 'internal', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'join_collapse_limit', '8', 'user', 'integer', '', '1', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'krb_caseins_users', 'off', 'sighup', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'krb_server_keyfile', '', 'sighup', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'krb_srvname', 'postgres', 'sighup', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'lc_collate', 'C', 'internal', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'lc_ctype', 'C', 'internal', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'lc_messages', 'C', 'superuser', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'lc_monetary', 'C', 'user', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'lc_numeric', 'C', 'user', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'lc_time', 'C', 'user', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'listen_addresses', '*', 'postmaster', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'lo_compat_privileges', 'off', 'superuser', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'local_preload_libraries', '', 'backend', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'lock_timeout', '0', 'user', 'integer', 'ms', '0', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_autovacuum_min_duration', '-1', 'sighup', 'integer', 'ms', '-1', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_checkpoints', 'off', 'sighup', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_connections', 'off', 'backend', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_destination', 'csvlog', 'sighup', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_directory', 'pg_log', 'sighup', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_disconnections', 'off', 'backend', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_duration', 'off', 'superuser', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_error_verbosity', 'default', 'superuser', 'enum', NULL, NULL, NULL, '{terse,default,verbose}');
INSERT INTO adbmgr.parm VALUES ('*', 'log_executor_stats', 'off', 'superuser', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_file_mode', '0600', 'sighup', 'integer', '', '0', '511', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_filename', 'postgresql-%Y-%m-%d_%H%M%S.log', 'sighup', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_hostname', 'off', 'sighup', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_line_prefix', '', 'sighup', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_lock_waits', 'off', 'superuser', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_min_duration_statement', '-1', 'superuser', 'integer', 'ms', '-1', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_min_error_statement', 'error', 'superuser', 'enum', NULL, NULL, NULL, '{debug5,debug4,debug3,debug2,debug1,info,notice,warning,error,log,fatal,panic}');
INSERT INTO adbmgr.parm VALUES ('*', 'log_min_messages', 'warning', 'superuser', 'enum', NULL, NULL, NULL, '{debug5,debug4,debug3,debug2,debug1,info,notice,warning,error,log,fatal,panic}');
INSERT INTO adbmgr.parm VALUES ('*', 'log_parser_stats', 'off', 'superuser', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_planner_stats', 'off', 'superuser', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_rotation_age', '1440', 'sighup', 'integer', 'min', '0', '35791394', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_rotation_size', '10240', 'sighup', 'integer', 'kB', '0', '2097151', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_statement', 'none', 'superuser', 'enum', NULL, NULL, NULL, '{none,ddl,mod,all}');
INSERT INTO adbmgr.parm VALUES ('*', 'log_statement_stats', 'off', 'superuser', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_temp_files', '-1', 'superuser', 'integer', 'kB', '-1', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_timezone', 'Hongkong', 'sighup', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_truncate_on_rotation', 'off', 'sighup', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'logging_collector', 'on', 'postmaster', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'maintenance_work_mem', '16384', 'user', 'integer', 'kB', '1024', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'max_connections', '100', 'postmaster', 'integer', '', '1', '8388607', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'max_files_per_process', '1000', 'postmaster', 'integer', '', '25', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'max_function_args', '100', 'internal', 'integer', '', '100', '100', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'max_identifier_length', '63', 'internal', 'integer', '', '63', '63', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'max_index_keys', '32', 'internal', 'integer', '', '32', '32', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'max_locks_per_transaction', '64', 'postmaster', 'integer', '', '10', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'max_pred_locks_per_transaction', '64', 'postmaster', 'integer', '', '10', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'max_prepared_transactions', '100', 'postmaster', 'integer', '', '0', '536870911', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'max_stack_depth', '2048', 'superuser', 'integer', 'kB', '100', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'max_standby_archive_delay', '30000', 'sighup', 'integer', 'ms', '-1', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'max_standby_streaming_delay', '30000', 'sighup', 'integer', 'ms', '-1', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'max_wal_senders', '5', 'postmaster', 'integer', '', '0', '8388607', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'password_encryption', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'port', '5100', 'postmaster', 'integer', '', '1', '65535', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'post_auth_delay', '0', 'backend', 'integer', 's', '0', '2147', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'pre_auth_delay', '0', 'sighup', 'integer', 's', '0', '60', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'quote_all_identifiers', 'off', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'random_page_cost', '4', 'user', 'real', NULL, '0', '1.79769e+308', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'restart_after_crash', 'on', 'sighup', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'search_path', '"$user",public', 'user', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'segment_size', '131072', 'internal', 'integer', '8kB', '131072', '131072', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'seq_page_cost', '1', 'user', 'real', NULL, '0', '1.79769e+308', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'server_encoding', 'UTF8', 'internal', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'server_version', '9.3.13 ADB 2.2devel 26b4b5d', 'internal', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'server_version_num', '90313', 'internal', 'integer', '', '90313', '90313', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'session_replication_role', 'origin', 'superuser', 'enum', NULL, NULL, NULL, '{origin,replica,local}');
INSERT INTO adbmgr.parm VALUES ('*', 'shared_buffers', '16384', 'postmaster', 'integer', '8kB', '16', '1073741823', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'shared_preload_libraries', '', 'postmaster', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('C', 'shared_preload_libraries', 'pg_stat_statements', 'postmaster', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'sql_inheritance', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'ssl', 'off', 'postmaster', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'ssl_ca_file', '', 'postmaster', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'ssl_cert_file', 'server.crt', 'postmaster', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'ssl_ciphers', 'DEFAULT:!LOW:!EXP:!MD5:@STRENGTH', 'postmaster', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'ssl_crl_file', '', 'postmaster', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'ssl_key_file', 'server.key', 'postmaster', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'ssl_renegotiation_limit', '0', 'user', 'integer', 'kB', '0', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'standard_conforming_strings', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'statement_timeout', '0', 'user', 'integer', 'ms', '0', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'stats_temp_directory', 'pg_stat_tmp', 'sighup', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'superuser_reserved_connections', '3', 'postmaster', 'integer', '', '0', '8388607', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'synchronize_seqscans', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'synchronous_commit', 'on', 'user', 'enum', NULL, NULL, NULL, '{local,remote_write,on,off}');
INSERT INTO adbmgr.parm VALUES ('*', 'synchronous_standby_names', '', 'sighup', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'syslog_facility', 'local0', 'sighup', 'enum', NULL, NULL, NULL, '{local0,local1,local2,local3,local4,local5,local6,local7}');
INSERT INTO adbmgr.parm VALUES ('*', 'syslog_ident', 'postgres', 'sighup', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'tcp_keepalives_count', '9', 'user', 'integer', '', '0', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'tcp_keepalives_idle', '7200', 'user', 'integer', 's', '0', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'tcp_keepalives_interval', '75', 'user', 'integer', 's', '0', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'temp_buffers', '1024', 'user', 'integer', '8kB', '100', '1073741823', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'temp_file_limit', '-1', 'superuser', 'integer', 'kB', '-1', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'temp_tablespaces', '', 'user', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'timezone_abbreviations', 'Default', 'user', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'trace_notify', 'off', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'trace_recovery_messages', 'log', 'sighup', 'enum', NULL, NULL, NULL, '{debug5,debug4,debug3,debug2,debug1,log,notice,warning,error}');
INSERT INTO adbmgr.parm VALUES ('*', 'trace_sort', 'off', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'track_activities', 'on', 'superuser', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'track_activity_query_size', '1024', 'postmaster', 'integer', '', '100', '102400', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'track_counts', 'on', 'superuser', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'track_functions', 'none', 'superuser', 'enum', NULL, NULL, NULL, '{none,pl,all}');
INSERT INTO adbmgr.parm VALUES ('*', 'track_io_timing', 'off', 'superuser', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'transaction_deferrable', 'off', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'transaction_isolation', 'read committed', 'user', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'transaction_read_only', 'off', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'transform_null_equals', 'off', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'unix_socket_directories', '/tmp', 'postmaster', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'unix_socket_group', '', 'postmaster', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'unix_socket_permissions', '0777', 'postmaster', 'integer', '', '0', '511', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'update_process_title', 'on', 'superuser', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'vacuum_cost_delay', '0', 'user', 'integer', 'ms', '0', '100', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'vacuum_cost_limit', '200', 'user', 'integer', '', '1', '10000', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'vacuum_cost_page_dirty', '20', 'user', 'integer', '', '0', '10000', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'vacuum_cost_page_hit', '1', 'user', 'integer', '', '0', '10000', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'vacuum_cost_page_miss', '10', 'user', 'integer', '', '0', '10000', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'vacuum_defer_cleanup_age', '0', 'sighup', 'integer', '', '0', '1000000', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'vacuum_freeze_min_age', '50000000', 'user', 'integer', '', '0', '1000000000', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'vacuum_freeze_table_age', '150000000', 'user', 'integer', '', '0', '2000000000', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'vacuum_multixact_freeze_min_age', '5000000', 'user', 'integer', '', '0', '1000000000', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'vacuum_multixact_freeze_table_age', '150000000', 'user', 'integer', '', '0', '2000000000', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'wal_block_size', '8192', 'internal', 'integer', '', '8192', '8192', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'wal_buffers', '512', 'postmaster', 'integer', '8kB', '-1', '262143', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'wal_debug', 'off', 'superuser', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'wal_keep_segments', '32', 'sighup', 'integer', '', '0', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'wal_level', 'hot_standby', 'postmaster', 'enum', NULL, NULL, NULL, '{minimal,archive,hot_standby}');
INSERT INTO adbmgr.parm VALUES ('*', 'wal_receiver_status_interval', '10', 'sighup', 'integer', 's', '0', '2147483', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'wal_receiver_timeout', '60000', 'sighup', 'integer', 'ms', '0', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'wal_segment_size', '2048', 'internal', 'integer', '8kB', '2048', '2048', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'wal_sender_timeout', '60000', 'sighup', 'integer', 'ms', '0', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'wal_sync_method', 'fdatasync', 'sighup', 'enum', NULL, NULL, NULL, '{fsync,fdatasync,open_sync,open_datasync}');
INSERT INTO adbmgr.parm VALUES ('*', 'wal_writer_delay', '200', 'sighup', 'integer', 'ms', '1', '10000', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'work_mem', '1024', 'user', 'integer', 'kB', '64', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'xmlbinary', 'base64', 'user', 'enum', NULL, NULL, NULL, '{base64,hex}');
INSERT INTO adbmgr.parm VALUES ('*', 'xmloption', 'content', 'user', 'enum', NULL, NULL, NULL, '{content,document}');
INSERT INTO adbmgr.parm VALUES ('*', 'zero_damaged_pages', 'off', 'superuser', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'adb_ha_param_delimiter', '$&#$', 'user', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'agtm_host', 'localhost', 'sighup', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'agtm_port', '6666', 'sighup', 'integer', '', '1', '65535', NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'enable_adb_ha_sync', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'enable_adb_ha_sync_select', 'off', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'enable_fast_query_shipping', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'enable_remotegroup', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'enable_remotejoin', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'enable_remotelimit', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'enable_remotesort', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'enforce_two_phase_commit', 'on', 'superuser', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'grammar', 'postgres', 'user', 'enum', NULL, NULL, NULL, '{postgres,oracle}');
INSERT INTO adbmgr.parm VALUES ('#', 'max_coordinators', '16', 'postmaster', 'integer', '', '2', '65535', NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'max_datanodes', '16', 'postmaster', 'integer', '', '2', '65535', NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'max_pool_size', '100', 'postmaster', 'integer', '', '1', '65535', NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'min_pool_size', '1', 'postmaster', 'integer', '', '1', '65535', NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'nls_date_format', 'YYYY-MM-DD', 'user', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'nls_timestamp_format', 'YYYY-MM-DD HH24:MI:SS.US', 'user', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'nls_timestamp_tz_format', 'YYYY-MM-DD HH24:MI:SS.US TZ', 'user', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'persistent_datanode_connections', 'off', 'backend', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'pgxc_node_name', 'cn1', 'postmaster', 'string', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'pgxcnode_cancel_delay', '10', 'user', 'integer', 'ms', '0', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'pool_remote_cmd_timeout', '10', 'postmaster', 'integer', 'ms', '0', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'remotetype', 'application', 'backend', 'enum', NULL, NULL, NULL, '{application,coordinator,datanode,rxactmgr}');
INSERT INTO adbmgr.parm VALUES ('#', 'require_replicated_table_pkey', 'on', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'snapshot_level', 'mvcc', 'user', 'enum', NULL, NULL, NULL, '{mvcc,now,self,any,toast,dirty}');
INSERT INTO adbmgr.parm VALUES ('#', 'xc_enable_node_tcp_log', 'off', 'user', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'xc_maintenance_mode', 'off', 'superuser', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'pg_stat_statements.max', '1000', 'postmaster', 'integer', '', '100', '2147483647', NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'pg_stat_statements.track', 'top', 'superuser', 'enum', NULL, NULL, NULL, '{none,top,all}');
INSERT INTO adbmgr.parm VALUES ('*', 'pg_stat_statements.save', 'on', 'sighup', 'bool', NULL, NULL, NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'pg_stat_statements.track_utility', 'on', 'superuser', 'bool', NULL, NULL, NULL, NULL);
