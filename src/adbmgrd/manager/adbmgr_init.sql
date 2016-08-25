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
	parmunit		AS	unit,
	parmcontext		AS	context,
	parmvartype		AS	vartype,
	parmminval		AS	minval,
	parmmaxval		AS	maxval
FROM pg_catalog.mgr_parm;

CREATE VIEW adbmgr.updateparm AS
SELECT
	updateparmparmtype		AS	parmtype,
	updateparmnodename		AS	nodename,
	CASE updateparmnodetype
		WHEN 'c' THEN 'coordinator'::text
		WHEN 'd' THEN 'datanode master'::text
		WHEN 'b' THEN 'datanode slave'::text
		WHEN 'n' THEN 'datanode extra'::text
		WHEN 'g' THEN 'gtm master'::text
		WHEN 'p' THEN 'gtm slave'::text
		WHEN 'e' THEN 'gtm extra'::text
	END AS nodetype,
	updateparmkey			AS	key,
	updateparmvalue			AS	value
FROM pg_catalog.mgr_updateparm order by updateparmnodename;

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
    mgrnode.nodepath    AS  path,
    mgrnode.nodeprimary AS primary,
    mgrnode.nodeinited  AS  initialized,
    mgrnode.nodeincluster AS incluster
  FROM pg_catalog.mgr_node AS mgrnode LEFT JOIN pg_catalog.mgr_host ON mgrnode.nodehost = pg_catalog.mgr_host.oid LEFT JOIN pg_catalog.mgr_node AS node_alise
  ON node_alise.oid = mgrnode.nodemasternameoid order by 3;

CREATE VIEW adbmgr.monitor_datanode_all AS
	select * from mgr_monitor_dnmaster_all()
	union all
	select * from mgr_monitor_dnslave_all();

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
    SELECT 'stop datanode master' AS "operation type", * FROM mgr_stop_dn_master(NULL)
    UNION all
    SELECT 'stop datanode slave' AS "operation type", * FROM mgr_stop_dn_slave(NULL)
    UNION all
    SELECT 'stop datanode extra' AS "operation type", * FROM mgr_stop_dn_extra(NULL);

CREATE VIEW adbmgr.stop_datanode_all_f AS
    SELECT 'stop datanode master' AS "operation type", * FROM mgr_stop_dn_master_f(NULL)
    UNION all
    SELECT 'stop datanode slave' AS "operation type", * FROM mgr_stop_dn_slave_f(NULL)
    UNION all
    SELECT 'stop datanode extra' AS "operation type", * FROM mgr_stop_dn_extra_f(NULL);

CREATE VIEW adbmgr.stop_datanode_all_i AS
    SELECT 'stop datanode master' AS "operation type", * FROM mgr_stop_dn_master_i(NULL)
    UNION all
    SELECT 'stop datanode slave' AS "operation type", * FROM mgr_stop_dn_slave_i(NULL)
    UNION all
    SELECT 'stop datanode extra' AS "operation type", * FROM mgr_stop_dn_extra_i(NULL);

--stop all
CREATE VIEW adbmgr.stopall AS
    SELECT 'stop coordinator' AS "operation type", * FROM mgr_stop_cn_master(NULL)
    UNION all
    SELECT 'stop datanode master' AS "operation type", * FROM mgr_stop_dn_master(NULL)
    UNION all
    SELECT 'stop datanode slave' AS "operation type", * FROM mgr_stop_dn_slave(NULL)
    UNION all
    SELECT 'stop datanode extra' AS "operation type", * FROM mgr_stop_dn_extra(NULL)
    UNION all
    SELECT 'stop gtm extra' AS "operation type", * FROM mgr_stop_gtm_extra(NULL)
    UNION all
    SELECT 'stop gtm slave' AS "operation type", * FROM mgr_stop_gtm_slave(NULL)
    UNION all
    SELECT 'stop gtm master' AS "operation type", * FROM mgr_stop_gtm_master(NULL);

CREATE VIEW adbmgr.stopall_f AS
    SELECT 'stop coordinator' AS "operation type", * FROM mgr_stop_cn_master_f(NULL)
    UNION all
    SELECT 'stop datanode master' AS "operation type", * FROM mgr_stop_dn_master_f(NULL)
    UNION all
    SELECT 'stop datanode slave' AS "operation type", * FROM mgr_stop_dn_slave_f(NULL)
    UNION all
    SELECT 'stop datanode extra' AS "operation type", * FROM mgr_stop_dn_extra_f(NULL)
    UNION all
    SELECT 'stop gtm extra' AS "operation type", * FROM mgr_stop_gtm_extra_f(NULL)
    UNION all
    SELECT 'stop gtm slave' AS "operation type", * FROM mgr_stop_gtm_slave_f(NULL)
    UNION all
    SELECT 'stop gtm master' AS "operation type", * FROM mgr_stop_gtm_master_f(NULL);

CREATE VIEW adbmgr.stopall_i AS
    SELECT 'stop coordinator' AS "operation type", * FROM mgr_stop_cn_master_i(NULL)
    UNION all
    SELECT 'stop datanode master' AS "operation type", * FROM mgr_stop_dn_master_i(NULL)
    UNION all
    SELECT 'stop datanode slave' AS "operation type", * FROM mgr_stop_dn_slave_i(NULL)
    UNION all
    SELECT 'stop datanode extra' AS "operation type", * FROM mgr_stop_dn_extra_i(NULL)
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


--insert data into mgr.parm
INSERT INTO adbmgr.parm VALUES ('*', 'allow_system_table_mods', NULL, 'postmaster', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'application_name', NULL, 'user', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'archive_command', NULL, 'sighup', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'archive_mode', NULL, 'postmaster', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'archive_timeout', 's', 'sighup', 'integer', '0', '1073741823');
INSERT INTO adbmgr.parm VALUES ('*', 'array_nulls', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'authentication_timeout', 's', 'sighup', 'integer', '1', '600');
INSERT INTO adbmgr.parm VALUES ('*', 'autovacuum', NULL, 'sighup', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'autovacuum_analyze_scale_factor', NULL, 'sighup', 'real', '0', '100');
INSERT INTO adbmgr.parm VALUES ('*', 'autovacuum_analyze_threshold', '', 'sighup', 'integer', '0', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'autovacuum_freeze_max_age', '', 'postmaster', 'integer', '100000', '2000000000');
INSERT INTO adbmgr.parm VALUES ('*', 'autovacuum_max_workers', '', 'postmaster', 'integer', '1', '8388607');
INSERT INTO adbmgr.parm VALUES ('G', 'autovacuum_multixact_freeze_max_age', '', 'postmaster', 'integer', '10000', '2000000000');
INSERT INTO adbmgr.parm VALUES ('*', 'autovacuum_multixact_freeze_max_age', '', 'postmaster', 'integer', '10000000', '2000000000');
INSERT INTO adbmgr.parm VALUES ('*', 'autovacuum_naptime', 's', 'sighup', 'integer', '1', '2147483');
INSERT INTO adbmgr.parm VALUES ('*', 'autovacuum_vacuum_cost_delay', 'ms', 'sighup', 'integer', '-1', '100');
INSERT INTO adbmgr.parm VALUES ('*', 'autovacuum_vacuum_cost_limit', '', 'sighup', 'integer', '-1', '10000');
INSERT INTO adbmgr.parm VALUES ('*', 'autovacuum_vacuum_scale_factor', NULL, 'sighup', 'real', '0', '100');
INSERT INTO adbmgr.parm VALUES ('*', 'autovacuum_vacuum_threshold', '', 'sighup', 'integer', '0', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'backslash_quote', NULL, 'user', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'bgwriter_delay', 'ms', 'sighup', 'integer', '10', '10000');
INSERT INTO adbmgr.parm VALUES ('*', 'bgwriter_lru_maxpages', '', 'sighup', 'integer', '0', '1000');
INSERT INTO adbmgr.parm VALUES ('*', 'bgwriter_lru_multiplier', NULL, 'sighup', 'real', '0', '10');
INSERT INTO adbmgr.parm VALUES ('*', 'block_size', '', 'internal', 'integer', '8192', '8192');
INSERT INTO adbmgr.parm VALUES ('*', 'bonjour', NULL, 'postmaster', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'bonjour_name', NULL, 'postmaster', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'bytea_output', NULL, 'user', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('G', 'superuser', NULL, 'superuser', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'check_function_bodies', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'checkpoint_completion_target', NULL, 'sighup', 'real', '0', '1');
INSERT INTO adbmgr.parm VALUES ('*', 'checkpoint_segments', '', 'sighup', 'integer', '1', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'checkpoint_timeout', 's', 'sighup', 'integer', '30', '3600');
INSERT INTO adbmgr.parm VALUES ('*', 'checkpoint_warning', 's', 'sighup', 'integer', '0', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'client_encoding', NULL, 'user', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'client_min_messages', NULL, 'user', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'commit_delay', '', 'superuser', 'integer', '0', '100000');
INSERT INTO adbmgr.parm VALUES ('*', 'commit_siblings', '', 'user', 'integer', '0', '1000');
INSERT INTO adbmgr.parm VALUES ('*', 'config_file', NULL, 'postmaster', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'constraint_exclusion', NULL, 'user', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'copy_batch_rows', '', 'user', 'integer', '1', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'copy_batch_size', 'kB', 'user', 'integer', '1', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'cpu_index_tuple_cost', NULL, 'user', 'real', '0', '1.79769e+308');
INSERT INTO adbmgr.parm VALUES ('*', 'cpu_operator_cost', NULL, 'user', 'real', '0', '1.79769e+308');
INSERT INTO adbmgr.parm VALUES ('*', 'cpu_tuple_cost', NULL, 'user', 'real', '0', '1.79769e+308');
INSERT INTO adbmgr.parm VALUES ('*', 'cursor_tuple_fraction', NULL, 'user', 'real', '0', '1');
INSERT INTO adbmgr.parm VALUES ('*', 'data_checksums', NULL, 'internal', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'data_directory', NULL, 'postmaster', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'DateStyle', NULL, 'user', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'db_user_namespace', NULL, 'sighup', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'deadlock_timeout', 'ms', 'superuser', 'integer', '1', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'debug_assertions', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'debug_pretty_print', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'debug_print_grammar', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'debug_print_parse', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'debug_print_plan', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'debug_print_rewritten', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'default_statistics_target', '', 'user', 'integer', '1', '10000');
INSERT INTO adbmgr.parm VALUES ('*', 'default_tablespace', NULL, 'user', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'default_text_search_config', NULL, 'user', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'default_transaction_deferrable', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'default_transaction_isolation', NULL, 'user', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'default_transaction_read_only', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'default_with_oids', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'dynamic_library_path', NULL, 'superuser', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'effective_cache_size', '8kB', 'user', 'integer', '1', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'effective_io_concurrency', '', 'user', 'integer', '0', '1000');
INSERT INTO adbmgr.parm VALUES ('*', 'enable_bitmapscan', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'enable_hashagg', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'enable_hashjoin', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'enable_indexonlyscan', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'enable_indexscan', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'enable_material', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'enable_mergejoin', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'enable_nestloop', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'enable_seqscan', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'enable_sort', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'enable_tidscan', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'escape_string_warning', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'event_source', NULL, 'postmaster', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'exit_on_error', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'external_pid_file', NULL, 'postmaster', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'extra_float_digits', '', 'user', 'integer', '-15', '3');
INSERT INTO adbmgr.parm VALUES ('*', 'from_collapse_limit', '', 'user', 'integer', '1', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'fsync', NULL, 'sighup', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'full_page_writes', NULL, 'sighup', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'geqo', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'geqo_effort', '', 'user', 'integer', '1', '10');
INSERT INTO adbmgr.parm VALUES ('*', 'geqo_generations', '', 'user', 'integer', '0', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'geqo_pool_size', '', 'user', 'integer', '0', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'geqo_seed', NULL, 'user', 'real', '0', '1');
INSERT INTO adbmgr.parm VALUES ('*', 'geqo_selection_bias', NULL, 'user', 'real', '1.5', '2');
INSERT INTO adbmgr.parm VALUES ('*', 'geqo_threshold', '', 'user', 'integer', '2', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'gin_fuzzy_search_limit', '', 'user', 'integer', '0', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'hba_file', NULL, 'postmaster', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'hot_standby', NULL, 'postmaster', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'hot_standby_feedback', NULL, 'sighup', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'ident_file', NULL, 'postmaster', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'ignore_checksum_failure', NULL, 'superuser', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'ignore_system_indexes', NULL, 'backend', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'integer_datetimes', NULL, 'internal', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'IntervalStyle', NULL, 'user', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'join_collapse_limit', '', 'user', 'integer', '1', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'krb_caseins_users', NULL, 'sighup', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'krb_server_keyfile', NULL, 'sighup', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'krb_srvname', NULL, 'sighup', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'lc_collate', NULL, 'internal', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'lc_ctype', NULL, 'internal', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'lc_messages', NULL, 'superuser', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'lc_monetary', NULL, 'user', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'lc_numeric', NULL, 'user', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'lc_time', NULL, 'user', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'listen_addresses', NULL, 'postmaster', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'lo_compat_privileges', NULL, 'superuser', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'local_preload_libraries', NULL, 'backend', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'lock_timeout', 'ms', 'user', 'integer', '0', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'log_autovacuum_min_duration', 'ms', 'sighup', 'integer', '-1', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'log_checkpoints', NULL, 'sighup', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_connections', NULL, 'backend', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_destination', NULL, 'sighup', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_directory', NULL, 'sighup', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_disconnections', NULL, 'backend', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_duration', NULL, 'superuser', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_error_verbosity', NULL, 'superuser', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_executor_stats', NULL, 'superuser', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_file_mode', '', 'sighup', 'integer', '0', '511');
INSERT INTO adbmgr.parm VALUES ('*', 'log_filename', NULL, 'sighup', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_hostname', NULL, 'sighup', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_line_prefix', NULL, 'sighup', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_lock_waits', NULL, 'superuser', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_min_duration_statement', 'ms', 'superuser', 'integer', '-1', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'log_min_error_statement', NULL, 'superuser', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_min_messages', NULL, 'superuser', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_parser_stats', NULL, 'superuser', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_planner_stats', NULL, 'superuser', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_rotation_age', 'min', 'sighup', 'integer', '0', '35791394');
INSERT INTO adbmgr.parm VALUES ('*', 'log_rotation_size', 'kB', 'sighup', 'integer', '0', '2097151');
INSERT INTO adbmgr.parm VALUES ('*', 'log_statement', NULL, 'superuser', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_statement_stats', NULL, 'superuser', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_temp_files', 'kB', 'superuser', 'integer', '-1', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'log_timezone', NULL, 'sighup', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'log_truncate_on_rotation', NULL, 'sighup', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'logging_collector', NULL, 'postmaster', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'maintenance_work_mem', 'kB', 'user', 'integer', '1024', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'max_connections', '', 'postmaster', 'integer', '1', '8388607');
INSERT INTO adbmgr.parm VALUES ('*', 'max_files_per_process', '', 'postmaster', 'integer', '25', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'max_function_args', '', 'internal', 'integer', '100', '100');
INSERT INTO adbmgr.parm VALUES ('*', 'max_identifier_length', '', 'internal', 'integer', '63', '63');
INSERT INTO adbmgr.parm VALUES ('*', 'max_index_keys', '', 'internal', 'integer', '32', '32');
INSERT INTO adbmgr.parm VALUES ('*', 'max_locks_per_transaction', '', 'postmaster', 'integer', '10', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'max_pred_locks_per_transaction', '', 'postmaster', 'integer', '10', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'max_stack_depth', 'kB', 'superuser', 'integer', '100', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'max_standby_archive_delay', 'ms', 'sighup', 'integer', '-1', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'max_standby_streaming_delay', 'ms', 'sighup', 'integer', '-1', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'max_wal_senders', '', 'postmaster', 'integer', '0', '8388607');
INSERT INTO adbmgr.parm VALUES ('*', 'password_encryption', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'port', '', 'postmaster', 'integer', '1', '65535');
INSERT INTO adbmgr.parm VALUES ('*', 'post_auth_delay', 's', 'backend', 'integer', '0', '2147');
INSERT INTO adbmgr.parm VALUES ('*', 'pre_auth_delay', 's', 'sighup', 'integer', '0', '60');
INSERT INTO adbmgr.parm VALUES ('*', 'quote_all_identifiers', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'random_page_cost', NULL, 'user', 'real', '0', '1.79769e+308');
INSERT INTO adbmgr.parm VALUES ('*', 'restart_after_crash', NULL, 'sighup', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'search_path', NULL, 'user', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'segment_size', '8kB', 'internal', 'integer', '131072', '131072');
INSERT INTO adbmgr.parm VALUES ('*', 'seq_page_cost', NULL, 'user', 'real', '0', '1.79769e+308');
INSERT INTO adbmgr.parm VALUES ('*', 'server_encoding', NULL, 'internal', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'server_version', NULL, 'internal', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'server_version_num', '', 'internal', 'integer', '90313', '90313');
INSERT INTO adbmgr.parm VALUES ('*', 'session_replication_role', NULL, 'superuser', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'shared_buffers', '8kB', 'postmaster', 'integer', '16', '1073741823');
INSERT INTO adbmgr.parm VALUES ('*', 'shared_preload_libraries', NULL, 'postmaster', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'sql_inheritance', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'ssl', NULL, 'postmaster', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'ssl_ca_file', NULL, 'postmaster', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'ssl_cert_file', NULL, 'postmaster', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'ssl_ciphers', NULL, 'postmaster', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'ssl_crl_file', NULL, 'postmaster', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'ssl_key_file', NULL, 'postmaster', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'ssl_renegotiation_limit', 'kB', 'user', 'integer', '0', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'standard_conforming_strings', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'statement_timeout', 'ms', 'user', 'integer', '0', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'stats_temp_directory', NULL, 'sighup', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'superuser_reserved_connections', '', 'postmaster', 'integer', '0', '8388607');
INSERT INTO adbmgr.parm VALUES ('*', 'synchronize_seqscans', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'synchronous_commit', NULL, 'user', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'synchronous_standby_names', NULL, 'sighup', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'syslog_facility', NULL, 'sighup', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'syslog_ident', NULL, 'sighup', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'tcp_keepalives_count', '', 'user', 'integer', '0', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'tcp_keepalives_idle', 's', 'user', 'integer', '0', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'tcp_keepalives_interval', 's', 'user', 'integer', '0', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'temp_buffers', '8kB', 'user', 'integer', '100', '1073741823');
INSERT INTO adbmgr.parm VALUES ('*', 'temp_file_limit', 'kB', 'superuser', 'integer', '-1', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'temp_tablespaces', NULL, 'user', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'TimeZone', NULL, 'user', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'timezone_abbreviations', NULL, 'user', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'trace_notify', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'trace_recovery_messages', NULL, 'sighup', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'trace_sort', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'track_activities', NULL, 'superuser', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'track_activity_query_size', '', 'postmaster', 'integer', '100', '102400');
INSERT INTO adbmgr.parm VALUES ('*', 'track_counts', NULL, 'superuser', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'track_functions', NULL, 'superuser', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'track_io_timing', NULL, 'superuser', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'transaction_deferrable', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'transaction_isolation', NULL, 'user', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'transaction_read_only', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'transform_null_equals', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'unix_socket_directories', NULL, 'postmaster', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'unix_socket_group', NULL, 'postmaster', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'unix_socket_permissions', '', 'postmaster', 'integer', '0', '511');
INSERT INTO adbmgr.parm VALUES ('*', 'update_process_title', NULL, 'superuser', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'vacuum_cost_delay', 'ms', 'user', 'integer', '0', '100');
INSERT INTO adbmgr.parm VALUES ('*', 'vacuum_cost_limit', '', 'user', 'integer', '1', '10000');
INSERT INTO adbmgr.parm VALUES ('*', 'vacuum_cost_page_dirty', '', 'user', 'integer', '0', '10000');
INSERT INTO adbmgr.parm VALUES ('*', 'vacuum_cost_page_hit', '', 'user', 'integer', '0', '10000');
INSERT INTO adbmgr.parm VALUES ('*', 'vacuum_cost_page_miss', '', 'user', 'integer', '0', '10000');
INSERT INTO adbmgr.parm VALUES ('*', 'vacuum_defer_cleanup_age', '', 'sighup', 'integer', '0', '1000000');
INSERT INTO adbmgr.parm VALUES ('*', 'vacuum_freeze_min_age', '', 'user', 'integer', '0', '1000000000');
INSERT INTO adbmgr.parm VALUES ('*', 'vacuum_freeze_table_age', '', 'user', 'integer', '0', '2000000000');
INSERT INTO adbmgr.parm VALUES ('*', 'vacuum_multixact_freeze_min_age', '', 'user', 'integer', '0', '1000000000');
INSERT INTO adbmgr.parm VALUES ('*', 'vacuum_multixact_freeze_table_age', '', 'user', 'integer', '0', '2000000000');
INSERT INTO adbmgr.parm VALUES ('*', 'wal_block_size', '', 'internal', 'integer', '8192', '8192');
INSERT INTO adbmgr.parm VALUES ('*', 'wal_buffers', '8kB', 'postmaster', 'integer', '-1', '262143');
INSERT INTO adbmgr.parm VALUES ('*', 'wal_keep_segments', '', 'sighup', 'integer', '0', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'wal_level', NULL, 'postmaster', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'wal_receiver_status_interval', 's', 'sighup', 'integer', '0', '2147483');
INSERT INTO adbmgr.parm VALUES ('*', 'wal_receiver_timeout', 'ms', 'sighup', 'integer', '0', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'wal_segment_size', '8kB', 'internal', 'integer', '2048', '2048');
INSERT INTO adbmgr.parm VALUES ('*', 'wal_sender_timeout', 'ms', 'sighup', 'integer', '0', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'wal_sync_method', NULL, 'sighup', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'wal_writer_delay', 'ms', 'sighup', 'integer', '1', '10000');
INSERT INTO adbmgr.parm VALUES ('*', 'work_mem', 'kB', 'user', 'integer', '64', '2147483647');
INSERT INTO adbmgr.parm VALUES ('*', 'xmlbinary', NULL, 'user', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'xmloption', NULL, 'user', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('*', 'zero_damaged_pages', NULL, 'superuser', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'agtm_host', NULL, 'postmaster', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'agtm_port', '', 'postmaster', 'integer', '1', '65535');
INSERT INTO adbmgr.parm VALUES ('#', 'enable_adb_ha_sync', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'enable_adb_ha_sync_select', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'enable_fast_query_shipping', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'enable_remotegroup', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'enable_remotejoin', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'enable_remotelimit', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'enable_remotesort', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'enforce_two_phase_commit', NULL, 'superuser', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'grammar', NULL, 'user', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'max_coordinators', '', 'postmaster', 'integer', '2', '65535');
INSERT INTO adbmgr.parm VALUES ('#', 'max_datanodes', '', 'postmaster', 'integer', '2', '65535');
INSERT INTO adbmgr.parm VALUES ('#', 'max_pool_size', '', 'postmaster', 'integer', '1', '65535');
INSERT INTO adbmgr.parm VALUES ('#', 'max_prepared_transactions', '', 'postmaster', 'integer', '0', '536870911');
INSERT INTO adbmgr.parm VALUES ('G', 'max_prepared_transactions', '', 'postmaster', 'integer', '0', '8388607');
INSERT INTO adbmgr.parm VALUES ('#', 'min_pool_size', '', 'postmaster', 'integer', '1', '65535');
INSERT INTO adbmgr.parm VALUES ('#', 'nls_date_format', NULL, 'user', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'nls_timestamp_format', NULL, 'user', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'nls_timestamp_tz_format', NULL, 'user', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'persistent_datanode_connections', NULL, 'backend', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'pgxc_node_name', NULL, 'postmaster', 'string', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'pgxcnode_cancel_delay', 'ms', 'user', 'integer', '0', '2147483647');
INSERT INTO adbmgr.parm VALUES ('#', 'pool_remote_cmd_timeout', 'ms', 'postmaster', 'integer', '0', '2147483647');
INSERT INTO adbmgr.parm VALUES ('#', 'remotetype', NULL, 'backend', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'require_replicated_table_pkey', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'snapshot_level', NULL, 'user', 'enum', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'xc_enable_node_tcp_log', NULL, 'user', 'bool', NULL, NULL);
INSERT INTO adbmgr.parm VALUES ('#', 'xc_maintenance_mode', NULL, 'superuser', 'bool', NULL, NULL);
