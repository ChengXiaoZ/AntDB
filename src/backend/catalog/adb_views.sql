/*
 * PGXC system view to look for prepared transaction GID list in a cluster
 */
CREATE OR REPLACE FUNCTION pgxc_prepared_xact()
RETURNS setof text
AS $$
DECLARE
	text_output text;
	row_data record;
	row_name record;
	query_str text;
	query_str_nodes text;
	BEGIN
		--Get all the node names
		query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type = ''D''';
		FOR row_name IN EXECUTE(query_str_nodes) LOOP
			query_str := 'EXECUTE DIRECT ON ("' || row_name.node_name || '") ''SELECT gid FROM pg_prepared_xact()''';
			FOR row_data IN EXECUTE(query_str) LOOP
				return next row_data.gid;
			END LOOP;
		END LOOP;
		return;
	END; $$
LANGUAGE 'plpgsql';

CREATE VIEW pgxc_prepared_xacts AS
    SELECT DISTINCT * from pgxc_prepared_xact();

CREATE OR REPLACE VIEW rxact_get_running AS
    SELECT r.gid,r.database,CASE WHEN n.node_name IS NULL THEN 'AGTM' ELSE n.node_name END,r.backend,r.type,r.status[r.i]
      FROM (SELECT *,generate_series(1,array_length(nodes,1)) AS i
        FROM (SELECT r.gid,d.datname AS database,r.type,r.backend,r.nodes,r.status
          FROM pg_catalog.rxact_get_running() AS r
            LEFT JOIN pg_catalog.pg_database AS d
            ON r.dbid=d.oid
          ) AS r
      ) AS r
        LEFT JOIN pg_catalog.pgxc_node AS n
        ON r.nodes[r.i] = n.oid;
