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
			query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT gid FROM pg_prepared_xact()''';
			FOR row_data IN EXECUTE(query_str) LOOP
				return next row_data.gid;
			END LOOP;
		END LOOP;
		return;
	END; $$
LANGUAGE 'plpgsql';

CREATE VIEW pgxc_prepared_xacts AS
    SELECT DISTINCT * from pgxc_prepared_xact();
