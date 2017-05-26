#include "postgres_fe.h"

#include "libpq-fe.h"
#include "libpq-int.h"
#include "lib/stringinfo.h"

int main(int argc, char **argv)
{
	PGconn 			*pg_conn_agtm;
	PGconn			*pg_conn_dn1;
	PGconn			*pg_conn_dn2;
	char			 port_buf[10];
	int				 AGtmPort    = 12998;
	char			*agtm_conninfo = "user=postgres host=10.1.226.202 port=12998 dbname=postgres options='-c lc_monetary=C -c DateStyle=iso,mdy -c	timezone=prc -c	geqo=on	-c intervalstyle=postgres'";
	char 			*dn1_conninfo  = "user=adb2.2 host=10.1.226.202 port=17998 dbname=postgres options='-c grammar=postgres -c remotetype=coordinator  -c lc_monetary=C -c DateStyle=iso,mdy -c	timezone=prc -c	geqo=on	-c intervalstyle=postgres'";
	char			*dn2_conninfo  = "user=adb2.2 host=10.1.226.201 port=17998 dbname=postgres options='-c grammar=postgres -c remotetype=coordinator  -c lc_monetary=C -c DateStyle=iso,mdy -c	timezone=prc -c	geqo=on	-c intervalstyle=postgres'";
	char			*agtm_port = NULL;
	PGresult   		*res;
	const char 		*buffer = "321\n";
	sprintf(port_buf, "%d", AGtmPort);

	pg_conn_agtm = PQconnectdb(agtm_conninfo);
	if(pg_conn_agtm == NULL)
	{

	}

	if (PQstatus((PGconn*)pg_conn_agtm) != CONNECTION_OK)
		PQerrorMessage((PGconn*)pg_conn_agtm);

	else
		agtm_port = (char*)PQparameterStatus(pg_conn_agtm, "agtm_port");

	pg_conn_dn1 = PQconnectdb(dn1_conninfo);
	if (PQstatus((PGconn*)pg_conn_dn1) != CONNECTION_OK)
		PQerrorMessage((PGconn*)pg_conn_dn1);

	pg_conn_dn2 = PQconnectdb(dn2_conninfo);
	if (PQstatus((PGconn*)pg_conn_dn2) != CONNECTION_OK)
		PQerrorMessage((PGconn*)pg_conn_dn2);

	if (pqSendAgtmListenPort(pg_conn_dn1, atoi(agtm_port)) < 0)
		fprintf(stderr, "could not send agtm port: %s", PQerrorMessage(pg_conn_dn1));

	res = PQexec(pg_conn_dn1, "COPY test FROM STDIN DELIMITER ',';");
 	if (PQresultStatus(res) != PGRES_COPY_IN)
    {
    	fprintf(stderr, "could not send buff: %s", PQerrorMessage(pg_conn_dn1));
  	}
	if (PQputCopyData(pg_conn_dn1, buffer, strlen(buffer)) <= 0)
		fprintf(stderr, "could not send buff: %s", PQerrorMessage(pg_conn_dn1));

	if (PQputCopyEnd(pg_conn_dn1, NULL) == 1)
	{
		res = PQgetResult(pg_conn_dn1);
		if (PQresultStatus(res) == PGRES_COMMAND_OK)
		{

		}
	}
	return 0;
}

