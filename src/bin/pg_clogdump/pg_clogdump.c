/*-------------------------------------------------------------------------
 *
 * pg_clogdump.c - decode and display transaction status of any transaction id
 *
 * Copyright (c) 2014-2017, ADB Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_clogdump/pg_clogdump.c
 *-------------------------------------------------------------------------
 */
#define FRONTEND 1
#include "postgres.h"

#include <dirent.h>
#include <unistd.h>

#include "access/clog.h"
#include "access/transam.h"
#include "common/fe_memutils.h"
#include "getopt_long.h"

/* copy from src/backend/access/transam/clog.c */
#define CLOG_BITS_PER_XACT				2
#define CLOG_XACTS_PER_BYTE 			4
#define CLOG_XACTS_PER_PAGE 			(BLCKSZ * CLOG_XACTS_PER_BYTE)
#define CLOG_XACT_BITMASK				((1 << CLOG_BITS_PER_XACT) - 1)

#define TransactionIdToPage(xid)		((xid) / (TransactionId) CLOG_XACTS_PER_PAGE)
#define TransactionIdToPgIndex(xid) 	((xid) % (TransactionId) CLOG_XACTS_PER_PAGE)
#define TransactionIdToByte(xid)		(TransactionIdToPgIndex(xid) / CLOG_XACTS_PER_BYTE)
#define TransactionIdToBIndex(xid)		((xid) % (TransactionId) CLOG_XACTS_PER_BYTE)

/* copy from src/include/access/slru.h */
#define SLRU_PAGES_PER_SEGMENT	32

#define TransactionIdToSegment(xid)		(TransactionIdToPage(xid) / SLRU_PAGES_PER_SEGMENT)
#define TransactionIdToSegIndex(xid)	(TransactionIdToPage(xid) % SLRU_PAGES_PER_SEGMENT)

#define CLOGDIR							"pg_clog"
#define debug_log						logstart(DEBUG1, __FILE__, __LINE__, PG_FUNCNAME_MACRO), logfinish
#define info_log						logstart(INFO, __FILE__, __LINE__, PG_FUNCNAME_MACRO), logfinish
#define warn_log						logstart(WARNING, __FILE__, __LINE__, PG_FUNCNAME_MACRO), logfinish
#define error_log						logstart(ERROR, __FILE__, __LINE__, PG_FUNCNAME_MACRO), logfinish
#define fatal_log						logstart(FATAL, __FILE__, __LINE__, PG_FUNCNAME_MACRO), logfinish

typedef struct CLogDumpConfig
{
	/* private options */
	char	   *inpath;							/* -p */

	/* display options */
	int 		stop_after_records;				/* -n */
	int 		already_displayed_records;

	/* filter options */
	TransactionId	start_xid;					/* -x */
} CLogDumpConfig;

static const char  *progname;
static bool			fatal_exit = false;
static bool			log_verbose = false;		/* -v */

static const char *loglevel(int elevel);
static void logstart(int elevel, const char *filename, int lineno, const char *funcname);
static void logfinish(const char *fmt,...) __attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));
static bool verify_directory(const char *directory);
static int fuzzy_open_file(const char *directory, const char *fname);
static void output_xid_status(TransactionId xid, char status);
static void display_xid_clog(CLogDumpConfig *config);

static const char *loglevel(int elevel)
{
	switch(elevel)
	{
		case DEBUG1:
			return "DEBUG";
		case INFO:
			return "INFO";
		case WARNING:
			return "WARNING";
		case ERROR:
			return "ERROR";
		case FATAL:
			return "FATAL";
		default:
			break;
	}
	return "UNKNOWN";
}

static void
logstart(int elevel, const char *filename, int lineno, const char *funcname)
{
	fflush(stdout);
	if (log_verbose)
		fprintf(stdout,	"LOCATION:  %s, %s:%d\n%s: %s:  ",
				funcname, filename, lineno,
				progname, loglevel(elevel));
	else
		fprintf(stdout,	"%s: %s:  ",
				progname, loglevel(elevel));

	if (elevel == FATAL)
		fatal_exit = true;
}

static void
logfinish(const char *fmt,...)
{
	va_list		args;

	va_start(args, fmt);
	vfprintf(stdout, fmt, args);
	va_end(args);
	fputc('\n', stdout);

	if (fatal_exit)
	{
		fprintf(stdout,
			"\nTry \"%s --help\" for more information.\n",
			progname);

		exit(EXIT_FAILURE);
	}
}

/*
 * Check whether directory exists and whether we can open it. Keep errno set so
 * that the caller can report errors somewhat more accurately.
 */
static bool
verify_directory(const char *directory)
{
	DIR		   *dir = opendir(directory);

	if (dir == NULL)
		return false;
	closedir(dir);
	return true;
}

/*
 * Try to find the file in several places:
 * if directory == NULL:
 *	 fname
 *	 CLOGDIR / fname
 *	 $PGDATA / CLOGDIR / fname
 * else
 *	 directory / fname
 *	 directory / CLOGDIR / fname
 *
 * return a read only fd
 */
static int
fuzzy_open_file(const char *directory, const char *fname)
{
	int			fd = -1;
	char		fpath[MAXPGPATH];

	if (directory == NULL)
	{
		const char *datadir;

		/* fname */
		fd = open(fname, O_RDONLY | PG_BINARY, S_IRUSR | S_IWUSR);
		if (fd < 0 && errno != ENOENT)
			return -1;
		else if (fd >= 0)
			return fd;

		/* CLOGDIR / fname */
		snprintf(fpath, MAXPGPATH, "%s/%s",
				 CLOGDIR, fname);
		fd = open(fpath, O_RDONLY | PG_BINARY, S_IRUSR | S_IWUSR);
		if (fd < 0 && errno != ENOENT)
			return -1;
		else if (fd >= 0)
			return fd;

		datadir = getenv("PGDATA");
		/* $PGDATA / CLOGDIR / fname */
		if (datadir != NULL)
		{
			snprintf(fpath, MAXPGPATH, "%s/%s/%s",
					 datadir, CLOGDIR, fname);
			fd = open(fpath, O_RDONLY | PG_BINARY, S_IRUSR | S_IWUSR);
			if (fd < 0 && errno != ENOENT)
				return -1;
			else if (fd >= 0)
				return fd;
		}
	}
	else
	{
		/* directory / fname */
		snprintf(fpath, MAXPGPATH, "%s/%s",
				 directory, fname);
		fd = open(fpath, O_RDONLY | PG_BINARY, S_IRUSR | S_IWUSR);
		if (fd < 0 && errno != ENOENT)
			return -1;
		else if (fd >= 0)
			return fd;

		/* directory / CLOGDIR / fname */
		snprintf(fpath, MAXPGPATH, "%s/%s/%s",
				 directory, CLOGDIR, fname);
		fd = open(fpath, O_RDONLY | PG_BINARY, S_IRUSR | S_IWUSR);
		if (fd < 0 && errno != ENOENT)
			return -1;
		else if (fd >= 0)
			return fd;
	}
	return -1;
}

/*
 * Output the transaction status of TransactionId xid.
 */
static void
output_xid_status(TransactionId xid, char status)
{
	const char *statusstr = NULL;

	switch (status)
	{
		case TRANSACTION_STATUS_IN_PROGRESS:
			statusstr = "IN PROGRESS";
			break;
		case TRANSACTION_STATUS_COMMITTED:
			statusstr = "COMMITTED";
			break;
		case TRANSACTION_STATUS_ABORTED:
			statusstr = "ABORTED";
			break;
		case TRANSACTION_STATUS_SUB_COMMITTED:
			statusstr = "SUB COMMITTED";
			break;
		default:
			statusstr = "UNKNOWN";
			break;
	}
	fprintf(stdout, "TransactionId: %u, status: %s\n", xid, statusstr);
}

/*
 * display transaction status of one or more xid.
 *
 * Note:
 *		try to display "config->stop_after_records" transactions' status.
 */
static void
display_xid_clog(CLogDumpConfig *config)
{
	TransactionId	xid;					/* start with xid */
	int				segno, sv_segno;		/* xid to segment, 0x0000 ~ 0x0FFF */
	int				rpageno, sv_rpageno;	/* xid to page index of segment, 0 ~  (SLRU_PAGES_PER_SEGMENT - 1) */
	int				offset;					/* page index of segment to offset, rpageno * BLCKSZ */
	int				byteno;					/* xid to byte number */
	int				bshift;					/* xid to bits index */
	int				fd;						/* file descriptor to segment file */
	char			clogfname[5];			/* 4 hex digits and '\0' */
	char			byteptr[BLCKSZ];		/* read one page with BLCKSZ bytes each time */
	char			xidstatus;

	AssertArg(config);
	xid = config->start_xid;
	Assert(TransactionIdIsValid(xid));

	sv_segno = -1;							/* decide to switch next segment */
	sv_rpageno = -1;						/* decide to read next page */
	fd = -1;
	while (true)							/* enough to quit or error to quit */
	{
		/* enough records to quit */
		if (config->already_displayed_records >= config->stop_after_records)
			break;

		/* xid to segment */
		segno = TransactionIdToSegment(xid);

		/* close old segment and open new segment */
		if (segno != sv_segno)
		{
			/* close old segment */
			if (fd > 0)
			{
				close(fd);
				fd = -1;
			}

			/* open new segment */
			snprintf(clogfname, sizeof(clogfname), "%04X", segno);
			fd = fuzzy_open_file(config->inpath, clogfname);
			if (fd < 0)
			{
				fatal_log("could not open file \"%s\" for transaction %u: %m.",
					clogfname, xid);
			}

			/* clear errno */
			errno = 0;

			/* keep segment number to decide to switch next segment */
			sv_segno = segno;
			sv_rpageno = -1;
		}

		/* xid to page index of a segment */
		rpageno = TransactionIdToSegIndex(xid);

		/* read next page */
		if (rpageno != sv_rpageno)
		{
			/* offset of page head */
			offset = rpageno * BLCKSZ;
			if (lseek(fd, (off_t) offset, SEEK_SET) < 0)
			{
				close(fd);
				fatal_log("Could not seek in file \"%s\" to page %d "
						  "offset %u for tansaction %u: %m.",
						  clogfname, rpageno, offset, xid);
			}

			/* read whole page bytes */
			if (read(fd, byteptr, BLCKSZ) != BLCKSZ)
			{
				close(fd);
				fatal_log("Could not read from file \"%s\" at page %d "
						  "offset %u for transaction %u: %m.",
						  clogfname, rpageno, offset, xid);
			}

			/* keep page index of the segment to decide to read next page  */
			sv_rpageno = rpageno;
		}

		byteno = TransactionIdToByte(xid);
		bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;
		xidstatus = (*(byteptr + byteno) >> bshift) & CLOG_XACT_BITMASK;
		output_xid_status(xid, xidstatus);
		config->already_displayed_records++;
		TransactionIdAdvance(xid);
	}
}

static void
usage(void)
{
	printf("%s decodes and displays PostgreSQL transaction status for debugging.\n\n",
		   progname);
	printf("Usage:\n");
	printf("  %s [OPTION]\n", progname);
	printf("\nOptions:\n");
	printf("  -n, --limit=N          number of records to display\n");
	printf("  -p, --path=PATH        directory in which to find commit log segment files or a\n");
	printf("                         directory with a ./pg_clog that contains such files\n"
		   "                         (default: current directory, ./pg_clog, PGDATA/pg_clog)\n");
	printf("  -v, --verbose          output verbose information, include funcname, lineno and filename\n");
	printf("  -V, --version          output version information, then exit\n");
	printf("  -x, --xid=XID          start show status of TransactionId XID\n");
	printf("  -?, --help             show this help, then exit\n");
}

int main(int argc, char * const argv[])
{
	CLogDumpConfig	config;
	static struct option long_options[] = {
			{"help", no_argument, NULL, '?'},
			{"limit", required_argument, NULL, 'n'},
			{"path", required_argument, NULL, 'p'},
			{"xid", required_argument, NULL, 'x'},
			{"verbose", no_argument, NULL, 'v'},
			{"version", no_argument, NULL, 'V'},
			{NULL, 0, NULL, 0}
		};
	
	int 		option;
	int 		optindex = 0;

	progname = get_progname(argv[0]);

	memset(&config, 0, sizeof(CLogDumpConfig));

	config.inpath = NULL;
	config.stop_after_records = 1;
	config.already_displayed_records = 0;
	config.start_xid = InvalidTransactionId;

	if (argc <= 1)
		fatal_log("%s: no arguments specified", progname);

	while ((option = getopt_long(argc, argv, "n:p:vV?x:",
								 long_options, &optindex)) != -1)
	{
		switch (option)
		{
			case '?':
				usage();
				exit(EXIT_SUCCESS);
				break;
			case 'n':
				if (sscanf(optarg, "%d", &config.stop_after_records) != 1)
				{
					fatal_log("%s: could not parse limit \"%s\"",
						progname, optarg);
				}
				if (config.stop_after_records <= 0)
					config.stop_after_records = 1;
				break;
			case 'p':
				config.inpath = pg_strdup(optarg);
				break;
			case 'v':
				log_verbose = true;
				break;
			case 'V':
				puts("pg_clogdump (PostgreSQL) " PG_VERSION);
				exit(EXIT_SUCCESS);
				break;
			case 'x':
				if (sscanf(optarg, "%u", &config.start_xid) != 1)
				{
					fatal_log("%s: could not parse \"%s\" as a valid xid",
						progname, optarg);
				}

				if (!TransactionIdIsValid(config.start_xid))
					fatal_log("Invalid xid %u", config.start_xid);
				break;
			default:
				fatal_log("Unknown argument(%c)", option);
				break;
		}
	}

	if ((optind + 2) < argc)
	{
		fatal_log("%s: too many command-line arguments (first is \"%s\")",
			progname, argv[optind + 2]);
	}

	if (config.inpath != NULL)
	{
		/* validate path points to directory */
		if (!verify_directory(config.inpath))
		{
			fatal_log("%s: path \"%s\" cannot be opened: %s",
				progname, config.inpath, strerror(errno));
		}
	}

	if (TransactionIdIsValid(config.start_xid))
		display_xid_clog(&config);
	else
		fatal_log("A valid xid is needed.");

	return EXIT_SUCCESS;
}
