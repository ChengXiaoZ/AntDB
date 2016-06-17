#include "postgres.h"

#include "libpq/libpq.h"
#include "libpq/pqnone.h"

static void pq_none_comm_reset(void);
static int	pq_none_flush(void);
static int	pq_none_flush_if_writable(void);
static bool pq_none_is_send_pending(void);
static int	pq_none_putmessage(char msgtype, const char *s, size_t len);
static void pq_none_putmessage_noblock(char msgtype, const char *s, size_t len);
static void pq_none_startcopyout(void);
static void pq_none_endcopyout(bool errorAbort);
static int	pq_none_get_id(void);

static PQcommMethods PqCommoNoneMethods = {
	pq_none_comm_reset,
	pq_none_flush,
	pq_none_flush_if_writable,
	pq_none_is_send_pending,
	pq_none_putmessage,
	pq_none_putmessage_noblock,
	pq_none_startcopyout,
	pq_none_endcopyout,
	pq_none_get_id
};

void pq_switch_to_none(void)
{
	PqCommMethods = &PqCommoNoneMethods;
}

static void pq_none_comm_reset(void)
{
}

static int	pq_none_flush(void)
{
	return 0;
}

static int	pq_none_flush_if_writable(void)
{
	return 0;
}

static bool pq_none_is_send_pending(void)
{
	return false;
}

static int	pq_none_putmessage(char msgtype, const char *s, size_t len)
{
	return 0;
}

static void pq_none_putmessage_noblock(char msgtype, const char *s, size_t len)
{
}

static void pq_none_startcopyout(void)
{
}

static void pq_none_endcopyout(bool errorAbort)
{
}

static int pq_none_get_id(void)
{
	return INVALID_PQ_ID;
}
