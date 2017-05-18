#include "postgres_fe.h"

#include <stdio.h>
#include <pthread.h>

#include "linebuf.h"
#include "utils/palloc.h"

#define IsInitedLineBuf()	(max_nodes > 0)
#define DEFAULT_BUF_LEN		512
#define DEFAULT_BUF_STEP	512

static int max_nodes = -1;
static dlist_head buf_head = DLIST_STATIC_INIT(buf_head);
static pthread_mutex_t buf_mutex = PTHREAD_MUTEX_INITIALIZER;
static bool *bit_all_marked = NULL;
static bool *bit_all_unmarked = NULL;

static void destroy_linebuf(LineBuffer *buf);
static bool appendLineBufInfoVA(LineBuffer *buf,
								int *need,
								const char *fmt,
								va_list args);

void init_linebuf(int max_node)
{
	Assert(max_node > 0);

	bit_all_marked = palloc(max_node * 2);
	bit_all_unmarked = &(bit_all_marked[max_node]);
	memset(bit_all_marked, true, max_node);
	memset(bit_all_unmarked, false, max_node);
	if(pthread_mutex_init(&buf_mutex, NULL) != 0)
	{
		fprintf(stderr, "Can not initialize mutex:%s\n", strerror(errno));
		exit(1);
	}
	max_nodes = max_node;
}

void end_linebuf(void)
{
	dlist_node *node;
	LineBuffer *buf;
	if(!IsInitedLineBuf())
		return;

	while(!dlist_is_empty(&buf_head))
	{
		node = dlist_pop_head_node(&buf_head);
		buf = dlist_container(LineBuffer, dnode, node);
		destroy_linebuf(buf);
	}
	pthread_mutex_destroy(&buf_mutex);
	pfree(bit_all_marked);
	bit_all_marked = bit_all_unmarked = NULL;
	max_nodes = -1;
}

LineBuffer* get_linebuf(void)
{
	LineBuffer *buf;
	dlist_node *node;
	Assert(IsInitedLineBuf());

	pthread_mutex_lock(&buf_mutex);
	if(!dlist_is_empty(&buf_head))
	{
		node = dlist_pop_head_node(&buf_head);
		pthread_mutex_unlock(&buf_mutex);
		buf = dlist_container(LineBuffer, dnode, node);
		memset(buf->data, 0, DEFAULT_BUF_LEN);
		buf->len = 0;		
	}else
	{
		pthread_mutex_unlock(&buf_mutex);
		buf = palloc(offsetof(LineBuffer, marks) + max_nodes);
		buf->maxlen = DEFAULT_BUF_LEN;
		buf->data = palloc(DEFAULT_BUF_LEN);
		buf->len = 0;
	}
	unmarkall_linebuf(buf);
	return buf;
}

void release_linebuf(LineBuffer *buf)
{
	AssertArg(buf);
	Assert(IsInitedLineBuf());
	pthread_mutex_lock(&buf_mutex);
	dlist_push_tail(&buf_head, &(buf->dnode));
	pthread_mutex_unlock(&buf_mutex);
}

void markall_linebuf(LineBuffer *buf)
{
	Assert(IsInitedLineBuf());
	AssertArg(max_nodes > 0);
	memcpy(buf->marks, bit_all_marked, max_nodes);
}

void unmarkall_linebuf(LineBuffer *buf)
{
	Assert(IsInitedLineBuf());
	AssertArg(max_nodes > 0);
	memcpy(buf->marks, bit_all_unmarked, max_nodes);
}

bool is_markedall_linebuf(const LineBuffer *buf)
{
	Assert(IsInitedLineBuf());
	AssertArg(max_nodes > 0);
	return memcmp(buf->marks, bit_all_marked, max_nodes) == 0 ? true:false;
}

bool is_unmarkedall_linebuf(const LineBuffer *buf)
{
	Assert(IsInitedLineBuf());
	AssertArg(max_nodes > 0);
	return memcmp(buf->marks, bit_all_unmarked, max_nodes) == 0 ? true:false;
}

static void destroy_linebuf(LineBuffer *buf)
{
	if(buf)
	{
		if(buf->data)
			pfree(buf->data);
		pfree(buf);
	}
}

static bool appendLineBufInfoVA(LineBuffer *buf, int *need, const char *fmt, va_list args)
{
	int			avail,
				nprinted;

	Assert(buf != NULL);

	/*
	 * If there's hardly any space, don't bother trying, just fail to make the
	 * caller enlarge the buffer first.
	 */
	avail = buf->maxlen - buf->len - 1;
	if (avail < 16)
		return false;

	/*
	 * Assert check here is to catch buggy vsnprintf that overruns the
	 * specified buffer length.  Solaris 7 in 64-bit mode is an example of a
	 * platform with such a bug.
	 */
#ifdef USE_ASSERT_CHECKING
	buf->data[buf->maxlen - 1] = '\0';
#endif

	nprinted = vsnprintf(buf->data + buf->len, avail, fmt, args);

	Assert(buf->data[buf->maxlen - 1] == '\0');

	/*
	 * Note: some versions of vsnprintf return the number of chars actually
	 * stored, but at least one returns -1 on failure. Be conservative about
	 * believing whether the print worked.
	 */
	if (nprinted >= 0 && nprinted < avail - 1)
	{
		/* Success.  Note nprinted does not include trailing null. */
		buf->len += nprinted;
		return true;
	}

	/* Restore the trailing null so that buf is unmodified. */
	buf->data[buf->len] = '\0';
	if(need)
	{
		if(nprinted > 0)
			*need = (nprinted+1);
		else
			*need = buf->maxlen + DEFAULT_BUF_STEP;
	}
	return false;
}

void appendLineBufInfo(LineBuffer *buf, const char *fmt, ...)
{
	va_list		args;
	int			need;
	bool		success;

	for (;;)
	{
		/* Try to format the data. */
		va_start(args, fmt);
		success = appendLineBufInfoVA(buf, &need, fmt, args);
		va_end(args);

		if (success)
			break;

		enlargeLineBuf(buf, need);
	}
}

void appendLineBufInfoString(LineBuffer *buf, const char *str)
{
	appendLineBufInfoBinary(buf, str, strlen(str));
}

void appendLineBufInfoBinary(LineBuffer *buf, const void *bin, int len)
{
	Assert(buf != NULL);

	/* Make more room if needed */
	enlargeLineBuf(buf, len);

	/* OK, append the data */
	memcpy(buf->data + buf->len, bin, len);
	buf->len += len;

	/*
	 * Keep a trailing null in place, even though it's probably useless for
	 * binary data.  (Some callers are dealing with text but call this because
	 * their input isn't null-terminated.)
	 */
	buf->data[buf->len] = '\0';
}

void enlargeLineBuf(LineBuffer *buf, int needed)
{
	int new_size;
	int need_maxsize;
	Assert(buf != NULL);

	if(buf->maxlen - buf->len > needed)
		return;

	need_maxsize = buf->maxlen + needed + 1;
	new_size = (need_maxsize % DEFAULT_BUF_STEP) * DEFAULT_BUF_STEP;
	if(new_size < need_maxsize)
		new_size += DEFAULT_BUF_STEP;
	buf->data = realloc(buf->data, new_size);
}
