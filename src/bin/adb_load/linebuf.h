#ifndef LINE_BUFFER_H_
#define LINE_BUFFER_H_

#include "lib/ilist.h"

typedef struct LineBuffer
{
	char		   *data;
	int				lineno;
	int				fileline;
	int				len;
	int				maxlen;
	dlist_node		dnode;
	bool			marks[1];	/* (VARIABLE LENGTH) */
}LineBuffer;

LineBuffer* get_linebuf(void);
void release_linebuf(LineBuffer *buf);

void init_linebuf(int max_node);
void end_linebuf(void);

void markall_linebuf(LineBuffer *buf);
void unmarkall_linebuf(LineBuffer *buf);
#define mark_linebuf(buf,n) ((buf)->marks[n] = true)
#define unmark_linebuf(buf,n) ((buf)->marks[n] = false)
#define is_marked_linebuf(buf,n) ((buf)->marks[n] ? true:false)
#define is_unmarked_linebuf(buf,n) ((buf)->marks[n] ? false:true)
bool is_markedall_linebuf(const LineBuffer *buf);
bool is_unmarkedall_linebuf(const LineBuffer *buf);

void appendLineBufInfo(LineBuffer *buf, const char *fmt, ...)
	__attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));
void appendLineBufInfoString(LineBuffer *buf, const char *str);
void appendLineBufInfoBinary(LineBuffer *buf, const void *bin, int len);
void enlargeLineBuf(LineBuffer *buf, int needed);

#endif /* LINE_BUFFER_H_ */
