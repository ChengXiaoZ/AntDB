#include <string.h>
#include <stdlib.h>

/*
#ifndef _LOG_H_
#define _LOG_H_

#include <string.h>
#include <stdlib.h>
#include <log4c.h>

#define LOG_PRI_ERROR 		LOG4C_PRIORITY_ERROR
#define LOG_PRI_WARN 		LOG4C_PRIORITY_WARN
#define LOG_PRI_NOTICE 		LOG4C_PRIORITY_NOTICE
#define LOG_PRI_DEBUG 		LOG4C_PRIORITY_DEBUG
#define LOG_PRI_TRACE 		LOG4C_PRIORITY_TRACE

extern int log_open(const char *category);
extern void log_message(int priority ,const char* fmt, ...);
extern void log_trace(const char *file , int line , const char *func, const char *fmt ,...);
extern int log_close();

#define LOG_ERROR(fmt , args...)	\
	log_message(LOG_PRI_ERROR, fmt, ##args)

#define LOG_WARN(fmt, args...)		\
	log_message(LOG_PRI_WARN, fmt , ##args)

#define LOG_NOTICE(fmt , args...)	\
	log_message(LOG_PRI_NOTICE, fmt , ##args)

#define LOG_DEBUG(fmt , args...)	\
	log_message(LOG_PRI_DEBUG, fmt , ##args)

#define LOG_TRACE(fmt,args...) 		\
	log_trace(__FILE__ , __LINE__ , __FUNCTION__ , fmt , ##args)

#define LOG(priority, fmt , args...)                                     \
do                                                                       \
{                                                                        \
	if (priority == LOG_PRI_ERROR)                                       \
		log_message(LOG_PRI_ERROR, fmt, __VA_ARGS__);                    \
	if (priority == LOG_PRI_WARN)                                        \
		log_message(LOG_PRI_WARN, fmt, __VA_ARGS__);                     \
	if (priority == LOG_PRI_NOTICE)                                      \
		log_message(LOG_PRI_NOTICE, fmt, __VA_ARGS__);                   \
	if (priority == LOG_PRI_DEBUG)                                       \
		log_message(LOG_PRI_DEBUG, fmt, __VA_ARGS__);                    \
	if (priority == LOG_PRI_TRACE)                                       \
		log_trace(__FILE__ , __LINE__ , __FUNCTION__ , fmt ,__VA_ARGS__);\ 
}while(0)


#endif
*/


