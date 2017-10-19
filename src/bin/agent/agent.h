#ifndef AGENT_H
#define AGENT_H

#include "c.h"
#include "utils/elog.h"
#include "utils/palloc.h"

#define CHECK_FOR_INTERRUPTS()		\
	do { 							\
		if (InterruptPending)		\
			agt_ProcessInterrupts();	\
	} while(0)

#define HOLD_CANCEL_INTERRUPTS()  (QueryCancelHoldoffCount++)

#define RESUME_CANCEL_INTERRUPTS() \
do { \
	Assert(QueryCancelHoldoffCount > 0); \
	QueryCancelHoldoffCount--; \
} while(0)

extern PGDLLIMPORT volatile bool InterruptPending;
extern PGDLLIMPORT volatile bool QueryCancelPending;
extern PGDLLIMPORT volatile bool ProcDiePending;
extern PGDLLIMPORT volatile uint32 QueryCancelHoldoffCount;
extern void agt_ProcessInterrupts(void);

/*
 * These declarations supports the assertion-related macros in c.h.
 * assert_enabled is here because that file doesn't have PGDLLIMPORT in the
 * right place, and ExceptionalCondition must be present, for the backend only,
 * even when assertions are not enabled.
 */
extern PGDLLIMPORT bool assert_enabled;
extern const char *agent_argv0;

extern void ExceptionalCondition(const char *conditionName,
					 const char *errorType,
			 const char *fileName, int lineNumber) __attribute__((noreturn));

extern void agent_backend(pgsocket fd) __attribute__((noreturn));

#endif /* AGENT_H */