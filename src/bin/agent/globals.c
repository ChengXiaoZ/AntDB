#include "agent.h"

#ifdef USE_ASSERT_CHECKING
bool assert_enabled = true;
#endif /* USE_ASSERT_CHECKING */

volatile bool InterruptPending = false;
volatile bool QueryCancelPending = false;
volatile bool ProcDiePending = false;
volatile uint32 QueryCancelHoldoffCount = 0;