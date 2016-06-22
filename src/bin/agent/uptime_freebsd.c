#include "get_uptime.h"

#include <sys/types.h>
#include <sys/sysctl.h>

time_t get_uptime(void)
{
	int             mib[2];
	size_t          len;
	struct timeval  uptime;

	mib[0] = CTL_KERN;
	mib[1] = KERN_BOOTTIME;

	len = sizeof(uptime);

	if (0 != sysctl(mib, 2, &uptime, &len, NULL, 0))
		return 0;

	return uptime.tv_sec;
}
