#include <get_uptime.h>
#include <sys/sysinfo.h>

time_t get_uptime(void)
{
	struct sysinfo sinfo;
	if(0 != sysinfo(&sinfo))
		return 0;
	return (time_t)sinfo.uptime;
}

