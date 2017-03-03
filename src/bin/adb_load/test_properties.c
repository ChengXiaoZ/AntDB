#include "postgres_fe.h"

#include <stdio.h>
#include "properties.h"

int main(int argc, char **argv)
{
	int ret;
	char *value;
    const char *filepath = "/home/lvcx/adbload/src/bin/adb_load/adb_load.conf.example";

    ret = initConfig(filepath);
    if (ret != 0) {
        printf("env init error:%d\n",ret);
        return 0;
    }
	value = getConfValue("ip");
	value = getConfValue("SERVER_IP");
	printConfig();
	return 0;
}

