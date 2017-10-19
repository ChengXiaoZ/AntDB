#ifndef AGENT_UTILITY_H
#define AGENT_UTILITY_H

#include "lib/stringinfo.h"

/* in agt_cmd.c */
void do_agent_command(StringInfo buf);

/* in agent_utility.c */
int exec_shell(const char *exec, StringInfo out);

#endif /* AGENT_UTILITY_H */