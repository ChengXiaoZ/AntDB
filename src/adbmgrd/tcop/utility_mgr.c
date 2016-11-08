#include "postgres.h"

#include "mgr/mgr_cmds.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "parser/mgr_node.h"
#include "tcop/utility.h"


const char *mgr_CreateCommandTag(Node *parsetree)
{
	const char *tag;
	AssertArg(parsetree);

	switch(nodeTag(parsetree))
	{
	case T_MGRAddHost:
		tag = "ADD HOST";
		break;
	case T_MGRDropHost:
		tag = "DROP HOST";
		break;
	case T_MGRAlterHost:
		tag = "ALTER HOST";
		break;
	case T_MGRAddNode:
		tag = "ADD NODE";
		break;
	case T_MGRAlterNode:
		tag = "ALTER NODE";
		break;
	case T_MGRDropNode:
		tag = "DROP NODE";
		break;
	case T_MGRDeplory:
		tag = "DEPLORY";
		break;
	case T_MGRUpdateparm:
		tag = "SET PARAM";
		break;
	case T_MGRUpdateparmReset:
		tag = "RESET PARAM";
		break;
	case T_MGRShowParam:
		tag = "SHOW PARAM";
		break;
	case T_MGRMonitorAgent:
		tag = "MONITOR AGENT";
		break;
	default:
		ereport(WARNING, (errmsg("unrecognized node type: %d", (int)nodeTag(parsetree))));
		tag = "???";
		break;
	}
	return tag;
}

void mgr_ProcessUtility(Node *parsetree, const char *queryString,
									ProcessUtilityContext context, ParamListInfo params,
									DestReceiver *dest,
									char *completionTag)
{
	AssertArg(parsetree);
	switch(nodeTag(parsetree))
	{
	case T_MGRAddHost:
		mgr_add_host((MGRAddHost*)parsetree, params, dest);
		break;
	case T_MGRDropHost:
		mgr_drop_host((MGRDropHost*)parsetree, params, dest);
		break;
	case T_MGRAlterHost:
		mgr_alter_host((MGRAlterHost*)parsetree, params, dest);
		break;
	case T_MGRAddNode:
		mgr_add_node((MGRAddNode*)parsetree, params, dest);
		break;
	case T_MGRAlterNode:
		mgr_alter_node((MGRAlterNode*)parsetree, params, dest);
		break;
	case T_MGRDropNode:
		mgr_drop_node((MGRDropNode*)parsetree, params, dest);
		break;
	case T_MGRDeplory:
		mgr_deplory((MGRDeplory*)parsetree, params, dest);
		break;
	case T_MGRUpdateparm:
		mgr_add_updateparm((MGRUpdateparm*)parsetree, params, dest);
		break;
	case T_MGRUpdateparmReset:
		mgr_reset_updateparm((MGRUpdateparmReset*)parsetree, params, dest);
		break;
	case T_MGRShowParam:
		mgr_showparam((MGRShowParam*)parsetree, params, dest);
		break;
	case T_MGRMonitorAgent:
		mgr_monitor_agent((MGRMonitorAgent*)parsetree, params, dest);
		break;
	default:
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			,errmsg("unrecognized node type: %d", (int)nodeTag(parsetree))));
		break;
	}
}
