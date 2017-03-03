#include "postgres_fe.h"

#include "lib/ilist.h"

typedef struct Location
{
	char * location;
	slist_node	next;
} Location;

int main(int argc, char **argv)
{
	int flag;
	slist_iter	cache_iter;
	slist_mutable_iter siter;
	slist_head head;
	slist_init(&head);

	for (flag = 0; flag < 10; flag++)
	{
		Location *loc = (Location*)palloc0(sizeof(Location));
		if (flag == 8)
			loc->location = pg_strdup("dingqs");
		else
			loc->location = pg_strdup("abc");	
			
		slist_push_head(&head, &loc->next);
	}

	slist_foreach(cache_iter, &head)
	{
		Location * loc = slist_container(Location, next, cache_iter.cur);
		printf("location : %s \n",loc->location);
	}

	slist_foreach_modify (siter, &head)
	{
		Location * loc = slist_container(Location, next, siter.cur);
		if (strcmp(loc->location, "dingqs") == 0)
			slist_delete_current(&siter);
	}

	printf("----------------------------------------------------\n");
	slist_foreach(cache_iter, &head)
	{
		Location * loc = slist_container(Location, next, cache_iter.cur);
		printf("location : %s \n",loc->location);
	}
	return 0;
}