#ifndef PROPERIES_H
#define PROPERIES_H

int 	initConfig (const char *filepath);
char  * getConfValue (const char *key);
void    getConfValues (const char **key, char **values, int count);
int 	setValue (const char *key, const char *value);
int		propsRelease (void);
int 	propsDel (const char *key);
int		propsAdd (const char *key, const char *value);
int		propsGetCount (void);
void 	printConfig (void);
#endif