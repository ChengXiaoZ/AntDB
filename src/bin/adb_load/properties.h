#ifndef PROPERIES_H
#define PROPERIES_H

int 	InitConfig (const char *filepath);
char  * GetConfValue (const char *key);
void    GetConfValues (const char **key, char **values, int count);
int 	SetValue (const char *key, const char *value);
int		PropsRelease (void);
int 	PropsDel (const char *key);
int		PropsAdd (const char *key, const char *value);
int		PropsGetCount (void);
void 	PrintConfig (void);
void 	DestoryConfig (void);
#endif