#ifndef ADB_LOAD_PROPERIES_H
#define ADB_LOAD_PROPERIES_H

extern int InitConfig (const char *filepath);
extern char *GetConfValue (const char *key);
extern void GetConfValues (const char **key, char **values, int count);
extern int SetValue (const char *key, const char *value);
extern int PropsRelease (void);
extern int PropsDel (const char *key);
extern int PropsAdd (const char *key, const char *value);
extern int PropsGetCount (void);
extern void PrintConfig (void);
extern void DestoryConfig (void);

#endif /* ADB_LOAD_PROPERIES_H */