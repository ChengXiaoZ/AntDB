/*-------------------------------------------------------------------------
 *
 * Slot.h
 *  Routines for Slot management
 *
 * src/include/pgxc/slot.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef Slot_H
#define Slot_H

#include "nodes/parsenodes.h"

extern bool enable_slot;
extern bool adb_slot_enable_mvcc;
extern bool adb_slot_enable_clean;

#define SLOTSIZE 1024
#define SLOTBEGIN 	0
#define SLOTEND 	(SLOTSIZE-1)

#define UNINIT_SLOT_VALUE	  -2

#define INVALID_SLOT_VALUE    -1

#define SlotStatusOnlineInDB	1
#define SlotStatusMoveInDB		2
#define SlotStatusCleanInDB		3

extern char*	PGXCNodeName;


extern void SlotShmemInit(void);
extern Size SlotShmemSize(void);

//extern void SlotUploadToSharedMem(bool login);
extern void SlotUploadLogin(void);

extern void SlotGetInfo(int slotid, int* pnodeindex, int* pstatus);

extern void SlotAlter(AlterSlotStmt *stmt);
extern void SlotCreate(CreateSlotStmt *stmt);
extern void SlotRemove(DropSlotStmt *stmt);

extern void SlotFlush(FlushSlotStmt* stmt);
extern void SlotClean(CleanSlotStmt* stmt);

extern bool HeapTupleSatisfiesSlot(Relation rel, HeapTuple tuple);
extern int GetHeapTupleSlotId(Relation rel, HeapTuple tuple);
extern int GetValueSlotId(Relation rel, Datum value, AttrNumber	attrNum);

#endif
