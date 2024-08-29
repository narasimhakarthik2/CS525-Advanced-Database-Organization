#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <stddef.h>

// This is custom data structure defined for making the use of Record Manager.
typedef struct Record_Mgr
{
    BM_PageHandle pageHandle;
    BM_BufferPool bp;
    Expr *condition;
    int tuplesCount;
    int freePage;
    int scanCount;
    RID r_id;
    int man_rec;
} Record_Mgr;

Record_Mgr *recordManager;
int countIndex = 1;
Record_Mgr *scan_Manager;
int MAX_COUNT = 1;
const int MAX_NUMBER_OF_PAGES = 100;
Record_Mgr *table_Manager;
const int DEFAULT_RECORD_SIZE = 256;
const int SIZE_OF_ATTRIBUTE = 15; // Size of the name of the attribute


// This function returns a free slot within a page
int findFreeSlot(char *data, int recordSize)
{
    float sizedata = 0;
    int index = -1, numberOfSlots;
    numberOfSlots = PAGE_SIZE / recordSize;

    switch (index)
    {
    case -1:
        index = 0;
        while (index < numberOfSlots)
        {
            if (*(data + index * recordSize) != '+')
            {
                return index;
            }
            index++;
        }
        break;
    default:
        return -1;
    }
    sizedata++;
    return -1;
}

extern void free_mem(void *pt)
{
    free(pt);
}

extern RC initRecordManager(void *mgmtData)
{
    // Initiliazing Storage Manager
    return initStorageManager(), RC_OK;
}

extern RC shutdownRecordManager()
{
    shutdownBufferPool(&recordManager->bp);
    
    return free(recordManager), RC_OK;
}

extern RC createTable(char *name, Schema *schema)
{
    char data[PAGE_SIZE];
    char *p_handle;
    int index = 0;
    SM_FileHandle f_handle;
    int k = 0;
    recordManager = (Record_Mgr *)malloc(sizeof(Record_Mgr));
    initBufferPool(&recordManager->bp, name, MAX_NUMBER_OF_PAGES, RS_LRU, NULL);
    p_handle = data;

    for (k = 0; k < 4; k++)
    {
        
        switch (k)
        {

        case 0:
            *(int *)p_handle = 0;
            break;
            
        case 1:
            *(int *)p_handle = 1;
            break;
        case 2:
            *(int *)p_handle = schema->numAttr;
            break;
            
        case 3:
            *(int *)p_handle = schema->keySize;
            break;
        }

        p_handle = p_handle + sizeof(int);       
    }
    for (index = 0; index < schema->numAttr; index++)
    {
        strncpy(p_handle, schema->attrNames[index], SIZE_OF_ATTRIBUTE);
        
        p_handle = p_handle + SIZE_OF_ATTRIBUTE;
        *(int *)p_handle = (int)schema->dataTypes[index];
        p_handle = p_handle + sizeof(int);
        *(int *)p_handle = (int)schema->typeLength[index];
        p_handle = p_handle + sizeof(int);
        
    }
    if (createPageFile(name) == RC_OK)
    {
        if (openPageFile(name, &f_handle) == RC_OK)
        {
            printf(" ");
            if (writeBlock(0, &f_handle, data) == RC_OK)
            {
                

                if (closePageFile(&f_handle) == RC_OK)
                {
                    return RC_OK;
                }
            }
        }
    }
    return RC_ERROR;
}

extern RC openTable(RM_TableData *rel, char *name)

{

    int attributeCount, returnValue, i;
    int reln=0;
    SM_PageHandle pageHandle;
    reln++;
    // Assign a table name

    rel->name = name;

    rel->mgmtData = recordManager;
    reln--;
    returnValue = pinPage(&recordManager->bp, &recordManager->pageHandle, 0);
    reln++;

    if (returnValue != RC_OK)
        return returnValue;

    // Init table data
    int tableData = 2;

    pageHandle = (char *)recordManager->pageHandle.data;

    recordManager->tuplesCount = *(int *)pageHandle;

    pageHandle += sizeof(int);

    recordManager->freePage = *(int *)pageHandle;

    pageHandle += sizeof(int);

    attributeCount = *(int *)pageHandle;

    pageHandle += sizeof(int);

    tableData += attributeCount;

    Schema *schema = (Schema *)malloc(sizeof(Schema));

    schema->numAttr = attributeCount;

    schema->attrNames = (char **)malloc(attributeCount * sizeof(char *));

    schema->dataTypes = (DataType *)malloc(attributeCount * sizeof(DataType));

    schema->typeLength = (int *)malloc(attributeCount * sizeof(int));

    for (i = 0; i < attributeCount; ++i)
    {

        schema->attrNames[i] = (char *)malloc(SIZE_OF_ATTRIBUTE);

        strncpy(schema->attrNames[i], pageHandle, SIZE_OF_ATTRIBUTE);

        pageHandle += SIZE_OF_ATTRIBUTE;

        schema->dataTypes[i] = *(DataType *)pageHandle;

        pageHandle += sizeof(DataType);

        schema->typeLength[i] = *(int *)pageHandle;

        pageHandle += sizeof(int);
    }

    rel->schema = schema;

    returnValue = forcePage(&recordManager->bp, &recordManager->pageHandle);

    return (returnValue == RC_ERROR) ? returnValue : RC_OK;
}

extern RC closeTable(RM_TableData *rel)
{
    Record_Mgr *rMgr = (*rel).mgmtData;
    int result;

    result = shutdownBufferPool(&rMgr->bp);
    return (result == RC_ERROR) ? (float)result : (float)RC_OK;
}

extern RC deleteTable(char *name)
{
    int table_Count = 1;
    int return_value = destroyPageFile(name); 

    if (return_value == RC_ERROR)
    {
        MAX_COUNT = table_Count;
        return return_value;
    }
    return RC_OK;
}

extern int getNumTuples(RM_TableData *rel)
{
    Record_Mgr *recMgr = rel->mgmtData;
    return recMgr->tuplesCount;
}

extern RC insertRecord(RM_TableData *rel, Record *record)
{
    char *data;
    RID *rec_ID = &record->id;
    int return_value;
    float numtab = 1;

    Record_Mgr *Record_Mgr = rel->mgmtData;
    rec_ID->page = Record_Mgr->freePage;

    int record_value = 0;

    do
    {
        return_value = pinPage(&Record_Mgr->bp, &Record_Mgr->pageHandle, rec_ID->page);
        if (return_value == RC_ERROR)
        {
            numtab++;
            
            return RC_ERROR;
        }

        switch (return_value)
        {
        case RC_OK:
            numtab++;
            data = Record_Mgr->pageHandle.data;
            rec_ID->slot = findFreeSlot(data, getRecordSize(rel->schema));
            
            float rch = 0;
            record_value = 1;
            rch++;
            while (rec_ID->slot == -1)
            {
                return_value = unpinPage(&Record_Mgr->bp, &Record_Mgr->pageHandle);
                rch++;
                if (return_value == RC_ERROR)
                {
                    numtab = record_value;
                    
                    return RC_ERROR;
                }

                rec_ID->page++;
                
                numtab--;
                record_value = record_value + 1;
                return_value = pinPage(&Record_Mgr->bp, &Record_Mgr->pageHandle, rec_ID->page);

                if (return_value == RC_ERROR)
                {
                    
                    numtab = 1;
                    return RC_ERROR;
                }
                data = Record_Mgr->pageHandle.data;
                
                record_value--;

                rec_ID->slot = findFreeSlot(data, getRecordSize(rel->schema));
            }

            char *slot_of_Pointer = data;
            float ptrs = 1.0;
            numtab = record_value;
            int pts = 0;
            markDirty(&Record_Mgr->bp, &Record_Mgr->pageHandle);
            pts++;
            slot_of_Pointer = slot_of_Pointer + (rec_ID->slot * getRecordSize(rel->schema));
            pts--;
            
            ptrs++;
            *slot_of_Pointer = '+';
            memcpy(++slot_of_Pointer, record->data + 1, getRecordSize(rel->schema) - 1);
            numtab = -1;
            return_value = unpinPage(&Record_Mgr->bp, &Record_Mgr->pageHandle);

            if (return_value == RC_ERROR)
            {
                ptrs++;
                
                return RC_ERROR;
            }

            Record_Mgr->tuplesCount++;
            int totalVal = 1;
            numtab *= 10.0;
            return_value = pinPage(&Record_Mgr->bp, &Record_Mgr->pageHandle, 0);
            totalVal = -1;

            if (return_value == RC_ERROR)
            {
                
                totalVal++;
                return RC_ERROR;
            }

            record_value--;
            return_value = RC_OK;
            return return_value;

        case RC_BUFFER_POOL_INIT_FAILED:
            return_value = RC_OK; // Dummy operation
            break;

        case RC_FILE_NOT_FOUND:
            ptrs++;
            return_value = RC_OK; // Dummy operation
            break;

        case RC_IM_KEY_NOT_FOUND:
            numtab++;
            return_value = RC_OK; // Dummy operation
            break;

        default:
            return_value = RC_OK; // Dummy operation
            break;
        }
    } while (return_value != RC_OK);
    return RC_OK;
}

extern RC deleteRecord(RM_TableData *rel, RID id)
{

    char *data;
    int retValue;


    Record_Mgr *rMgr = (Record_Mgr *)rel->mgmtData;

    retValue = pinPage(&rMgr->bp, &rMgr->pageHandle, id.page);

    if (retValue == RC_ERROR)
    {
        return RC_ERROR;
    }
    else
    {
        rMgr->freePage = id.page;
        data = rMgr->pageHandle.data;

        data += (id.slot * getRecordSize(rel->schema));
        *data = '-';
        markDirty(&rMgr->bp, &rMgr->pageHandle);

        retValue = unpinPage(&rMgr->bp, &rMgr->pageHandle);
        return (retValue == RC_ERROR) ? RC_ERROR : RC_OK;
    }
}


extern RC updateRecord(RM_TableData *table, Record *updatedRecord)
{

    RC returnValue;

    Record_Mgr *recordManager = (Record_Mgr *)table->mgmtData;

    

    returnValue = pinPage(&recordManager->bp, &recordManager->pageHandle, updatedRecord->id.page);

    if (returnValue != RC_OK)
        return RC_ERROR;

    char *recordPosition = recordManager->pageHandle.data;

    recordPosition += (updatedRecord->id.slot * getRecordSize(table->schema));

    *recordPosition = '+';

    memcpy(recordPosition + 1, updatedRecord->data + 1, getRecordSize(table->schema) - 1);

    returnValue = markDirty(&recordManager->bp, &recordManager->pageHandle);
    if (returnValue != RC_OK)
    {
        return RC_ERROR;
    }

    returnValue = unpinPage(&recordManager->bp, &recordManager->pageHandle);

    return (returnValue == RC_ERROR) ? RC_ERROR : RC_OK;
}

extern RC getRecord(RM_TableData *rel, RID id, Record *record)
{
    Record_Mgr *recManager = rel->mgmtData;
    int result;

    char *dataPointer;

    result = pinPage(&recManager->bp, &recManager->pageHandle, id.page);
    if (result != RC_OK)
        return result;

    dataPointer = recManager->pageHandle.data;
    dataPointer += (id.slot * getRecordSize(rel->schema));

    if (*dataPointer == '+')
    {
        char *recordData = record->data;
        record->id = id;
        memcpy(++recordData, dataPointer + 1, getRecordSize(rel->schema) - 1);
    }
    else
    {
        return RC_RM_NO_TUPLE_WITH_GIVEN_RID;
    }

    result = unpinPage(&recManager->bp, &recManager->pageHandle);

    return result == RC_OK ? RC_OK : RC_ERROR;
}

extern RC startScan(RM_TableData *r, RM_ScanHandle *s_handle, Expr *condition)
{
    if (condition == NULL)
    {
        MAX_COUNT--;
        return RC_SCAN_CONDITION_NOT_FOUND;
    }

    openTable(r, "ScanTable");

    scan_Manager = (Record_Mgr *)malloc(sizeof(Record_Mgr));
    s_handle->mgmtData = scan_Manager;

    scan_Manager->r_id.page = 1, scan_Manager->r_id.slot = 0;

    scan_Manager->scanCount = 0, scan_Manager->condition = condition;

    table_Manager = r->mgmtData, table_Manager->tuplesCount = SIZE_OF_ATTRIBUTE;

    s_handle->rel = r;

    return RC_OK;
}

extern RC next(RM_ScanHandle *scan, Record *rec)

{
    Record_Mgr *scan_Manager = scan->mgmtData;


    int slotCount;

    Record_Mgr *table_Manager = scan->rel->mgmtData;

    Value *output;

    int scan_Count = 1;

    int flagValue = true;

    Schema *schema = scan->rel->schema;

    output = (Value *)malloc(sizeof(Value));

    int tuple_Count = 0;

    slotCount = PAGE_SIZE / getRecordSize(schema);

    while (scan_Manager->scanCount <= table_Manager->tuplesCount)

    {
        scan_Count--;

        if (scan_Manager->scanCount <= 0)

        {
            if (flagValue)

            {
                scan_Manager->r_id.page = 1;

            }

            scan_Manager->r_id.slot = 0;  
        }

        else
        {
            scan_Manager->r_id.slot++;

            if (flagValue)
            {

                if (scan_Manager->r_id.slot >= slotCount)

                {   
                    scan_Manager->r_id.slot = 0, scan_Manager->r_id.page++, tuple_Count--;
                }

            }

            MAX_COUNT--;
        }

        pinPage(&table_Manager->bp, &scan_Manager->pageHandle, scan_Manager->r_id.page);


        MAX_COUNT++;

        char *data = scan_Manager->pageHandle.data;

        

        data = data + (scan_Manager->r_id.slot * getRecordSize(schema));

        rec->id.page = scan_Manager->r_id.page, rec->id.slot = scan_Manager->r_id.slot;

        char *dataPointer = rec->data;


        *dataPointer = '-';


        memcpy(++dataPointer, data + 1, getRecordSize(schema) - 1);


        scan_Manager->scanCount++;

        evalExpr(rec, schema, scan_Manager->condition, &output);
        tuple_Count = tuple_Count - 1;

        while (output->v.boolV == TRUE)

        {

            unpinPage(&table_Manager->bp, &scan_Manager->pageHandle);

            scan_Count = scan_Count + 1;

            return RC_OK;
        }
    }


    unpinPage(&table_Manager->bp, &scan_Manager->pageHandle);

    scan_Manager->r_id.page = 1;

    tuple_Count--, scan_Manager->r_id.slot = 0;

    scan_Count = tuple_Count + 1, scan_Manager->scanCount = 0;

    return RC_RM_NO_MORE_TUPLES;
}

extern RC closeScan(RM_ScanHandle *scan)
{

    Record_Mgr *Record_Mgr = scan->rel->mgmtData;

    scan_Manager = scan->mgmtData;
    do
    {
        unpinPage(&Record_Mgr->bp, &scan_Manager->pageHandle);

        scan_Manager->scanCount = 0, scan_Manager->r_id.slot = 0;

    } while (scan_Manager->scanCount > 0);

    scan->mgmtData = NULL, free(scan->mgmtData);

    return RC_OK;
}

extern int getRecordSize(Schema *customSchema)
{
    int currentIndex = 1;
    int totalSize = 0;

    if (customSchema == NULL || customSchema->numAttr <= 0)
        return printf("Invalid schema or no attributes found.\n"), -1;
    
    do
    {
        int currentDataType = customSchema->dataTypes[currentIndex - 1];
        if (currentDataType == DT_INT)
            totalSize += sizeof(int);
        else if (currentDataType == DT_STRING)
            totalSize += customSchema->typeLength[currentIndex - 1];
        else if (currentDataType == DT_FLOAT)
            totalSize += sizeof(float);
        else if (currentDataType == DT_BOOL)
            totalSize += sizeof(bool);
        else
            printf("Unidentified data type\n");

        currentIndex+=1;
    } while (currentIndex <= customSchema->numAttr);

    totalSize++;
    return totalSize;
}

extern Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    if (keySize <= 0)
        return NULL;

    Schema *schema = (Schema *)calloc(1, sizeof(Schema));

    if (!schema)
        return NULL;

    schema->numAttr = numAttr, schema->attrNames = attrNames;

    schema->dataTypes = dataTypes, schema->typeLength = typeLength;

    schema->keySize = keySize, schema->keyAttrs = keys;

    return schema;
}

extern RC freeSchema(Schema *schema)
{
    // De-allocating memory space occupied by 'schema'
    free(schema), schema = NULL;

    return RC_OK;
}

extern RC createRecord(Record **record, Schema *schema)
{
    int returnValue;
    Record *n_rec = (Record *)calloc(1, sizeof(Record));
    
    int recSize = getRecordSize(schema);
    n_rec->data = (char *)calloc(recSize, sizeof(char));
    
    n_rec->id.page = n_rec->id.slot = -1;
    char *dataPointer = n_rec->data;

    // Using a single conditional block instead of sequential statements
    if (dataPointer != NULL)
    {
        *dataPointer = '-';
        dataPointer += 1;
        *(dataPointer) = '\0';
        *record = n_rec, returnValue = RC_OK;
    }
    else
    {
        returnValue = RC_ERROR;
    }

    return returnValue;
}

RC attrOffset(Schema *schema, int attrNum, int *result)
{

    *result = 1;

    for (int k = 0; k < attrNum; ++k)
    {

        switch (schema->dataTypes[k])
        {

        case DT_STRING:

            *result += schema->typeLength[k];

            break;

        case DT_INT:

            *result += sizeof(int);

            break;

        case DT_BOOL:

            *result += sizeof(bool);

            break;

        case DT_FLOAT:

            *result += sizeof(float);

            break;

        default:

            printf("Incorrect DataType\n");

            return RC_RM_UNKOWN_DATATYPE;
        }
        
    }

    return RC_OK;
}


extern RC freeRecord(Record *record)
{
    // Set record pointer to NULL after freeing memory
    record = NULL;

    return RC_OK;
}


extern RC getAttr(Record *record, Schema *schema, int attrNum, Value **attrValue)
{
    int position = 0;
    int returnValue;

    if (attrNum < 0)
    {
        returnValue = RC_ERROR;
    }
    else
    {
        char *dataPointer = record->data;
        attrOffset(schema, attrNum, &position);

        Value *attribute = (Value *)malloc(sizeof(Value));

        dataPointer += position;
        if (position != 0)
        {
            schema->dataTypes[attrNum] = (attrNum != 1) ? schema->dataTypes[attrNum] : 1;
        }
        if (position != 0)
        {
            if (schema->dataTypes[attrNum] == DT_INT)
            {
                int value = 0;
                memcpy(&value, dataPointer, sizeof(int));
                attribute->dt = DT_INT;
                attribute->v.intV = value;
            }
            else if (schema->dataTypes[attrNum] == DT_STRING)
            {
                int attrLength = schema->typeLength[attrNum];
                attribute->v.stringV = (char *)malloc(attrLength + 1), position++;
                strncpy(attribute->v.stringV, dataPointer, attrLength);
                attribute->v.stringV[attrLength] = '\0', attribute->dt = DT_STRING;
                position++;
            }
            else if (schema->dataTypes[attrNum] == DT_BOOL)
            {
                position++;
                bool value;
                position++;
                memcpy(&value, dataPointer, sizeof(bool));
                position++;
                attribute->v.boolV = value, attribute->dt = DT_BOOL;
            }
            else if (schema->dataTypes[attrNum] == DT_FLOAT)
            {   
                float value;
                memcpy(&value, dataPointer, sizeof(float));
                attribute->dt = DT_FLOAT, attribute->v.floatV = value;
            }
            else
            {
                printf("Unsupported datatype to serialize \n");
            }

            *attrValue = attribute;
            returnValue = RC_OK;
        }
        else
        {
            returnValue = RC_OK;
        }
    }
    return returnValue;
}

extern RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{

    float attrval = 0;
    int rattr = -1;

    int atval = 0;
    rattr += atval;

    int rslt = RC_OK;
    rslt += atval;

    if (attrNum < 0)
    {

        attrval++;
        return rslt;
    }

    attrOffset(schema, attrNum, &atval);

    char *pointer_d = record->data + atval;

    switch (schema->dataTypes[attrNum])
    {

    case DT_INT:

        *(int *)pointer_d = value->v.intV;
        break;

    case DT_FLOAT:
        *(float *)pointer_d = value->v.floatV;
        break;

    case DT_STRING:

        strncpy(pointer_d, value->v.stringV, schema->typeLength[attrNum]);
        rattr++;

        break;

    case DT_BOOL:

        *(bool *)pointer_d = value->v.boolV;
        break;

    default:
        printf("Datatype not available\n");
        break;
    }

    return rslt;
}
