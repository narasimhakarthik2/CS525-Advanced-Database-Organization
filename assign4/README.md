# CS525: Advanced Database Organization
## Assignment 4 - B+ Tree
## Group 17
## Team Members
### Ananya Menon A20538657
### Rasika Venkataramanan A20536353
### G Narasimha Karthik A20536058
## Steps to run the code:
1. Execute `make clean` command to clear all the previously compiled files (.o files).
2. Execute `make` command to compile the program files, link and create executables.
3. Execute `make run` command to run the test cases.

### BTree Manager
1. BTreeMtdt *initializeBTreeMetadata(DataType keyType, int n)
2. RC initIndexManager(void *memVal) 
3. RC shutdownIndexManager()
4. void cleanupBTree(BTreeMtdt *memVal, char *headerData)
5. RC createBtree (char *idxId, DataType keyType, int n)
6. RC openBtree(BTreeHandle **tree, char *idxId)
7. RC closeBtree(BTreeHandle *tree)
8. RC deleteBtree (char *idxId)
9. RC getNumNodesHelper(BTreeHandle *tree, int *result)
10. RC getNumNodes (BTreeHandle *tree, int *result)
11. RC getNumEntries(BTreeHandle *tree, int *result)
12. RC getKeyType(BTreeHandle *tree, DataType *result)
13. int compareValue(Value *key, Value *sign)
14. BTreeNode *findLeafNode(BTreeNode *node, Value *key)
15. RID *findEntryInNode(BTreeNode *node, Value *key)
16. RC findKey(BTreeHandle *tree, Value *key, RID *result)
17. BTreeNode *createNode(BTreeMtdt *mgmtData)
18. BTreeNode *createLeafNode(BTreeMtdt *mgmtData)
19. BTreeNode *createNonLeafNode(BTreeMtdt *mgmtDint getInsertPos(BTreeNode *node, Value *key)
20. int getInsertPos(BTreeNode *node, Value *key)
21. RID *buildRID(RID *rid)
22. void insertIntoLeafNode(BTreeNode *node, Value *key, RID *rid, BTreeMtdt *mgmtData)
23. BTreeNode* splitLeafNode(BTreeNode* node, BTreeMtdt* mgmtData)
24. void splitNonLeafNode(BTreeNode* node, BTreeMtdt *mgmtData)
25. void insertIntoParentNode(BTreeNode* lnode, Value *key, BTreeMtdt *mgmtData) 
26. RC insertKey(BTreeHandle *tree, Value *key, RID rid)
27. void deleteFromLeafNode(BTreeNode *node, Value *key, BTreeMtdt *mgmtData)
28. bool isEnoughSpace(int keyNums, BTreeNode *node, BTreeMtdt *mgmtData)
29. void deleteParentEntry(BTreeNode *node)
30. void updateParentEntry(BTreeNode *node, Value *key, Value *newkey)
31. bool redistributeFromSibling(BTreeNode *node, Value *key, BTreeMtdt *mgmtData)
32. BTreeNode *checkSiblingCapacity(BTreeNode *node, BTreeMtdt *mgmtData)
33. void freeResourceAfterMerge(BTreeNode *curNode, BTreeMtdt *bTreeMedadata)
34. void mergeSibling(BTreeNode *node, BTreeNode *sibling, BTreeMtdt *mgmtData)
35. void mergeWithSibling(Value *key, BTreeMtdt *bTreeMetadata, BTreeNode *leafNode)
36. RC deleteKey (BTreeHandle *tree, Value *key)
37. RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
38. RC nextEntry (BT_ScanHandle *handle, RID *result)
39. RC closeTreeScan (BT_ScanHandle *handle)
40. char *printTree(BTreeHandle *tree)