
#include "ix.h"

#include <stdlib.h>
#include <cstring>

#include "../rbf/pfm.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    return -1;
}

RC IndexManager::destroyFile(const string &fileName)
{
    return -1;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    return -1;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return -1;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	// Insert one key at a time, check and store key type
	// get hold of the root page
	// compare the key and read the correct pageNum PTR (if< key read left PTR else (if >= than key read right PTR)
	// while loop until offset is got-> search Intermediate/Leaf based on the flag (call accordingly)
	// contd. read of pageNum PTR the position to insert --all logic happens here
	// contd. return the pageNum PTR(intermediate search) & offset to insert(node page search)
	// based on offset call insertIntoLeafPage()
	// if overflow of leaf node page then create a new leafPage an update the required overheads and move data into new page->
	// contd. insert the <key,rid> and update the upper intermediate nodes with the LOWEST_KEY of the newly created leaf page ->insertIntermediatePage(this lowestkey)
	// contd. if an overflow is also found in the intermediate nodes because of leaf getting split (as usual pass LOWEST_KEY up) and then
	// create a new intermediate parent Node and split by putting half the data into the new intermediate one
	// contd. replace the HIGHEST_KEY of the old (overFlown intermediate node page) with the LOWEST KEY of leafNode Page while
	// contd. ***** copying to its parent intermediate node-> if parent Intermediate Node is not in existence then create a
	// contd. a new rootKey page(HIGEST_KEY of child intermediate Node after split and NOTE: *dont forget about replacing it in child by other to maintain rule )
	// contd. and update the hidden page with the new root pageNum PTR.
	// The new root Key will have left pointer pointing to the old Intermediate Node and right will be to the Newly created Intermediate Node

	return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}

RC IndexManager::createNewIntermediatePage(IXFileHandle &ixfileHandle, PageNum leftPage, PageNum rightPage){
	return 0;
}

RC IndexManager::searchLeafNode(const void *key, void *buffer, int type){

	//corner case test for intermediate page
	short flag;
	memcpy(&flag,(char *)buffer+4094,2);
	if(!flag){
		return -1;//if the node is an intermediate node
	}

	//to loop through until you find the match greater than the of the key
	short num_of_key_slots;
	memcpy(&num_of_key_slots,(char*)buffer+4088,2);
	int buffer_offset=-1;
	for(int i=0; i< (int) num_of_key_slots; i++){
		int char_len =0;
		void *comparisonEntry = calloc(100,1);

		//to compare the key if var char type ==2 else compare 4 bytes
		if(type == 2){
			memcpy(&char_len, (char *)buffer + buffer_offset,4);

			memcpy(comparisonEntry, (char *)buffer + buffer_offset,char_len);

		}else{
			memcpy(comparisonEntry, (char *)buffer + buffer_offset,4);
			char_len =4;
		}
		if(compareEntryKeyIndex(key, comparisonEntry, type, char_len) == 1){//check if key Parameter is right or not

			free(comparisonEntry);
			break;
		}
		if(type == 2) {
			buffer_offset += 4;
		}
		buffer_offset += char_len;//length of var_char else 4
		buffer_offset +=RID_BLOCK_SIZE;
		free(comparisonEntry);
	}

	return buffer_offset;
	//in the calling function check for value for offset based on which decide to insert or split and insert
}

RC IndexManager::compareEntryKeyIndex(const void *key, void *comparisonEntry, int type, int compare_len){
	int result =0;

	if (type == 0) {

			int compare1 = 0;
			memcpy(&compare1, (char *) comparisonEntry, compare_len);

			int compare_key = *(int *)key;
			result = (compare_key < compare1)?1:0;

	} else if (type == 1) {

			float compare1 = 0;
			memcpy(&compare1, (char *) comparisonEntry, compare_len);

			float compare_key = *(float *)key;
			result = (compare_key < compare1)?1:0;

	} else if (type == 2) {

			char str1_Array[compare_len];
			memcpy(&str1_Array, (char *) comparisonEntry, compare_len); //copy the varchar entry into string format
			string str1(str1_Array, compare_len); //convert the char array to string

			int str2_len = 0;//parameter key
			memcpy(&str2_len, (char *) key, 4);

			char str2_Array[str2_len];
			memcpy(&str2_Array, (char *) key + 4, str2_len); //copy the key into string format
			string str2_Key(str2_Array, str2_len); //convert the char array to string

			result = (str2_Key < str1)?1:0;
		}
	return result;
}

RC readLeafVarcharKey(const void *key, PageNum pagePtr, void *buffer,int offset, int char_len){
	return 0;
}
RC IndexManager::insertIntoIntermediatePage(IXFileHandle &ixfileHandle, const void *key, PageNum leftPage, PageNum rightPage){

	return 0;
}
RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    return -1;
}

