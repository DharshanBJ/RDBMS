#include "ix.h"

#include <stdlib.h>
#include <cstring>
#include <sys/stat.h>

#include "../rbf/pfm.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance() {
	if (!_index_manager)
		_index_manager = new IndexManager();

	return _index_manager;
}

IndexManager::IndexManager() {
}

IndexManager::~IndexManager() {
}

RC IndexManager::createFile(const string &fileName) {
	RC result = pfm->createFile(fileName);
	return result;
}

RC IndexManager::destroyFile(const string &fileName) {
	RC result = pfm->destroyFile(fileName);
	return result;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle) {
	RC result = pfm->openFile(fileName, ixfileHandle.fileHandle);
	return result;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle) {
	RC result = pfm->closeFile(ixfileHandle.fileHandle);
	return result;

}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle,
		const Attribute &attribute, const void *key, const RID &rid) {
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

	//read Page 0-to accomoda

//	int type =0;
//	if(attribute.type == TypeInt){
//		type =0;
//	}else if(attribute.type == TypeReal){
//		type =1;
//	}else{
//		type =2;
//	}
//	ixfileHandle.readPage(page_ptr, buffer);
//	insertIntoIntermediatePage(ixfileHandle,key,page_ptr,type,buffer,free_space_of_page,num_of_slots,char_len);
	//write back the buffer into the page
//	ixfileHandle.writePage(page_ptr, buffer);
	return -1;
}

RC IndexManager::insertIntoIntermediatePage(IXFileHandle &ixfileHandle,
		const void *key, PageNum page_ptr, int type, void *buffer,
		int free_space_of_page, int num_of_slots, int char_len) {

//	char *buffer = (char *) calloc(PAGE_SIZE, 1);
//	ixfileHandle.readPage(page_ptr, buffer);

	//to read free space available on the page
//	int free_space_of_page = 0;
//	memcpy(&free_space_of_page, buffer + FREE_SPACE_BLOCK, 2);

	//to read the present number of indexes slot on the page
//	int num_of_slots = 0;
//	memcpy(&num_of_slots, buffer + NUM_OF_INDEX_BLOCK, 2);

	int page_num_ptr = 0;
	int buffer_offset_to_insert = searchIntermediateNode(key, page_num_ptr,
			buffer, type);	//offset where the insert should happen

	//compaction starts here
//------------needs to be checked before this function
//	int char_len = 4;
//	if (type == 2) {
//		memcpy(&char_len, (char *) key, 4);
//		char_len += 4;	//char length + 4 bytes representing it
//	}
//
//	//if available free space cannot accommodate new key and page PTR
//	if (free_space_of_page < (char_len + 4)) {
////		free(buffer);
//		return -1;
//	}
//-------------------------------------------------------------

	//find length and move right
	int move_len = PAGE_SIZE - 6 - free_space_of_page - buffer_offset_to_insert;//diff between last data  offset - place to be inserted
	memmove(
			(char*) buffer + buffer_offset_to_insert + char_len
					+ PAGE_NUM_PTR_SIZE,
			(char*) buffer + buffer_offset_to_insert, move_len);

	//update the buffer with new key and page ptr to be inserted
	memcpy((char*) buffer + buffer_offset_to_insert, (char*) key, char_len);
	memcpy((char*) buffer + buffer_offset_to_insert + char_len, &page_ptr,
	PAGE_NUM_PTR_SIZE);

	//update the number of indexes slot on the page
	num_of_slots += 1;
	memcpy((char *) buffer + NUM_OF_INDEX_BLOCK, &num_of_slots, 2);

	//update the newly available free space on the page
	free_space_of_page = free_space_of_page - PAGE_NUM_PTR_SIZE - char_len;
	memcpy(&free_space_of_page, (char *) buffer + FREE_SPACE_BLOCK, 2);

//	free(buffer);
	return 0;
}

RC IndexManager::insertIntoLeafPage(IXFileHandle &ixfileHandle, const void *key,
		const RID &rid, int type, void *buffer, int free_space_of_page,
		int num_of_slots, int char_len) {

	//search the position where the key has to be inserted
	int buffer_offset_to_insert = searchLeafNode(key, buffer, type);
	int move_len = PAGE_SIZE - 10 - free_space_of_page
			- buffer_offset_to_insert;//diff between last data  offset - place to be inserted

	//find length and move right
	memmove(
			(char*) buffer + buffer_offset_to_insert + char_len + RID_BLOCK_SIZE,
			(char*) buffer + buffer_offset_to_insert, move_len);

	//update the buffer with new key and page ptr to be inserted
	memcpy((char*) buffer + buffer_offset_to_insert, (char*) key, char_len);
	memcpy((char*) buffer + buffer_offset_to_insert + char_len, &rid,
	RID_BLOCK_SIZE);

	//update the number of indexes slot on the page
	num_of_slots += 1;
	memcpy((char *) buffer + NUM_OF_INDEX_BLOCK, &num_of_slots, 2);

	//update the newly available free space on the page
	free_space_of_page = free_space_of_page - RID_BLOCK_SIZE - char_len;
	memcpy(&free_space_of_page, (char*) buffer + FREE_SPACE_BLOCK, 2);
	return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle,
		const Attribute &attribute, const void *key, const RID &rid) {
	return -1;
}

RC IndexManager::searchIntermediateNode(const void *key, int &pagePtr,
		void *buffer, int type) {

	//corner case test for intermediate page
	short flag;
	memcpy(&flag, (char *) buffer + NODE_FLAG_BLOCK, 2);
	if (flag) {
		return -1;	//if the node is a leaf node
	}

	//to return the correct page Num for the give Key
	short num_of_index_slots;
	memcpy(&num_of_index_slots, (char*) buffer + NUM_OF_INDEX_BLOCK, 2);

	int buffer_offset = 4;	//4->left pointer of first key
	for (int i = 0; i < (int) num_of_index_slots; i++) {
		int char_len = 0;
		void *comparisonEntry = calloc(100, 1);

		//to compare the key if var char type ==2 else compare 4 bytes
		if (type == 2) {
			memcpy(&char_len, (char *) buffer + buffer_offset, 4);

			memcpy(comparisonEntry, (char *) buffer + buffer_offset + 4,
					char_len);

		} else {
			memcpy(comparisonEntry, (char *) buffer + buffer_offset, 4);
			char_len = 4;
		}

		if (compareEntryKeyIndex(key, comparisonEntry, type, char_len) == 1) {//check if key Parameter is right or not
			free(comparisonEntry);
			break;
		}

		if (type == 2) {
			buffer_offset += 4;
		}

		buffer_offset += char_len;	//length of var_char else 4 for int/real
		buffer_offset += PAGE_NUM_PTR_SIZE;

		free(comparisonEntry);
	}
	memcpy(&pagePtr, (char *) buffer + buffer_offset - 4, 4);//read left page number Ptr onto pagePTR
	return buffer_offset;
	//pagePtr is updated with appropriate page num for child intermediate/leaf node and buffer_offset for insert index -> returned (4<default> , max<4090>)
}

RC IndexManager::searchLeafNode(const void *key, void *buffer, int type) {

	//corner case test for intermediate page
	short flag;
	memcpy(&flag, (char *) buffer + NODE_FLAG_BLOCK, 2);
	if (flag == 0) {
		return -1;	//if the node is an intermediate node
	}

	//to loop through until you find the match greater than the of the key
	short num_of_key_slots;
	memcpy(&num_of_key_slots, (char*) buffer + NUM_OF_INDEX_BLOCK, 2);
	int buffer_offset = -1;
	for (int i = 0; i < (int) num_of_key_slots; i++) {
		int char_len = 0;
		void *comparisonEntry = calloc(100, 1);

		//to compare the key if var char type ==2 else compare 4 bytes
		if (type == 2) {
			memcpy(&char_len, (char *) buffer + buffer_offset, 4);

			memcpy(comparisonEntry, (char *) buffer + buffer_offset + 4,
					char_len);

		} else {
			memcpy(comparisonEntry, (char *) buffer + buffer_offset, 4);
			char_len = 4;
		}
		if (compareEntryKeyIndex(key, comparisonEntry, type, char_len) == 1) {//check if key Parameter is right or not

			free(comparisonEntry);
			break;
		}
		if (type == 2) {
			buffer_offset += 4;
		}
		buffer_offset += char_len;	//length of var_char else 4
		buffer_offset += RID_BLOCK_SIZE
		;
		free(comparisonEntry);
	}

	return buffer_offset;
	//in the calling function check for value for offset based on which decide to insert or split and insert
	//-1 implies no keys exists
	//4086 implies the page is full
}

RC IndexManager::compareEntryKeyIndex(const void *key, void *comparisonEntry,
		int type, int compare_len) {	//, short node_type) {
	int result = 0;

	if (type == 0) {

		int compare1 = 0;
		memcpy(&compare1, (char *) comparisonEntry, compare_len);

		int compare_key = *(int *) key;
//		if (node_type == 1) {
		result = (compare_key < compare1) ? 1 : 0;
//		} else {
//
//		}

	} else if (type == 1) {

		float compare1 = 0;
		memcpy(&compare1, (char *) comparisonEntry, compare_len);

		float compare_key = *(float *) key;
//		if (node_type == 1) {
		result = (compare_key < compare1) ? 1 : 0;
//		} else {
//
//		}

	} else if (type == 2) {

		char str1_Array[compare_len];
		memcpy(&str1_Array, (char *) comparisonEntry, compare_len); //copy the varchar entry into string format
		string str1(str1_Array, compare_len); //convert the char array to string

		int str2_len = 0; //parameter key
		memcpy(&str2_len, (char *) key, 4);

		char str2_Array[str2_len];
		memcpy(&str2_Array, (char *) key + 4, str2_len); //copy the key into string format
		string str2_Key(str2_Array, str2_len); //convert the char array to string

//		if (node_type == 1) {
		result = (str2_Key < str1) ? 1 : 0;
//		} else {
//			result = (str2_Key > str1) ? 1 : 0;
//		}

	}
	return result;
}

RC readLeafVarcharKey(const void *key, PageNum pagePtr, void *buffer,
		int offset, int char_len) {
	return 0;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const void *lowKey, const void *highKey, bool lowKeyInclusive,
		bool highKeyInclusive, IX_ScanIterator &ix_ScanIterator) {
	return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle,
		const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator() {
}

IX_ScanIterator::~IX_ScanIterator() {
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
	return -1;
}

RC IX_ScanIterator::close() {
	return -1;
}

IXFileHandle::IXFileHandle() {
	ixReadPageCounter = 0;
	ixWritePageCounter = 0;
	ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle() {
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount,
		unsigned &writePageCount, unsigned &appendPageCount) {
	fileHandle.collectCounterValues(readPageCount, writePageCount,
			appendPageCount);

	ixReadPageCounter = readPageCount;
	ixWritePageCounter = writePageCount;
	ixAppendPageCounter = appendPageCount;
	return 0;
}

