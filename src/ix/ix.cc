#include "ix.h"

#include <stdlib.h>
#include <cstring>
#include <iostream>

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
	_index_manager = NULL;
}

RC IndexManager::createFile(const string &fileName) {
	RC result = pfm->createFile(fileName);

	// failed to create a file
	if (result < 0)
		return result;

	// Open up the file so we can initialize it with some data
	IXFileHandle ixfileHandle;
	int res = pfm->openFile(fileName, ixfileHandle.fileHandle);
	if (res < 0)
		return res;

	// Create the root<leaf> page, mark it as a leaf, it's page 1
	res = newPage(ixfileHandle.fileHandle, 1, true, 0, 0);
	if (res < 0)
		return res;

	// Store the new page to page 0
	res = updateRootPage(ixfileHandle.fileHandle, 1);
	if (res < 0)
		return res;

	// When done-> close the file
	res = pfm->closeFile(ixfileHandle.fileHandle);
	if (res < 0)
		return res;

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

RC IndexManager::updateRootPage(FileHandle& fileHandle,
		unsigned root_page_num) {

	//update the root page number in the hidden page-> 0
	fileHandle.writeRootPageNumber(root_page_num);
	return 0;
}

RC IndexManager::readRootPage(FileHandle& fileHandle) {
	int root_page_num = 1;
	void *root_page_buffer = calloc(4096, 1);

	//read root page in the page 0 of the file
	fileHandle.readPage(0, root_page_buffer);
	memcpy(&root_page_num, (char*) root_page_buffer + 12, 4);

	free(root_page_buffer);
	return root_page_num;
}

RC IndexManager::newPage(FileHandle& fileHandle, PageNum page_num, bool is_leaf,
		PageNum next_leaf_page_num, PageNum left_child_page_num) {

	const unsigned currentNumPages = fileHandle.getNumberOfPages();

	int leftChild = (int) left_child_page_num;
	short isLeafPage = (short) (is_leaf ? 1 : 0);
	short freeSpace = 0;
	short numSlots = 0;
	int next_page_num = is_leaf ? (int) next_leaf_page_num : -1;

	//create a buffer for page
	void *new_page_buffer = calloc(PAGE_SIZE, 1);

	//prepare the buffer with footer values
	memcpy((char *) new_page_buffer + NODE_FLAG_BLOCK, &isLeafPage, 2);
	freeSpace = 4096 - 10; //10 overhead bytes at foot of page for leaf nodes and 4 byte left child pointer and 6 overhead footer of non leaf page
	memcpy((char *) new_page_buffer + FREE_SPACE_BLOCK, &freeSpace, 2);
	memcpy((char *) new_page_buffer + NUM_OF_INDEX_BLOCK, &numSlots, 2);

	//for non-leaf first 4 bytes is left child page num and for leaf -> 4bytes for next page num at head of footer
	if (!is_leaf) {
		memcpy((char *) new_page_buffer, &leftChild, 4);
	} else {
		memcpy((char *) new_page_buffer + NEXT_PAGE_PTR, &next_page_num, 4);
	}

	// If we are 'new'ing a previously allocated page then we are zeroing it and over writing
	int res = 0;

	if (page_num <= currentNumPages) { //check if < | <= makes sense
		res = fileHandle.writePage(page_num, new_page_buffer);
		if (res < 0)
			cout << "File->write page at new page() failed" << endl;

	} else { //append page
		res = fileHandle.appendPage(new_page_buffer);
		if (res < 0)
			cout << "File->append page at new page() failed" << endl;
	}

	free(new_page_buffer);
	return 0;
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

	cout << "No. of pages :" << ixfileHandle.fileHandle.getNumberOfPages()
			<< endl;

	int type = (attribute.type == TypeVarChar) ? 2 :
				(attribute.type == TypeReal) ? 1 : 0;

	// To keep track of pages we've traversed so that we can find their parent pointers
	std::vector<int> parents;

	// Traverse down the tree to the leaf, using non-leaves along the way
	int insert_dest = readRootPage(ixfileHandle.fileHandle);
	void *insert_enrty_buffer = calloc(4096, 1);

	//pull the root page
	ixfileHandle.fileHandle.readPage(insert_dest, insert_enrty_buffer);

	//read leaf flag on the root page
	int is_leaf_page = 0;
	memcpy(&is_leaf_page, (char *) insert_enrty_buffer + NODE_FLAG_BLOCK, 2);

	while (is_leaf_page == 0) {
		parents.push_back(insert_dest);

		searchIntermediateNode(key, insert_dest, insert_enrty_buffer, type);

		// pull and read the designated page into memory and refresh the leaf flag
		memset(insert_enrty_buffer, 0, PAGE_SIZE);
		ixfileHandle.fileHandle.readPage(insert_dest, insert_enrty_buffer);
		memcpy(&is_leaf_page, (char *) insert_enrty_buffer + NODE_FLAG_BLOCK,
				2);
	}

	//check if we are at leaf page
	cout << "is_leaf_page :" << is_leaf_page << endl;

	// To insert into root node
	short free_space_of_page;
	short num_of_slots;
	int char_len;
	readOverHeads(key, type, insert_enrty_buffer, free_space_of_page,
			num_of_slots, char_len);

	cout<<"num_of_slots before insert :"<<num_of_slots<<endl;
	cout<<"free_space_of_page after insert :"<<free_space_of_page<<endl;

	insertIntoLeafPage(ixfileHandle, key, rid, type, insert_enrty_buffer,
			free_space_of_page, num_of_slots, char_len);

	if(ixfileHandle.fileHandle.writePage(insert_dest, insert_enrty_buffer)<0)return -1;

	free(insert_enrty_buffer);//todo: have to fix double free caused here
	return 0;
}

RC IndexManager::readOverHeads(const void *key, int type, void *buffer,
		short &free_space_of_page, short &num_of_slots, int &char_len) {

	memcpy(&free_space_of_page, (char *) buffer + FREE_SPACE_BLOCK, 2);
	memcpy(&num_of_slots, (char *) buffer + NUM_OF_INDEX_BLOCK, 2);
	if (type == 2) {
		memcpy(&char_len, (char *) key, 4);
		char_len += 4;	//char length + 4 bytes representing it
	} else {
		char_len = 4;
	}

	return 0;
}

RC IndexManager::insertIntoIntermediatePage(IXFileHandle &ixfileHandle,
		const void *key, PageNum page_ptr, int type, void *buffer,
		short free_space_of_page, short num_of_slots, int char_len) {//char_len =4 for real and int and 4+var_char for others has to be passed along

	//offset where the insert should happen
	int page_num_ptr = 0;
	int buffer_offset_to_insert = searchIntermediateNode(key, page_num_ptr,
			buffer, type);

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

	return 0;
}

RC IndexManager::insertIntoLeafPage(IXFileHandle &ixfileHandle, const void *key,
		const RID &rid, int type, void *buffer, short free_space_of_page,
		short num_of_slots, int char_len) {	//char_len =4 for int/real n 4+lenght of varchar for varchar

	//search the position where the key has to be inserted
	int buffer_offset_to_insert = searchLeafNode(key, buffer, type);
	int move_len = PAGE_SIZE - 10 - free_space_of_page
			- buffer_offset_to_insert;//difference between last data  offset - place to be inserted

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
	cout<<"num_of_slots after insert :"<<num_of_slots<<endl;

	//update the newly available free space on the page
	free_space_of_page = free_space_of_page - RID_BLOCK_SIZE - char_len;
	memcpy((char*) buffer + FREE_SPACE_BLOCK,&free_space_of_page, 2);
	cout<<"free_space_of_page after insert :"<<free_space_of_page<<endl;
	return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle,
		const Attribute &attribute, const void *key, const RID &rid) {

	int result =-1;
	cout << "No. of pages :" << ixfileHandle.fileHandle.getNumberOfPages()
			<< endl;

	int type = (attribute.type == TypeVarChar) ? 2 :
				(attribute.type == TypeReal) ? 1 : 0;

	// To keep track of pages we've traversed so that we can find their parent pointers
	std::vector<int> parents;

	// Traverse down the tree to the leaf, using non-leaves along the way
	int insert_dest = readRootPage(ixfileHandle.fileHandle);
	void *buffer = calloc(4096, 1);

	//pull the root page
	ixfileHandle.fileHandle.readPage(insert_dest, buffer);

	//read leaf flag on the root page
	int is_leaf_page = 0;
	memcpy(&is_leaf_page, (char *) buffer + NODE_FLAG_BLOCK, 2);

	while (is_leaf_page == 0) {
		parents.push_back(insert_dest);

		searchIntermediateNode(key, insert_dest, buffer, type);

		// pull and read the designated page into memory and refresh the leaf flag
		memset(buffer, 0, PAGE_SIZE);
		ixfileHandle.fileHandle.readPage(insert_dest, buffer);
		memcpy(&is_leaf_page, (char *) buffer + NODE_FLAG_BLOCK, 2);
	}

	//check if we are at leaf page
	cout << "is_leaf_page :" << is_leaf_page << endl;

	//to delete index entry from leaf node
	short free_space_of_page;
	short num_of_slots;
	int char_len;
	readOverHeads(key, type, buffer, free_space_of_page, num_of_slots,
			char_len);

	if(num_of_slots !=0)
	result = deleteEntryInLeaf(ixfileHandle, key, rid, type, buffer, free_space_of_page,
			num_of_slots, char_len);

	if(ixfileHandle.fileHandle.writePage(insert_dest, buffer)<0)return -1;

	free(buffer);
	return result;
}

RC IndexManager::deleteEntryInLeaf(IXFileHandle &ixfileHandle, const void *key,
		const RID &rid, int type, void *buffer, short free_space_of_page,
		short num_of_slots, int char_key_len) {	//char_key_len =4 for int/real n 4+lenght of varchar for varchar

	int result =-1;

	//corner case test for intermediate page
	short flag;
	memcpy(&flag, (char *) buffer + NODE_FLAG_BLOCK, 2);
	if (flag == 0) {
		return -1;	//if the node is an intermediate node
	}

	int buffer_offset_to_delete = 0;	//searchLeafNode(key, buffer, type);
	void *comparisonEntry = calloc(4096, 1);
	for (int i = 0; i < (int) num_of_slots; i++) {
		int char_len = 0;

		//to compare the key if var char type ==2 else compare 4 bytes
		if (type == 2) {
			memcpy(&char_len, (char *) buffer + buffer_offset_to_delete, 4);

			memcpy(comparisonEntry,
					(char *) buffer + buffer_offset_to_delete + 4,
					char_len + 8);

		} else {
			memcpy(comparisonEntry, (char *) buffer + buffer_offset_to_delete,
					4 + 8);
			char_len = 4;
		}
		if (compareDeleteEntryKeyRID(key, comparisonEntry, rid, type, char_len)
				== 1) {	//check if key Parameter n rid are matched
			result = 0;
			break;
		}
		if (type == 2) {
			buffer_offset_to_delete += 4;
		}
		buffer_offset_to_delete += char_len;	//length of var_char else 4
		buffer_offset_to_delete += RID_BLOCK_SIZE;

	}

	//find length and move right
	int move_len = PAGE_SIZE - 10 - free_space_of_page - buffer_offset_to_delete
			- char_key_len - RID_BLOCK_SIZE;//diff between last data  offset - offset of next index place to be deleted
	memmove((char*) buffer + buffer_offset_to_delete,
			(char*) buffer + buffer_offset_to_delete + char_key_len
					+ RID_BLOCK_SIZE, move_len);

	//update the number of indexes slot on the page
	num_of_slots -= 1;
	memcpy((char *) buffer + NUM_OF_INDEX_BLOCK, &num_of_slots, 2);

	//update the newly available free space on the page
	free_space_of_page = free_space_of_page + RID_BLOCK_SIZE + char_key_len;
	memcpy(&free_space_of_page, (char*) buffer + FREE_SPACE_BLOCK, 2);

	free(comparisonEntry);
	return result;
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
	void *comparisonEntry = calloc(4096, 1);
	for (int i = 0; i < (int) num_of_index_slots; i++) {
		int char_len = 0;

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
//			free(comparisonEntry);
			break;
		}

		if (type == 2) {
			buffer_offset += 4;
		}

		buffer_offset += char_len;	//length of var_char else 4 for int/real
		buffer_offset += PAGE_NUM_PTR_SIZE;

	}
	memcpy(&pagePtr, (char *) buffer + buffer_offset - 4, 4);//read left page number Ptr onto pagePTR

	free(comparisonEntry);
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
	int buffer_offset = 0;
	void *comparisonEntry = calloc(4096, 1);
	for (int i = 0; i < (int) num_of_key_slots; i++) {
		int char_len = 0;

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

//			free(comparisonEntry);
			break;
		}
		if (type == 2) {
			buffer_offset += 4;
		}
		buffer_offset += char_len;	//length of var_char else 4
		buffer_offset += RID_BLOCK_SIZE;

	}

	free(comparisonEntry);
	return buffer_offset;
	//in the calling function check for value for offset based on which decide to insert or split and insert
	//-1 implies no keys exists
	//4086 implies the page is full
}

RC IndexManager::compareEntryKeyIndex(const void *key, void *comparisonEntry,
		int type, int compare_len) {
	int result = 0;

	if (type == 0) {

		int compare1 = 0;
		memcpy(&compare1, (char *) comparisonEntry, compare_len);

		int compare_key = *(int *) key;
		result = (compare_key < compare1) ? 1 : 0;

	} else if (type == 1) {

		float compare1 = 0;
		memcpy(&compare1, (char *) comparisonEntry, compare_len);

		float compare_key = *(float *) key;
		result = (compare_key < compare1) ? 1 : 0;

	} else if (type == 2) {

		char str1_Array[compare_len];
		memcpy(&str1_Array, (char *) comparisonEntry, compare_len); //copy the varchar entry into string format
		string str1(str1_Array, compare_len); //convert the char array to string

		int str2_len = 0; //parameter key
		memcpy(&str2_len, (char *) key, 4);

		char str2_Array[str2_len];
		memcpy(&str2_Array, (char *) key + 4, str2_len); //copy the key into string format
		string str2_Key(str2_Array, str2_len); //convert the char array to string

		result = (str2_Key < str1) ? 1 : 0;

	}
	return result;
}

RC IndexManager::compareDeleteEntryKeyRID(const void *key,
		void *comparisonEntry, const RID &rid, int type, int compare_len) {
	int result = 0;

	if (type == 0) {

		int compare1 = 0;
		memcpy(&compare1, (char *) comparisonEntry, compare_len);

		int compare_key = *(int *) key;
		result = (compare_key == compare1) ? 1 : 0;

	} else if (type == 1) {

		float compare1 = 0;
		memcpy(&compare1, (char *) comparisonEntry, compare_len);

		float compare_key = *(float *) key;
		result = (compare_key == compare1) ? 1 : 0;

	} else if (type == 2) {

		char str1_Array[compare_len];
		memcpy(&str1_Array, (char *) comparisonEntry, compare_len); //copy the varchar entry into string format
		string str1(str1_Array, compare_len); //convert the char array to string

		int str2_len = 0; //parameter key
		memcpy(&str2_len, (char *) key, 4);

		char str2_Array[str2_len];
		memcpy(&str2_Array, (char *) key + 4, str2_len); //copy the key into string format
		string str2_Key(str2_Array, str2_len); //convert the char array to string

		result = (str2_Key == str1) ? 1 : 0;

	}

	//check if RIDS of the given key to delete matches with the one of the index
	if (result == 1) {
		unsigned compare1_page_num = -1, compare1_slot_num = -1;

		memcpy(&compare1_page_num, (char *) comparisonEntry + compare_len, 4);
		memcpy(&compare1_slot_num, (char *) comparisonEntry + compare_len + 4,
				4);

		result =
				((compare1_page_num == rid.pageNum)
						&& (compare1_slot_num == rid.slotNum)) ? 1 : 0;
	}

	return result;
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

