#include "rm.h"

#include "../ix/ix.h"

#include <cmath>
#include <cstdlib>
#include <cstring>
#include <iostream>

#include "../rbf/pfm.h"

RelationManager* RelationManager::instance() {
	static RelationManager _rm;
	return &_rm;
}

RelationManager::RelationManager() {

}

RelationManager::~RelationManager() {
}

RC RelationManager::createCatalog() {
	RecordBasedFileManager* _rbfm = RecordBasedFileManager::instance();

	FileHandle fileHandle;

	if (_rbfm->openFile("Tables", fileHandle) != 0
			|| _rbfm->openFile("Columns", fileHandle) != 0
			|| _rbfm->openFile("Indexes", fileHandle) != 0) {

		_rbfm->createFile("Tables");
		_rbfm->createFile("Columns");
		_rbfm->createFile("Indexes");

		vector<Attribute> tableDesc;
		vector<Attribute> attributeDesc;
		vector<Attribute> IndexDesc;

		getTableDescription(tableDesc);
		getAttributeDescription(attributeDesc);
		getIndexDescription(IndexDesc);

		createTable("Tables", tableDesc); //create system table-Tables
		createTable("Columns", attributeDesc); //create system table-Columns
		createTable("Indexes", IndexDesc); //create system table-Indexes

	} else {
		_rbfm->closeFile(fileHandle);
	}
	return 0;
}

RC RelationManager::getTableDescription(vector<Attribute> &recordDescriptor) {
	Attribute tableId, tableName, fileName;

	tableId.name = "table-id";
	tableId.type = TypeInt;
	tableId.length = sizeof(int);
	recordDescriptor.push_back(tableId);

	tableName.name = "table-name";
	tableName.type = TypeVarChar;
	tableName.length = 50;
	recordDescriptor.push_back(tableName);

	fileName.name = "file-name";
	fileName.type = TypeVarChar;
	fileName.length = 50;
	recordDescriptor.push_back(fileName);
	return 0;
}

RC RelationManager::getAttributeDescription(
		vector<Attribute> &recordDescriptor) {
	Attribute descTableId, descName, descType, descLength, descColumnId;
	recordDescriptor.clear();

	descTableId.name = "table-id";
	descTableId.type = TypeInt;
	descTableId.length = sizeof(int);
	recordDescriptor.push_back(descTableId);

	descName.name = "column-name";
	descName.type = TypeVarChar;
	descName.length = 50;
	recordDescriptor.push_back(descName);

	descType.name = "column-type";
	descType.type = TypeInt;
	descType.length = sizeof(TypeInt);
	recordDescriptor.push_back(descType);

	descLength.name = "column-length";
	descLength.type = TypeInt;
	descLength.length = sizeof(int);
	recordDescriptor.push_back(descLength);

	descColumnId.name = "column-position";
	descColumnId.type = TypeInt;
	descColumnId.length = sizeof(int);
	recordDescriptor.push_back(descColumnId);

	return 0;
}

RC RelationManager::getIndexDescription(vector<Attribute> &indexDescriptor) {
	Attribute tableId, columnName, indexName;
	tableId.name = "table-id";
	tableId.type = TypeInt;
	tableId.length = sizeof(int);
	indexDescriptor.push_back(tableId);

	columnName.name = "column-name";
	columnName.type = TypeVarChar;
	columnName.length = 50;
	indexDescriptor.push_back(columnName);

	indexName.name = "index-name";
	indexName.type = TypeVarChar;
	indexName.length = 50;
	indexDescriptor.push_back(indexName);

	return 0;
}

RC RelationManager::TableName_check(const string &tableName) {
	if (tableName == "Tables" || tableName == "Columns"
			|| tableName == "Indexes") {
		return -1;
	}
	return 0;
}
RC RelationManager::deleteCatalog() {
	RecordBasedFileManager* _rbfm = RecordBasedFileManager::instance();
	_rbfm->destroyFile("Tables");
	_rbfm->destroyFile("Columns");
	_rbfm->destroyFile("Indexes");
	return 0;
}

RC RelationManager::createTable(const string &tableName,
		const vector<Attribute> &attrs) {

	FileHandle fileHandle;
	RecordBasedFileManager* _rbfm = RecordBasedFileManager::instance();

	vector<Attribute> tableInfo; //creating an attribute
	getTableDescription(tableInfo); //creating record descriptor

	//TODO:Need to calculate the table attribute length
	int dataLength = 0;
	int tableNameLength = 0;
	int nullFieldsIndicatorActualSize = ceil((double) 3 / 8);
	void *data = calloc(4096, 1);

	int offset = 0;

	RID rid;

	if (_rbfm->openFile("Tables", fileHandle) != 0) //open the tables file
			{
		free(data);
		return -1;
	}

	int tableId = fileHandle.readTableId() + 1; //rand() % 100000+1;
	fileHandle.writeTableId(tableId);
	tableNameLength = tableName.length();
	//data has to be in insert record format
	memset(data, 0, dataLength);	  	//set the buffer as 0 for all locations

	//copy the null indicator size bits
	memset((char *) data + offset, 0, nullFieldsIndicatorActualSize);
	offset += nullFieldsIndicatorActualSize;

	memcpy((char *) data + offset, &tableId, sizeof(int)); //copy the table id
	offset += sizeof(int);

	memcpy((char *) data + offset, &tableNameLength, sizeof(int)); //copy the tablename length
	offset += sizeof(int);
	//cout << "table name length : " << tableNameLength << endl;

	memcpy((char *) data + offset, tableName.c_str(), tableNameLength); //copy the tablename
	offset += tableNameLength;

	memcpy((char *) data + offset, &tableNameLength, sizeof(int)); //copy the filename length
	offset += sizeof(int);
	cout << "file name length : " << tableNameLength << endl;

	memcpy((char *) data + offset, tableName.c_str(), tableName.length()); //copy the filename
	offset += tableName.length();

	if (_rbfm->insertRecord(fileHandle, tableInfo, data, rid) != 0) //insert the created tables details in 'Tables' table
			{

		free(data);
		return -1;
	}

	//populate attribute catalog

	int columnIndexCount = 1;
	RID descRid;
	FileHandle columnFileHandle;
	if (_rbfm->openFile("Columns", columnFileHandle) != 0) //open columns table
			{
		return -1;
	}
	int attrs_size = attrs.size();

	// for(vector<Attribute>::const_iterator it = attrs.begin(); it != attrs.end(); it++)//iterate through every attribute
	for (int i = 0; i < attrs_size; i++) {
		vector<Attribute> columnInfo;
		getAttributeDescription(columnInfo);

		int attrNameLength = 0;
		int nullFieldsIndicatorActualSize = ceil((double) 5 / 8);

		attrNameLength = attrs[i].name.length();

		void* attrData = malloc(4096);

		memset(attrData, 0, 4096); //set the buffer as zero for all locations

		int descOffset = 0;
		//copy the null indicator size bits
		memset((char *) attrData + descOffset, 0,
				nullFieldsIndicatorActualSize);
		descOffset += nullFieldsIndicatorActualSize;

		memcpy((char *) attrData + descOffset, &tableId, sizeof(int)); //table-id
		descOffset += sizeof(int);

		memcpy((char *) attrData + descOffset, &attrNameLength, sizeof(int)); //column name length
		descOffset += sizeof(int);
		cout << "attrNameLength : " << attrNameLength << endl;

		memcpy((char *) attrData + descOffset, (attrs[i].name).c_str(),
				attrNameLength); //column name
		// memcpy((char *)attrData + descOffset, (it->name).c_str(), attrNameLength);//column name
		descOffset += attrNameLength;

		memcpy((char *) attrData + descOffset, &(attrs[i].type),
				sizeof(TypeInt));	                    //column type
		// memcpy((char *)attrData + descOffset, &(it->type), sizeof(TypeInt));
		descOffset += sizeof(TypeInt);

		memcpy((char *) attrData + descOffset, &(attrs[i].length), sizeof(int));//column length
		// memcpy((char *)attrData + descOffset, &(it->length), sizeof(int));
		descOffset += sizeof(int);
		cout << "attrName column Length : " << attrs[i].length << endl;

		memcpy((char *) attrData + descOffset, &columnIndexCount, sizeof(int));	//column index count is the column position
		columnIndexCount++;
		descOffset += sizeof(int);

		// memcpy((char *)attrData + descOffset, &isExisted, sizeof(int));
		//insertRecord(FileHandle &fileHandle,const vector<Attribute> &recordDescriptor, const void *data, RID &rid)
		// readRecord(FileHandle &fileHandle,const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
		//    printRecord(const vector<Attribute> &recordDescriptor, const void *data)

		//	                      void *data=malloc(4096);
		//	                      _rbfm->readRecord(columnFileHandle,columnInfo,descRid,data);
		//	                      _rbfm->printRecord(columnInfo,data);
		//	                      free(data);

		if (_rbfm->insertRecord(columnFileHandle, columnInfo, attrData, descRid)
				!= 0)//insert the created tables attribute details in 'Columns' table
				{
			free(attrData);
			return -1;
		}
		free(attrData);

	}
	if (tableName != "Tables" && tableName != "Columns"
			&& tableName != "Indexes") {
		// Create data file
		if (_rbfm->createFile(tableName) != 0)//creating file with the name same as table name
				{
			return -1;
		}

		//initialize data file
	}
	return 0;
}
RC RelationManager::deleteTable(const string &tableName) {
	RecordBasedFileManager* _rbfm = RecordBasedFileManager::instance();
	//delete data file
	if (tableName == "Columns" || tableName == "Tables"
			|| tableName == "Indexes")
		return -1;
	_rbfm->destroyFile(tableName);
	return 0;
	//delete table attributes
	//1.get all the columns associated with tableName from tables

}

RC RelationManager::getAttributes(const string &tableName,
		vector<Attribute> &attrs) {
	RecordBasedFileManager* _rbfm = RecordBasedFileManager::instance();
	//gets the attributes of a table called tableName by looking into the catalog table
	if (tableName == "Tables") {
		return getTableDescription(attrs); //for 'Tables' table
	} else if (tableName == "Columns") {
		return getAttributeDescription(attrs); //for 'Columns' table
	} else if (tableName == "Indexes") {
		return getIndexDescription(attrs);
	}
//TODO:
	//get tableid from table name
	RM_ScanIterator rm_si;

	vector<string> Attrib;
	Attrib.push_back("table-id");

	void * value = calloc(4096, 1);
	int tableName_length = tableName.length();
	int offset = 0;
	memcpy((char*) value + offset, &tableName_length, sizeof(int));
	offset += sizeof(int);
	memcpy((char*) value + offset, tableName.c_str(), tableName_length);

	//scan("Tables", "table-name", EQ_OP, value, Attrib, rm_si);//value should be in the format length,data
	FileHandle fileHandle;

	RM_ScanIterator rmsi1;
	_rbfm->openFile("Tables", fileHandle);

	vector<Attribute> tableInfo;	//creating an attribute
	getTableDescription(tableInfo);	//creating record descriptor

	_rbfm->scan(fileHandle, tableInfo, "table-name", EQ_OP, value, Attrib,
			rmsi1.rbfmsi);

	RID rid;
	void* data = malloc(100);
	rmsi1.getNextTuple(rid, data);
	rmsi1.close();

	void * tableId = calloc(100, 1);
	memcpy(tableId, (char*) data + 1, sizeof(int));

	_rbfm->closeFile(fileHandle);

	//search columns according to tableid

	RM_ScanIterator rmsi2;
	_rbfm->openFile("Columns", fileHandle);

	vector<string> Attrb;
	Attrb.push_back("column-name");
	Attrb.push_back("column-type");
	Attrb.push_back("column-length");

//	char returnedData[PAGE_SIZE];

	// scan("Columns", "table-id", EQ_OP, (char*)data+1, Attrb, rm_si);
	vector<Attribute> getColumnInfo;	//creating an attribute
	getAttributeDescription(getColumnInfo);	//creating record descriptor

	_rbfm->scan(fileHandle, getColumnInfo, "table-id", EQ_OP, tableId, Attrb,
			rmsi2.rbfmsi);

	//RecordBasedFileManager* _rbfm = RecordBasedFileManager::instance();
	//RBFM_ScanIterator* rbfm_si = new RBFM_ScanIterator();
	while (rmsi2.getNextTuple(rid, data) != RM_EOF) {
		Attribute attribute;

		// int columnId;
		AttrType columnType;
		AttrLength columnLength;

		int offset = 0, columnNameLength = 0;

		//add the null bit indicator offset
		int nullindicatorsize = 1;//ceil(3/8) //for getnextrecord data should go in the format nullbits,length,data
		offset += nullindicatorsize;

		memcpy(&columnNameLength, (char *) data + offset, sizeof(int)); //copy column name length
		offset += sizeof(int);

		char columnNameCharArray[columnNameLength];
		memcpy(&columnNameCharArray, (char *) data + offset, columnNameLength); //copy the column name in string format
		//  columnNameCharArray[columnNameLength] = '\0';
		string columnName(columnNameCharArray, columnNameLength); //convert the char array to string
		offset += columnNameLength;

		memcpy(&columnType, (char *) data + offset, sizeof(int)); //copy the column type(as int)
		offset += sizeof(int);

		memcpy(&columnLength, (char *) data + offset, sizeof(int)); //copy the column length
		offset += sizeof(int);

//	            memcpy(&columnId, returnedData+offset, sizeof(int));//copy the column id
//	            offset += sizeof(int);

		attribute.name = columnName;
		attribute.type = columnType;
		attribute.length = columnLength;

		attrs.push_back(attribute);
	}

	rmsi2.close();

	_rbfm->closeFile(fileHandle);
	free(data);
	free(tableId);
	free(value);
	return 0;

}

RC RelationManager::insertTuple(const string &tableName, const void *data,
		RID &rid) {

	if (tableName == "Tables" || tableName == "Columns")
		return -1;

	//get the filename from the table name
	RecordBasedFileManager* _rbfm = RecordBasedFileManager::instance();
	RM_ScanIterator rm_si;
	IndexManager *_ix = IndexManager::instance();

	FileHandle fileHandle;
	if (_rbfm->openFile(tableName, fileHandle) != 0) {
		return -1;
	}
	vector<Attribute> attr;
	if (getAttributes(tableName, attr) != 0) {
		return -1;
	}
	if (_rbfm->insertRecord(fileHandle, attr, data, rid) != 0) {
		return -1;
	}
	if (_rbfm->closeFile(fileHandle) != 0) {
		return -1;
	}
//	if(insertIndex(tableName, data, rid) != 0)
//	{
//		return -1;
//	}

	//---------insert index-----------

	vector<string> Attrib;
	Attrib.push_back("table-id");

	void *value = calloc(4096, 1);
	int tableName_length = tableName.length();
	int offset = 0;
	memcpy((char *) value + offset, &tableName_length, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) value + offset, tableName.c_str(), tableName_length);
//close file releases the file handle???
	_rbfm->openFile("Tables", fileHandle);

	vector<Attribute> tableInfo;    //creating an attribute
	getTableDescription(tableInfo);    //creating record descriptor

	_rbfm->scan(fileHandle, tableInfo, "table-name", EQ_OP, value, Attrib,
			rm_si.rbfmsi);

	RID ridtid;
	void *data2 = malloc(100);
	rm_si.getNextTuple(ridtid, data2);
	rm_si.close();

	void *tableId = calloc(100, 1);
	memcpy(tableId, (char *) data2 + 1, sizeof(int));

	_rbfm->closeFile(fileHandle);

	//scan the indexes table with the table id and get the existing index file names
	vector<string> existingIndexes;
	vector<string> attribNames;
	attribNames.push_back("column-name");

	vector<Attribute> indexInfo;    //creating an attribute
	getIndexDescription(indexInfo);

	_rbfm->openFile("Indexes", fileHandle);

	_rbfm->scan(fileHandle, indexInfo, "table-id", EQ_OP, tableId, attribNames,
			rm_si.rbfmsi);

	RID rid2;
	void *returnedData = malloc(4096);
	while (rm_si.getNextTuple(rid2, returnedData) != RM_EOF) {
		//push the attributes into a vector
		int length = 0, offset = 1;
		memcpy(&length, (char *) returnedData + offset, sizeof(int));
		offset += sizeof(int);
		string attrbName((char *) returnedData + offset, length);
		existingIndexes.push_back(attrbName);
	}
	vector<Attribute> attrs;
	getAttributes(tableName, attrs);
	vector<int> positions;

	for (unsigned i = 0; i < attrs.size(); i++) {
		for (unsigned j = 0; j < existingIndexes.size(); j++) {
			if (attrs[i].name == existingIndexes[j]) {
				positions.push_back(i); //get the positions of the attribute descriptor of the table for which index files are created
			}
		}
	}
	if (positions.size() != 0) {
		offset = 0;
		int length = 0;
		int recordFieldSize = attrs.size();
		int inputNullBytes = ceil((double) recordFieldSize / 8); //calculate the no. of null bytes
		char* nullBytes = (char *) calloc(inputNullBytes, sizeof(char)); //malloc(inputNullBytes);
		memcpy(nullBytes, data, inputNullBytes);
		offset += inputNullBytes;
		void *key = malloc(4096);
		unsigned j = 0;

		for (int i = 0; i < recordFieldSize; i++) {
			int num_of_null_bit = i % 8;
			int num_of_null_bytes = i / 8;
			if (nullBytes[num_of_null_bytes] & (1 << (7 - num_of_null_bit))) {
				//cout << attrs[i].name << ":" << "NULL" << endl;
			} else {

				switch (attrs[i].type) {
				case TypeInt: {
					if (i == positions[j]) {
						j++;
						memcpy(key, (char *) data + offset, sizeof(int));
						IXFileHandle ixfileHandle;
						//string indexName = getIndexName(tableName, indexedAttr.at(i).name);
						string indexName = tableName + "_" + attrs[i].name;
						if (_ix->openFile(indexName, ixfileHandle) != 0) //open the corresponding index file
								{
							return -1;
						}
						if (_ix->insertEntry(ixfileHandle, attrs[i], key, rid)
								!= 0) //insert the entry into corresponding index file
								{
							return -1;
						}
						if (_ix->closeFile(ixfileHandle) != 0) //close the index file
								{
							return -1;
						}

					}
					offset += 4;
					break;
				}
				case TypeReal: {
					if (i == positions[j]) {

						j++;
						memcpy(key, (char *) data + offset, sizeof(int));
						IXFileHandle ixfileHandle;
						//string indexName = getIndexName(tableName, indexedAttr.at(i).name);
						string indexName = tableName + "_" + attrs[i].name;
						if (_ix->openFile(indexName, ixfileHandle) != 0) //open the corresponding index file
								{
							return -1;
						}
						if (_ix->insertEntry(ixfileHandle, attrs[i], key, rid)
								!= 0) //insert the entry into corresponding index file
								{
							return -1;
						}
						if (_ix->closeFile(ixfileHandle) != 0) //close the index file
								{
							return -1;
						}

					}
					offset += 4;
					break;
				}
				case TypeVarChar: {
					if (i == positions[j]) {
						j++;
						memcpy(&length, (char *) data + offset, sizeof(int));

						memcpy(key, (char *) data + offset + sizeof(int),
								length);

						IXFileHandle ixfileHandle;
						//string indexName = getIndexName(tableName, indexedAttr.at(i).name);
						string indexName = tableName + "_" + attrs[i].name;
						if (_ix->openFile(indexName, ixfileHandle) != 0) //open the corresponding index file
								{
							return -1;
						}
						if (_ix->insertEntry(ixfileHandle, attrs[i], key, rid)
								!= 0) //insert the entry into corresponding index file//should insert varchar along with its length
								{
							return -1;
						}
						if (_ix->closeFile(ixfileHandle) != 0) //close the index file
								{
							return -1;
						}
					}
					offset += sizeof(int) + length;
					break;
				}
				}
				if (j >= positions.size()) {
					break;
				}
			}

		}

		free(nullBytes);
		free(key);
	}

	//---------------------------------------------------
	return 0;

}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
	RecordBasedFileManager* _rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	//RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle,const vector<Attribute> &recordDescriptor, const RID &rid)
	if (tableName == "Tables" || tableName == "Columns")
		return -1;
	// Read data to delete
	void *data = malloc(4096);
	memset(data, 0, 4096);

	if (readTuple(tableName, rid, data) != 0) {
		return -1;
	}

	if (_rbfm->openFile(tableName, fileHandle) != 0) //open file
			{
		return -1;
	}

	vector<Attribute> attr;
	if (getAttributes(tableName, attr) != 0) //get file attribute
			{
		return -1;
	}

	if (_rbfm->deleteRecord(fileHandle, attr, rid) != 0) //delete record of rid
			{
		return -1;
	}

	if (_rbfm->closeFile(fileHandle) != 0) //close file
			{
		return -1;
	}
//need to delete that particular tuple from index file as well
	if (deleteIndex(tableName, data, rid) != 0) {
		return -1;
	}

	return 0;
}

RC RelationManager::deleteIndex(const string &tableName, const void *data,
		const RID &rid) {

	//---------delete index-----------
	RecordBasedFileManager* _rbfm = RecordBasedFileManager::instance();
	RM_ScanIterator rm_si;
	IndexManager *_ix = IndexManager::instance();
	FileHandle fileHandle;

	vector<string> Attrib;
	Attrib.push_back("table-id");

	void *value = calloc(4096, 1);
	int tableName_length = tableName.length();
	int offset = 0;
	memcpy((char *) value + offset, &tableName_length, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) value + offset, tableName.c_str(), tableName_length);
//close file releases the file handle???
	_rbfm->openFile("Tables", fileHandle);

	vector<Attribute> tableInfo;    //creating an attribute
	getTableDescription(tableInfo);    //creating record descriptor

	_rbfm->scan(fileHandle, tableInfo, "table-name", EQ_OP, value, Attrib,
			rm_si.rbfmsi);

	RID ridtid;
	void *data2 = malloc(100);
	rm_si.getNextTuple(ridtid, data2);
	rm_si.close();

	void *tableId = calloc(100, 1);
	memcpy(tableId, (char *) data2 + 1, sizeof(int));

	_rbfm->closeFile(fileHandle);
	free(data2);

	//scan the indexes table with the table id and get the existing index file names
	vector<string> existingIndexes;
	vector<string> attribNames;
	attribNames.push_back("column-name");

	vector<Attribute> indexInfo;    //creating an attribute
	getIndexDescription(indexInfo);

	_rbfm->openFile("Indexes", fileHandle);

	_rbfm->scan(fileHandle, indexInfo, "table-id", EQ_OP, tableId, attribNames,
			rm_si.rbfmsi);

	RID rid2;
	void *returnedData = malloc(4096);
	while (rm_si.getNextTuple(rid2, returnedData) != RM_EOF) {
		//push the attributes into a vector
		int length = 0, offset = 0;
		memcpy(&length, (char *) returnedData + offset, sizeof(int));
		offset += sizeof(int);
		string attrbName((char *) returnedData + offset, length);
		existingIndexes.push_back(attrbName);
	}
	vector<Attribute> attrs;
	getAttributes(tableName, attrs);
	vector<int> positions;

	for (unsigned i = 0; i < attrs.size(); i++) {
		for (unsigned j = 0; j < existingIndexes.size(); j++) {
			if (attrs[i].name == existingIndexes[j]) {
				positions.push_back(i); //get the positions of the attribute descriptor of the table for which index files are created
			}
		}
	}

	if (positions.size() != 0) {
		offset = 0;
		int length = 0;
		int recordFieldSize = attrs.size();
		int inputNullBytes = ceil((double) recordFieldSize / 8); //calculate the no. of null bytes
		char* nullBytes = (char *) calloc(inputNullBytes, sizeof(char)); //malloc(inputNullBytes);
		memcpy(nullBytes, data, inputNullBytes);
		offset += inputNullBytes;
		void *key = malloc(4096);
		unsigned j = 0;

		for (int i = 0; i < recordFieldSize; i++) {
			int num_of_null_bit = i % 8;
			int num_of_null_bytes = i / 8;
			if (nullBytes[num_of_null_bytes] & (1 << (7 - num_of_null_bit))) {
				//cout << attrs[i].name << ":" << "NULL" << endl;
			} else {

				switch (attrs[i].type) {
				case TypeInt: {
					if (i == positions[j]) {
						j++;
						memcpy(key, (char *) data + offset, sizeof(int));
						IXFileHandle ixfileHandle;
						//string indexName = getIndexName(tableName, indexedAttr.at(i).name);
						string indexName = tableName + "_" + attrs[i].name;
						if (_ix->openFile(indexName, ixfileHandle) != 0) //open the corresponding index file
								{
							return -1;
						}
						//RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle,const Attribute &attribute, const void *key, const RID &rid)

						if (_ix->deleteEntry(ixfileHandle, attrs[i], key, rid)
								!= 0) //insert the entry into corresponding index file
								{
							return -1;
						}
						if (_ix->closeFile(ixfileHandle) != 0) //close the index file
								{
							return -1;
						}

					}
					offset += 4;
					break;
				}
				case TypeReal: {
					if (i == positions[j]) {

						j++;
						memcpy(key, (char *) data + offset, sizeof(int));
						IXFileHandle ixfileHandle;
						//string indexName = getIndexName(tableName, indexedAttr.at(i).name);
						string indexName = tableName + "_" + attrs[i].name;
						if (_ix->openFile(indexName, ixfileHandle) != 0) //open the corresponding index file
								{
							return -1;
						}
						if (_ix->deleteEntry(ixfileHandle, attrs[i], key, rid)
								!= 0) //insert the entry into corresponding index file
								{
							return -1;
						}
						if (_ix->closeFile(ixfileHandle) != 0) //close the index file
								{
							return -1;
						}

					}
					offset += 4;
					break;
				}
				case TypeVarChar: {
					if (i == positions[j]) {
						j++;
						memcpy(&length, (char *) data + offset, sizeof(int));

						memcpy(key, (char *) data + offset + sizeof(int),
								length);

						IXFileHandle ixfileHandle;
						//string indexName = getIndexName(tableName, indexedAttr.at(i).name);
						string indexName = tableName + "_" + attrs[i].name;
						if (_ix->openFile(indexName, ixfileHandle) != 0) //open the corresponding index file
								{
							return -1;
						}
						if (_ix->deleteEntry(ixfileHandle, attrs[i], key, rid)
								!= 0) //insert the entry into corresponding index file//should insert varchar along with its length
								{
							return -1;
						}
						if (_ix->closeFile(ixfileHandle) != 0) //close the index file
								{
							return -1;
						}
					}
					offset += sizeof(int) + length;
					break;
				}
				}
				if (j >= positions.size()) {
					break;
				}
			}

		}

		free(nullBytes);
		free(key);
		//---------------------------------------------------
	}

	return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data,
		const RID &rid) {
	RecordBasedFileManager* _rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;

	if (_rbfm->openFile(tableName, fileHandle) != 0) {
		return -1;
	}
	vector<Attribute> attr;
	if (getAttributes(tableName, attr) != 0) {
		return -1;
	}
	if (_rbfm->updateRecord(fileHandle, attr, data, rid) != 0) {
		return -1;
	}
	if (_rbfm->closeFile(fileHandle) != 0) {
		return -1;
	}
	return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid,
		void *data) {
	RecordBasedFileManager* _rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;

	if (_rbfm->openFile(tableName, fileHandle) != 0) //open the file
			{
		return -1;
	}
	vector<Attribute> attr;
	if (getAttributes(tableName, attr) != 0) //get attributes of the file
			{
		return -1;
	}
	if (_rbfm->readRecord(fileHandle, attr, rid, data) != 0) //read the data
			{
		return -1;
	}

	if (_rbfm->closeFile(fileHandle) != 0) //close the file
			{
		return -1;
	}
	return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs,
		const void *data) {
	//RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data)

	RecordBasedFileManager* _rbfm = RecordBasedFileManager::instance();
	_rbfm->printRecord(attrs, data);
	return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid,
		const string &attributeName, void *data) {

	RecordBasedFileManager* _rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	if (_rbfm->openFile(tableName, fileHandle) != 0) //open the file
			{
		return -1;
	}

	vector<Attribute> attr;
	if (getAttributes(tableName, attr) != 0) //get attributes of the file
			{
		return -1;
	}

	if (_rbfm->readAttribute(fileHandle, attr, rid, attributeName, data) != 0) //read the given attribute
			{
		return -1;
	}

	if (_rbfm->closeFile(fileHandle) != 0) //close the file
			{
		return -1;
	}

	return 0;
}

RC RelationManager::scan(const string &tableName,
		const string &conditionAttribute, const CompOp compOp,
		const void *value, const vector<string> &attributeNames,
		RM_ScanIterator &rm_ScanIterator) {
	RecordBasedFileManager* _rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	if (_rbfm->openFile(tableName, fileHandle) != 0) {
		return -1;
	}
	vector<Attribute> attr;
	if (tableName == "Tables") {
		getTableDescription(attr);

	} else if (tableName == "Columns") {
		getAttributeDescription(attr);

	} else if (tableName == "Indexes") {
		getIndexDescription(attr);

	} else if (getAttributes(tableName, attr) != 0) {
		return -1;
	}

	if (_rbfm->scan(fileHandle, attr, conditionAttribute, compOp, value,
			attributeNames, rm_ScanIterator.rbfmsi) != 0) {
		return -1;
	}

	return 0;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName,
		const string &attributeName) {
	return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName,
		const Attribute &attr) {
	return -1;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
	if (rbfmsi.getNextRecord(rid, data) != RBFM_EOF) {
		return 0;
	} else {
		return RBFM_EOF;
	}
}

RC RM_ScanIterator::close() {
	rbfmsi.close();

	return 0;
}

//added-nov24

RC RelationManager::createIndex(const string &tableName,
		const string &attributeName) {

	IndexManager *_ix = IndexManager::instance();
	string indexName = tableName + "_" + attributeName;

	IXFileHandle ixfileHandle;
	_ix->createFile(indexName);
	_ix->openFile(indexName, ixfileHandle);

	//get the data from tablename and insert the attribute name data into a new file

	RM_ScanIterator rm_si;

	vector<string> Attrbs;
	Attrbs.push_back(attributeName);

	scan(tableName, "", NO_OP, NULL, Attrbs, rm_si); //attributeName=attribute.name

	RID rid;
	char returnedData[4096];
	Attribute attr;
	//get single attribute from get attributes
	if (getOneAttributeFromName(tableName, attributeName, attr) != 0) { //attr will have complete attribute
		return -1;
	}
	//insert the data corresponding to attr into the index file
	while (rm_si.getNextTuple(rid, returnedData) != RM_EOF) {
		if (_ix->insertEntry(ixfileHandle, attr, returnedData, rid) != 0) {
			return -1;
		}
	}
	if (_ix->closeFile(ixfileHandle) != 0) {
		return -1;
	}

	rm_si.close();

	//update the Indexes table

	vector<Attribute> indexAttribs;
	getIndexDescription(indexAttribs);

	//Indexes table has table id,attribute name,index name

	//get the tableid from tablename
	RM_ScanIterator rmsi;

	vector<string> Attrib;
	Attrib.push_back("table-id");

	void *value = calloc(4096, 1);
	int tableName_length = tableName.length();
	int offset = 0;

	memcpy((char *) value + offset, &tableName_length, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) value + offset, tableName.c_str(), tableName_length);

	RecordBasedFileManager *_rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;

	_rbfm->openFile("Tables", fileHandle);

	vector<Attribute> tableInfo;
	getTableDescription(tableInfo);

	_rbfm->scan(fileHandle, tableInfo, "table-name", EQ_OP, value, Attrib,
			rmsi.rbfmsi);

	RID rid1;
	void *data = malloc(100);
	rmsi.getNextTuple(rid1, data);
	rmsi.close();

	void *tableId = calloc(100, 1);
	memcpy(tableId, (char *) data + 1, sizeof(int));

	_rbfm->closeFile(fileHandle);
	free(value);
	free(data);

	void *indexBuffer = calloc(4096, 1);

	int attributeNameLength = attributeName.length();
	int indexNameLength = indexName.length();

	offset = 0;

	//copy the null indicator size bits
	memset((char *) data + offset, 0, 1); //ceil(3/8) == 1
	offset += 1;

	memcpy((char *) indexBuffer + offset, tableId, sizeof(int));
	offset += sizeof(int);

	memcpy((char *) indexBuffer + offset, &attributeNameLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) indexBuffer + offset, attributeName.c_str(),
			attributeName.length());
	offset += attributeName.length();

	memcpy((char *) indexBuffer + offset, &indexNameLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) indexBuffer + offset, indexName.c_str(),
			indexName.length());
	offset += indexName.length();

	RID indexFileRid;
	FileHandle fh;
	if (_rbfm->openFile("Indexes", fh) != 0) {
		return -1;
	}
	_rbfm->printRecord(indexAttribs, indexBuffer);
	if (_rbfm->insertRecord(fh, indexAttribs, indexBuffer, indexFileRid) != 0) {
		return -1;
	}
	free(indexBuffer);

	return 0;
}

RC RelationManager::getOneAttributeFromName(const string &tableName,
		const string &attributeName, Attribute &attr) {
	vector<Attribute> attrs;
	if (getAttributes(tableName, attrs) != 0) {
		return -1;
	}

	for (unsigned i = 0; i < attrs.size(); i++) {
		if (attrs[i].name == attributeName) {
			attr = attrs[i];
			return 0;
		}
	}
	return -1;
}

RC RelationManager::destroyIndex(const string &tableName,
		const string &attributeName) {
	//destroy the index file
	//delete its record from the index file
	string indexName = tableName + "_" + attributeName;

	RecordBasedFileManager* _rbfm = RecordBasedFileManager::instance();
	IndexManager *_ix = IndexManager::instance();
	RM_ScanIterator rmsi;

	vector<string> projectedAttr;
	projectedAttr.push_back("index-name");

	int offset = 0;
	int indexNameLength = indexName.length();
	void *indexNameData = malloc(4096);

	memcpy(indexNameData, &indexNameLength, sizeof(int));
	offset += sizeof(int);

	memcpy((char *) indexNameData + offset, indexName.c_str(), indexNameLength);

	scan("Indexes", "index-name", EQ_OP, indexNameData, projectedAttr, rmsi);
	free(indexNameData);

	RID rid;
	char returnedData[4096];
	FileHandle fileHandle;
	vector<Attribute> attrs;
	if (getIndexDescription(attrs) != 0) {
		return -1;
	}
	if (_rbfm->openFile("Indexes", fileHandle) != 0) {
		return -1;
	}
	while (rmsi.getNextTuple(rid, returnedData) != RM_EOF) {
		if (_rbfm->deleteRecord(fileHandle, attrs, rid) != 0) {
			return -1;
		}
		break;
	}
	if (_rbfm->closeFile(fileHandle) != 0) {
		return -1;
	}

	// Delete index file
	if (_ix->destroyFile(indexName) != 0) {
		return -1;
	}

	return 0;

}

RC RelationManager::indexScan(const string &tableName,
		const string &attributeName, const void *lowKey, const void *highKey,
		bool lowKeyInclusive, bool highKeyInclusive,
		RM_IndexScanIterator &rm_IndexScanIterator) {

	string indexName = tableName + "_" + attributeName;
//    RecordBasedFileManager* _rbfm = RecordBasedFileManager::instance();
	IndexManager *_ix = IndexManager::instance();
//	IXFileHandle ixfileHandle;

	if (_ix->openFile(indexName,rm_IndexScanIterator.ixfileHandle) != 0) {
		return -1;
	}

	Attribute attr;
	if (getOneAttributeFromName(tableName, attributeName, attr) != 0) {
		return -1;
	}

	if (_ix->scan(rm_IndexScanIterator.ixfileHandle, attr, lowKey, highKey, lowKeyInclusive,
			highKeyInclusive, rm_IndexScanIterator._ixsi)) {
		return -1;
	}

//	rm_IndexScanIterator.setIXSI(rm_IndexScanIterator._ixsi);

	return 0;
}

//RC RM_IndexScanIterator::setIXSI(IX_ScanIterator _ixsi) {
//	this->_ixsi = _ixsi;
//	return 0;
//}
RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
	if (_ixsi.getNextEntry(rid, key) != RBFM_EOF) {
		return 0;
	} else {
		return RBFM_EOF;
	}
}

RC RM_IndexScanIterator::close() {
	_ixsi.close();

	return 0;
}

//RC RelationManager::insertIndex(const string &tableName, const void *data, const RID &rid)
//{
//	IndexManager *_ix = IndexManager::instance();
//	vector<Attribute> attribute;
//	vector<Attribute> indexedAttr;
//	vector<void *> keys;
//	if(getAttributes(tableName, attribute) != 0)
//	{
//		return -1;
//	}
//	if(searchExistedIndex(tableName, attribute, indexedAttr, data, keys) != 0) //check if the indexfile is present for any of the attributes//indexedAttr will have the attributes for which the index files are created
//	{
//		return -1;
//	}
//	if(indexedAttr.size() == 0) //no index file for any of the attributes
//	{
//		return 0;
//	}
//	for(int i = 0; i < indexedAttr.size(); i++)
//	{
//		IXFileHandle ixfileHandle;
//		//string indexName = getIndexName(tableName, indexedAttr.at(i).name);
//		string indexName=tableName+"_"+indexedAttr[i].name;
//		if(_ix->openFile(indexName, ixfileHandle) != 0)//open the corresponding index file
//		{
//			return -1;
//		}
//		if(_ix->insertEntry(ixfileHandle, indexedAttr[i], keys[i], rid) != 0)//insert the entry into corresponding index file
//		{
//			return -1;
//		}
//		if(_ix->closeFile(ixfileHandle) != 0) //close the index file
//		{
//			return -1;
//		}
//		free(keys[i]);
//	}
//	return 0;
//}
//
//RC RelationManager::searchExistedIndex(const string &tableName, vector<Attribute> &attribute, vector<Attribute> &indexedAttr,
//									   const void *data, vector<void *> &keys) {
//	indexedAttr.clear();
//
//	//get tableid from table name
//	RM_ScanIterator rm_si;
//	RecordBasedFileManager *_rbfm = RecordBasedFileManager::instance();
//	IndexManager *_ix = IndexManager::instance();
//
//	vector<string> Attrib;
//	Attrib.push_back("table-id");
//
//	void *value = calloc(4096, 1);
//	int tableName_length = tableName.length();
//	int offset = 0;
//	memcpy((char *) value + offset, &tableName_length, sizeof(int));
//	offset += sizeof(int);
//	memcpy((char *) value + offset, tableName.c_str(), tableName_length);
//
//	FileHandle fileHandle;
//
//	_rbfm->openFile("Tables", fileHandle);
//
//	vector<Attribute> tableInfo;    //creating an attribute
//	getTableDescription(tableInfo);    //creating record descriptor
//
//	_rbfm->scan(fileHandle, tableInfo, "table-name", EQ_OP, value, Attrib,
//				rm_si.rbfmsi);
//
//	RID rid;
//	void *data2 = malloc(100);
//	rm_si.getNextTuple(rid, data2);
//	rm_si.close();
//
//	void *tableId = calloc(100, 1);
//	memcpy(tableId, (char *) data2 + 1, sizeof(int));
//
//	_rbfm->closeFile(fileHandle);
//
//	//scan the indexes table with the table id and get the existing index file names
//	vector<string> existingIndexes;
//	vector<string> attribNames;
//	attribNames.push_back("column-name");
//
//	vector<Attribute> indexInfo;    //creating an attribute
//	getIndexDescription(indexInfo);
//
//	_rbfm->openFile("Indexes", fileHandle);
//
//	_rbfm->scan(fileHandle, indexInfo, "table-id", EQ_OP, tableId, attribNames,
//				rm_si.rbfmsi);
//
//	RID rid2;
//	void *returnedData = malloc(4096);
//	while (rm_si.getNextTuple(rid2, returnedData) != RM_EOF) {
//		//push the attributes into a vector
//		int length = 0, offset = 0;
//		memcpy(&length, (char *) returnedData, sizeof(int));
//		offset += sizeof(int);
//		string attrbName((char *) returnedData + offset, length);
//		existingIndexes.push_back(attrbName);
//	}
//
//	vector<int> positions;
//	for (int i = 0; i < attribute.size(); i++) {
//		for (int j = 0; j < existingIndexes.size(); j++) {
//			if (attribute[i].name == existingIndexes[j]) {
//				positions.push_back(i);
//			}
//		}
//	}
//
//	offset = 0;
//	int offsetmove=0,length=0;
//	int recordFieldSize = attribute.size();
//	int inputNullBytes = ceil((double) recordFieldSize / 8);//calculate the no. of null bytes
//	char* nullBytes = (char *) calloc(inputNullBytes, sizeof(char));//malloc(inputNullBytes);
//	memcpy(nullBytes, data, inputNullBytes);
//	offset += inputNullBytes;
//	void *key = malloc(4096);
//	int j=0;
//
//	for (int i = 0; i < recordFieldSize; i++) {
//		int num_of_null_bit = i % 8;
//		int num_of_null_bytes = i / 8;
//		if (nullBytes[num_of_null_bytes] & (1 << (7 - num_of_null_bit))) {
//			cout << attribute[i].name << ":" << "NULL" << endl;
//		}
//		else {
//
//			switch (attribute[i].type) {
//				case TypeInt: {
//					if(i==positions[j]){
//						j++;
//						 memcpy(key,(char *)data+offsetmove,sizeof(int));
//					}
//						offsetmove+=4;
//					break;
//				}
//				case TypeReal: {
//					if(i==positions[j]){
//						j++;
//						memcpy(key,(char *)data+offsetmove,sizeof(int));
//					}
//					offsetmove+=4;
//					break;
//				}
//				case TypeVarChar: {
//					if(i==positions[j]){
//						j++;
//						memcpy(&length,(char *)data+offsetmove,sizeof(int));
//						offsetmove+=4;
//						memcpy(key,(char *)data+offsetmove,length);
//					}
//					offsetmove+=length;
//					break;
//				}
//			}
//		}
//		if(j>=positions.size()){
//			break;
//		}
//	}
//
//	cout << endl;
//	free(nullBytes);
//}
