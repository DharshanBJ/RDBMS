#include "rm.h"

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
//int tableId=0;
RC RelationManager::createCatalog() {
	RecordBasedFileManager* _rbfm = RecordBasedFileManager::instance();
//-----------------------------------
//	string meta_tableid = "meta_tableid";
//	_rbfm->createFile(meta_tableid);
//
//	FileHandle fileHandle_table_Id;
//	_rbfm->openFile(meta_tableid,fileHandle_table_Id);
//
////	void * tableid_data=calloc(4,1);
////	memset(tableid_data,0,sizeof(int));
//	int tableid_data=0;
//	fileHandle_table_Id.appendPage(&tableid_data);
//
//	//void * tableid_data=calloc(4,1);
//
////	fileHandle_table_Id.appendPage(tableid_data);
//	//tableIdGeneration(meta_tableid);
//
//	_rbfm->closeFile(fileHandle_table_Id);

//-----------------------------------------------------------

	FileHandle fileHandle;

	if (_rbfm->openFile("Tables", fileHandle) != 0
			|| _rbfm->openFile("Columns", fileHandle) != 0) {
		_rbfm->createFile("Tables");
		_rbfm->createFile("Columns");
		vector<Attribute> tableDesc;
		vector<Attribute> attributeDesc;
		getTableDescription(tableDesc);
		getAttributeDescription(attributeDesc);
		//TODO:insert first record for tables and columns
		createTable("Tables", tableDesc); //create system table-Tables
		createTable("Columns", attributeDesc); //create system table-Columns

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

RC RelationManager::TableName_check(const string &tableName) {
	if (tableName == "Tables" || tableName == "Columns") {
		return -1;
	}
	return 0;
}
RC RelationManager::deleteCatalog() {
	RecordBasedFileManager* _rbfm = RecordBasedFileManager::instance();
	_rbfm->destroyFile("Tables");
	_rbfm->destroyFile("Columns");
	return 0;
}

//RC RelationManager::createTable(const string &tableName,
//		const vector<Attribute> &attrs) {
//
//	FileHandle fileHandle;
//	RecordBasedFileManager* _rbfm = RecordBasedFileManager::instance();
//
//	vector<Attribute> tableInfo; //creating an attribute
//	getTableDescription(tableInfo); //creating record descriptor
//
//	//TODO:Need to calculate the table attribute length
//	int dataLength = 0;
//	int tableNameLength = 0;
//	int nullFieldsIndicatorActualSize = ceil((double) 3 / 8);
//	void *data = calloc(4096, 1);
//
//	int offset = 0;
//	tableId++;
//	tableNameLength = tableName.length();
//	//data has to be in insert record format
//	memset(data, 0, dataLength);	  	//set the buffer as 0 for all locations
//
//	//copy the null indicator size bits
//	memset((char *) data + offset, 0, nullFieldsIndicatorActualSize);
//	offset += nullFieldsIndicatorActualSize;
//
//	memcpy((char *) data + offset, &tableId, sizeof(int)); //copy the table id
//	offset += sizeof(int);
//
//	memcpy((char *) data + offset, &tableNameLength, sizeof(int)); //copy the tablename length
//	offset += sizeof(int);
//	//cout << "table name length : " << tableNameLength << endl;
//
//	memcpy((char *) data + offset, tableName.c_str(), tableNameLength); //copy the tablename
//	offset += tableNameLength;
//
//	memcpy((char *) data + offset, &tableNameLength, sizeof(int)); //copy the filename length
//	offset += sizeof(int);
//	cout << "file name length : " << tableNameLength << endl;
//
//	memcpy((char *) data + offset, tableName.c_str(), tableName.length()); //copy the filename
//	offset += tableName.length();
//
//	RID rid;
//
//	if (_rbfm->openFile("Tables", fileHandle) != 0) //open the tables file
//			{
//		free(data);
//		return -1;
//	}
//	if (_rbfm->insertRecord(fileHandle, tableInfo, data, rid) != 0) //insert the created tables details in 'Tables' table
//			{
//
//		free(data);
//		return -1;
//	}
//
//	//populate attribute catalog
//
//	int columnIndexCount = 1;
//	RID descRid;
//	FileHandle columnFileHandle;
//	if (_rbfm->openFile("Columns", columnFileHandle) != 0) //open columns table
//			{
//		return -1;
//	}
//	int attrs_size = attrs.size();
//
//	// for(vector<Attribute>::const_iterator it = attrs.begin(); it != attrs.end(); it++)//iterate through every attribute
//	for (int i = 0; i < attrs_size; i++) {
//		vector<Attribute> columnInfo;
//		getAttributeDescription(columnInfo);
//
//		int attrNameLength = 0;
//		int nullFieldsIndicatorActualSize = ceil((double) 5 / 8);
//
//		attrNameLength = attrs[i].name.length();
//
//		void* attrData = malloc(4096);
//
//		memset(attrData, 0, 4096); //set the buffer as zero for all locations
//
//		int descOffset = 0;
//		//copy the null indicator size bits
//		memset((char *) attrData + descOffset, 0,
//				nullFieldsIndicatorActualSize);
//		descOffset += nullFieldsIndicatorActualSize;
//
//		memcpy((char *) attrData + descOffset, &tableId, sizeof(int)); //table-id
//		descOffset += sizeof(int);
//
//		memcpy((char *) attrData + descOffset, &attrNameLength, sizeof(int)); //column name length
//		descOffset += sizeof(int);
//		cout << "attrNameLength : " << attrNameLength << endl;
//
//		memcpy((char *) attrData + descOffset, (attrs[i].name).c_str(),
//				attrNameLength); //column name
//		// memcpy((char *)attrData + descOffset, (it->name).c_str(), attrNameLength);//column name
//		descOffset += attrNameLength;
//
//		memcpy((char *) attrData + descOffset, &(attrs[i].type),
//				sizeof(TypeInt));	                    //column type
//		// memcpy((char *)attrData + descOffset, &(it->type), sizeof(TypeInt));
//		descOffset += sizeof(TypeInt);
//
//		memcpy((char *) attrData + descOffset, &(attrs[i].length), sizeof(int));//column length
//		// memcpy((char *)attrData + descOffset, &(it->length), sizeof(int));
//		descOffset += sizeof(int);
//		cout << "attrName column Length : " << attrs[i].length << endl;
//
//		memcpy((char *) attrData + descOffset, &columnIndexCount, sizeof(int));	//column index count is the column position
//		columnIndexCount++;
//		descOffset += sizeof(int);
//
//		// memcpy((char *)attrData + descOffset, &isExisted, sizeof(int));
//		//insertRecord(FileHandle &fileHandle,const vector<Attribute> &recordDescriptor, const void *data, RID &rid)
//		// readRecord(FileHandle &fileHandle,const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
//		//    printRecord(const vector<Attribute> &recordDescriptor, const void *data)
//
//		//	                      void *data=malloc(4096);
//		//	                      _rbfm->readRecord(columnFileHandle,columnInfo,descRid,data);
//		//	                      _rbfm->printRecord(columnInfo,data);
//		//	                      free(data);
//
//		if (_rbfm->insertRecord(columnFileHandle, columnInfo, attrData, descRid)
//				!= 0)//insert the created tables attribute details in 'Columns' table
//				{
//			free(attrData);
//			return -1;
//		}
//		free(attrData);
//
//		//print the inserted records
//		//	                      void *data1=malloc(4096);
//		//	                      _rbfm->readRecord(columnFileHandle,columnInfo,descRid,data1);
//		//	                      _rbfm->printRecord(columnInfo,data1);
//		//	                      free(data1);
//
//	}
//	if (tableName != "Tables" && tableName != "Columns") {
//		// Create data file
//		if (_rbfm->createFile(tableName) != 0)//creating file with the name same as table name
//				{
//			return -1;
//		}
//
//		//initialize data file
//	}
//	return 0;
//}

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

//	tableId++;
	int tableId = rand() % 1000+1;


	//----Reading meta data of table ID-----

//	int tableId = 0;
//	FileHandle fh;
//	_rbfm->openFile("meta_tableid", fh);
//
//	void *meta = malloc(sizeof(int));
//	fh.readPage(0, meta);
//
//	memcpy(&tableId, meta, sizeof(int));
//	tableId++;
//	cout<<tableId<<endl;
//
//	_rbfm->closeFile(fh);
	//----------------------------------------

	tableId++;
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

	RID rid;

	if (_rbfm->openFile("Tables", fileHandle) != 0) //open the tables file
			{
		free(data);
		return -1;
	}
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

		//print the inserted records
		//	                      void *data1=malloc(4096);
		//	                      _rbfm->readRecord(columnFileHandle,columnInfo,descRid,data1);
		//	                      _rbfm->printRecord(columnInfo,data1);
		//	                      free(data1);

		//----copy the tableid back to file-----

//		FileHandle fh;
//		_rbfm->openFile("meta_tableid", fh);
//        void * data=calloc(4,1);
//        memcpy(data,&tableId,sizeof(int));
//		fh.writePage(1,data);
//		_rbfm->closeFile(fh);
		//----------------------------------------


	}
	if (tableName != "Tables" && tableName != "Columns") {
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
	if(tableName == "Columns" || tableName == "Tables")return -1;
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
	rmsi1.getNextTuple(rid,data);
	rmsi1.close();

	void * tableId = calloc(100, 1);
	memcpy(tableId, (char*) data + 1, sizeof(int));

	_rbfm->closeFile(fileHandle);

	//search columns according to tableid
	FileHandle fileHandle2;
	RM_ScanIterator rmsi2;
	_rbfm->openFile("Columns", fileHandle2);

	vector<string> Attrb;
	Attrb.push_back("column-name");
	Attrb.push_back("column-type");
	Attrb.push_back("column-length");

//	char returnedData[PAGE_SIZE];

	// scan("Columns", "table-id", EQ_OP, (char*)data+1, Attrb, rm_si);
	vector<Attribute> getColumnInfo;	//creating an attribute
	getAttributeDescription(getColumnInfo);	//creating record descriptor

	_rbfm->scan(fileHandle2, getColumnInfo, "table-id", EQ_OP, tableId, Attrb,
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

	_rbfm->closeFile(fileHandle2);
	free(data);
	free(tableId);
	free(value);
	return 0;

}

RC RelationManager::insertTuple(const string &tableName, const void *data,
		RID &rid) {
	//get the filename from the table name
	RecordBasedFileManager* _rbfm = RecordBasedFileManager::instance();
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
	return 0;

}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
	RecordBasedFileManager* _rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	//RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle,const vector<Attribute> &recordDescriptor, const RID &rid)
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

RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
    if(rbfmsi.getNextRecord(rid, data) != RBFM_EOF)
    {
        return 0;
    } else
    {
        return RBFM_EOF;
    }
}

RC RM_ScanIterator::close()
{
	rbfmsi.close();

    return 0;
}
