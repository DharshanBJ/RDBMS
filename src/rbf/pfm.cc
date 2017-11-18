#include "pfm.h"

#include <stdlib.h>
#include <sys/stat.h>
#include <cstdio>
#include <cstring>


PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();
    
    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}

RC createHiddenPage(FILE *file){
    void *pageBuffer=calloc(4096,1);
    int value=0;
    int offset = 0;
    //readPageCounter
    memcpy((char *)pageBuffer+offset, &value, sizeof(int));
    offset += sizeof(int);
    
    //writePageCounter
    memcpy((char *)pageBuffer+offset, &value, sizeof(int));
    offset += sizeof(int);
    
    
    //appendPageCounter
    memcpy((char *)pageBuffer+offset, &value, sizeof(int));
    offset += sizeof(int);
    
    //root pointer pageNum
    memcpy((char *)pageBuffer+offset, &value, sizeof(int));
    offset += sizeof(int);
    
    
    fwrite(pageBuffer, 4096, 1, file);
    fflush(file);
    
    free(pageBuffer);
    return 0;
}

RC PagedFileManager::createFile(const string &fileName)
{
    FILE * file;
    struct stat stFileInfo;
    
    if(stat(fileName.c_str(), &stFileInfo) == 0){// check if the filename already exists
        return -1;
    }
    file =fopen (fileName.c_str(),"w+" );//open the file with fileName
    if(file==NULL){//throw error if file opening failed
        perror("Creating file failed!");
        return -1;
    }
    //create hidden page
    createHiddenPage(file);
    if(fclose(file)!=0){//if file not closed properly then throw error
        perror("Closing file failed!");
        return -1;
    }
    return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    if(remove(fileName.c_str()) != 0){
        return -1;
    }
    return 0;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    struct stat stFileInfo;
    
    if(stat(fileName.c_str(), &stFileInfo) != 0){// check if the filename doesn't exists
        return -1;
    }
    FILE * file = fopen(fileName.c_str(),"rw+");
    if(file==NULL){
        perror("opening a file file failed");
        return -1;
    }
    if( fileHandle.getPageFilePtr() != NULL)return -1;//check if the file handle is already a handle for some page
    //    fileHandle.setPageFileName(fileName);//set file handle
    fileHandle.setPageFilePtr(file);
    return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    
    //    if(fileHandle.getPageFileName()!= "")return -1;//check if the file has been opened already
    //    if(fileHandle.getPageFileName() != ""){ todo-------------------------------------------------------
    //        FILE * file = fopen((fileHandle.getPageFileName()).c_str(),"r");
    //        if(file!=NULL)
    //        {
    //            if(fclose(fileHandle.getPageFilePtr())!=0)return-1;
    //
    //        }
    //    }
    if(fclose(fileHandle.getPageFilePtr())!=0)return -1;// check if the page is closed successfully
    fileHandle.setPageFilePtr(NULL);//make the pointer to null after close
    return 0;
    //should we free any redundant spaces here?
}


FileHandle::FileHandle():fileName("") , file_Ptr(NULL)
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}


FileHandle::~FileHandle()
{
}


RC incrementCounterValue(FILE* file, int offset){
    if(offset == 0){
        fseek(file, offset*4, SEEK_SET);
        int val = 0;
        fread(&val, sizeof(int), 1, file);
        val++;
        fseek(file, offset*4, SEEK_SET);
        fwrite(&val, sizeof(int), 1, file);
        fflush(file);
    }
    else if(offset == 1){
        fseek(file, offset*4, SEEK_SET);
        int val = 0;
        fread(&val, sizeof(int), 1, file);
        val++;
        fseek(file, offset*4, SEEK_SET);
        fwrite(&val, sizeof(int), 1, file);
        fflush(file);
    }
    else if(offset == 2){
        fseek(file, offset*4, SEEK_SET);
        int val = 0;
        fread(&val, sizeof(int), 1, file);
        val++;
        fseek(file, offset*4, SEEK_SET);
        fwrite(&val, sizeof(int), 1, file);
        fflush(file);
    }
    return 0;
}

//decrement counter values
//RC decrementCounterValue(FILE* file, int offset){
//    if(offset == 0){
//        fseek(file, offset*4, SEEK_SET);
//        int val = 0;
//        fread(&val, sizeof(int), 1, file);
//        val--;
//        fseek(file, offset*4, SEEK_SET);
//        fwrite(&val, sizeof(int), 1, file);
//        fflush(file);
//    }
//    else if(offset == 1){
//        fseek(file, offset*4, SEEK_SET);
//        int val = 0;
//        fread(&val, sizeof(int), 1, file);
//        val--;
//        fseek(file, offset*4, SEEK_SET);
//        fwrite(&val, sizeof(int), 1, file);
//        fflush(file);
//    }
//    else if(offset == 2){
//        fseek(file, offset*4, SEEK_SET);
//        int val = 0;
//        fread(&val, sizeof(int), 1, file);
//        val--;
//        fseek(file, offset*4, SEEK_SET);
//        fwrite(&val, sizeof(int), 1, file);
//        fflush(file);
//    }
//    return 0;
//}

RC FileHandle::readPage(PageNum pageNum, void *data)
{
    unsigned num = getNumberOfPages();
    if(pageNum+1 > num)return -1;
    FILE* pageFile = getPageFilePtr();
    if(pageFile==NULL)return -1;//? check to if file exists or not?
    fseek ( pageFile , (pageNum+1)*PAGE_SIZE , SEEK_SET );
    fread(data,PAGE_SIZE,1,pageFile);
    if(ferror(pageFile))return -1;// read page failed
    rewind(pageFile);
    readPageCounter = readPageCounter +1;
    incrementCounterValue(pageFile, 0);
    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    unsigned num = getNumberOfPages();
    if(pageNum+1 > num)return -1;//to check if the page exists in the file
    FILE* pageFile = getPageFilePtr();
    fseek ( pageFile , (pageNum+1)*PAGE_SIZE , SEEK_SET );
    fwrite(data, PAGE_SIZE,1, pageFile);
    if(ferror(pageFile))return -1; // writing page failed
    rewind(pageFile);
    writePageCounter = writePageCounter+1;
    incrementCounterValue(pageFile, 1);
    return 0;
}


RC FileHandle::appendPage(const void *data)
{
    FILE* pageFile = getPageFilePtr();
    fseek(pageFile, 0, SEEK_END);
    fwrite(data,1,PAGE_SIZE, pageFile);
    if(ferror(pageFile)){//error indicator if non-zero then append error
        return -1;
    }
    rewind(pageFile);
    appendPageCounter = appendPageCounter+1;
    incrementCounterValue(pageFile, 2);
    return 0;
}


unsigned FileHandle::getNumberOfPages()
{
    FILE* pageFile;
    long size;
    pageFile = getPageFilePtr();
    fseek (pageFile, 0, SEEK_END);
    size= ftell(pageFile);
    rewind(pageFile);
    return size/PAGE_SIZE-1;
}

//for IX
//add root pageNum write
RC FileHandle::writeRootPageNumber(unsigned pageNum){
    FILE* file = getPageFilePtr();
    int offset = 3;
    fseek(file, offset*4, SEEK_SET);
    fwrite(&pageNum, sizeof(int), 1, file);
    fflush(file);
    
    return 0;
}

RC FileHandle::readRootPageNumber(){
    FILE* file = getPageFilePtr();
    int offset = 3;
    fseek(file, offset*4, SEEK_SET);
    int val = 0;
    fread(&val, sizeof(int), 1, file);
    return val;
}

RC getCounterValues(FILE* file, int offset){
    if(offset == 0){
        fseek(file, offset*4, SEEK_SET);
        int val = 0;
        fread(&val, sizeof(int), 1, file);
        return val;
    }
    else if(offset == 1){
        fseek(file, offset*4, SEEK_SET);
        int val = 0;
        fread(&val, sizeof(int), 1, file);
        return val;
    }
    else if(offset == 2){
        fseek(file, offset*4, SEEK_SET);
        int val = 0;
        fread(&val, sizeof(int), 1, file);
        return val;
    }
    return -1;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = getCounterValues(getPageFilePtr() , 0);
    writePageCount = getCounterValues(getPageFilePtr() , 1);;
    appendPageCount = getCounterValues(getPageFilePtr() , 2);;
    return 0;
}

FILE* FileHandle::getPageFilePtr(){
    return file_Ptr;
}

string FileHandle::getPageFileName(){
    return fileName;
}

RC FileHandle::setPageFilePtr(FILE* ptr){
    file_Ptr = ptr;
    return 0;
}

RC FileHandle::setPageFileName(const string& name){
    fileName = name;
    return 0;
}

