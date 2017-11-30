#ifndef _pfm_h_
#define _pfm_h_

typedef unsigned PageNum;
typedef int RC;
typedef char byte;

#define PAGE_SIZE 4096
#include <string>
#include <climits>
using namespace std;

class FileHandle;

class PagedFileManager
{
public:
    static PagedFileManager* instance();                                  // Access to the _pf_manager instance
    
    RC createFile    (const string &fileName);                            // Create a new file
    RC destroyFile   (const string &fileName);                            // Destroy a file
    RC openFile      (const string &fileName, FileHandle &fileHandle);    // Open a file
    RC closeFile     (FileHandle &fileHandle);                            // Close a file
    
protected:
    PagedFileManager();                                                   // Constructor
    ~PagedFileManager();                                                  // Destructor
    
private:
    static PagedFileManager *_pf_manager;
};


class FileHandle
{
public:
    // variables to keep the counter for each operation
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;
    
    FileHandle();                                                         // Default constructor
    ~FileHandle();                                                        // Destructor
    
    RC readPage(PageNum pageNum, void *data);                             // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                      // Write a specific page
    RC appendPage(const void *data);                                      // Append a specific page
    unsigned getNumberOfPages();                                          // Get the number of pages in the file
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);  // Put the current counter values into variables
    
    FILE* getPageFilePtr();                                               //Get the File pointer the file handle
    string getPageFileName();                                             //Get the FileName of the file Handle
    RC setPageFilePtr(FILE* ptr);                                         //Set file pointer to the handle
    RC setPageFileName(const string &name);                               //Set file Name to the Handle
    RC writeRootPageNumber(int pageNum);							  //hidden page root page number store write
    RC readRootPageNumber();											  //hidden page root number read
    RC readTableId();											  		//hidden page table-ID read
    RC writeTableId(int pageNum);							  //hidden page tablle-Id store write
private:
    string fileName;
    FILE* file_Ptr;
};

#endif

