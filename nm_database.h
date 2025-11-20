#ifndef NM_DATABASE_H
#define NM_DATABASE_H

#include <pthread.h>
#include <time.h>
#include "protocol.h"

// Constants
#define HASH_TABLE_SIZE 1009    // A prime number
#define MAX_SS_COUNT 100
#define NM_METADATA_FILE "nm_metadata.dat"

extern pthread_mutex_t db_mutex;

// Data structures

// Represents a single user in an access list
typedef struct UserAccessNode{
    char username[100];
    struct UserAccessNode *next;
} UserAccessNode;

// Represents a single registered user
typedef struct UserNode{
    char username[100];
    struct UserNode *next;
} UserNode;

// Represents a single active Storage Server
typedef struct StorageServerInfo{
    char ip[16];
    int nm_port;
    int client_port;
} StorageServerInfo;

// Represents metadata for a single file
typedef struct FileMetadata{
    char owner[100];
    UserAccessNode *read_permissions_head;      // Read permissions linked list
    UserAccessNode *write_permissions_head;     // Write permissions linked list
    StorageServerInfo *location;                // Pointer to the SS that has this file (NULL if offline)
    size_t size;                                // File size in bytes
    size_t word_count;
    size_t char_count;
    time_t time_created;
    time_t time_last_modified;
    time_t time_last_accessed;
    char user_last_accessed[100];
} FileMetadata;

// A node for the hashmap's linked list
typedef struct FileNode{
    char filename[256];
    FileMetadata metadata;
    struct FileNode *next;
} FileNode;

// Database function declarations

// Initialises the database (hashmaps, SS list, mutexes)
void db_init();

/*
 * Loads metadata from NM_METADATA_FILE
 * This is called once on server startup
*/
void db_load_from_disk();

/*
 * Saves the entire metadata map back to this disk
 * This is called on create, delete, or access change
*/
void db_save_to_disk();

// Same as db_save_to_disk but caller has to lock the database
void db_save_to_disk_locked();

// Finds the metadata for a file
FileMetadata *db_find_file(const char *filename);

// Adds a user to the master user list if they don't exist
void db_add_user(const char *username);

// Parses the registration string, stores the SS info and re-links its files
void db_register_ss(const char *reg_string);

// Finds all the users and makes a newline-terminated string by concatenating them
char* db_get_all_users();

// Checks if a user has a certain permission for a file
int db_check_permission(FileMetadata *metadata, const char *username, char permission);

/*
 * Gets a string of all the files based on user and flags
 * show_details is 1 if -l is present and 0 otherwise
 * show_all is 1 if -a is present and 0 otherwise
*/
char *db_get_file_list(const char *username, int show_all, int show_details);

// Returns the file given a filename
FileNode *db_find_node_internal(const char *filename);

/*
 * This function checks for file existence, selects an SS,
 * and adds the new file metadata to the database, then saves to the disk
*/
int db_create_file(const char *filename, const char *owner, StorageServerInfo *ss_out);

// Deletes a file entry from the database and saves updated metadata to the disk
int db_delete_file(const char* filename);

// Adds read/write permission for a user to a file and returns appropriate success/error code  
int db_add_permission(const char *filename, const char *requestor, const char *target_user, char type);

// Removes all access for a user from a file and returns appropriate success/error code
int db_remove_permission(const char *filename, const char *requestor, const char *target_user);

/**
 * Updates the last accessed time/user and persists the change to disk
 * return 0 on success, 404 if file is not found
*/
int db_update_access_time(const char* filename, const char* username);

// Updates file stats (Called when SS reports a change)
int db_update_file_stats(const char *filename, size_t size, size_t words, size_t chars);

#endif




