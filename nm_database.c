#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "nm_database.h"
#include "utils.h"

// Global static variables and helper functions
static FileNode *hash_table[HASH_TABLE_SIZE];       // Main hash table for files
static StorageServerInfo ss_list[MAX_SS_COUNT];     // List of connected storage servers
static UserNode *user_list_head = NULL;             // Master list of all registered users
pthread_mutex_t db_mutex;                           // Mutex to protect all database operations

// The hash function for the filename string
// djb2 string hash function
static unsigned long db_hash(const char *str){
    unsigned long hash = 5381;  // Large prime
    int c;
    while((c = *str++)){
        hash = ((hash << 5) + hash) + c;
    }
    return hash;
}

// Function to add new file data to the hash table
// Time complexity - O(1) [Adding before the current head of the linked list]
static void db_insert_node(FileNode *new_node){
    if(new_node == NULL) return;
    unsigned long hash_index = db_hash(new_node->filename) % HASH_TABLE_SIZE;
    new_node->next = hash_table[hash_index];
    hash_table[hash_index] = new_node;
}

// Function to find file in the hash table
// Average time complexity - O(1)
// Worst case time complexity - O(k) [k = No. of files]
FileNode *db_find_node_internal(const char *filename){
    unsigned long hash_index = db_hash(filename) % HASH_TABLE_SIZE;
    FileNode *current = hash_table[hash_index];
    while(current != NULL){
        if(strcmp(current->filename, filename) == 0){
            return current;
        }
        current = current->next;
    }
    return NULL;
}

void db_init(){
    memset(hash_table, 0, sizeof(hash_table));  // Clearing the hash table
    memset(ss_list, 0, sizeof(ss_list));        // Clearing the storage server information list

    if(pthread_mutex_init(&db_mutex, NULL) != 0){
        perror("db_mutex init failed");
        log_event(LOG_LEVEL_ERROR, "Failed to initialise database mutex");
        exit(EXIT_FAILURE);
    }

    log_event(LOG_LEVEL_INFO, "Database initialised");
}

// Function to find file metadata
FileMetadata *db_find_file(const char *filename){
    pthread_mutex_lock(&db_mutex);  // Locking before searching for file node 
    FileNode *node = db_find_node_internal(filename);
    pthread_mutex_unlock(&db_mutex);
    return node == NULL ? NULL : &(node->metadata);
}

/*
 * Persistence File Format: 
 * FILE|<filename>|<owner>|<size>|<word_c>|<char_c>|<t_create>|<t_mod>|<t_access>|<user_last_access>
 * PERM|<filename>|R|<username>
 * PERM|<filename>|W|<username>
*/

// Loading files metadata from the disk and filling up the data structures
void db_load_from_disk(){
    log_event(LOG_LEVEL_INFO, "Loading metadata from disk...");
    FILE *file = fopen(NM_METADATA_FILE, "r");
    if(file == NULL){
        log_event(LOG_LEVEL_WARN, "Metadata file not found. Starting fresh.");
        return;
    }

    pthread_mutex_lock(&db_mutex);

    char line[MAX_BUFFER];
    while(fgets(line, sizeof(line), file) != NULL){
        line[strcspn(line, "\n")] = 0;

        char *token_type = strtok(line, "|");
        if(token_type == NULL) continue;

        if(strcmp(token_type, "FILE") == 0){
            // Format: FILE|<filename>|<owner>|<size>|<word_c>|<char_c>|<t_create>|<t_mod>|<t_access>|<user_last_access>

            // Parse the remaining tokens
            char *filename = strtok(NULL, "|");
            char *owner = strtok(NULL, "|");
            char *size_str = strtok(NULL, "|");
            char *word_c_str = strtok(NULL, "|");
            char *char_c_str = strtok(NULL, "|");
            char *t_create_str = strtok(NULL, "|");
            char *t_mod_str = strtok(NULL, "|");
            char *t_access_str = strtok(NULL, "|");
            char *user_last_access = strtok(NULL, "|");

            // Creating the file node for the file and filling the details
            if(filename && owner && size_str && word_c_str && char_c_str && t_create_str && t_mod_str && t_access_str && user_last_access){
                FileNode *new_node = (FileNode *)malloc(sizeof(FileNode));
                strcpy(new_node->filename, filename);
                strcpy(new_node->metadata.owner, owner);

                new_node->metadata.read_permissions_head = NULL;
                new_node->metadata.write_permissions_head = NULL;
                new_node->metadata.location = NULL;

                new_node->metadata.size = atol(size_str);
                new_node->metadata.word_count = atol(word_c_str);
                new_node->metadata.char_count = atol(char_c_str);
                new_node->metadata.time_created = atol(t_create_str);
                new_node->metadata.time_last_modified = atol(t_mod_str);
                new_node->metadata.time_last_accessed = atol(t_access_str);
                strcpy(new_node->metadata.user_last_accessed, user_last_access);

                new_node->next = NULL;

                db_insert_node(new_node);   // Inserting file into the file hash table
            }
        }

        else if(strcmp(token_type, "PERM") == 0){
            // Format: PERM|<filename>|R/W|<username>

            // Parse the remaining tokens
            char *filename = strtok(NULL, "|");
            char *type = strtok(NULL, "|");
            char *username = strtok(NULL, "|");

            if(filename && type && username){
                FileNode *file_node = db_find_node_internal(filename);
                if(file_node){
                    UserAccessNode *user_node = (UserAccessNode *)malloc(sizeof(UserAccessNode));
                    strcpy(user_node->username, username);

                    if(strcmp(type, "R") == 0){
                        user_node->next = file_node->metadata.read_permissions_head;
                        file_node->metadata.read_permissions_head = user_node;
                    }
                    else if(strcmp(type, "W") == 0){
                        user_node->next = file_node->metadata.write_permissions_head;
                        file_node->metadata.write_permissions_head = user_node;
                        

                    }
                }
            }
        }
    }

    fclose(file);
    pthread_mutex_unlock(&db_mutex);
    log_event(LOG_LEVEL_INFO, "Metadata load complete.");
}

// Function to save all the file metadata to the disk (for persistence purposes)
void db_save_to_disk(){
    log_event(LOG_LEVEL_DEBUG, "Saving metadata to disk...");

    pthread_mutex_lock(&db_mutex);
    
    FILE *file = fopen(NM_METADATA_FILE, "w");
    if(file == NULL){
        log_event(LOG_LEVEL_ERROR, "Could not open metadata file for writing.");
        pthread_mutex_unlock(&db_mutex);
        return;
    }

    // Traversing through the hash table to visit every file node
    for(int i = 0; i < HASH_TABLE_SIZE; i++){
        FileNode *current_file = hash_table[i];

        while(current_file != NULL){
            // 1. Write the main file entry with new field
            fprintf(file, "FILE|%s|%lu|%lu|%lu|%ld|%ld|%ld|%s\n",
                current_file->filename,
                current_file->metadata.size,
                current_file->metadata.word_count,
                current_file->metadata.char_count,
                current_file->metadata.time_created,
                current_file->metadata.time_last_modified,
                current_file->metadata.time_last_accessed,
                current_file->metadata.user_last_accessed
            );

            // 2. Write all READ permissions
            UserAccessNode *current_user = current_file->metadata.read_permissions_head;
            while(current_user){
                fprintf(file, "PERM|%s|R|%s\n", current_file->filename, current_user->username);
                current_user = current_user->next;
            }

            // 3. Write all WRITE permissions
            current_user = current_file->metadata.write_permissions_head;
            while(current_user){
                fprintf(file, "PERM|%s|W|%s\n", current_file->filename, current_user->username);
                current_user = current_user->next;
            }

            current_file = current_file->next;
        }
    }

    fclose(file);
    pthread_mutex_unlock(&db_mutex);
    log_event(LOG_LEVEL_DEBUG, "Metadata save complete.");
}

/*
 * Non-locking save function (called in db_get_and_update_info())
 * The caller locks and unlocks accordingly
 * Same as db_save_to_disk other than the locking part
*/

/*static void db_save_to_disk_locked(){
    FILE *file = fopen(NM_METADATA_FILE, "w");
    if(file == NULL){
        log_event(LOG_LEVEL_ERROR, "Could not open metadata file for writing.");
        return;
    }

    // Traversing through the hash table to visit every file node
    for(int i = 0; i < HASH_TABLE_SIZE; i++){
        FileNode *current_file = hash_table[i];

        while(current_file != NULL){
            // 1. Write the main file entry with new field
            fprintf(file, "FILE|%s|%lu|%lu|%lu|%ld|%ld|%ld|%s\n",
                current_file->filename,
                current_file->metadata.size,
                current_file->metadata.word_count,
                current_file->metadata.char_count,
                current_file->metadata.time_created,
                current_file->metadata.time_last_modified,
                current_file->metadata.time_last_accessed,
                current_file->metadata.user_last_accessed
            );

            // 2. Write all READ permissions
            UserAccessNode *current_user = current_file->metadata.read_permissions_head;
            while(current_user){
                fprintf(file, "PERM|%s|R|%s\n", current_file->filename, current_user->username);
                current_user = current_user->next;
            }

            // 3. Write all WRITE permissions
            current_user = current_file->metadata.write_permissions_head;
            while(current_user){
                fprintf(file, "PERM|%s|W|%s\n", current_file->filename, current_user->username);
                current_user = current_user->next;
            }

            current_file = current_file->next;
        }
    }

    fclose(file);
    log_event(LOG_LEVEL_DEBUG, "Metadata save complete.");
}*/

// Function to add a new user to the master list of users
void db_add_user(const char *username){
    char log_msg[200];
    pthread_mutex_lock(&db_mutex);

    // 1. Check if the user already exits (don't want duplicate users)
    UserNode *current = user_list_head;
    while(current){
        if(strcmp(current->username, username) == 0){
            // User is already there in the list
            pthread_mutex_unlock(&db_mutex);
            return;
        }
        current = current->next;
    }

    // 2. User not found, add them to the head of the linked list 
    UserNode *new_user = (UserNode *)malloc(sizeof(UserNode));
    if(new_user == NULL){
        log_event(LOG_LEVEL_ERROR, "Malloc failed in db_add_user.");
        pthread_mutex_unlock(&db_mutex);
        return;
    }

    strncpy(new_user->username, username, sizeof(new_user->username) - 1);
    new_user->username[sizeof(new_user->username) - 1] = '\0';

    new_user->next = user_list_head;
    user_list_head = new_user;

    sprintf(log_msg, "Added new user to master list: %s", username);
    log_event(LOG_LEVEL_DEBUG, log_msg);

    pthread_mutex_unlock(&db_mutex);
}

// Function to register an SS and re-link its files
void db_register_ss(const char *reg_string){
    char buffer[MAX_BUFFER];
    char log_msg[MAX_BUFFER + 200];

    // Parsing the registration message
    strncpy(buffer, reg_string, MAX_BUFFER - 1);
    buffer[MAX_BUFFER - 1] = '\0';

    char *command = strtok(buffer, " \n");   // "REG_SS"
    char *ip = strtok(NULL, " \n");
    char *nm_port_str = strtok(NULL, " \n");
    char *client_port_str = strtok(NULL, " \n");

    if(!command || !ip || !nm_port_str || !client_port_str){
        log_event(LOG_LEVEL_ERROR, "Malformed REG_SS string received.");
        return;
    }

    // Actually adding it to the ss_list
    pthread_mutex_lock(&db_mutex);

    // 1. Find an empty slot in the ss_list
    StorageServerInfo *ss_slot = NULL;
    int i;
    for(i = 0; i < MAX_SS_COUNT; i++){
        if(ss_list[i].ip[0] == '\0'){
            // Slot is empty, can add new server details here
            ss_slot = &ss_list[i];
            break;
        }
    }

    // No free slot
    if(ss_slot == NULL){
        log_event(LOG_LEVEL_ERROR, "Failed to register new SS: Server list is full.");
        pthread_mutex_unlock(&db_mutex);
        return;
    }

    // 2. Fill in the StorageServerInfo struct
    strncpy(ss_slot->ip, ip, sizeof(ss_slot->ip) - 1);
    ss_slot->nm_port = atoi(nm_port_str);
    ss_slot->client_port = atoi(client_port_str);

    sprintf(log_msg, "Registered new SS (Slot %d): IP = %s, NM_Port = %d, Client_Port = %d",
        i, ss_slot->ip, ss_slot->nm_port, ss_slot->client_port
    );
    log_event(LOG_LEVEL_INFO, log_msg);

    // 3. Iterate through the file list and re-link
    char *filename = strtok(NULL, " \n");
    while(filename){
        // Find the file in our metadata hashmap
        FileNode *node = db_find_node_internal(filename);

        if(node){
            // Found the file. Re-link to new slot location.
            node->metadata.location = ss_slot;
            sprintf(log_msg, "Re-linked existing file: %s", filename);
            log_event(LOG_LEVEL_DEBUG, log_msg);
        }
        else{
            // This file is in the SS but not in out metadata.
            // We assumed such an "orphaned" file doesn't exist
            // so the code should never reach here but we'll just log it
            sprintf(log_msg, "Orphaned file found on SS: %s (Ignored)", filename);
            log_event(LOG_LEVEL_WARN, log_msg);
        }

        filename = strtok(NULL, " \n"); // To continue re-linking
    }

    pthread_mutex_unlock(&db_mutex);
}


char *db_get_all_users(){
    pthread_mutex_lock(&db_mutex);

    // Allocating space for the string
    int buffer_size = MAX_BUFFER;   // Start with a reasonable size
    char *user_string = (char *)malloc(sizeof(char) * buffer_size);
    if(user_string == NULL){
        log_event(LOG_LEVEL_ERROR, "malloc failed in db_get_all_users");
        pthread_mutex_unlock(&db_mutex);
        return NULL;
    }

    user_string[0] = '\0';  // Starting with an empty string

    UserNode *current = user_list_head;
    while(current){
        // First check if the buffer has enough space (with newline and null character)
        if(strlen(user_string) + strlen(current->username) + 2 > buffer_size){
            // Not enough size, so we realloc to double the buffer size
            buffer_size *= 2;
            char *new_buffer = (char *)realloc(user_string, buffer_size);
            if(new_buffer == NULL){
                log_event(LOG_LEVEL_ERROR, "realloc failed in db_get_all_users");
                free(user_string);
                pthread_mutex_unlock(&db_mutex);
                return NULL;
            }
            user_string = new_buffer;
        }

        // Appending the username and a newline to the string
        strcat(user_string, current->username);
        strcat(user_string, "\n");

        current = current->next;
    }

    pthread_mutex_unlock(&db_mutex);
    return user_string;
}

static int is_user_in_list(UserAccessNode *head, const char *username){
    UserAccessNode *current = head;
    while(current){
        if(strcmp(current->username, username) == 0){
            return 1;
        }
        current = current->next;
    }
    return 0;
}

int db_check_permission(FileMetadata *metadata, const char *username, char permission){
    // 1. The owner always has both read and write permissions
    if(strcmp(metadata->owner, username) == 0){
        return 1;
    }

    // 2. Checking for read permission
    if(permission == 'R'){
        if(is_user_in_list(metadata->read_permissions_head, username)){
            return 1;
        }
        if(is_user_in_list(metadata->write_permissions_head, username)){
            return 1;
        }
    }

    // 3. Checking for write permission
    if(permission == 'W'){
        if(is_user_in_list(metadata->write_permissions_head, username)){
            return 1;
        }
    }

// --- The "transactional" function is REMOVED ---
    return 0;   // No permission found
}

// Gets a formatted string of files based on VIEW command flags
char *db_get_file_list(const char *username, int show_all, int show_details){
    // Start with a large buffer
    size_t buffer_size = MAX_BUFFER * 2;
    char *file_list_str = (char *)malloc(sizeof(char) * buffer_size);
    if(file_list_str == NULL){
        log_event(LOG_LEVEL_ERROR, "malloc failed in db_get_file_list");
        return NULL;
    }

    file_list_str[0] = '\0';

    char line_buffer[1024];
    char time_str[30];      // Buffer for the formatted time string

    // Lock the database
    pthread_mutex_lock(&db_mutex);

    if(show_details){
        snprintf(line_buffer, sizeof(line_buffer), "%-20s | %-8s | %-8s | %-26s | %s\n",
                "Filename", "Words", "Chars", "Last Access Time", "Owner");        
        strcat(file_list_str, line_buffer);

        snprintf(line_buffer, sizeof(line_buffer),
                "---------------------|----------|----------|----------------------------|------\n");
        strcat(file_list_str, line_buffer);
    }

    // Iterate over every bucket (index) in the hash table
    for(int i = 0; i < HASH_TABLE_SIZE; i++){
        FileNode *current_file = hash_table[i];

        // Iterate over every file in the bucket's linked list
        while(current_file){
            int has_read = db_check_permission(&(current_file->metadata), username, 'R');
        
            // If the user has read access or the -a flag is present
            if(has_read || show_all){
                if(strlen(file_list_str) + 1024 > buffer_size){
                    buffer_size *= 2;
                    char *new_buffer = (char *)realloc(file_list_str, buffer_size);
                    if(new_buffer == NULL){
                        log_event(LOG_LEVEL_ERROR, "realloc failed in db_get_file_list");
                        free(file_list_str);
                        pthread_mutex_unlock(&db_mutex);
                        return NULL;
                    }
                    file_list_str = new_buffer;
                }

                // Format the output string
                if(show_details){
                    // Formatting time string
                    ctime_r(&(current_file->metadata.time_last_accessed), time_str);
                    time_str[strcspn(time_str, "\n")] = '\0';   // Remove the newline from the time string

                    // -l flag is present so we have to print all the details
                    snprintf(line_buffer, sizeof(line_buffer),
                            "%-20s | %-8lu | %-8lu | %-26s | %s\n",
                            current_file->filename,
                            current_file->metadata.word_count,
                            current_file->metadata.char_count,
                            time_str,
                            current_file->metadata.owner
                    );
                }
                else{
                    // No flags or just -a flag is present
                    snprintf(line_buffer, sizeof(line_buffer), "%s\n", current_file->filename);
                }

                // Concatenating with current file_list_str
                strcat(file_list_str, line_buffer);
            }

            current_file = current_file->next;
        }
    }

    pthread_mutex_unlock(&db_mutex);
    return file_list_str;
}

// Atomic function to find file, check permissions, and update access time
/*int db_get_and_update_info(const char *filename, const char *username, FileMetadata *meta_out){
    int error_code = 0;

    // 1. Lock the database 
    pthread_mutex_lock(&db_mutex);

    // 2. Find the file
    FileNode *node = db_find_node_internal(filename);
    if(node == NULL){
        error_code = 404;   // File not found
        pthread_mutex_unlock(&db_mutex);
        return error_code;
    }

    // 3. Check permission
    if(!db_check_permission(&(node->metadata), username, 'R')){
        error_code = 401;   // Unauthorised
        pthread_mutex_unlock(&db_mutex);
        return error_code;
    }

    // 4. Update the metadata
    node->metadata.time_last_accessed = time(NULL);
    strncpy(node->metadata.user_last_accessed, username, sizeof(node->metadata.user_last_accessed) - 1);

    // 5. Save the changes to the disk to ensure persistence
    db_save_to_disk_locked();  // This function assumes the caller locks

    // 6. Copy the data to the output struct for the user
    memcpy(meta_out, &(node->metadata), sizeof(FileMetadata));

    // 7. Unlock the database before returning
    pthread_mutex_unlock(&db_mutex);

    return 0;
}*/