#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#include "user_func.h"
#include "nm_database.h"
#include "utils.h"
#include "protocol.h"

#define LOG_BUFFER_SIZE 512

// A helper function to send a standard error message
void send_error_message(int client_socket, const char *message){
    char buffer[MAX_BUFFER];
    snprintf(buffer, sizeof(buffer), "ERROR: %s\n", message);
    write(client_socket, buffer, strlen(buffer));
}

// Handles the "LIST" command
void handle_list_command(int client_socket, const char *username, const char *args){
    char log_msg[LOG_BUFFER_SIZE];
    sprintf(log_msg, "User '%s' requested LIST", username);
    log_event(LOG_LEVEL_INFO, log_msg);

    // Get the user list from the database
    char *user_list = db_get_all_users();

    if(user_list){
        if(strlen(user_list) == 0){
            write(client_socket, "No users registered.\n", 21);
        }
        else{
            // Send the list back to the client
            write(client_socket, user_list, strlen(user_list));
        }
        free(user_list);
    }
    else{
        send_error_message(client_socket, "Could not retrieve user list from database.\n");
    }
}

// Handles the "INFO" command
void handle_info_command(int client_socket, const char *username, const char *args){
    char log_msg[LOG_BUFFER_SIZE];
    char filename[256];

    // Getting the filename from the args
    if(sscanf(args, "%s", filename) != 1){
        send_error_message(client_socket, "Filename not provided. Usage: INFO <filename>");
        return;
    }

    sprintf(log_msg, "User '%s' requested INFO for '%s'", username, filename);
    log_event(LOG_LEVEL_INFO, log_msg);

    // Lock the database
    pthread_mutex_lock(&db_mutex);

    // Code for db_find_file because db_find_file also locks the database
    FileNode *node = db_find_node_internal(filename);
    if(node == NULL){
        pthread_mutex_unlock(&db_mutex);
        write(client_socket, MSG_FILE_NOT_FOUND, strlen(MSG_FILE_NOT_FOUND));
        return;
    }

    FileMetadata *metadata = &(node->metadata);
    // End of db_find_file code

    if(metadata == NULL){
        pthread_mutex_unlock(&db_mutex);
        send_error_message(client_socket, "File not found.");
        return;
    }

    if(!db_check_permission(metadata, username, 'R')){
        pthread_mutex_unlock(&db_mutex);
        write(client_socket, MSG_UNAUTHORIZED, strlen(MSG_UNAUTHORIZED));
        return;
    }

    // Format the response string
    char response[MAX_BUFFER * 5];
    char perm_buffer[MAX_BUFFER];   // The permission string: username1 (RW) username2 (R)
    char time_str[30];

    // Building the permission string
    perm_buffer[0] = '\0';
    // 1. Add the owner
    snprintf(perm_buffer, sizeof(perm_buffer), "%s (RW)", metadata->owner);

    // 2. Add all the users with write permission (other than the owner)
    UserAccessNode *current_user = metadata->write_permissions_head;
    while(current_user){
        if(strcmp(current_user->username, metadata->owner) != 0){
            strcat(perm_buffer, ", ");
            strcat(perm_buffer, current_user->username);
            strcat(perm_buffer, " (RW)");
        }
        current_user = current_user->next;
    }

    // 3. Add all read-only users (they must not be on the write list)
    current_user = metadata->read_permissions_head;
    while(current_user){
        if(strcmp(current_user->username, metadata->owner) != 0 && !db_check_permission(metadata, current_user->username, 'W')){
            strcat(perm_buffer, ", ");
            strcat(perm_buffer, current_user->username);
            strcat(perm_buffer, " (R)");
        }
        current_user = current_user->next;
    }

    // Format the times
    ctime_r(&(metadata->time_created), time_str);
    time_str[strcspn(time_str, "\n")] = 0;
    char create_time[30];
    strcpy(create_time, time_str);

    ctime_r(&(metadata->time_last_modified), time_str);
    time_str[strcspn(time_str, "\n")] = 0;
    char mod_time[30];
    strcpy(mod_time, time_str);

    ctime_r(&(metadata->time_last_accessed), time_str);
    time_str[strcspn(time_str, "\n")] = 0;
    char access_time[30];
    strcpy(access_time, time_str);

    snprintf(response, sizeof(response),
            "File: %s\n"
            "Owner: %s\n"
            "Size (Bytes): %lu\n"
            "Created: %s\n"
            "Last Modified: %s\n"
            "Last Accessed: %s by %s\n"
            "Access: %s\n",
            filename,
            metadata->owner,
            metadata->size,
            create_time,
            mod_time,
            access_time, metadata->user_last_accessed,
            perm_buffer
    );

    pthread_mutex_unlock(&db_mutex);

    write(client_socket, response, strlen(response));
}

// Handles "VIEW" command with flags "l" and "a"
void handle_view_command(int client_socket, const char *username, const char *args){
    char log_msg[LOG_BUFFER_SIZE];
    sprintf(log_msg, "User '%s' requested VIEW with args '%s'", username, args);
    log_event(LOG_LEVEL_INFO, log_msg);

    // Parse flags
    int show_all = 0;
    int show_details = 0;

    if(args[0] == '-'){
        for(int i = 1; args[i] != '\0'; i++){
            if(args[i] == 'a'){
                show_all = 1;
            }
            else if(args[i] == 'l'){
                show_details = 1;
            }
            else{
                write(client_socket, MSG_MALFORMED, strlen(MSG_MALFORMED));
                return;
            }
        }
    }

    // Call db_get_file_list() to get formatted response string
    char *file_list = db_get_file_list(username, show_all, show_details);

    if(file_list){
        if(strlen(file_list) == 0){
            write(client_socket, "No files found.\n", 16);
        }
        else{
            write(client_socket, file_list, strlen(file_list));
        }
        free(file_list);
    }
    else{
        send_error_message(client_socket, "Could not retrieve file list.");
    }
}

// Helper function to connect to a storage server: send a command and get a reply
int send_command_to_ss(StorageServerInfo *ss, const char *command){
    int ss_sock = 0;
    struct sockaddr_in ss_addr;
    char buffer[MAX_BUFFER] = {0};

    // 1. Create socket
    ss_sock = socket(AF_INET, SOCK_STREAM, 0);
    if(ss_sock < 0){
        log_event(LOG_LEVEL_ERROR, "Failed to create socket for SS.");
        return 500; // ERROR_CANNOT_CREATE_FILE 
    }

    // 2. Configure SS address
    memset(&ss_addr, 0, sizeof(ss_addr));   // Clear the address struct
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_port = htons(ss->nm_port);  // Connect to the SS's NM port
    if(inet_pton(AF_INET, ss->ip, &ss_addr.sin_addr) <= 0){
        log_event(LOG_LEVEL_ERROR, "Invalid SS IP address.");
        close(ss_sock);
        return 500;
    }

    // 3. Connect to the SS
    if(connect(ss_sock, (struct sockaddr *)&ss_addr, sizeof(ss_addr)) < 0){
        log_event(LOG_LEVEL_ERROR, "Failed to connect to SS.");
        close(ss_sock);
        return 500;
    }

    // 4. Send the command
    if(write(ss_sock, command, strlen(command)) < 0){
        log_event(LOG_LEVEL_ERROR, "Failed to write command to SS.");
        close(ss_sock);
        return 500;
    }

    // 5. Get reply
    int bytes_read = read(ss_sock, buffer, MAX_BUFFER - 1);
    close(ss_sock);

    if(bytes_read <= 0){
        log_event(LOG_LEVEL_ERROR, "Failed to get reply from SS.");
        return 500;
    }

    buffer[bytes_read] = '\0';

    // 6. Check reply
    if(strncmp(buffer, "200", 3) == 0){
        return 200; // SUCCESS
    }

    return 500; // SS FAILED
}

// Handles the "CREATE <filename>" command
void handle_create_command(int client_socket, const char * username, const char *args){
    char filename[256];
    char log_msg[LOG_BUFFER_SIZE];

    // 1. Parse filename
    if(sscanf(args, "%s", filename) != 1){
        write(client_socket, MSG_MALFORMED, strlen(MSG_MALFORMED));
        return;
    }

    sprintf(log_msg, "User '%s' requesting CREATE for '%s'", username, filename);
    log_event(LOG_LEVEL_INFO, log_msg);

    // 2. Call the database function to create the metadata
    StorageServerInfo target_ss; // This will be filled by the db function
    int db_result = db_create_file(filename, username, &target_ss);

    // 3. Handle database errors
    if(db_result == 409){
        write(client_socket, MSG_FILE_ALREADY_EXISTS, strlen(MSG_FILE_ALREADY_EXISTS));
        return;
    }
    if(db_result == 503){
        write(client_socket, MSG_NO_STORAGE_SERVERS, strlen(MSG_NO_STORAGE_SERVERS));
        return;
    }
    if(db_result != 0){
        send_error_message(client_socket, "Internal server error.");
        return;
    }

    // 4. Metadata created; now command the SS to create the file.
    char ss_command[MAX_BUFFER];
    snprintf(ss_command, sizeof(ss_command), "%s %s\n", CMD_SS_CREATE, filename);

    int ss_result = send_command_to_ss(&target_ss, ss_command);

    // 5. Relay SS response to client
    if(ss_result == 200){
        write(client_socket, MSG_SUCCESS, strlen(MSG_SUCCESS));
    }
    else{
        // Rollback
        char err_msg[MAX_BUFFER];
        sprintf(err_msg, "SS failed to create file. Rolling back metadata for: %s", filename);
        log_event(LOG_LEVEL_WARN, err_msg);

        // Delete the metadata we just created so the DB stays consistent
        db_delete_file(filename);

        // TODO: Rollback the metadata creation (i.e., delete the file metadata)
        send_error_message(client_socket, "Storage server failed to create file.");
    }
}

// Handles the "DELETE <filename>" command
void handle_delete_command(int client_socket, const char *username, const char *args){
    char filename[256];
    char log_msg[LOG_BUFFER_SIZE];

    if(sscanf(args, "%s", filename) != 1){
        write(client_socket, MSG_MALFORMED, strlen(MSG_MALFORMED));
        return;
    }

    sprintf(log_msg, "User '%s' requesting DELETE for '%s'", username, filename);
    log_event(LOG_LEVEL_INFO, log_msg);

    // 1. Check existence and ownership
    // We need to lock to safely check metadata
    pthread_mutex_lock(&db_mutex);
    FileNode* node = db_find_node_internal(filename);

    if(node == NULL){
        pthread_mutex_unlock(&db_mutex);
        write(client_socket, MSG_FILE_NOT_FOUND, strlen(MSG_FILE_NOT_FOUND));
        return;
    }

    // Check if user is OWNER (only owner can delete)
    if(strcmp(node->metadata.owner, username) != 0){
        pthread_mutex_unlock(&db_mutex);
        write(client_socket, MSG_UNAUTHORIZED, strlen(MSG_UNAUTHORIZED));
        return;
    }

    // Check if file is online (has an SS)
    if(node->metadata.location == NULL){
        pthread_mutex_unlock(&db_mutex);
        send_error_message(client_socket, "File is offline (SS down) and cannot be deleted.");
        return;
    }

    // Copy SS info so we can talk to it after unlocking
    StorageServerInfo ss_copy = *(node->metadata.location);
    pthread_mutex_unlock(&db_mutex);

    // 2. Send DELETE command to SS
    char ss_command[MAX_BUFFER];
    snprintf(ss_command, sizeof(ss_command), "%s %s\n", CMD_SS_DELETE, filename);

    int ss_result = send_command_to_ss(&ss_copy, ss_command);

    if(ss_result == 200){
        // 3. SS deletion successful, now remove from DB
        int db_result = db_delete_file(filename);
        if(db_result == 0){
            write(client_socket, MSG_SUCCESS, strlen(MSG_SUCCESS));
        } 
        else{
            // IDK, file deleted from SS but vanished from DB in between?
            // Still considered a success for the user since the file is gone
            write(client_socket, MSG_SUCCESS, strlen(MSG_SUCCESS));
        }
    }
    else{
        send_error_message(client_socket, "Storage server failed to delete file.");
    }
}

// Handles "ADDACCESS -[R|W] <filename> <username>" command
void handle_addaccess_command(int client_socket, const char *username, const char *args){
    char flag[5];
    char filename[256];
    char target_user[100];
    char log_msg[LOG_BUFFER_SIZE];

    // Parse args
    if(sscanf(args, "%s %s %s", flag, filename, target_user) != 3){
        write(client_socket, MSG_MALFORMED, strlen(MSG_MALFORMED));
        return;
    }

    char type = ' ';
    if(strcmp(flag, "-R") == 0){
        type = 'R';
    }
    else if(strcmp(flag, "-W") == 0){
        type = 'W';
    }
    else{
        write(client_socket, MSG_MALFORMED, strlen(MSG_MALFORMED));
        return;
    }

    sprintf(log_msg, "User '%s' adding %c access for '%s' to file '%s'", 
            username, type, target_user, filename);
    log_event(LOG_LEVEL_INFO, log_msg);

    int result = db_add_permission(filename, username, target_user, type);

    if(result == 200){
        write(client_socket, MSG_SUCCESS, strlen(MSG_SUCCESS));
    } 
    else if(result == 404){
        write(client_socket, MSG_FILE_NOT_FOUND, strlen(MSG_FILE_NOT_FOUND));
    } 
    else if(result == 401){
        write(client_socket, MSG_UNAUTHORIZED, strlen(MSG_UNAUTHORIZED));
    } 
    else if(result == 440){
        write(client_socket, MSG_USER_NOT_FOUND, strlen(MSG_USER_NOT_FOUND));
    } 
    else{
        send_error_message(client_socket, "Internal Server Error");
    }
}

// Handles "REMACCESS <filename> <username>"
void handle_remaccess_command(int client_socket, const char *username, const char *args){
    char filename[256];
    char target_user[100];
    char log_msg[LOG_BUFFER_SIZE];

    // Parse args
    if(sscanf(args, "%s %s", filename, target_user) != 2){
        write(client_socket, MSG_MALFORMED, strlen(MSG_MALFORMED));
        return;
    }

    sprintf(log_msg, "User '%s' removing access for '%s' from file '%s'", 
            username, target_user, filename);
    log_event(LOG_LEVEL_INFO, log_msg);

    int result = db_remove_permission(filename, username, target_user);

    if(result == 200){
        write(client_socket, MSG_SUCCESS, strlen(MSG_SUCCESS));
    } 
    else if(result == 404){
        write(client_socket, MSG_FILE_NOT_FOUND, strlen(MSG_FILE_NOT_FOUND));
    } 
    else if(result == 401){
        write(client_socket, MSG_UNAUTHORIZED, strlen(MSG_UNAUTHORIZED));
    } 
    else if(result == 441){
        write(client_socket, MSG_CANNOT_REMOVE_ACCESS_FROM_OWNER, strlen(MSG_CANNOT_REMOVE_ACCESS_FROM_OWNER));
    }
    else{
        send_error_message(client_socket, "Internal Server Error");
    }
}

// Handles the "READ <filename>" command
void handle_read_command(int client_socket, const char *username, const char *args){
    char log_msg[LOG_BUFFER_SIZE];
    char filename[256];

    // Parse filename
    if(sscanf(args, "%s", filename) != 1){
        write(client_socket, MSG_MALFORMED, strlen(MSG_MALFORMED));
        return;
    }

    sprintf(log_msg, "User '%s' requested READ for '%s'", username, filename);
    log_event(LOG_LEVEL_INFO, log_msg);

    // Lock the database
    pthread_mutex_lock(&db_mutex);

    // 1. Find the file
    FileNode *node = db_find_node_internal(filename); // Checks cache and then the hash map

    if(node == NULL){
        pthread_mutex_unlock(&db_mutex);
        write(client_socket, MSG_FILE_NOT_FOUND, strlen(MSG_FILE_NOT_FOUND));
        return;
    }

    // 2. Check permissions (read access)
    if(!db_check_permission(&(node->metadata), username, 'R')){
        pthread_mutex_unlock(&db_mutex);
        write(client_socket, MSG_UNAUTHORIZED, strlen(MSG_UNAUTHORIZED));
        return;
    }

    // 3. Get the Storage Server info
    StorageServerInfo *ss = node->metadata.location;
    if(ss == NULL){
        pthread_mutex_unlock(&db_mutex);
        send_error_message(client_socket, "File is offline (SS down).");
        return;
    }

    // 4. Update and save last access time
    db_update_access_time(filename, username);

    // 5. Send redirect message: "200 <IP> <Client_Port>"
    char response[MAX_BUFFER];
    snprintf(response, sizeof(response), "200 %s %d\n", ss->ip, ss->client_port);

    pthread_mutex_unlock(&db_mutex);

    // Send the IP/Port to the client
    write(client_socket, response, strlen(response));
}

// Handles the "STREAM <filename>" command
void handle_stream_command(int client_socket, const char *username, const char *args){
    char log_msg[LOG_BUFFER_SIZE];
    char filename[256];

    // Parse filename
    if(sscanf(args, "%s", filename) != 1){
        write(client_socket, MSG_MALFORMED, strlen(MSG_MALFORMED));
        return;
    }

    sprintf(log_msg, "User '%s' requested STREAM for '%s'", username, filename);
    log_event(LOG_LEVEL_INFO, log_msg);

    // Lock the database
    pthread_mutex_lock(&db_mutex);

    // 1. Find the file
    FileNode *node = db_find_node_internal(filename); // Checks cache and then the hash map

    if(node == NULL){
        pthread_mutex_unlock(&db_mutex);
        write(client_socket, MSG_FILE_NOT_FOUND, strlen(MSG_FILE_NOT_FOUND));
        return;
    }

    // 2. Check permissions (read access)
    if(!db_check_permission(&(node->metadata), username, 'R')){
        pthread_mutex_unlock(&db_mutex);
        write(client_socket, MSG_UNAUTHORIZED, strlen(MSG_UNAUTHORIZED));
        return;
    }

    // 3. Get the Storage Server info
    StorageServerInfo *ss = node->metadata.location;
    if(ss == NULL){
        pthread_mutex_unlock(&db_mutex);
        send_error_message(client_socket, "File is offline (SS down).");
        return;
    }

    // 4. Update and save last access time
    db_update_access_time(filename, username);

    // 5. Send redirect message: "200 <IP> <Client_Port>"
    char response[MAX_BUFFER];
    snprintf(response, sizeof(response), "200 %s %d\n", ss->ip, ss->client_port);

    pthread_mutex_unlock(&db_mutex);

    // Send the IP/Port to the client
    write(client_socket, response, strlen(response));
}

/*
 * Handles the "WRITE <filename> <sentence_index>" command
 * Validates user and redirects to SS
*/
void handle_write_command(int client_socket, const char *username, const char *args){
    char filename[256];
    int sentence_idx;

    // Parse "filename sentence_idx"
    if(sscanf(args, "%s %d", filename, &sentence_idx) != 2){
        write(client_socket, MSG_MALFORMED, strlen(MSG_MALFORMED));
        return;
    }

    char log_msg[LOG_BUFFER_SIZE];
    snprintf(log_msg, sizeof(log_msg), "User '%s' requested WRITE on '%s' at index %d", username, filename, sentence_idx);
    log_event(LOG_LEVEL_INFO, log_msg);

    pthread_mutex_lock(&db_mutex);
    FileNode *node = db_find_node_internal(filename);

    if(node == NULL){
        pthread_mutex_unlock(&db_mutex);
        write(client_socket, MSG_FILE_NOT_FOUND, strlen(MSG_FILE_NOT_FOUND));
        return;
    }

    // Check Write Permission
    if(!db_check_permission(&(node->metadata), username, 'W')){
        pthread_mutex_unlock(&db_mutex);
        write(client_socket, MSG_UNAUTHORIZED, strlen(MSG_UNAUTHORIZED));
        return;
    }

    StorageServerInfo *ss = node->metadata.location;
    if(ss == NULL){
        pthread_mutex_unlock(&db_mutex);
        send_error_message(client_socket, "File is offline.");
        return;
    }

    // Update last access time and user ---
    // Even though the write happens on SS, the request counts as an access
    db_update_access_time(filename, username);

    // Send Redirect: "200 <IP> <Client_Port>"
    char response[MAX_BUFFER];
    snprintf(response, sizeof(response), "200 %s %d\n", ss->ip, ss->client_port);

    pthread_mutex_unlock(&db_mutex);
    write(client_socket, response, strlen(response));
}

// Handles the "UNDO <filename>" command
void handle_undo_command(int client_socket, const char *username, const char *args){
    char filename[256];
    char log_msg[LOG_BUFFER_SIZE];

    // Parsing the filename
    char clean_args[MAX_BUFFER];
    strncpy(clean_args, args, sizeof(clean_args) - 1);
    clean_args[sizeof(clean_args) - 1] = '\0';
    clean_args[strcspn(clean_args, "\n")] = 0; // Strip newline

    if(sscanf(clean_args, "%s", filename) != 1){
        write(client_socket, MSG_MALFORMED, strlen(MSG_MALFORMED));
        return;
    }

    sprintf(log_msg, "User '%s' requested UNDO for '%s'", username, filename);
    log_event(LOG_LEVEL_INFO, log_msg);

    pthread_mutex_lock(&db_mutex);
    FileNode *node = db_find_node_internal(filename);

    if(node == NULL){
        pthread_mutex_unlock(&db_mutex);
        write(client_socket, MSG_FILE_NOT_FOUND, strlen(MSG_FILE_NOT_FOUND));
        return;
    }

    // Check permissions (need write access to undo)
    if(!db_check_permission(&(node->metadata), username, 'W')){
        pthread_mutex_unlock(&db_mutex);
        write(client_socket, MSG_UNAUTHORIZED, strlen(MSG_UNAUTHORIZED));
        return;
    }

    StorageServerInfo *ss = node->metadata.location;
    if(ss == NULL){
        pthread_mutex_unlock(&db_mutex);
        send_error_message(client_socket, "File is offline.");
        return;
    }

    // Update access time
    db_update_access_time(filename, username);

    // Send Redirect
    char response[MAX_BUFFER];
    snprintf(response, sizeof(response), "200 %s %d\n", ss->ip, ss->client_port);

    pthread_mutex_unlock(&db_mutex);
    write(client_socket, response, strlen(response));
}

// Helper to download file from SS to a local temp file
int nm_download_file_to_temp(StorageServerInfo *ss, const char *filename, const char *temp_path){
    int ss_sock = socket(AF_INET, SOCK_STREAM, 0);
    if(ss_sock < 0) return -1;

    struct sockaddr_in ss_addr;
    memset(&ss_addr, 0, sizeof(ss_addr));
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_port = htons(ss->client_port); // Use client port for data
    if(inet_pton(AF_INET, ss->ip, &ss_addr.sin_addr) <= 0){
        close(ss_sock);
        return -1;
    }

    if(connect(ss_sock, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0){
        close(ss_sock);
        return -1;
    }

    // Send SS_READ command
    char cmd[MAX_BUFFER];
    snprintf(cmd, sizeof(cmd), "%s %s\n", CMD_SS_READ, filename);
    if(write(ss_sock, cmd, strlen(cmd)) < 0){
        close(ss_sock);
        return -1;
    }

    // Read data and write to temp file
    FILE *fp = fopen(temp_path, "w");
    if(!fp){
        close(ss_sock);
        return -1;
    }

    char buf[4096];
    int n;
    while((n = read(ss_sock, buf, sizeof(buf))) > 0){
        fwrite(buf, 1, n, fp);
    }

    fclose(fp);
    close(ss_sock);
    return 0;
}

// Handles "EXEC <filename>" command
void handle_exec_command(int client_socket, const char *username, const char *args){
    char filename[256];
    char log_msg[LOG_BUFFER_SIZE];

    // Parse filename
    if(sscanf(args, "%s", filename) != 1){
        write(client_socket, MSG_MALFORMED, strlen(MSG_MALFORMED));
        return;
    }

    snprintf(log_msg, sizeof(log_msg), "User '%s' requested EXEC for '%s'", username, filename);
    log_event(LOG_LEVEL_INFO, log_msg);

    pthread_mutex_lock(&db_mutex);

    // 1. Find File
    FileNode *node = db_find_node_internal(filename);
    if(node == NULL){
        pthread_mutex_unlock(&db_mutex);
        write(client_socket, MSG_FILE_NOT_FOUND, strlen(MSG_FILE_NOT_FOUND));
        write(client_socket, "<<END>>\n", 8);
        return;
    }

    // 2. Check Permissions (EXEC requires Read access)
    if(!db_check_permission(&(node->metadata), username, 'R')){
        pthread_mutex_unlock(&db_mutex);
        write(client_socket, MSG_UNAUTHORIZED, strlen(MSG_UNAUTHORIZED));
        write(client_socket, "<<END>>\n", 8);
        return;
    }

    // 3. Get SS Info
    StorageServerInfo *ss = node->metadata.location;
    if(ss == NULL){
        pthread_mutex_unlock(&db_mutex);
        send_error_message(client_socket, "File is offline.");
        write(client_socket, "<<END>>\n", 8);
        return;
    }

    // Make a copy of SS info to use after unlocking
    StorageServerInfo ss_copy = *ss;

    // Update access time
    db_update_access_time(filename, username);

    pthread_mutex_unlock(&db_mutex);

    // 4. Create unique temp file name
    char temp_path[512];
    // Using username and filename to ensure uniqueness per request
    snprintf(temp_path, sizeof(temp_path), "temp_exec_%s_%s.sh", username, filename);

    // 5. Download File from SS
    if(nm_download_file_to_temp(&ss_copy, filename, temp_path) != 0){
        send_error_message(client_socket, "Failed to retrieve file for execution.");
        write(client_socket, "<<END>>\n", 8);
        remove(temp_path);
        return;
    }

    // 6. Execute the file (using 'sh' interpreter)
    // We redirect stderr to stdout (2>&1) to capture errors too
    char exec_cmd[1024];
    snprintf(exec_cmd, sizeof(exec_cmd), "sh %s 2>&1", temp_path);

    FILE *pipe = popen(exec_cmd, "r");
    if(!pipe){
        send_error_message(client_socket, "Failed to execute script.");
        write(client_socket, "<<END>>\n", 8);
        remove(temp_path);
        return;
    }

    // 7. Read output and send to client
    char output_buf[MAX_BUFFER];
    int total_sent = 0;

    while(fgets(output_buf, sizeof(output_buf), pipe) != NULL){
        write(client_socket, output_buf, strlen(output_buf));
        total_sent++;
    }

    if(total_sent == 0){
        char *msg = "(No Output)\n";
        write(client_socket, msg, strlen(msg));
    }

    // Send terminator
    char *term = "<<END>>\n"; 
    write(client_socket, term, strlen(term));

    // 8. Cleanup
    pclose(pipe);
    remove(temp_path); // Delete the temp file
}
