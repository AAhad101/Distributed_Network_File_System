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

// A helper function to send a standard error message
void send_error_message(int client_socket, const char *message){
    char buffer[MAX_BUFFER];
    snprintf(buffer, sizeof(buffer), "ERROR: %s\n", message);
    write(client_socket, buffer, strlen(buffer));
}

// Handles the "LIST" command
void handle_list_command(int client_socket, const char *username, const char *args){
    char log_msg[200];
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
    char log_msg[200];
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
    char log_msg[200];
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
    char log_msg[300];

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
        // TODO: Rollback the metadata creation (i.e., delete the file metadata)
        send_error_message(client_socket, "Storage server failed to create file.");
    }
}

// Handles the "DELETE <filename>" command
void handle_delete_command(int client_socket, const char *username, const char *args){
    char filename[256];
    char log_msg[300];

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
    char log_msg[300];

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
    char log_msg[300];

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
    else{
        send_error_message(client_socket, "Internal Server Error");
    }
}