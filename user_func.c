#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>

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