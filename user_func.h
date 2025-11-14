#ifndef USER_FUNC_H
#define USER_FUNC_H

/*
 * Handle the "LIST" command
 * Retrieves all users from the database and sends them to the client
*/
void handle_list_command(int client_socket, const char *username, const char *args);

/*
 * Handles the "INFO <filename>" command
 * Finds the file in the hash table and prints its metadata
*/
void handle_info_command(int client_socket, const char *username, const char *args);

/*
 * Handles the "CREATE <filename>" command
 * Creates a new file in an SS 
*/
void handle_create_command(int client_socket, const char *username, const char *args);

/*
 * Handles the "ADDACCESS -[R|W] <filename> <username>" command
 * Finds the file and adds username to the appropriate permissions linked list
*/
void handle_addaccess_command(int client_socket, const char *username, const char *args);

/*
 * Handles the "REMACCESS -[R|W] <filename> <username>" command
 * Finds the file and removes the username from the appropriate permissions linked list
*/
void handle_remaccess_command(int client_socket, const char *username, const char *args);

#endif