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
 * Handles the "VIEW -l/-a" command
 * Retrieves the appropriate files and displays to the user
*/
void handle_view_command(int client_socket, const char *username, const char *args);

/*
 * Handles the "CREATE <filename>" command
 * Creates a new file in an SS 
*/
void handle_create_command(int client_socket, const char *username, const char *args);

// Handles the "DELETE <filename>" command
void handle_delete_command(int client_socket, const char *username, const char *args);

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

/*
 * Handles the "READ <filename>" command
 * Communicates directly with the SS to get the file data
*/
void handle_read_command(int client_socket, const char *username, const char *args);

/*
 * Handles the "STREAM <filename>" command
 * Mostly identical in logic to READ except with added time logic for streaming delay
*/
void handle_stream_command(int client_socket, const char *username, const char *args);


/*
 * Handles the "WRITE <filename> <sentence_index>" command
 * Validates user and redirects to SS
*/
void handle_write_command(int client_socket, const char *username, const char *args);

// Handles the "UNDO filename" command
void handle_undo_command(int client_socket, const char *username, const char *args);

#endif