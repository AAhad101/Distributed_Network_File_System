#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "protocol.h"
#include "utils.h"
#include <ftw.h>
#include <libgen.h>
#include <pthread.h>
#include <sys/stat.h>
#include <fcntl.h> 

// Global variables for ftw
static char file_list_buffer[MAX_FILE_BUFFER] = {0};
static char global_storage_path[1024];

// File callback function for ftw
int file_callback(const char *fpath, const struct stat *sb, int typeflag){
    // We care only about regular files (FTW_F), not directories and stuff
    if(typeflag == FTW_F){
        char *filename = basename((char *)fpath);   // fpath is the full path, basename gives only the file name 

        // Add a space before adding the file (unless the buffer is empty)
        if(strlen(file_list_buffer) > 0){
            strcat(file_list_buffer, " ");
        }

        strcat(file_list_buffer, filename);
    }

    return 0;   // This tells ftw to continue
}

/*
 * This handles a single command from the NM (e.g., "SS_CREATE")
 * This runs in its own thread
*/
void *ss_handle_nm_command(void *socket_desc){
    int nm_socket = *(int *)socket_desc;
    free(socket_desc);

    char buffer[MAX_BUFFER];
    char log_msg[MAX_BUFFER + 200];

    // Read the command from NM
    int bytes_read = read(nm_socket, buffer, MAX_BUFFER - 1);

    if(bytes_read <= 0){
        log_event(LOG_LEVEL_WARN, "NM disconnected without sending command.");
        close(nm_socket);
        pthread_exit(NULL);
    }

    buffer[bytes_read] = '\0';          // Null terminate the message read
    buffer[strcspn(buffer, "\n")] = 0;  // Strip newline

    // Parse the command
    char *command = strtok(buffer, " ");
    char *filename = strtok(NULL, " ");

    if(command == NULL || filename == NULL){
        log_event(LOG_LEVEL_ERROR, "Malformed command from NM.");
        write(nm_socket, MSG_MALFORMED, strlen(MSG_MALFORMED));
        close(nm_socket);
        pthread_exit(NULL);
    }

    // Handle SS_CREATE
    if(strcmp(command, CMD_SS_CREATE) == 0){
        char file_path[2048];
        snprintf(file_path, sizeof(file_path), "%s/%s", global_storage_path, filename);

        sprintf(log_msg, "Executing CREATE for: %s", file_path);
        log_event(LOG_LEVEL_INFO, log_msg);

        // Create the empty file
        int fd = open(file_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if(fd == -1){
            perror("open (create file)");
            log_event(LOG_LEVEL_ERROR, "Failed to create file locally.");
            write(nm_socket, MSG_CANNOT_CREATE_FILE, strlen(MSG_CANNOT_CREATE_FILE));
        }
        else{
            close(fd);
            write(nm_socket, MSG_SUCCESS, strlen(MSG_SUCCESS));
        }
    }
    else if(strcmp(command, CMD_SS_DELETE) == 0){
        char file_path[2048];
        snprintf(file_path, sizeof(file_path), "%s/%s", global_storage_path, filename);

        sprintf(log_msg, "Executing DELETE for: %s", file_path);
        log_event(LOG_LEVEL_INFO, log_msg);

        // Delete the file from the filesystem
        if(remove(file_path) == 0){
            write(nm_socket, MSG_SUCCESS, strlen(MSG_SUCCESS));
        }
        else{
            perror("remove failed");
            log_event(LOG_LEVEL_ERROR, "Failed to delete file locally.");
            write(nm_socket, MSG_CANNOT_DELETE_FILE, strlen(MSG_CANNOT_DELETE_FILE));
        }
    }
    else{
        log_event(LOG_LEVEL_WARN, "Unknown command from NM.");
        write(nm_socket, MSG_UNKNOWN, strlen(MSG_UNKNOWN));
    }

    close(nm_socket);
    pthread_exit(NULL);
}

/*
 * This is the main server loop for the SS
 * It listens for connections from the NM on the NM_PORT
 * This runs in its own thread
*/
void *ss_listen_for_nm(void *port_arg){
    int my_nm_port = *(int *)port_arg;
    free(port_arg);

    int server_fd, new_socket;
    struct sockaddr_in address;
    int addrlen;

    // 1. Create a socket
    if((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0){
        perror("socket failed (ss_listen)");
        log_event(LOG_LEVEL_ERROR, "Failed to create SS listener socket.");
        pthread_exit(NULL);
    }

    // 2. Bind to the NM_PORT
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(my_nm_port);

    if(bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0){
        perror("bind failed (ss_listen)");
        log_event(LOG_LEVEL_ERROR, "Failed to bind SS listener socket.");
        close(server_fd);
        pthread_exit(NULL);
    }

    // 3. Listen
    if(listen(server_fd, 5) < 0){
        perror("listen failed (ss_listen)");
        close(server_fd);
        pthread_exit(NULL);
    }

    char log_msg[100];
    sprintf(log_msg, "SS now listening on port %d for NM commands.", my_nm_port);
    log_event(LOG_LEVEL_INFO, log_msg);

    // 4. Accept loop
    while(1){
        addrlen = sizeof(address);

        new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t *)&addrlen);
        if(new_socket < 0){
            perror("accept failed (ss_listen)");
            continue;
        }

        log_event(LOG_LEVEL_DEBUG, "NM has connected to SS.");

        // Create a new thread to handle the command
        pthread_t thread_id;
        int *new_sock_ptr = (int *)malloc(sizeof(int));
        *new_sock_ptr = new_socket;

        if(pthread_create(&thread_id, NULL, ss_handle_nm_command, (void *)new_sock_ptr) < 0){
            perror("pthread_create failed (ss_listen)");
            free(new_sock_ptr);
            close(new_socket);
        }
        pthread_detach(thread_id);
    }
}


int main(int argc, char *argv[]){
    // 0. Initialising logging
    utils_init("storage_server.log");   // To initialise logging

    // 1. Command line argument validation
    if(argc != 4){
        // Log to stderr and our log file
        fprintf(stderr, "Usage: %s <nm_port> <client_port> <storage_path>\n", argv[0]);
        char err_msg[100];
        sprintf(err_msg, "Invalid arguments: Expected 2, got %d", argc - 1);
        log_event(LOG_LEVEL_ERROR, err_msg);
        exit(EXIT_FAILURE);
    }

    // Parse ports from command line
    int my_nm_port = atoi(argv[1]);
    int my_client_port = atoi(argv[2]);
    strncpy(global_storage_path, argv[3], sizeof(global_storage_path) - 1); // Store storage_path in global variable
    char log_msg[MAX_BUFFER + 200];     // Buffer for log messages

    // Basic port validation
    if(my_nm_port <= 1024 || my_client_port <= 1024){
        log_event(LOG_LEVEL_ERROR, "Invalid port: Ports must be > 1024.");
        exit(EXIT_FAILURE);
    }

    // Getting IP of SS
    char my_ip[16];
    get_local_ip(my_ip, sizeof(my_ip));
    
    sprintf(log_msg, "SS starting. NM Port: %d, Client Port: %d, Path: %s", my_nm_port, my_client_port, global_storage_path);
    log_event(LOG_LEVEL_INFO, log_msg);

    // 2. Scanning storage before connecting
    log_event(LOG_LEVEL_INFO, "Scanning storage directory...");

    if(ftw(global_storage_path, file_callback, 10) == -1){
        perror("ftw failed");
        log_event(LOG_LEVEL_ERROR, "Failed to scan storage directory.");
        exit(EXIT_FAILURE);
    }

    sprintf(log_msg, "Found files: %s", file_list_buffer);
    log_event(LOG_LEVEL_INFO, log_msg);

    // Socket and connect stuff starts here
    int sock = 0;
    struct sockaddr_in nm_addr;
    char buffer[MAX_BUFFER] = {0};

    // 3. Create client socket
    sock = socket(AF_INET, SOCK_STREAM, 0);
    if(sock == -1){
        perror("socket error");
        log_event(LOG_LEVEL_ERROR, "Failed to create socket.");
        exit(EXIT_FAILURE);
    }

    // 4. Configure Name Server address
    memset(&nm_addr, 0, sizeof(nm_addr));   // Clear the struct
    nm_addr.sin_family = AF_INET;
    nm_addr.sin_port = htons(NM_PORT);      // Converts the port number to network byte order

    // Converting the IP string to binary format
    if(inet_pton(AF_INET, NM_IP, &nm_addr.sin_addr) <= 0){
        perror("invalid address");
        log_event(LOG_LEVEL_ERROR, "Invalid NM IP address.");
        exit(EXIT_FAILURE);
    }

    // 5. Connect to Name Server
    sprintf(log_msg, "Connecting to Name Server at %s:%d...", NM_IP, NM_PORT);
    log_event(LOG_LEVEL_INFO, log_msg);
    if(connect(sock, (struct sockaddr *)&nm_addr, sizeof(nm_addr)) < 0){
        perror("connection failed");
        exit(EXIT_FAILURE);
    }
    log_event(LOG_LEVEL_INFO, "Connected to Name Server.");

    // 6. Format and send handshake (registration) message

    /*// TODO: I guess I have to scan some directory to get this list
    const char *file_list = "file1.txt file2.txt project_doc.txt";*/

    int n = sprintf(buffer, "%s %s %d %d %s\n", CMD_REG_SS, my_ip, my_nm_port, my_client_port, file_list_buffer);

    sprintf(log_msg, "Sending registration: %s", buffer);
    log_event(LOG_LEVEL_DEBUG, log_msg); // DEBUG level, as it's verbose
    if(write(sock, buffer, n) < 0){
        perror("write failed");
        log_event(LOG_LEVEL_ERROR, "Failed to send registration to NM.");
        close(sock);
        exit(EXIT_FAILURE);
    }
    
    // 7. Waiting for acknowledgement (ACK)
    log_event(LOG_LEVEL_INFO, "Waiting for server acknowledgment...");
    memset(buffer, 0, sizeof(buffer));    // Clear the buffer
    int bytes_read = read(sock, buffer, MAX_BUFFER - 1);

    if(bytes_read > 0){
        buffer[bytes_read] = '\0';    // Null-terminating the buffer
        buffer[strcspn(buffer, "\n")] = 0;  // Remove trailing newline
        sprintf(log_msg, "Name Server replied: %s", buffer);
        log_event(LOG_LEVEL_INFO, log_msg);

        // Check if the reply was a success
        if(strncmp(buffer, "200", 3) == 0){
            log_event(LOG_LEVEL_INFO, "Registration successful.");   
            
            // 8. Start the listener for NM commands
            pthread_t nm_listener_thread;
            int *nm_port_ptr = malloc(sizeof(int));
            *nm_port_ptr = my_nm_port;
            if(pthread_create(&nm_listener_thread, NULL, ss_listen_for_nm, (void *)nm_port_ptr) < 0){
                perror("pthread_create (nm_listener)");
                exit(EXIT_FAILURE);
            }
            pthread_detach(nm_listener_thread); // So the OS cleans up its resources when it's done (we do not call pthread_join() on this)

            // TODO: Start the listener for client commands (on my_client_port)

            // 9. Keeping the main thread alive
            while(1){
                sleep(60);  // Sleep and wait for connections
            }

        }
        else{
            log_event(LOG_LEVEL_ERROR, "Registration failed. Check server logs.");
        }
    }
    else{
        log_event(LOG_LEVEL_WARN, "Failed to get reply from server.");
    }


    // 8. Close connection
    close(sock);
    utils_cleanup(); // Clean up the logging mutex

    // TODO: It has to listen to commands and stuff, the server should not stop here    
    
    log_event(LOG_LEVEL_INFO, "Storage Server shutting down.");

    return 0;
}