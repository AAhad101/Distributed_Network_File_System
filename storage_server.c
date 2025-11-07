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

// Global variables for ftw
static char file_list_buffer[MAX_FILE_BUFFER] = {0};

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

#define MY_IP "127.0.0.1"

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
    char *storage_path = argv[3];
    char log_msg[MAX_BUFFER + 200];     // Buffer for log messages

    // Basic port validation
    if(my_nm_port <= 1024 || my_client_port <= 1024){
        log_event(LOG_LEVEL_ERROR, "Invalid port: Ports must be > 1024.");
        exit(EXIT_FAILURE);
    }
    
    sprintf(log_msg, "SS starting. NM Port: %d, Client Port: %d", my_nm_port, my_client_port);
    log_event(LOG_LEVEL_INFO, log_msg);


    // 2. Scanning storage before connecting
    log_event(LOG_LEVEL_INFO, "Scanning storage directory...");

    if(ftw(storage_path, file_callback, 10) == -1){
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

    int n = sprintf(buffer, "%s %s %d %d %s", CMD_REG_SS, MY_IP, my_nm_port, my_client_port, file_list_buffer);

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
            
            while(1){
                sleep(10);
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