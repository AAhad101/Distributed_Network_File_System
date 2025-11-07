#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>
#include <arpa/inet.h>

#include "protocol.h"
#include "utils.h"
#include "nm_database.h"

void *handle_connection(void *socket_desc){
    // 1. Getting the socket_fd from the argument
    int client_socket = *(int *)socket_desc;
    free(socket_desc);

    char buffer[MAX_BUFFER];
    char log_msg[MAX_BUFFER + 200];

    // 2. Reading the handshake message from the sender
    int bytes_read = read(client_socket, buffer, MAX_BUFFER - 1);

    if(bytes_read <= 0){
        log_event(LOG_LEVEL_INFO, "A client disconnected without sending data.");
    }
    else{
        // Null-terminate the data received
        buffer[bytes_read] = '\0';

        // 3. Parse the handshake message

        // If the sender is a client
        if(strncmp(buffer, CMD_REG_CLIENT, strlen(CMD_REG_CLIENT)) == 0){
            char username[100];
            // Parsing the username
            if(sscanf(buffer, CMD_REG_CLIENT " %s", username) == 1){
                sprintf(log_msg, "Client connected: %s", username);
                log_event(LOG_LEVEL_INFO, log_msg);

                // Adding user to the master list of users
                db_add_user(username);

                // Sending SUCCESS reply
                write(client_socket, MSG_SUCCESS, strlen(MSG_SUCCESS));
            }
            else{
                log_event(LOG_LEVEL_ERROR, "Malformed REG_CLIENT request.");

                // Sending MALFORMED reply
                write(client_socket, MSG_MALFORMED, strlen(MSG_MALFORMED));
            }
        }

        // If the sender is a storage server
        else if(strncmp(buffer, CMD_REG_SS, strlen(CMD_REG_SS)) == 0){
            // Adds the SS info to the SS list and re-links its files
            db_register_ss(buffer);

            // Sending SUCCESS reply
            write(client_socket, MSG_SUCCESS, strlen(MSG_SUCCESS));

            // TODO: Parse and store storage server details
            // Question: What data is even gettin printed here? Shouldn't the SS have some name also?
        }
        
        // Invalid sender (or wrong handshake)
        else{
            sprintf(log_msg, "Unknown connection attempt: %s", buffer);
            log_event(LOG_LEVEL_WARN, log_msg);

            // Sending UNKNOWN_MESSAGE reply
            write(client_socket, MSG_UNKNOWN, strlen(MSG_UNKNOWN));
        }
    }

    // 4. Ending the connection
    close(client_socket);       // TODO: Apparently this should become a loop later on
    pthread_exit(NULL);
}

int main(){
    int server_fd, new_socket;
    struct sockaddr_in address;
    int addrlen = sizeof(address);

    // 0. Initialise our utilities
    utils_init("name_server.log");
    db_init();
    db_load_from_disk();    // This populates the hashmap

    // 1. Creating the server socket
    //printf("Creating server socket...\n");
    server_fd = socket(AF_INET, SOCK_STREAM, 0);    // AF_INET = IPv4   SOCK_STREAM with Protocol = 0 is for TCP
    if(server_fd == -1){
        perror("socket failed");    // Prints OS-level error while log_event() is for application archive
        exit(EXIT_FAILURE);
    }

    // 2. Configure the server address
    memset(&address, 0, sizeof(address));     // Clears the struct
    address.sin_family = AF_INET;             // We are using IPv4
    address.sin_addr.s_addr = INADDR_ANY;     // Accept connections from any IP on this machine
    address.sin_port = htons(NM_PORT);        // Converts the port number to network byte order

    // 3. Bind the socket to our port and address
    //printf("Binding socket to port %d...\n", NM_PORT);
    if(bind(server_fd, (struct sockaddr *)&address, sizeof(address)) == -1){
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    //  4. Put the socket in listening mode
    //printf("Listening for connection...\n");
    if(listen(server_fd, 3) == -1){     // 3 is the backlog i.e. the no. of pending connections to queue up
        perror("listen failed");
        exit(EXIT_FAILURE);
    }
    
    // Logging the successful launch of the Name Server
    log_event(LOG_LEVEL_INFO, "Name Server started. Waiting for connections...");

    // 5. The main server loop to accept connections
    while(1){
        // accept() is a blocking syscall so the program will pause here until a client connects
        new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t *)&addrlen);
        if(new_socket == -1){
            perror("accept failed");
            log_event(LOG_LEVEL_ERROR, "Failed to accept new connection.");
            continue;   // Move on to check for other connections
        }

        // 6. Getting the client's IP address and logging the connection
        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &address.sin_addr, client_ip, INET_ADDRSTRLEN);  // Converts IP address to human readable form

        // Logging new connection
        char log_msg[100];
        sprintf(log_msg, "New connection accepted from %s:%d", client_ip, ntohs(address.sin_port));
        log_event(LOG_LEVEL_INFO, log_msg);

        // 7. Creating a new thread for new connection
        pthread_t thread_id;
        int *new_sock_ptr = (int *)malloc(sizeof(int));
        *new_sock_ptr = new_socket;

        if(pthread_create(&thread_id, NULL, handle_connection, (void *)new_sock_ptr) < 0){
            perror("pthread_create failed");
            log_event(LOG_LEVEL_ERROR, "Failed to create new thread.");
            free(new_sock_ptr);
            close(new_socket);
        }

        // Detach the thread so its resources are cleaned up automatically
        pthread_detach(thread_id);
    }

    // Closes the Name Server
    close(server_fd);

    // Clean up our utilities
    utils_cleanup();

    return 0;
}
