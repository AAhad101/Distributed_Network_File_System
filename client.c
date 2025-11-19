#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "protocol.h"

int main(int argc, char *argv[]){
    int sock = 0;
    struct sockaddr_in nm_addr;
    char username[100];
    char buffer[MAX_BUFFER] = {0};  // Used for sending to and receiving from NM

    // 1. Get client username from command-line argument
    if(argc != 2){
        fprintf(stderr, "Usage: %s <username>\n", argv[0]);
        exit(EXIT_FAILURE);
    }
    strncpy(username, argv[1], sizeof(username) - 1);   // Copying the username from the argument
    username[sizeof(username) - 1] = '\0';              // Ensuring it is null-terminated

    // 2. Socket and connect
    sock = socket(AF_INET, SOCK_STREAM, 0);
    if(sock == -1){
        perror("socket error");
        exit(EXIT_FAILURE);
    }

    memset(&nm_addr, 0, sizeof(nm_addr));   // Clear the struct
    nm_addr.sin_family = AF_INET;
    nm_addr.sin_port = htons(NM_PORT);      // Converts the port number to network byte order

    if(inet_pton(AF_INET, NM_IP, &nm_addr.sin_addr) <= 0){
        perror("invalid address");
        exit(EXIT_FAILURE);
    }

    printf("Connecting to Name Server at %s:%d...\n", NM_IP, NM_PORT);
    if(connect(sock, (struct sockaddr *)&nm_addr, sizeof(nm_addr)) < 0){
        perror("connection failed");
        exit(EXIT_FAILURE);
    }
    printf("Connected to Name Server as '%s'.\n", username);

    // 3. Format and send handshake (registration) message
    int n = sprintf(buffer, "%s %s\n", CMD_REG_CLIENT, username);

    printf("Sending registration...\n");
    if(write(sock, buffer, n) < 0){
        perror("write failed");
        close(sock);
        exit(EXIT_FAILURE);
    }

    // 4. Wait for acknowledgement (ACK)
    printf("Waiting for server acknowledgement...\n");
    memset(buffer, 0, sizeof(buffer));    // Clear buffer
    int bytes_read = read(sock, buffer, MAX_BUFFER - 1);

    if(bytes_read > 0){
        buffer[bytes_read] = '\0';
        printf("Name Server replied: %s", buffer);

        // Check if the reply was a success
        if(strncmp(buffer, "200", 3) == 0){
            printf("Registration successful.\n");

            // 5. Client command loop
            while(1){
                printf("> ");

                char input_buffer[MAX_BUFFER];  // Stores the user's command

                memset(input_buffer, 0, MAX_BUFFER);
                if(fgets(input_buffer, MAX_BUFFER - 1, stdin) == NULL){
                    printf("\nDisconnecting...\n");
                    break;
                }

                // Send the command to the server
                if(write(sock, input_buffer, strlen(buffer)) < 0){
                    perror("write to server failed");
                    break;
                }

                // Read the server's reply (NM's response or the redirect)
                memset(buffer, 0, MAX_BUFFER);
                bytes_read = read(sock, buffer, MAX_BUFFER - 1);

                if(bytes_read <= 0){
                    printf("Server disconnected.\n");
                    break;
                }
                buffer[bytes_read] = '\0';

                // Start READ redirect handling
                char ss_ip[16];
                int ss_port;

                // Check if response is a READ redirect ("200 <IP> <Port>")
                if(strncmp(buffer, "200", 3) == 0 && sscanf(buffer, "200 %s %d", ss_ip, &ss_port) == 2){
                    // 1. Get filename and original command from saved input_buffer
                    char filename[256];
                    char original_command[100];

                    // Parse the command and filename from the saved input
                    if(sscanf(input_buffer, "%s %s", original_command, filename) != 2){
                        printf("ERROR: Internal client error during command parsing.\n");
                        continue;
                    }

                    // 2. Determine the correct SS protocol command
                    char *ss_cmd_protocol;
                    if(strcmp(original_command, "STREAM") == 0){
                        ss_cmd_protocol = CMD_SS_STREAM;
                    } 
                    else{
                        // TODO: Should add different else if's for UNDO, WRITE and EXEC I guess
                        // Default to READ (covers the regular READ command)
                        ss_cmd_protocol = CMD_SS_READ; 
                    }

                    printf("Initiating file download from %s:%d...\n", ss_ip, ss_port);
                    
                    // 3. Create new socket for SS
                    int ss_sock = socket(AF_INET, SOCK_STREAM, 0);
                    struct sockaddr_in ss_addr;

                    memset(&ss_addr, 0, sizeof(ss_addr));
                    ss_addr.sin_family = AF_INET;
                    ss_addr.sin_port = htons(ss_port);

                    if(inet_pton(AF_INET, ss_ip, &ss_addr.sin_addr) <= 0){
                        printf("ERROR: Invalid SS IP.\n");
                        close(ss_sock);
                        continue;
                    }

                    // 4. Connect to SS
                    if(connect(ss_sock, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0){
                        printf("ERROR: Failed to connect to Storage Server.\n");
                        close(ss_sock);
                        continue;
                    }

                    // 5. Send Request: "<CMD_SS_X> <filename>"
                    char ss_req[MAX_BUFFER];
                    sprintf(ss_req, "%s %s\n", ss_cmd_protocol, filename);
                    write(ss_sock, ss_req, strlen(ss_req));

                    // 6. Stream Data from SS
                    char file_chunk[4096];
                    int n_read;

                    while((n_read = read(ss_sock, file_chunk, sizeof(file_chunk)-1)) > 0){
                        file_chunk[n_read] = '\0';
                        printf("%s", file_chunk);

                        fflush(stdout);
                    }

                    close(ss_sock);
                }
                else{
                    // For LIST, INFO, or Error messages (404, 401) from NM
                    printf("%s", buffer);
                }
            }
        } 
        else{
            printf("Registration failed. Check server logs.\n");
        }
    } 
    else{
        printf("Failed to get reply from server.\n");
    }

    // 6. Close connection
    close(sock);
    
    printf("Client shutting down.\n");

    return 0;
}