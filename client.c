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

                // Parse the command token (e.g., "READ", "WRITE", "EXEC")
                char original_command[100];
                // Use a copy to avoid corrupting input_buffer
                char parse_copy[MAX_BUFFER];
                strncpy(parse_copy, input_buffer, MAX_BUFFER);
                sscanf(parse_copy, "%s", original_command);

                // Determine if Redirect is Expected (READ/STREAM/WRITE/UNDO)
                int is_redirect_expected = 0;
                int is_exec_command = 0;
                char *ss_cmd_protocol = CMD_SS_READ; // Default

                if(strcmp(original_command, "READ") == 0){
                    is_redirect_expected = 1;
                    ss_cmd_protocol = CMD_SS_READ;
                }
                else if(strcmp(original_command, "STREAM") == 0){
                    is_redirect_expected = 1;
                    ss_cmd_protocol = CMD_SS_STREAM;
                }
                else if(strcmp(original_command, "WRITE") == 0){
                    is_redirect_expected = 1;
                    ss_cmd_protocol = CMD_WRITE;
                }
                else if(strcmp(original_command, "UNDO") == 0){
                    is_redirect_expected = 1;
                    ss_cmd_protocol = CMD_UNDO;
                }
                else if(strcmp(original_command, "EXEC") == 0){
                    is_exec_command = 1;
                }

                // Send the command to the server
                if(write(sock, input_buffer, strlen(input_buffer)) < 0){
                    perror("write to server failed");
                    break;
                }

                if(is_exec_command){
                    while(1){
                        memset(buffer, 0, MAX_BUFFER);
                        // We keep reading until we see the terminator
                        bytes_read = read(sock, buffer, MAX_BUFFER - 1);
                        if(bytes_read <= 0) {
                            printf("Server disconnected.\n");
                            close(sock);
                            exit(0);
                        }
                        buffer[bytes_read] = '\0';

                        // Check for terminator
                        char *term_ptr = strstr(buffer, "<<END>>\n");
                        if(term_ptr){
                            *term_ptr = '\0'; // Cut off the terminator
                            printf("%s", buffer);
                            break; // Stop reading
                        } 
                        else{
                            printf("%s", buffer);
                        }
                    }
                    continue; // Skip the rest of the loop, start next command
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

                // Check if response is a redirect ("200 <IP> <Port>") AND we expected one
                if(is_redirect_expected && strncmp(buffer, "200", 3) == 0 && sscanf(buffer, "200 %s %d", ss_ip, &ss_port) == 2){
                    // 1. Create new socket for SS
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

                    // 2. Connect to SS
                    if(connect(ss_sock, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0){
                        printf("ERROR: Failed to connect to Storage Server.\n");
                        close(ss_sock);
                        continue;
                    }

                    // 3. Handle protocol specific logic

                    // Case A: READ or STREAM
                    if(strcmp(ss_cmd_protocol, CMD_SS_READ) == 0 || strcmp(ss_cmd_protocol, CMD_SS_STREAM) == 0){
                        printf("Initiating file download from %s:%d...\n", ss_ip, ss_port);
                        
                        char filename[256];
                        sscanf(input_buffer, "%*s %s", filename); // Skip command, get filename
                        filename[strcspn(filename, "\n")] = 0;

                        char ss_req[MAX_BUFFER];
                        sprintf(ss_req, "%s %s\n", ss_cmd_protocol, filename);
                        write(ss_sock, ss_req, strlen(ss_req));

                        char file_chunk[4096];
                        int n_read;
                        while((n_read = read(ss_sock, file_chunk, sizeof(file_chunk)-1)) > 0){
                            file_chunk[n_read] = '\0';
                            printf("%s", file_chunk);
                            fflush(stdout); // Crucial for STREAM delay visibility
                        }

                        printf("\n");
                    }

                    // Case B: WRITE
                    else if(strcmp(ss_cmd_protocol, CMD_WRITE) == 0){
                        char filename[256];
                        int s_idx;

                        // Parse "WRITE filename index"
                        if(sscanf(input_buffer, "%*s %s %d", filename, &s_idx) != 2){
                            printf("ERROR: Invalid WRITE syntax. Usage: WRITE <filename> <sentence_index>\n");
                            close(ss_sock);
                            continue;
                        }

                        // Send initial WRITE command to SS
                        char ss_req[MAX_BUFFER];
                        sprintf(ss_req, "%s %s %d\n", CMD_WRITE, filename, s_idx);
                        write(ss_sock, ss_req, strlen(ss_req));

                        // Wait for "200 READY"
                        memset(buffer, 0, MAX_BUFFER);
                        read(ss_sock, buffer, MAX_BUFFER);

                        if(strncmp(buffer, "200", 3) == 0){
                            printf("%s", buffer);
                            printf("Write mode enabled. Enter '<word_idx> <text>'. Type 'ETIRW' to finish.\n");

                            while(1){
                                printf(">> ");
                                memset(input_buffer, 0, MAX_BUFFER);
                                if(fgets(input_buffer, MAX_BUFFER, stdin) == NULL) break;
                                
                                // Send line to SS
                                write(ss_sock, input_buffer, strlen(input_buffer));
                                
                                if(strncmp(input_buffer, "ETIRW", 5) == 0) break;
                            }

                            // Wait for final success/failure from SS
                            memset(buffer, 0, MAX_BUFFER);
                            read(ss_sock, buffer, MAX_BUFFER);
                            printf("%s", buffer);
                        }
                        else{
                            printf("Server refused WRITE: %s", buffer);
                        }
                    }

                    // Case C: UNDO
                    else if(strcmp(ss_cmd_protocol, CMD_UNDO) == 0){
                        char filename[256];
                        
                        if(sscanf(input_buffer, "%*s %s", filename) != 1){
                            printf("ERROR: Invalid UNDO syntax. Usage: UNDO <filename>\n");
                            close(ss_sock);
                            continue;
                        }

                        // 1. Send "UNDO <filename>"
                        char ss_req[MAX_BUFFER];
                        sprintf(ss_req, "%s %s\n", CMD_UNDO, filename);
                        printf("filename: %s\n", filename);
                        write(ss_sock, ss_req, strlen(ss_req));

                        // 2. Wait for response
                        memset(buffer, 0, MAX_BUFFER);
                        int n = read(ss_sock, buffer, MAX_BUFFER - 1);
                        if(n > 0){
                            buffer[n] = '\0';
                            printf("%s", buffer);
                        }
                    }
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