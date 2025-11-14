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
    char buffer[MAX_BUFFER] = {0};

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

                memset(buffer, 0, MAX_BUFFER);
                if(fgets(buffer, MAX_BUFFER - 1, stdin) == NULL){
                    printf("\nDisconnecting...\n");
                    break;
                }

                if(write(sock, buffer, strlen(buffer)) < 0){
                    perror("write to server failed");
                    break;
                }

                memset(buffer, 0, MAX_BUFFER);
                bytes_read = read(sock, buffer, MAX_BUFFER - 1);

                if(bytes_read <= 0){
                    printf("Server disconnected.\n");
                    break;
                }

                buffer[bytes_read] = '\0';

                // Print the server's reply (output of the command)
                printf("%s", buffer);
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