#include "utils.h"
#include "protocol.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>

// Global logging mutex definition
pthread_mutex_t log_mutex;

// Global variable to store the log file name
static char global_log_filename[100];

void utils_init(const char* log_filename){
    if(pthread_mutex_init(&log_mutex, NULL) != 0){
        perror("mutex init failed");
        exit(EXIT_FAILURE);
    }
    // Store the filename for log_event() to use
    strncpy(global_log_filename, log_filename, sizeof(global_log_filename) - 1);
}

void utils_cleanup(){
    pthread_mutex_destroy(&log_mutex);
}

const char* level_to_string(int level){
    switch(level){
        case LOG_LEVEL_DEBUG: return "DEBUG";
        case LOG_LEVEL_INFO:  return "INFO";
        case LOG_LEVEL_WARN:  return "WARN";
        case LOG_LEVEL_ERROR: return "ERROR";
        default:              return "UNKNOWN";
    }
}

void log_event(int level, const char *message){
    // 1. Get current time
    time_t now = time(NULL);
    char time_str[26];

    // Creating a formatted timestamp, e.g., "Sun Jan 01 00:00:00 2025"
    ctime_r(&now, time_str);
    time_str[strcspn(time_str, "\n")] = 0;

    // 2. Lock the mutex for thread-safe printing
    pthread_mutex_lock(&log_mutex);

    // 3. Print the log message to the terminal (if relevant)
    if(level >= LOG_LEVEL_INFO)
        printf("[%s] [%s] %s\n", time_str, level_to_string(level), message);

    // 4. Writing to the log file
    FILE *log_file = fopen(global_log_filename, "a");
    if(log_file != NULL){
        fprintf(log_file, "[%s] [%s] %s\n", time_str, level_to_string(level), message);
        fclose(log_file);
    }

    // 5. Release the lock
    pthread_mutex_unlock(&log_mutex);
}

void get_local_ip(char *buffer, size_t len){
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if(sock < 0){
        perror("Socket creation failed");
        // Fallback to localhost if socket fails
        strncpy(buffer, "127.0.0.1", len);
        return;
    }

    const char* kGoogleDnsIp = "8.8.8.8";
    uint16_t kDnsPort = 53;

    struct sockaddr_in serv;
    memset(&serv, 0, sizeof(serv));
    serv.sin_family = AF_INET;
    serv.sin_addr.s_addr = inet_addr(kGoogleDnsIp);
    serv.sin_port = htons(kDnsPort);

    // We don't actually send data, just "connect" to determine routing interface
    if(connect(sock, (const struct sockaddr*)&serv, sizeof(serv)) < 0){
        perror("Error connecting");
        close(sock);
        strncpy(buffer, "127.0.0.1", len);
        return;
    }

    struct sockaddr_in name;
    socklen_t namelen = sizeof(name);
    if(getsockname(sock, (struct sockaddr*)&name, &namelen) < 0){
        perror("getsockname failed");
        close(sock);
        strncpy(buffer, "127.0.0.1", len);
        return;
    }

    const char* p = inet_ntop(AF_INET, &name.sin_addr, buffer, len);
    if(p == NULL){
        perror("inet_ntop failed");
        strncpy(buffer, "127.0.0.1", len);
    }

    close(sock);
}