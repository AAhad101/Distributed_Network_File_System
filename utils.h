#ifndef UTILS_H
#define UTILS_H

// This tis the header file for our logging tools

#include <pthread.h>

// Global logging mutex
extern pthread_mutex_t log_mutex;

// Function declarations

/**
 * Initialises all utilities (the mutex)
 * Call this once at the start of name_server.c
**/
void utils_init(const char* log_filename);

/*
 * Cleans up all utilities (destroy the mutex)
 * Call this before the server exits
*/
void utils_cleanup();

// Helper function to convert 'int level' to string
const char* level_to_string(int level);

/*
 * A thread-safe logging function 
 * The parameter 'level' is for the log level (like "INFO", "ERROR", and "WARN")
*/
void log_event(int level, const char *message);

// Log levels
#define LOG_LEVEL_DEBUG 0
#define LOG_LEVEL_INFO  1
#define LOG_LEVEL_WARN  2
#define LOG_LEVEL_ERROR 3

// Finds the local IP address of this machine by connecting to a public DNS 
void get_local_ip(char *buffer, size_t len);

#endif