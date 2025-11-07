# Makefile for LangOS Project

# --- Compiler and Flags ---
CC = gcc
# -g = Add debug symbols
# -Wall = Turn on all warnings
# -pthread = Link the POSIX threads library
CFLAGS = -g -Wall -pthread
LDFLAGS = -pthread # Linker flags

# --- Executables ---
EXECUTABLES = name_server storage_server client

# --- Source Files ---
# Define the source files for each program
NM_SRCS = name_server.c utils.c nm_database.c
SS_SRCS = storage_server.c utils.c
C_SRCS = client.c

# --- Object Files ---
# Automatically create .o filenames from .c filenames
NM_OBJS = $(NM_SRCS:.c=.o)
SS_OBJS = $(SS_SRCS:.c=.o)
C_OBJS = $(C_SRCS:.c=.o)

# --- Main Target (Default) ---
# This is what runs when you just type "make"
all: $(EXECUTABLES)

# --- Linking Rules ---
# Rule to build the 'name_server' executable
name_server: $(NM_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "Linked $@ successfully."

# Rule to build the 'storage_server' executable
storage_server: $(SS_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "Linked $@ successfully."

# Rule to build the 'client' executable
client: $(C_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "Linked $@ successfully."

# --- Generic Compilation Rule ---
# A generic rule to build any .o file from its .c file
# This runs automatically for each dependency
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# --- Cleanup Rule ---
# This runs when you type "make clean"
clean:
	rm -f *.o $(EXECUTABLES) *.log
	@echo "Cleanup complete."
