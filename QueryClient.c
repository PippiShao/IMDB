#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <arpa/inet.h>

#include "includes/QueryProtocol.h"
#include "includes/htll/LinkedList.h"
#include "includes/Movie.h"
#include "includes/MovieIndex.h"
#include "includes/MovieReport.h"
#include "includes/MovieSet.h"
#include "includes/FileParser.h"
#include "queryclient.h"

char *port_string = "1500";
unsigned short int port;
char *ip = "127.0.0.1";
int MAX_RESULT_SIZE = 1000;

#define BUFFER_SIZE 1000

void RunQuery(char *query) {

  // Create the socket
  int s;
  int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
  struct addrinfo hints, *result;
  memset(&hints, 0, sizeof(struct addrinfo));
  hints.ai_family = AF_INET; // IPv4 only
  hints.ai_socktype = SOCK_STREAM; // TCP only

  // Find the address
  s = getaddrinfo(ip, port_string, &hints, &result);
  if (s != 0) {
    fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(s));
    exit(1);
  }

  // 1. Connect to the server
  if (connect(sock_fd, result->ai_addr, result->ai_addrlen) == -1) {
    perror("connect");
    exit(2);
  }

  // Do the query-protocol
  // ===============================================
  // 2. read() response from socket.
  // check if its ack by checkAck().
  char resp[MAX_RESULT_SIZE+1];
  int len = read(sock_fd, resp, MAX_RESULT_SIZE);
  resp[len] = '\0';
  if (CheckAck(resp) != 0) {
    printf("check ack fail.\n");
    exit(1);
  }

  // 3. send query to server by write().
  if (write(sock_fd, query, strlen(query)) == -1) {
    printf("error writing to socket\n");
    exit(1);
  }

  // 4, get number of responses. (number of movies)
  len = read(sock_fd, resp, MAX_RESULT_SIZE);
  resp[len] = '\0';
  int count = atoi(resp);
  printf("found %d movies.\n", count);

  if (count > 0) {
    LinkedList movies = CreateLinkedList();
    Movie *movie;
    // 5, send ack and get each movie as response
    for (int i = 0; i < count; i++) {
      if (SendAck(sock_fd) != 0) {
        printf("send %d ack fail\n", i);
        exit(2);
      }
      len = read(sock_fd, resp, MAX_RESULT_SIZE);
      resp[len] = '\0';
      //printf("%s\n", resp);
      // print out movies by genre. It can also be organized with year, type.
      movie = CreateMovieFromRow(resp);
      InsertLinkedList(movies, movie);
    }
    Index index = BuildMovieIndex(movies, Genre);
    PrintReport(index);
    DestroyTypeIndex(index);
  }

  // 6, send ack before goodbye.
  if (SendAck(sock_fd) != 0) {
    exit(2);
  }

  // 7, checkif response is goodbye by CheckGoodbye()
  len = read(sock_fd, resp, MAX_RESULT_SIZE);
  resp[len] = '\0';
  if (CheckGoodbye(resp) != 0) {
    printf("%s\n", "response is not goodbye. Exit directly.\n");
    exit(2);
  }
  // This is to show that a multiserver can take
  // multiple clients at the same time.
  sleep(4);
  // ===============================================

  // Close the connection
  close(sock_fd);
  free(result);
}

void RunPrompt() {
  char input[BUFFER_SIZE];

  while (1) {
    printf("Enter a term to search for, or q to quit: ");
    scanf("%s", input);

    printf("input was: %s\n", input);

    if (strlen(input) == 1) {
      if (input[0] == 'q') {
        printf("Thanks for playing! \n");
        return;
      }
    }
    if (strlen(input) <= 100) {
      printf("\n\n");
      RunQuery(input);
    } else {
      printf("\n\n");
      printf("Input exceeds 100 characters.\n");
    }
  }
}

int main(int argc, char **argv) {
  // Check/get arguments
  if (argc != 3) {
    printf("Wrong number of arguments.\n");
    printf("Rerun with ./queryclient [ipaddress] [port]\n");
    return 0;
  }

  // Get info from user
  ip = argv[1];
  port_string = argv[2];
  printf("Done Parsing Command Line...\n");

  // Run Query
  RunPrompt();

  return 0;
}
