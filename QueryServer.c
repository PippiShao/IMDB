#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <signal.h>

#include "includes/QueryProtocol.h"
#include "MovieSet.h"
#include "MovieIndex.h"
#include "DocIdMap.h"
#include "htll/Hashtable.h"
#include "QueryProcessor.h"
#include "FileParser.h"
#include "FileCrawler.h"

DocIdMap docs;
Index docIndex;
struct addrinfo hints, *result;

#define BUFFER_SIZE 1000
#define SEARCH_RESULT_LENGTH 1500
#define COUNT_STRING_SIZE 10

char movieSearchResult[SEARCH_RESULT_LENGTH];

int Cleanup();

void sigint_handler(int sig) {
  write(0, "Exit signal sent. Cleaning up...\n", 34);
  Cleanup(result);
  exit(0);
}

void Setup(char *dir) {
  printf("Crawling directory tree starting at: %s\n", dir);
  // Create a DocIdMap
  docs = CreateDocIdMap();
  CrawlFilesToMap(dir, docs);
  printf("Crawled %d files.\n", NumElemsInHashtable(docs));

  // Create the index
  docIndex = CreateIndex();

  // Index the files
  printf("Parsing and indexing files...\n");
  ParseTheFiles(docs, docIndex);
  printf("%d entries in the index.\n", NumElemsInHashtable(docIndex->ht));
}

int CreateMovieFromFileRow(char *file, long rowId, Movie** movie) {
  FILE *fp;
  char buffer[BUFFER_SIZE];
  fp = fopen(file, "r");
  int i=0;
  while (i <= rowId) {
    fgets(buffer, BUFFER_SIZE, fp);
    i++;
  }
  // taking \n out of the row
  buffer[strlen(buffer)-1] = ' ';
  // Create movie from row
  *movie = CreateMovieFromRow(buffer);
  fclose(fp);
  return 0;
}

int Cleanup() {
  DestroyOffsetIndex(docIndex);
  DestroyDocIdMap(docs);
  freeaddrinfo(result);

  return 0;
}

int main(int argc, char **argv) {
  // Get args
  if (argc != 3) {
    printf("Wrong number of arguments.\n");
    printf("Rerun with ./queryserver [datadir] [port]\n");
    return 0;
  }
  char *dir_to_crawl = argv[1];
  char *port_string = argv[2];
  printf("Done Parsing Command Line...\n");

  // Setup graceful exit
  struct sigaction kill;

  kill.sa_handler = sigint_handler;
  kill.sa_flags = 0;  // or SA_RESTART
  sigemptyset(&kill.sa_mask);

  if (sigaction(SIGINT, &kill, NULL) == -1) {
    perror("sigaction");
    exit(1);
  }

  Setup(dir_to_crawl);

  // Step 1: get address/port info to open
  // Step 2: Open socket
  int s;
  int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
  memset(&hints, 0, sizeof(struct addrinfo));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;

  // need to free it after done
  s = getaddrinfo("127.0.0.1", port_string, &hints, &result);

  if (s != 0) {
    fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(s));
    exit(1);
  }

  // Step 3: Bind socket
  if (bind(sock_fd, result->ai_addr, result->ai_addrlen) != 0) {
    perror("bind()");
    exit(1);
  }

  // Step 4: Listen on the socket
  // allow 10 requests to queue up.
  if (listen(sock_fd, 10) != 0) {
      perror("listen()");
      exit(1);
  }

  // Step 5: Handle clients that connect
  struct sockaddr_in *result_addr = (struct sockaddr_in *) result->ai_addr;
  printf("Listening on file descriptor %d, port %d\n", sock_fd, ntohs(result_addr->sin_port));

  while (1) {
    // 1. accept connet from the server.
    printf("Waiting for connection...\n");
    int client_fd = accept(sock_fd, NULL, NULL);
    printf("Connection made: client_fd=%d\n", client_fd);

    // 2. send ack to client.
    if (SendAck(client_fd) != 0) {
      printf("send ack fail\n");
      exit(2);
    }

    // 3. get query from client by read
    char query[BUFFER_SIZE];
    int len = read(client_fd, query, sizeof(query) - 1);
    query[len] = '\0';
    printf("query for: %s\n", query);

    // 4. send number of movies found to the client by write()
    SearchResultIter results;
    SearchResult sr;
    results = FindMovies(docIndex, query);
    int count;
    if (results == NULL) {
      count = 0;
    } else {
      count = NumResultsInIter(results);
      sr = (SearchResult)malloc(sizeof(*sr));
      if (sr == NULL) {
        printf("Couldn't malloc SearchResult in main.c\n");
        return 1;
      }
    }

    char count_string[COUNT_STRING_SIZE];
    sprintf(count_string, "%d", count);

    if (write(client_fd, count_string, strlen(count_string)) == -1) {
      printf("error writing to socket\n");
      exit(1);
    }

    char resp[BUFFER_SIZE];
    int flag;
    if (count > 0) {
      // 5, check ack from client and feed client with movies,
      // one at each time.
      for (int i = 0; i < count; i++) {
        SearchResultGet(results, sr);
        len = read(client_fd, resp, BUFFER_SIZE);
        resp[len] = '\0';
        if (CheckAck(resp) != 0) {
          printf("check ack fail.\n");
          exit(1);
        }
        CopyRowFromFile(sr, docs, movieSearchResult);
        if (write(client_fd, movieSearchResult, strlen(movieSearchResult)) == -1) {
          printf("error writing to socket\n");
          exit(1);
        }
        flag = SearchResultNext(results);
        if (flag < 0) {
          printf("error retrieving result\n");
          break;
        }
      }
      free(sr);

      DestroySearchResultIter(results);

      // 6, check final ack from client.
      len = read(client_fd, resp, BUFFER_SIZE);
      resp[len] = '\0';
      if (CheckAck(resp) != 0) {
        printf("check ack fail.\n");
        exit(1);
      }
    }

    // 7, send goodbye to client
    if (SendGoodbye(client_fd) != 0) {
      printf("error sending goodbye.\n");
      exit(1);
    }
  }

  // Step 6: Close the socket
  // Got Kill signal
  free(result_addr);
  close(sock_fd);
  Cleanup();

  return 0;
}
