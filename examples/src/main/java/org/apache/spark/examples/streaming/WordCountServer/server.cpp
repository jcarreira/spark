#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <time.h> 
#include <assert.h> 
#include <sys/time.h>

#define PORT 9999
#define MILISECOND 1000 // 1ms = 1000 microseconds
//#define SLEEP_TIME (MILISECOND*10)

#define RECORD_CONTENT "1234567890\n" //10 bytes of content + newline

//#define MESSAGES_PER_SEC 100
#define MSEC_IN_SEC 1000
#define USEC_IN_SEC 1000000

unsigned long int msec_diff_time(struct timeval begin, struct timeval end) {
    unsigned long int sec_begin = begin.tv_sec;
    unsigned long int usec_begin = begin.tv_usec;
    
    unsigned long int sec_end = end.tv_sec;
    unsigned long int usec_end = end.tv_usec;

    unsigned long int usec_end_total = usec_end + sec_end * USEC_IN_SEC;
    unsigned long int usec_begin_total = usec_begin + sec_begin * USEC_IN_SEC;

    return (usec_end_total - usec_begin_total) / MSEC_IN_SEC;
}

int main(int argc, char *argv[]) {

    // server <messages per second>
    assert(argc == 2);

    int MESSAGES_PER_SEC = atoi(argv[1]);

    int listenfd = 0, connfd = 0;
    struct sockaddr_in serv_addr; 

    char sendBuff[1025];
    time_t ticks; 

    listenfd = socket(AF_INET, SOCK_STREAM, 0);
    memset(&serv_addr, '0', sizeof(serv_addr));
    memset(sendBuff, '0', sizeof(sendBuff)); 

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(PORT); 

    int optval = 1;
    assert( setsockopt(listenfd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval)) != -1);

    if( bind(listenfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) == -1) {
        perror("bind");
    }

    assert ( listen(listenfd, 10) != -1);

    printf("Listening on port %d\n", PORT);

    //    while(1) {
    connfd = accept(listenfd, (struct sockaddr*)NULL, NULL); 

    printf("Accepted connection\n");

    ticks = time(NULL);
    //snprintf(sendBuff, sizeof(sendBuff), "%.24s\r\n", ctime(&ticks));
    strcpy(sendBuff, RECORD_CONTENT);

    while (1) {
        struct timeval begin, end;
        gettimeofday(&begin, NULL);

        for (int i = 0; i < MESSAGES_PER_SEC; ++i)
            write(connfd, sendBuff, strlen(sendBuff)); 
        
        gettimeofday(&end, NULL);
        unsigned long int msec_elapsed = msec_diff_time(begin, end);

        printf ("Elapsed: %lu msec\n", msec_elapsed);

        // we have to sleep 1sec - <time spent sending>


        // if we spend more than 1 sec sending, we are already running late

        if (msec_elapsed > MSEC_IN_SEC) {
        } else { // sleep
            unsigned long int to_sleep = (MSEC_IN_SEC - msec_elapsed) * 1000;
            assert(to_sleep > 0);

            printf("Sleeping %lu msec\n", to_sleep / 1000);
            usleep(to_sleep);
        }
    }

    printf("Wrote %s\n", sendBuff);

    close(connfd);
    sleep(1);
    //    }
    return 0;
}

