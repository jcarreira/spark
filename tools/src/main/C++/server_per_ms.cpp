#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string>
#include <cstring>
#include <sstream>
#include <sys/types.h>
#include <time.h> 
#include <assert.h> 
#include <sys/time.h>
#include <chrono>

//#define PORT 9999
#define MILISECOND 1000 // 1ms = 1000 microseconds
//#define SLEEP_TIME (MILISECOND*10)

char* CONTENT[] = {"1234567890\n", //10 bytes of content + newline
           "0000000000\n",
           "1234561337\n"};

#define MSEC_IN_SEC 1000
#define USEC_IN_SEC 1000000

unsigned long int usec_diff_time(struct timeval begin, struct timeval end) {
    unsigned long int sec_begin = begin.tv_sec;
    unsigned long int usec_begin = begin.tv_usec;
    
    unsigned long int sec_end = end.tv_sec;
    unsigned long int usec_end = end.tv_usec;

    unsigned long int usec_end_total = usec_end + sec_end * USEC_IN_SEC;
    unsigned long int usec_begin_total = usec_begin + sec_begin * USEC_IN_SEC;

    return (usec_end_total - usec_begin_total);
}

std::string get_time_now() {
    namespace sc = std::chrono;

    auto time = sc::system_clock::now();
    auto since_epoch = time.time_since_epoch();
    auto millis = sc::duration_cast<sc::milliseconds>(since_epoch);
    long now = millis.count();

    return std::to_string(now);
}

int main(int argc, char *argv[]) {

    // server <port> <messages per second>
    if (argc != 3) {
        puts("server_per_ms <port> <messages_per_ms>");
        exit(-1);
    }

    int MESSAGES_PER_MSEC = atoi(argv[2]);
    int PORT = atoi(argv[1]);

    int listenfd = 0, connfd = 0;
    struct sockaddr_in serv_addr; 


    listenfd = socket(AF_INET, SOCK_STREAM, 0);
    memset(&serv_addr, '0', sizeof(serv_addr));

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(PORT); 

    int optval = 1;
    assert( setsockopt(listenfd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval)) != -1);

    if( bind(listenfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) == -1) {
        perror("bind");
    }

    assert ( listen(listenfd, 1) != -1);

    printf("Listening on port %d\n", PORT);

    connfd = accept(listenfd, (struct sockaddr*)NULL, NULL); 

    printf("Accepted connection\n");


    int buf_len = strlen(CONTENT[0]);
    unsigned int counter = 0;

    //struct timeval start,end;
    //gettimeofday(&start, NULL);

    while (1) {
        struct timeval begin, end;
        gettimeofday(&begin, NULL);

        std::string string_to_send;
        std::string id_string;
        for (int i = 0; i < MESSAGES_PER_MSEC; ++i) {

            std::stringstream ss;
            ss << counter;
            string_to_send += get_time_now() + "-" + ss.str() + "\n";

            counter++;
        }
        printf("Writing %s..", string_to_send.c_str());
        write(connfd, string_to_send.c_str(), string_to_send.size());

        gettimeofday(&end, NULL);
        unsigned long int usec_elapsed = usec_diff_time(begin, end);

        printf ("Elapsed: %lu usec. Sent %d\n", usec_elapsed, MESSAGES_PER_MSEC);

/*
        if (usec_elapsed == 0) continue;
        double factor = (double)usec_elapsed / 1000.0;
        MESSAGES_PER_MSEC /= factor;
        if (MESSAGES_PER_MSEC == 0)
            MESSAGES_PER_MSEC = 1;
        */
        ///} else if (usec_elapsed < 1000) { // next time we can send more to fill the whole msec
        //    int factor = 1000 / usec_elapsed;
        //    MESSAGES_PER_MSEC *= factor;
        //}

        if (usec_elapsed < 1000) { // if took more than 1 msec adjust
          usleep(1000-usec_elapsed); // sleep 1 msec
        }
        
        //gettimeofday(&end, NULL);
    }

    close(connfd);
    sleep(1);
    return 0;
}

