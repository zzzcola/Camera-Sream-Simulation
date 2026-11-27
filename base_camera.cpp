#include <cstring>
#include <pthread.h>

#include <chrono>
#include <cstdio>
#include <thread>
#include <cstdlib>

#include <unistd.h>
#include "libtime/libtime.h"


#define RING_BUFFER_FRAME_COUNT 3

enum v_policy{
    WAIT_THE_MUTEX,
    DROP_THE_NEWEST,
    COVER_THE_OLDEST
};

struct Frame {
    int id;
    long long time_produce;
    long long time_enqueue;
    long long time_dequeue;
};


struct Streambuffer {
    Frame frame[RING_BUFFER_FRAME_COUNT];
    int cur_write;
    int cur_read;
    int count;
    int limit = RING_BUFFER_FRAME_COUNT;
    v_policy policy;
    pthread_mutex_t mtx;
    pthread_cond_t no_empty;
    pthread_cond_t no_full;
    bool shut_down;
    int produce_count, drops, frame_enqueue, cons_wait;
    int dorpped_newest,dropped_oldest,prosessed;
    long long prods_waits_time, cons_wait_time;
};

static Streambuffer sb;

static void sb_init(v_policy p) {
    sb.policy=p;
    pthread_mutex_init(&sb.mtx, nullptr);
    pthread_cond_init(&sb.no_empty, nullptr);
    pthread_cond_init(&sb.no_full, nullptr);
}

static void sb_done() {
    pthread_mutex_lock(&sb.mtx);
    sb.shut_down = 1;
    pthread_cond_broadcast(&sb.no_empty);
    pthread_cond_broadcast(&sb.no_full);
    pthread_mutex_unlock(&sb.mtx);
}

static int frame_put(Frame &frame) {
    pthread_mutex_lock(&sb.mtx);
    // while (sb.count == RING_BUFFER_FRAME_COUNT && !sb.shut_down) {
    //     pthread_cond_wait(&sb.no_full, &sb.mtx);
    // }
    if(sb.shut_down)
    {
        pthread_mutex_unlock(&sb.mtx);
        return -1;
    }
    if(sb.count == RING_BUFFER_FRAME_COUNT){
        switch (sb.policy) {
            case WAIT_THE_MUTEX:
                while (sb.count == RING_BUFFER_FRAME_COUNT && !sb.shut_down) 
                {
                    ++sb.prods_waits_time;
                    pthread_cond_wait(&sb.no_full, &sb.mtx);
                }
                break;
            case DROP_THE_NEWEST:
                sb.drops++;
                sb.produce_count++;
                sb.dorpped_newest++;
                pthread_mutex_unlock(&sb.mtx);
                return 1;
            case COVER_THE_OLDEST:
                sb.cur_read = (sb.cur_read + 1) % RING_BUFFER_FRAME_COUNT;
                sb.count--;
                sb.drops++;
                sb.dropped_oldest++;
                break;
            default:
                printf("err policy\n");
        }
    }


    sb.produce_count++;
    sb.frame_enqueue++;
    sb.frame[sb.cur_write] = frame;
    sb.frame[sb.cur_write].time_enqueue = get_tick_count();
    sb.cur_write = (sb.cur_write + 1) % RING_BUFFER_FRAME_COUNT;
    sb.count++;

    pthread_cond_signal(&sb.no_empty);
    pthread_mutex_unlock(&sb.mtx);
    return  0;
}

static int frame_get(Frame *out) {
    pthread_mutex_lock(&sb.mtx);
    while (sb.count == 0 && !sb.shut_down) {
        sb.cons_wait_time++;
        pthread_cond_wait(&sb.no_empty, &sb.mtx);
    }
    // finish the rest job
    if (sb.count == 0 && sb.shut_down) {
        pthread_mutex_unlock(&sb.mtx);
        return -1;
    }

    *out = sb.frame[sb.cur_read];
    out->time_dequeue=get_tick_count();
    sb.cur_read = (sb.cur_read + 1) % RING_BUFFER_FRAME_COUNT;
    sb.count--;

    pthread_cond_signal(&sb.no_full);
    pthread_mutex_unlock(&sb.mtx);
    return 0;
}

static void *producer(void *) {
    // generate a frame per 40ms
    int i = 0;
    while (i < 100) {
        usleep(40*1000);

        Frame f;
        f.id = i;
        f.time_produce=get_tick_count();
        i++;
        int ret = frame_put(f);
        if(ret == -1){
            printf("get signal shutdown!!!!!!!!!!!!\n");
        } 
        else if ( ret == 1) {
            printf("drop the newest frame!!!!!!!!!!!!\n");
        }
        else 
        {
            printf("frame_put %d\n", f.id);
        }

    }

    printf("shutdown!!!!!!!!!!!!\n");
    sb_done();
    return NULL;
}

static void *cumsumer(void *) {
    // get a frame
    Frame f;
    while (frame_get(&f) == 0) {
        // bianma frame
        sb.prosessed++;
        usleep(100*1000);
        printf("get frame %d,%lld,%lld,%lld\n", f.id,f.time_produce,f.time_enqueue,f.time_dequeue);
        printf("frame %d waits %lld for get in ringbuffer, waits for %lld for get out form ringbuffer\n",f.id,
            f.time_enqueue-f.time_produce, f.time_dequeue-f.time_enqueue);
    }

    printf("over !!!!!!!!\n");
    return NULL;
}

int main(int argc, char *argv[]) {

    if (argc != 2) {
        std::fprintf(stderr, "用法：%s <0|1|2>\n", argv[0]);
        return EXIT_FAILURE;
    }

    v_policy policy;
    if(std::strcmp("0",argv[1]) == 0){
        policy = WAIT_THE_MUTEX;
    } 
    else if(std::strcmp("1",argv[1]) == 0){
        policy = DROP_THE_NEWEST;
    }
    else if(std::strcmp("2", argv[1]) == 0) {
        policy = COVER_THE_OLDEST;
    }
    else{
        printf("err param");
        return -1;
    }

    sb_init(policy);
    init_time();

    pthread_t tp, tc;
    if (pthread_create(&tp, nullptr, producer, nullptr) != 0) {
        printf("err\n");
    }
    if (pthread_create(&tc, nullptr, cumsumer, nullptr) != 0) {
        printf("err\n");
    }

    pthread_join(tp, nullptr);
    pthread_join(tc, nullptr);

    printf("policy=%d, drops=%d, produce_count=%d, frame_enqueue=%d, processed=%d, "
           "dropped_newest=%d, dropped_oldest=%d, prod_waits=%lld, cons_waits=%lld\n",
           (int)sb.policy, sb.drops, sb.produce_count, sb.frame_enqueue, sb.prosessed,
           sb.dorpped_newest, sb.dropped_oldest, sb.prods_waits_time, sb.cons_wait_time);

    pthread_mutex_destroy(&sb.mtx);
    pthread_cond_destroy(&sb.no_empty);
    pthread_cond_destroy(&sb.no_full);

    return 0;
}
