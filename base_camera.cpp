#include <pthread.h>

#include <chrono>
#include <cstdio>
#include <thread>

#include <unistd.h>

#define RING_BUFFER_FRAME_COUNT 3

struct Frame {
    int id;
    long long time_produce;
    long long time_enqueue;
};

static double dur_ms(long long us) { return us / 1000.0; } /* 只为打印好看 */

struct Streambuffer {
    Frame frame[RING_BUFFER_FRAME_COUNT];
    int cur_write;
    int cur_read;
    int count;
    int limit = RING_BUFFER_FRAME_COUNT;
    pthread_mutex_t mtx;
    pthread_cond_t no_empty;
    pthread_cond_t no_full;
    bool shut_down;
    int max_count, drops, prod_waits, cons_wait;
    long long prods_waits_time, cons_wait_time;
};

static Streambuffer sb;

static void sb_init() {
    pthread_mutex_init(&sb.mtx, nullptr);
    pthread_cond_init(&sb.no_empty, nullptr);
    pthread_cond_init(&sb.no_full, nullptr);
}

static void sb_done() {
    pthread_mutex_lock(&sb.mtx);
    sb.shut_down = 1;
    pthread_cond_broadcast(&sb.no_empty);
    pthread_mutex_unlock(&sb.mtx);
}

static void frame_put(Frame &frame) {
    pthread_mutex_lock(&sb.mtx);
    while (sb.count == RING_BUFFER_FRAME_COUNT) {
        pthread_cond_wait(&sb.no_full, &sb.mtx);
    }

    sb.frame[sb.cur_write] = frame;
    sb.cur_write = (sb.cur_write + 1) % RING_BUFFER_FRAME_COUNT;
    sb.count++;

    pthread_cond_signal(&sb.no_empty);
    pthread_mutex_unlock(&sb.mtx);
}

static int frame_get(Frame *out) {
    pthread_mutex_lock(&sb.mtx);
    while (sb.count == 0 && !sb.shut_down) {
        pthread_cond_wait(&sb.no_empty, &sb.mtx);
    }

    if (sb.count == 0 && sb.shut_down) {
        pthread_mutex_unlock(&sb.mtx);
        return -1;
    }

    *out = sb.frame[sb.cur_read];
    sb.cur_read = (sb.cur_read + 1) % RING_BUFFER_FRAME_COUNT;
    sb.count--;

    pthread_cond_signal(&sb.no_full);
    pthread_mutex_unlock(&sb.mtx);
    return 0;
}

static void *producer(void *) {
    // generate a frame per 40ms
    int i = 0;
    while (i <= 10) {
        usleep(40);

        Frame f;
        f.id = i;
        i++;
        frame_put(f);

        printf("frame_put %d\n", f.id);
    }

    printf("shutdown!!!!!!!!!!!!");
    sb_done();
    return NULL;
}

static void *cumsumer(void *) {
    // get a frame
    Frame f;
    while (frame_get(&f) == 0) {
        // bianma frame
        usleep(100);
        printf("get frame %d\n", f.id);
    }

    printf("over !!!!!!!!");
    return NULL;
}

int main() {
    sb_init();

    pthread_t tp, tc;
    if (pthread_create(&tp, nullptr, producer, nullptr) != 0) {
        printf("err\n");
    }
    if (pthread_create(&tc, nullptr, cumsumer, nullptr) != 0) {
        printf("err\n");
    }

    pthread_join(tp, nullptr);
    pthread_join(tc, nullptr);

    pthread_mutex_destroy(&sb.mtx);
    pthread_cond_destroy(&sb.no_empty);
    pthread_cond_destroy(&sb.no_full);

    return 0;
}
