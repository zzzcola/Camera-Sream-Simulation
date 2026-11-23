#include "libtime.h"

#include <chrono>


static int64_t now_ms()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}


static int64_t start_time = now_ms();

int64_t init_time()
{
    start_time = now_ms();
    return start_time;
}

int64_t get_tick_count()
{
    return now_ms() - start_time;
}
