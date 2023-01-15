#pragma once

#include <queue>
#include <chrono>

//data passed between worker threads
template<class T>
class LockableQueue
{
public:
    void push(T&& val)
    {
        std::lock_guard<std::mutex> lock(mutex);
        queue.push(val);
        populatedNotifier.notify_one();
    }

    bool try_pop(T* item, std::chrono::milliseconds timeout = 1)
    {
        std::unique_lock<std::mutex> lock(mutex);

        if (!populatedNotifier.wait_for(lock, timeout, [this] { return !queue.empty(); }))
            return false;

        *item = std::move(queue.front());
        queue.pop();

        return true;
    }

    int try_pop_range(std::queue<T>* nextQueue, int maxToTake, std::chrono::milliseconds timeout = 1)
    {
        std::unique_lock<std::mutex> lock(mutex);

        if (!populatedNotifier.wait_for(lock, timeout, [this] { return !queue.empty(); }))
            return 0;

        int count = 0;
        while (maxToTake-- > 0 && queue.size() > 0)
        {
            ++count;
            nextQueue->push(std::move(queue.front()));
            queue.pop();
        }

        return count;
    }

    volatile int Length()
    {
        return (int)queue.size();
    }

protected:
    std::queue<T> queue;
    std::mutex mutex;
    std::condition_variable populatedNotifier;
};