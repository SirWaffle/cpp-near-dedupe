#pragma once

#include <queue>
#include <chrono>
#include <mutex>
#include <condition_variable>

//data passed between worker threads
template<class T>
class LockableRingBuffer
{
public:
    LockableRingBuffer(uint32_t capacity)
    {
        vec.resize(capacity);        
    }

    void push(T&& val)
    {
        std::lock_guard<std::mutex> lock(mutex);
        vec.push(val);
        populatedNotifier.notify_one();
    }

    void push_queue(std::queue<T>* inqueue)
    {
        std::lock_guard<std::mutex> lock(mutex);

        while (inqueue->size() > 0)
        {
            vec.push(std::move(inqueue->front()));
            inqueue->pop();
        }

        populatedNotifier.notify_all();
    }

    void push_vec(std::vector<T>* inqueue)
    {
        std::lock_guard<std::mutex> lock(mutex);

#pragma message("change to std::copy")
        while (inqueue->size() > 0)
        {
            vec.push(std::move(inqueue->front()));
            inqueue->pop();
        }

        populatedNotifier.notify_all();
    }

    bool try_pop(T* item, std::chrono::milliseconds timeout = 1)
    {
        std::unique_lock<std::mutex> lock(mutex);

        if (!populatedNotifier.wait_for(lock, timeout, [this] { return !vec.empty(); }))
            return false;

        *item = std::move(vec.front());
        vec.pop();

        return true;
    }

    int try_pop_range(std::queue<T>* nextQueue, int maxToTake, std::chrono::milliseconds timeout = 1)
    {
        std::unique_lock<std::mutex> lock(mutex);

        if (!populatedNotifier.wait_for(lock, timeout, [this] { return !vec.empty(); }))
            return 0;

        int count = 0;
        while (maxToTake-- > 0 && vec.size() > 0)
        {
            ++count;
            nextQueue->push(std::move(vec.front()));
            vec.pop();
        }

        return count;
    }

    int try_pop_range(std::vector<T>& vec, int maxToTake, std::chrono::milliseconds timeout = 1)
    {
        std::unique_lock<std::mutex> lock(mutex);

        if (!populatedNotifier.wait_for(lock, timeout, [this] { return !vec.empty(); }))
            return 0;

#pragma message("change to std::copy")
        int count = 0;
        while (--maxToTake >= 0 && vec.size() > 0)
        {            
            vec[count] = std::move(vec.front());
            ++count;
            vec.pop();
        }

        return count;
    }

    size_t size()
    {
        return (size_t)vec.size();
    }

protected:
    size_t inclusiveHead = 0;
    size_t exclusiveTail = 1;
    std::vector<T> vec;
    std::mutex mutex;
    std::condition_variable populatedNotifier;
};