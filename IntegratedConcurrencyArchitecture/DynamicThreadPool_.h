#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>


enum class TaskPriority
{
    LOW = 1,
    NORMAL = 2,
    HIGH = 3,
    CRITICAL = 4
};


struct Task
{
    std::function<void()> function;
    TaskPriority priority;
    std::chrono::steady_clock::time_point submitTime;
    std::string taskId;

    Task(std::function<void()> func,
        TaskPriority prio,
        const std::string& id = "");

    Task(const Task&) = default;
    Task& operator=(const Task&) = default;

    Task(Task&&) = default;
    Task& operator=(Task&&) = default;
};


struct TaskComparator
{
    bool operator()(const Task& a, const Task& b) const;
};


class DynamicThreadPool
{
private:

    std::vector<std::thread> workers_;

    std::priority_queue<
        Task,
        std::vector<Task>,
        TaskComparator
    > taskQueue_;

    mutable std::mutex queueMutex_;
    std::condition_variable condition_;

    std::atomic<bool> shutdown_{ false };
    std::atomic<size_t> activeThreads_{ 0 };
    std::atomic<size_t> totalTasksProcessed_{ 0 };

    // Dynamic scaling parameters
    std::atomic<size_t> minThreads_;
    std::atomic<size_t> maxThreads_;
    std::atomic<size_t> currentThreads_{ 0 };

    // Performance monitoring
    std::atomic<double> averageTaskTime_{ 0.0 };
    std::atomic<size_t> queueHighWaterMark_{ 0 };


    void workerThread();

    void updatePerformanceMetrics(double taskDuration);

    void scaleThreadPool();

    void addWorkerThread();


public:

    struct PoolStats
    {
        size_t currentThreads;
        size_t activeThreads;
        size_t queueSize;
        size_t totalTasksProcessed;
        double averageTaskTime;
        size_t queueHighWaterMark;
    };


    DynamicThreadPool(
        size_t minThreads = 2,
        size_t maxThreads =
        std::thread::hardware_concurrency() * 2
    );

    ~DynamicThreadPool();


    template<typename Func>
    void submit(
        Func&& func,
        TaskPriority priority = TaskPriority::NORMAL,
        const std::string& taskId = "")
    {
        {
            std::lock_guard<std::mutex> lock(queueMutex_);

            taskQueue_.emplace(
                std::forward<Func>(func),
                priority,
                taskId
            );
        }

        condition_.notify_one();

        // Trigger scaling evaluation
        scaleThreadPool();
    }


    size_t getQueueSize() const;

    PoolStats getStats() const;

    void shutdown();

    void printStats() const;
};

