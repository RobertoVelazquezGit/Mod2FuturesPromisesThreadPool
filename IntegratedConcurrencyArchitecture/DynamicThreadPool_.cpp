#include "DynamicThreadPool_.h"

#include <iostream>

// ============================================================
// Task
// ============================================================

Task::Task(std::function<void()> func, TaskPriority prio, const std::string& id)
    : function(std::move(func)),
      priority(prio),
      submitTime(std::chrono::steady_clock::now()),
      taskId(id)
{
}

// ============================================================
// TaskComparator
// ============================================================

bool TaskComparator::operator()(const Task& a, const Task& b) const
{
    // Higher priority comes first
    if (a.priority != b.priority)
    {
        return static_cast<int>(a.priority) < static_cast<int>(b.priority);
    }

    // Earlier submission time comes first
    return a.submitTime > b.submitTime;
}

// ============================================================
// DynamicThreadPool
// ============================================================

DynamicThreadPool::DynamicThreadPool(size_t minThreads, size_t maxThreads)
    : minThreads_(minThreads),
      maxThreads_(maxThreads)
{
    // Start with minimum threads
    for (size_t i = 0; i < minThreads; ++i)
    {
        addWorkerThread();
    }

    std::cout << "Dynamic thread pool initialized with " << minThreads
              << " threads (max: " << maxThreads << ")" << std::endl;
}

DynamicThreadPool::~DynamicThreadPool()
{
    shutdown();
}

// ============================================================
// Worker
// ============================================================

void DynamicThreadPool::workerThread()
{
    while (true)
    {
        Task task([]() {}, TaskPriority::LOW);
        bool hasTask = false;

        {
            std::unique_lock<std::mutex> lock(queueMutex_);
            condition_.wait(lock, [this] { return shutdown_.load() || !taskQueue_.empty(); });

            // Exit only when shutdown has been requested and there are no remaining tasks.
            if (shutdown_.load() && taskQueue_.empty())
            {
                break;
            }

            // Queue contains at least one task.
            task = std::move(const_cast<Task&>(taskQueue_.top()));
            taskQueue_.pop();
            hasTask = true;
        }

        if (hasTask)
        {
            activeThreads_.fetch_add(1);
            auto startTime = std::chrono::steady_clock::now();

            try
            {
                task.function();
            }
            catch (const std::exception& e)
            {
                std::cout << "Task " << task.taskId << " failed: " << e.what() << std::endl;
            }
            catch (...)
            {
                std::cout << "Task " << task.taskId << " failed with unknown exception" << std::endl;
            }

            auto endTime = std::chrono::steady_clock::now();
            auto duration = std::chrono::duration<double, std::milli>(endTime - startTime);
            updatePerformanceMetrics(duration.count());
            totalTasksProcessed_.fetch_add(1);
            activeThreads_.fetch_sub(1);
        }
    }

    currentThreads_.fetch_sub(1);
}

// ============================================================
// Performance metrics
// ============================================================

void DynamicThreadPool::updatePerformanceMetrics(double taskDuration)
{
    // Simple exponential moving average
    double currentAvg = averageTaskTime_.load();
    double newAvg = (currentAvg * 0.9) + (taskDuration * 0.1);
    averageTaskTime_.store(newAvg);
}

// ============================================================
// Dynamic scaling
// ============================================================

void DynamicThreadPool::scaleThreadPool()
{
    size_t queueSize = getQueueSize();
    size_t current = currentThreads_.load();
    size_t active = activeThreads_.load();

    // Update high water mark
    size_t currentHighWater = queueHighWaterMark_.load();
    if (queueSize > currentHighWater)
    {
        queueHighWaterMark_.store(queueSize);
    }

    // Scale up if queue is growing and we have capacity.
    if (queueSize > current * 2 && current < maxThreads_.load())
    {
        addWorkerThread();
    }

    // Scale down if threads are mostly idle.
    // Current implementation only detects the condition.
    // Actual thread termination is not implemented.
    if (queueSize == 0 && active < current / 2 && current > minThreads_.load())
    {
        // Controlled thread termination could be implemented here.
    }
}

void DynamicThreadPool::addWorkerThread()
{
    workers_.emplace_back(&DynamicThreadPool::workerThread, this);
    currentThreads_.fetch_add(1);
    std::cout << "Scaled up to " << currentThreads_.load() << " threads" << std::endl;
}

// ============================================================
// Public interface
// ============================================================

size_t DynamicThreadPool::getQueueSize() const
{
    std::lock_guard<std::mutex> lock(queueMutex_);
    return taskQueue_.size();
}

DynamicThreadPool::PoolStats DynamicThreadPool::getStats() const
{
    return PoolStats{
        currentThreads_.load(),
        activeThreads_.load(),
        getQueueSize(),
        totalTasksProcessed_.load(),
        averageTaskTime_.load(),
        queueHighWaterMark_.load()
    };
}

void DynamicThreadPool::shutdown()
{
    shutdown_.store(true);
    condition_.notify_all();

    for (auto& worker : workers_)
    {
        if (worker.joinable())
        {
            worker.join();
        }
    }

    workers_.clear();
    std::cout << "Thread pool shutdown completed" << std::endl;
}

void DynamicThreadPool::printStats() const
{
    auto stats = getStats();

    std::cout << "\n=== Thread Pool Statistics ===" << std::endl;
    std::cout << "Current threads: " << stats.currentThreads << std::endl;
    std::cout << "Active threads: " << stats.activeThreads << std::endl;
    std::cout << "Queue size: " << stats.queueSize << std::endl;
    std::cout << "Total tasks processed: " << stats.totalTasksProcessed << std::endl;
    std::cout << "Average task time: " << stats.averageTaskTime << " ms" << std::endl;
    std::cout << "Queue high water mark: " << stats.queueHighWaterMark << std::endl;
}
