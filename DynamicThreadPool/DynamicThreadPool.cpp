/*
Build an enterprise - grade thread pool that supports priority - based task scheduling, dynamic thread scaling, and comprehensive resource management for high - load scenarios.
Practice
Using the code below, create a load testing scenario that :
Submits tasks with different priorities and processing times
Monitors thread pool scaling behavior under varying loads
Compares performance with different min / max thread configurations
Analyzes queue dynamics and thread utilization patterns
Test with burst loads, sustained loads, and mixed priority scenarios.
*/

#include <queue>
#include <condition_variable>
#include <atomic>
#include <thread>
#include <functional>
#include <iostream>
#include <chrono>
#include <string>

//#define BASIC_TEST_001
//#define PRIORITY_TEST_002
//#define BURST_LOAD_TEST_003
#define SUSTAINED_LOAD_TEST_004

#if (defined(BASIC_TEST_001) + \
     defined(PRIORITY_TEST_002) + \
     defined(BURST_LOAD_TEST_003) + \
     defined(SUSTAINED_LOAD_TEST_004)) != 1
#error "Exactly one test must be enabled."
#endif

enum class TaskPriority {
    LOW = 1,
    NORMAL = 2,
    HIGH = 3,
    CRITICAL = 4
};

struct Task {
    std::function<void()> function;
    TaskPriority priority;
    std::chrono::steady_clock::time_point submitTime;
    std::string taskId;

    Task(std::function<void()> func,
        TaskPriority prio,
        const std::string& id = "")
        : function(std::move(func)),
        priority(prio),
        submitTime(std::chrono::steady_clock::now()),
        taskId(id)
    {
    }

    Task(const Task&) = default;
    Task& operator=(const Task&) = default;

    Task(Task&&) = default;
    Task& operator=(Task&&) = default;
};

struct TaskComparator {
    // For std::priority_queue: if compare(a, b) is true,
    // 'b' has higher priority than 'a'.
    bool operator()(const Task& a, const Task& b) const {
        if (a.priority != b.priority) {
            // if a.priority < b.priority --> true --> b goes before a in the priority queue    
            return static_cast<int>(a.priority) < static_cast<int>(b.priority);
        }
        // Earlier timestamp means higher priority (e.g. 10:00:00 before 10:00:05).
        // If (a.timestamp=10:00:00) > (b.timestamp=10:00:05) --> false --> a goes before b in the priority queue   
        return a.submitTime > b.submitTime; // Earlier submission has higher priority
    }
};

class DynamicThreadPool {
private:
    std::vector<std::thread> workers_;
    std::priority_queue<Task, std::vector<Task>, TaskComparator> taskQueue_;

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

#ifdef PRIORITY_TEST_002
    std::atomic<bool> startProcessing_{ false };
#endif

    void workerThread()
    {
        while (true)
        {
            Task task([]() {}, TaskPriority::LOW);
            bool hasTask = false;

            {
                std::unique_lock<std::mutex> lock(queueMutex_);

                condition_.wait(lock, [this]
                    {
#ifdef PRIORITY_TEST_002
                        return shutdown_.load() ||
                            (startProcessing_.load() && !taskQueue_.empty());
#else
                        return shutdown_.load() || !taskQueue_.empty();
#endif
                    });

                // Exit only when shutdown has been requested
                // and there are no remaining tasks to process.
                if (shutdown_.load() && taskQueue_.empty())
                {
                    break;
                }

                // At this point, the queue is guaranteed to contain at least one task.
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
                    std::cout << "Task " << task.taskId
                        << " failed: " << e.what() << std::endl;
                }
                catch (...)
                {
                    std::cout << "Task " << task.taskId
                        << " failed with unknown exception" << std::endl;
                }

                auto endTime = std::chrono::steady_clock::now();
                auto duration =
                    std::chrono::duration<double, std::milli>(endTime - startTime);

                updatePerformanceMetrics(duration.count());

                totalTasksProcessed_.fetch_add(1);
                activeThreads_.fetch_sub(1);
            }
        }

        currentThreads_.fetch_sub(1);
    }

    void updatePerformanceMetrics(double taskDuration) {
        // Simple exponential moving average
        double currentAvg = averageTaskTime_.load();
        double newAvg = (currentAvg * 0.9) + (taskDuration * 0.1);
        averageTaskTime_.store(newAvg);
    }

    void scaleThreadPool() {
        size_t queueSize = getQueueSize();
        size_t current = currentThreads_.load();
        size_t active = activeThreads_.load();

        // Update high water mark
        size_t currentHighWater = queueHighWaterMark_.load();
        if (queueSize > currentHighWater) {
            queueHighWaterMark_.store(queueSize);
        }

        // Scale up if queue is growing and we have capacity
        if (queueSize > current * 2 && current < maxThreads_.load()) {
            addWorkerThread();
        }

        // Scale down if threads are mostly idle (simplified logic)
        if (queueSize == 0 && active < current / 2 && current > minThreads_.load()) {
            // In a real implementation, we'd implement controlled thread termination
            // For simplicity, we'll just track that we could scale down
        }
    }

    void addWorkerThread() {
        workers_.emplace_back(&DynamicThreadPool::workerThread, this);
        currentThreads_.fetch_add(1);
        std::cout << "Scaled up to " << currentThreads_.load() << " threads" << std::endl;
    }

public:
    DynamicThreadPool(size_t minThreads = 2, size_t maxThreads = std::thread::hardware_concurrency() * 2)
        : minThreads_(minThreads), maxThreads_(maxThreads) {

        // Start with minimum threads
        for (size_t i = 0; i < minThreads; ++i) {
            addWorkerThread();
        }

        std::cout << "Dynamic thread pool initialized with " << minThreads << " threads (max: " << maxThreads << ")" << std::endl;
    }

    ~DynamicThreadPool() {
        shutdown();
    }

    template<typename Func>
    void submit(Func&& func, TaskPriority priority = TaskPriority::NORMAL, const std::string& taskId = "") {
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
			taskQueue_.emplace(std::forward<Func>(func), priority, taskId);  // building a Task object in place 
        }

        condition_.notify_one();

        // Trigger scaling evaluation
        scaleThreadPool();
    }

    size_t getQueueSize() const {
        std::lock_guard<std::mutex> lock(queueMutex_);
        return taskQueue_.size();
    }

    struct PoolStats {
        size_t currentThreads;
        size_t activeThreads;
        size_t queueSize;
        size_t totalTasksProcessed;
        double averageTaskTime;
        size_t queueHighWaterMark;
    };

    PoolStats getStats() const {
        return PoolStats{
            currentThreads_.load(),
            activeThreads_.load(),
            getQueueSize(),
            totalTasksProcessed_.load(),
            averageTaskTime_.load(),
            queueHighWaterMark_.load()
        };
    }

    void shutdown() {
        shutdown_.store(true);
        condition_.notify_all();

        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }

        workers_.clear();
        std::cout << "Thread pool shutdown completed" << std::endl;
    }

    void printStats() const {
        auto stats = getStats();
        std::cout << "\n=== Thread Pool Statistics ===" << std::endl;
        std::cout << "Current threads: " << stats.currentThreads << std::endl;
        std::cout << "Active threads: " << stats.activeThreads << std::endl;
        std::cout << "Queue size: " << stats.queueSize << std::endl;
        std::cout << "Total tasks processed: " << stats.totalTasksProcessed << std::endl;
        std::cout << "Average task time: " << stats.averageTaskTime << " ms" << std::endl;
        std::cout << "Queue high water mark: " << stats.queueHighWaterMark << std::endl;
    }

#ifdef PRIORITY_TEST_002
    void startProcessing()
    {
        startProcessing_.store(true);
        condition_.notify_all();
    }
#endif
};

#ifdef BASIC_TEST_001
int main()
{
    DynamicThreadPool pool(2, 4);

    pool.submit(
        []()
        {
            std::cout << "LOW task started\n";
            std::this_thread::sleep_for(std::chrono::milliseconds(800));
            std::cout << "LOW task finished\n";
        },
        TaskPriority::LOW,
        "LOW_1");

    pool.submit(
        []()
        {
            std::cout << "HIGH task started\n";
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            std::cout << "HIGH task finished\n";
        },
        TaskPriority::HIGH,
        "HIGH_1");

    pool.submit(
        []()
        {
            std::cout << "CRITICAL task started\n";
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            std::cout << "CRITICAL task finished\n";
        },
        TaskPriority::CRITICAL,
        "CRITICAL_1");

    pool.submit(
        []()
        {
            std::cout << "NORMAL task started\n";
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            std::cout << "NORMAL task finished\n";
        },
        TaskPriority::NORMAL,
        "NORMAL_1");

    std::this_thread::sleep_for(std::chrono::seconds(3));

    pool.shutdown();

    return 0;
}
#elif defined(PRIORITY_TEST_002)
int main()
{
    // Single worker thread to clearly demonstrate priority scheduling.
    DynamicThreadPool pool(1, 1);

    std::cout << "\n=== Priority Scheduling Test ===\n\n";

    pool.submit(
        []()
        {
            std::cout << "Executing LOW task\n";
        },
        TaskPriority::LOW,
        "LOW");

    pool.submit(
        []()
        {
            std::cout << "Executing NORMAL task\n";
        },
        TaskPriority::NORMAL,
        "NORMAL");

    pool.submit(
        []()
        {
            std::cout << "Executing HIGH task\n";
        },
        TaskPriority::HIGH,
        "HIGH");

    pool.submit(
        []()
        {
            std::cout << "Executing CRITICAL task\n";
        },
        TaskPriority::CRITICAL,
        "CRITICAL");

    // All tasks have been queued.
    // Allow worker threads to start processing them.
    pool.startProcessing();

    // Allow all tasks to complete.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    pool.shutdown();

    return 0;
}
#elif defined(BURST_LOAD_TEST_003)
int main()
{
    constexpr size_t NUM_TASKS = 100;

    DynamicThreadPool pool(2, 8);

    std::cout << "\n=== Burst Load Test ===\n";
    std::cout << "Submitting " << NUM_TASKS << " tasks...\n\n";

    for (size_t i = 0; i < NUM_TASKS; ++i)
    {
        TaskPriority priority;

        switch (i % 4)
        {
        case 0: priority = TaskPriority::LOW;      break;
        case 1: priority = TaskPriority::NORMAL;   break;
        case 2: priority = TaskPriority::HIGH;     break;
        default:priority = TaskPriority::CRITICAL; break;
        }

        pool.submit(
            [i]()
            {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(100));
            },
            priority,
            "Task_" + std::to_string(i));
    }

    // Monitor the thread pool while it is processing the burst.
    while (true)
    {
        auto stats = pool.getStats();

        pool.printStats();

        if (stats.totalTasksProcessed >= NUM_TASKS)
            break;

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    pool.shutdown();

    return 0;
}
#elif defined(SUSTAINED_LOAD_TEST_004)  
int main()
{
    constexpr auto TEST_DURATION = std::chrono::seconds(10);

    DynamicThreadPool pool(2, 8);

    std::cout << "\n=== Sustained Load Test ===\n\n";

    auto start = std::chrono::steady_clock::now();

    size_t taskId = 0;

    // std::chrono handles unit conversion automatically during the comparison.
    while (std::chrono::steady_clock::now() - start < TEST_DURATION)
    {
        TaskPriority priority;

        switch (taskId % 4)
        {
        case 0: priority = TaskPriority::LOW;      break;
        case 1: priority = TaskPriority::NORMAL;   break;
        case 2: priority = TaskPriority::HIGH;     break;
        default:priority = TaskPriority::CRITICAL; break;
        }

        pool.submit(
            [/*taskId*/]()
            {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(150));
            },
            priority,
            "Task_" + std::to_string(taskId));

        ++taskId;

        // Generate a constant stream of incoming tasks.
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        if ((taskId % 20) == 0)
        {
            pool.printStats();
            std::cout << std::endl;
        }
    }

    std::cout << "\nWaiting for remaining tasks...\n";

    while (pool.getStats().queueSize > 0 ||
        pool.getStats().activeThreads > 0)
    {
        pool.printStats();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    std::cout << "\nFinal statistics\n";
    pool.printStats();

    pool.shutdown();

    return 0;
}
#endif

