// ============================================================================
// Advanced Concurrency Patterns Lab - Solution Files
// Complete implementation of advanced concurrency concepts
// ============================================================================

#include <iostream>
#include <vector>
#include <thread>
#include <future>

#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <functional>
#include <chrono>
#include <random>
#include <string>

using namespace std;

// Global atomic counter for lock-free operations
atomic<int> globalCounter{ 0 };
atomic<bool> stopThreadPool{ false };

// Simple task queue for thread pool
queue<function<void()>> taskQueue;
mutex queueMutex;
condition_variable queueCondition;

// ============================================================================
// PART 1: Futures and Promises for Asynchronous Operations
// ============================================================================

// SOLUTION: Implement async calculation using promise/future
void asyncCalculation(promise<int> resultPromise, int inputValue) {
    // Simulate complex calculation
    this_thread::sleep_for(chrono::milliseconds(100));

    int result = inputValue * inputValue + 100;

    // Set the result in the promise
    resultPromise.set_value(result);
}

// SOLUTION: Create async task that might throw exception
void riskyAsyncTask(promise<string> resultPromise, int riskFactor) {
    try {
        if (riskFactor > 50) {
            throw runtime_error("Risk too high!");
        }

        string result = "Success with risk: " + to_string(riskFactor);
        resultPromise.set_value(result);

    }
    catch (...) {
        // Set exception in promise for caller to handle
        resultPromise.set_exception(current_exception());
    }
}

// ============================================================================
// PART 2: Basic Thread Pool Implementation
// ============================================================================

class SimpleThreadPool {
private:
    vector<thread> workers;
    int numThreads;

public:
    SimpleThreadPool(int threads) : numThreads(threads) {
        initializePool();
    }

    ~SimpleThreadPool() {
        shutdown();
    }

    // SOLUTION: Initialize the thread pool
    void initializePool() {
        // The stop flag is global in this example, so reset it when creating a new pool.
        // Without this, a pool created after a previous shutdown exits before processing tasks.
        stopThreadPool.store(false);

        for (int i = 0; i < numThreads; ++i) {
            workers.emplace_back(&SimpleThreadPool::workerFunction, this);
        }
        cout << "Thread pool initialized with " << numThreads << " worker threads" << endl;
    }

    // SOLUTION: Implement worker function that processes tasks from queue
    void workerFunction() {
        while (true) {  // originally while (!stopThreadPool.load()) {
            function<void()> task;

            {
                unique_lock<mutex> lock(queueMutex);

                // Wait for tasks or shutdown signal
                queueCondition.wait(lock, []() {
                    return !taskQueue.empty() || stopThreadPool.load();
                    });

                // Check if we should stop and queue is empty
                if (stopThreadPool.load() && taskQueue.empty()) {
                    break;
                }

                // The queue is guaranteed to be non-empty at this point.
                task = move(taskQueue.front());
                taskQueue.pop();
            }

            // Execute task outside of lock
            if (task) {
                task();
            }
        }
    }

    // SOLUTION: Add task to the thread pool
    void enqueue(function<void()> task) {
        {
            lock_guard<mutex> lock(queueMutex);
            taskQueue.push(move(task));
        }
        // Notify one waiting worker thread
        queueCondition.notify_one();
    }

    // SOLUTION: Shutdown the thread pool
    void shutdown() {
        // Signal shutdown to all threads
        stopThreadPool.store(true);

        // Wake up all waiting threads
        queueCondition.notify_all();

        // Wait for all threads to complete
        for (auto& worker : workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }

        workers.clear();
        cout << "Thread pool shutdown completed" << endl;
    }
};

// ============================================================================
// PART 3: Atomic Operations and Lock-Free Programming
// ============================================================================

// SOLUTION: Implement lock-free increment function
void atomicIncrement(int iterations) {
    for (int i = 0; i < iterations; ++i) {
        globalCounter.fetch_add(1);  // or simply globalCounter++
    }
}

// SOLUTION: Implement compare-and-swap operation
bool atomicCompareAndSwap(atomic<int>& target, int expected, int desired) {
    return target.compare_exchange_strong(expected, desired);
}

// ============================================================================
// PART 4: Combining Patterns - Async Task Processing
// ============================================================================

// SOLUTION: Process multiple tasks asynchronously and collect results
vector<future<int>> processTasksBatch(SimpleThreadPool& pool, const vector<int>& inputData) {
    vector<future<int>> futures;
    futures.reserve(inputData.size());

    for (int value : inputData) {
        // Create promise/future pair
        auto promise = make_shared<std::promise<int>>();
        future<int> future = promise->get_future();

        // Submit task to thread pool
        pool.enqueue([promise, value]() {
            // Simulate some processing
            this_thread::sleep_for(chrono::milliseconds(50));

            // Calculate result (square the input)
            int result = value * value;

            // Set the promise value
            promise->set_value(result);
            });

        // Store future for result collection
        futures.push_back(move(future));
    }

    return futures;
}

// Test functions for validation
void testFuturesAndPromises() {
    cout << "\n=== Testing Futures and Promises ===" << endl;

    // Test basic promise/future
    promise<int> calcPromise;
    future<int> calcFuture = calcPromise.get_future();

    // Move the promise into the thread because std::promise cannot be copied.
    // The worker thread takes ownership and uses it to deliver the result.
    thread calcThread(asyncCalculation, move(calcPromise), 10);

    cout << "Waiting for async calculation..." << endl;
    int result = calcFuture.get();
    cout << "Calculation result: " << result << " (expected: 200)" << endl;

    calcThread.join();

    // Test exception handling
    promise<string> riskPromise;
    future<string> riskFuture = riskPromise.get_future();

    thread riskThread(riskyAsyncTask, move(riskPromise), 25);

    try {
        string riskResult = riskFuture.get();
        cout << "Risk task result: " << riskResult << endl;
    }
    catch (const exception& e) {
        cout << "Risk task failed: " << e.what() << endl;
    }

    riskThread.join();

    // Test high-risk scenario
    promise<string> highRiskPromise;
    future<string> highRiskFuture = highRiskPromise.get_future();

    thread highRiskThread(riskyAsyncTask, move(highRiskPromise), 75);

    try {
        string highRiskResult = highRiskFuture.get();
        cout << "High risk task result: " << highRiskResult << endl;
    }
    catch (const exception& e) {
        cout << "High risk task failed (expected): " << e.what() << endl;
    }

    highRiskThread.join();

    cout << "- Futures and promises test completed" << endl;
}

void testThreadPool() {
    cout << "\n=== Testing Thread Pool ===" << endl;

    SimpleThreadPool pool(4);

    // Submit several tasks
    for (int i = 1; i <= 8; ++i) {
        pool.enqueue([i]() {
            cout << "Task " << i << " executing on thread "
                << this_thread::get_id() << endl;
            this_thread::sleep_for(chrono::milliseconds(200));
            cout << "Task " << i << " completed" << endl;
            });
    }

    // Let tasks complete
    cout << "Waiting for all tasks to complete..." << endl;
    this_thread::sleep_for(chrono::seconds(3));

    cout << "- Thread pool test completed" << endl;
}

void testAtomicOperations() {
    cout << "\n=== Testing Atomic Operations ===" << endl;

    globalCounter.store(0);

    // Test atomic increment with multiple threads
    vector<thread> incrementThreads;
    for (int i = 0; i < 4; ++i) {
        incrementThreads.emplace_back(atomicIncrement, 1000);
    }

    for (auto& t : incrementThreads) {
        t.join();
    }

    cout << "Expected counter value: 4000" << endl;
    cout << "Actual counter value: " << globalCounter.load() << endl;
    cout << "Atomic operations " << (globalCounter.load() == 4000 ? "PASSED" : "FAILED") << endl;

    // Test compare and swap
    atomic<int> testValue{ 10 };
    bool swapped = atomicCompareAndSwap(testValue, 10, 20);
    cout << "Compare and swap (10->20): " << (swapped ? "SUCCESS" : "FAILED")
        << ", value is now: " << testValue.load() << endl;

    bool notSwapped = atomicCompareAndSwap(testValue, 10, 30);
    cout << "Compare and swap (10->30): " << (notSwapped ? "SUCCESS" : "FAILED")
        << ", value is still: " << testValue.load() << endl;

    cout << "- Atomic operations test completed" << endl;
}

void testCombinedPatterns() {
    cout << "\n=== Testing Combined Patterns ===" << endl;

    SimpleThreadPool pool(3);
    vector<int> inputData = { 1, 2, 3, 4, 5, 6, 7, 8 };

    cout << "Processing batch of " << inputData.size() << " tasks..." << endl;

    auto futures = processTasksBatch(pool, inputData);

    cout << "Collecting results..." << endl;
    for (size_t i = 0; i < futures.size(); ++i) {
        int result = futures[i].get();
        int expected = inputData[i] * inputData[i];
        cout << "Task " << i + 1 << " (input=" << inputData[i] << ") result: "
            << result << " (expected: " << expected << ")" << endl;
    }

    cout << "- Combined patterns test completed" << endl;
}

int main() {
    cout << "=== Advanced Concurrency Patterns Lab ===" << endl;
    cout << "Learning futures/promises, thread pools, and atomic operations" << endl;

    testFuturesAndPromises();
    testThreadPool();
    testAtomicOperations();
    testCombinedPatterns();

    cout << "\n=== Lab Complete! ===" << endl;
    cout << "- Futures and promises for asynchronous result handling" << endl;
    cout << "- Basic thread pool with task queue and worker threads" << endl;
    cout << "- Atomic operations for lock-free programming" << endl;
    cout << "- Combined patterns integrating all concurrency techniques" << endl;
    cout << "\nYou've successfully implemented advanced concurrency patterns!" << endl;

    return 0;
}

/*
VALIDATION CHECKLIST COMPLETED:
- Async calculation implemented with promise.set_value() for result passing

- Exception handling in async tasks using promise.set_exception()

- Thread pool with proper worker thread creation and task queue processing

- Thread-safe task enqueuing with condition variable notification

- Atomic operations for lock-free counter increments without race conditions

- Compare-and-swap implemented using atomic compare_exchange_strong

- Combined patterns showing integration of futures, thread pool, and async processing

- Proper resource management and thread cleanup in all components

EXPECTED OUTPUT EXAMPLE:
=== Advanced Concurrency Patterns Lab ===
Learning futures/promises, thread pools, and atomic operations

=== Testing Futures and Promises ===
Thread pool initialized with 4 worker threads
Waiting for async calculation...
Calculation result: 200 (expected: 200)
Risk task result: Success with risk: 25
High risk task failed (expected): Risk too high!
- Futures and promises test completed

=== Testing Thread Pool ===
Task 1 executing on thread 140234567
Task 1 completed
[... more task executions ...]
- Thread pool test completed

=== Testing Atomic Operations ===
Expected counter value: 4000
Actual counter value: 4000
Atomic operations PASSED
Compare and swap (10->20): SUCCESS, value is now: 20
Compare and swap (10->30): FAILED, value is still: 20
- Atomic operations test completed

=== Testing Combined Patterns ===
Processing batch of 8 tasks...
Collecting results...
Task 1 (input=1) result: 1 (expected: 1)
[... more results ...]
- Combined patterns test completed

=== Lab Complete! ===
You've successfully implemented advanced concurrency patterns!
*/
