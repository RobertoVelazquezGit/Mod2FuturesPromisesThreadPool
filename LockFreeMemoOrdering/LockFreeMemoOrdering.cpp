/*
Implement sophisticated lock - free data structures using advanced atomic operations and memory ordering semantics for maximum performance in high - contention scenarios.
Practice
In the code below, compare performance of lock - free vs lock - based data structures :
Benchmark lock - free queue vs std::queue with mutex protection
Test with varying numbers of producer and consumer threads
Analyze memory ordering effects on performance
Measure contention and scalability characteristics
Experiment with different memory ordering constraints and their impact on correctness and performance.
*/

//#define BASIC_MAIN_TEST_001
#define TEST_CONFIGURATIONS_002

#if (defined(BASIC_MAIN_TEST_001) + \
     defined(TEST_CONFIGURATIONS_002) + \
     defined(BURST_LOAD_TEST_003) + \
     defined(SUSTAINED_LOAD_TEST_004)) != 1
#error "Exactly one test must be enabled."
#endif


#include <atomic>
#include <memory>
#include <type_traits>
#include <ostream>
#include <thread>
#include <vector>
#include <iostream>
#include <chrono>
#include <queue>
#include <mutex>
#include <array>
#include <array>
#include <iostream>

// Michael & Scott Lock-Free Queue algorithm    
// head                             tail
// |                                |
// v                                v
// Dummy-- > Node1-- > Node2-- > Node3*/
template<typename T>
class LockFreeQueue {
private:
    struct Node {
        std::atomic<T*> data{ nullptr };
        std::atomic<Node*> next{ nullptr };

        Node() = default;

        explicit Node(T item) {
            data.store(new T(std::move(item)), std::memory_order_relaxed);
        }
    };

    std::atomic<Node*> head_;
    std::atomic<Node*> tail_;
    std::atomic<size_t> size_{ 0 };

    // Memory reclamation using hazard pointers (simplified)
    static constexpr size_t MAX_HAZARD_POINTERS = 16;
    // thread_local: Each thread has its own independent instance of this variable.
    // std::array is like a vector of fixed size, each element is an atomic pointer to Node 
	thread_local static std::array<std::atomic<Node*>, MAX_HAZARD_POINTERS> hazardPointers;  
    static std::atomic<size_t> hazardPointerIndex;

    Node* acquireHazardPointer(Node* node) {
        size_t index = hazardPointerIndex.fetch_add(1, std::memory_order_relaxed) % MAX_HAZARD_POINTERS;
        hazardPointers[index].store(node, std::memory_order_release);
        return node;
    }

    void releaseHazardPointer(Node* node) {
        for (auto& hp : hazardPointers) {
            if (hp.load(std::memory_order_acquire) == node) {
                hp.store(nullptr, std::memory_order_release);
                break;
            }
        }
    }

    bool isHazardous(Node* node) {
        for (const auto& hp : hazardPointers) {
            if (hp.load(std::memory_order_acquire) == node) {
                return true;
            }
        }
        return false;
    }

public:
    LockFreeQueue() {
		// the queue is initialized with a dummy node to simplify the enqueue and dequeue operations    
        Node* dummy = new Node();
        head_.store(dummy, std::memory_order_relaxed);
        tail_.store(dummy, std::memory_order_relaxed);
    }

    ~LockFreeQueue() {
        while (Node* node = head_.load(std::memory_order_relaxed)) {
            head_.store(node->next.load(std::memory_order_relaxed), std::memory_order_relaxed);
            delete node;
        }
    }

    void enqueue(T item) {
        // Moves the item (T), not the Node.
        // Node(Node&&) would only be called by:
        // Node n1("Hello");
        // Node n2(std::move(n1));
        Node* newNode = new Node(std::move(item));

        while (true) {
            Node* last = tail_.load(std::memory_order_acquire);
            Node* next = last->next.load(std::memory_order_acquire);

            if (last == tail_.load(std::memory_order_acquire)) {
                if (next == nullptr) {
// Simplified prototype bool compare_exchange_weak(T& expected, T desired);
// Internally :
// if (counter == expected)
// {
//    counter = desired;
//    return true;
// }
// else
// {
//    expected = counter;
//    return false;
// }
                    // Try to link new node at the end of the list
                    if (last->next.compare_exchange_weak(next, newNode,
                        std::memory_order_release /*success_order*/,
                        std::memory_order_relaxed /*failure_order*/)) {
                        // Successfully added new node, try to swing tail
                        tail_.compare_exchange_weak(last, newNode,
                            std::memory_order_release,
                            std::memory_order_relaxed);
                        size_.fetch_add(1, std::memory_order_relaxed);
                        break;
                    }
                }
                else {
                    // Tail is lagging behind, try to advance it
                    tail_.compare_exchange_weak(last, next,
                        std::memory_order_release,
                        std::memory_order_relaxed);
                }
            }
        }
    }

    bool dequeue(T& result) {
        while (true) {
            Node* first = head_.load(std::memory_order_acquire);
            Node* last = tail_.load(std::memory_order_acquire);
            Node* next = first->next.load(std::memory_order_acquire);

            if (first == head_.load(std::memory_order_acquire)) {
                if (first == last) {
                    if (next == nullptr) {
                        // Queue is empty
                        return false;
                    }
                    // Tail is lagging behind, advance it
                    tail_.compare_exchange_weak(last, next,
                        std::memory_order_release,
                        std::memory_order_relaxed);
                }
                else {
                    // Read data before potential dequeue
                    if (next == nullptr) {
                        continue;
                    }

                    T* data = next->data.load(std::memory_order_acquire);
                    if (data == nullptr) {
                        continue;
                    }

                    // Try to swing head to next node
					// next becomes the new dummy node, and first can be safely deleted if no hazard pointers point to it   
                    if (head_.compare_exchange_weak(first, next,
                        std::memory_order_release,
                        std::memory_order_relaxed)) {
                        result = *data;  
                        delete data;
                        size_.fetch_sub(1, std::memory_order_relaxed);

                        // Safe to reclaim first node (simplified - in production use proper hazard pointers)
                        if (!isHazardous(first)) {
                            // TODO:
                            // Memory reclamation is intentionally disabled.
                            // This avoids use-after-free while studying the lock-free algorithm.
                            // Commented out delete first;
                        }

                        return true;
                    }
                }
            }
        }
    }

//    head
//     |
//     v
//    +-------+     +------+     +------+
//    | Dummy | --> |  10  | --> | 20   |
//    +-------+     +------+     +------+
//                                ^
//                                |
//                               tail

    bool empty() const {
        return size_.load(std::memory_order_acquire) == 0;
    }

    size_t size() const {
        return size_.load(std::memory_order_acquire);
    }
};

// Just initializing the static members of the LockFreeQueue class template 

// Thread-local storage initialization
template<typename T>
thread_local std::array<std::atomic<typename /*typename because Node is a type*/LockFreeQueue<T>::Node*>, LockFreeQueue<T>::MAX_HAZARD_POINTERS>
LockFreeQueue<T>::hazardPointers{};

template<typename T>
std::atomic<size_t> LockFreeQueue<T>::hazardPointerIndex{ 0 };

// Treiber Stack algorithm for lock-free stack implementation   
// Lock-free stack for comparison
template<typename T>
class LockFreeStack {
private:
    struct Node {
        T data;
        Node* next;

        Node(T item) : data(std::move(item)), next(nullptr) {}
    };

    std::atomic<Node*> head_{ nullptr };
    std::atomic<size_t> size_{ 0 };

public:
    void push(T item) {
        Node* newNode = new Node(std::move(item));
        newNode->next = head_.load(std::memory_order_relaxed/*the same here*/);

        while (!head_.compare_exchange_weak(newNode->next, newNode,
            std::memory_order_release,
            std::memory_order_relaxed/*the same here, this is when fail*/)) {
            // Loop until successful
        }
        size_.fetch_add(1, std::memory_order_relaxed);
        // Simplified prototype bool compare_exchange_weak(T& expected, T desired);
        // Internally :
        // if (counter == expected)
        // {
        //    counter = desired;
        //    return true;
        // }
        // else
        // {
        //    expected = counter;
        //    return false;
        // }
        //    head
        //    |
        //    v
        //    +------+
        //    | 30   |
        //    +------+
        //    |
        //    v
        //    +------+
        //    | 20   |
        //    +------+
        //    |
        //    v
        //    +------+
        //    | 10   |
        //    +------+
        //    |
        //    v
        //    nullptr
    }

    bool pop(T& result) {
        Node* head = head_.load(std::memory_order_acquire);

        while (head && !head_.compare_exchange_weak(head, head->next,
            std::memory_order_release,
            std::memory_order_relaxed)) {
            // Retry with updated head
        }

        if (!head) {
            return false;
        }

        result = std::move(head->data);
        size_.fetch_sub(1, std::memory_order_relaxed);

        // In production, use proper memory reclamation
        delete head;
        return true;
    }

    bool empty() const {
        return head_.load(std::memory_order_acquire) == nullptr;
    }

    size_t size() const {
        return size_.load(std::memory_order_acquire);
    }
};

template<typename T>
class MutexQueue
{
private:
    std::queue<T> queue_;
    mutable std::mutex mutex_;

public:

    MutexQueue() = default;

    void enqueue(T item)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push(std::move(item));
    }

    bool dequeue(T& result)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (queue_.empty())
        {
            return false;
        }

        result = std::move(queue_.front());
        queue_.pop();

        return true;
    }

    bool empty() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.empty();
    }

    size_t size() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }
};

// Performance benchmarking utility
class LockFreeBenchmark {
public:
    template<typename Container>
    static void benchmarkContainer(const std::string& containerName,
        int operations, int producerThreads, int consumerThreads) {
        Container container;
        std::atomic<bool> start{ false };
        std::atomic<int> itemsProduced{ 0 };
        std::atomic<int> itemsConsumed{ 0 };

        auto startTime = std::chrono::high_resolution_clock::now();

        // Producer threads
        std::vector<std::thread> producers;
        for (int i = 0; i < producerThreads; ++i) {
            producers.emplace_back([&, i]() {
                while (!start.load()) { /* spin wait */ }

                int itemsPerProducer = operations / producerThreads;
                for (int j = 0; j < itemsPerProducer; ++j) {
                    // Commented out container.push(i * 1000 + j);  // Do not use LockFreeStack
					container.enqueue(i * 1000 + j);    
                    itemsProduced.fetch_add(1);
                }
                });
        }

        // Consumer threads
        std::vector<std::thread> consumers;
        for (int i = 0; i < consumerThreads; ++i) {
            consumers.emplace_back([&]() {
                while (!start.load()) { /* spin wait */ }

                int item;
                while (itemsConsumed.load() < operations) {
                    // Commented out if (container.pop(item)) { Do not use LockFreeStack
                    if (container.dequeue(item)) {
                        itemsConsumed.fetch_add(1);
                    }
                    else {
                        std::this_thread::yield();
                    }
                }
                });
        }

        // Start benchmark
        start.store(true);

        // Wait for completion
        for (auto& t : producers) t.join();
        for (auto& t : consumers) t.join();

        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

        std::cout << containerName << " Benchmark Results:" << std::endl;
        std::cout << "  Operations: " << operations << std::endl;
        std::cout << "  Producer threads: " << producerThreads << std::endl;
        std::cout << "  Consumer threads: " << consumerThreads << std::endl;
        std::cout << "  Duration: " << duration.count() << " ms" << std::endl;
        std::cout << "  Items produced: " << itemsProduced.load() << std::endl;
        std::cout << "  Items consumed: " << itemsConsumed.load() << std::endl;
        std::cout << "  Throughput: " << (operations * 1000.0 / duration.count()) << " ops/sec" << std::endl;
        std::cout << std::endl;
    }
};

#ifdef BASIC_MAIN_TEST_001
int main()
{
    constexpr int OPERATIONS = 100000;

    LockFreeBenchmark::benchmarkContainer<LockFreeQueue<int>>(
        "LockFreeQueue",
        OPERATIONS,
        2,
        2);

    LockFreeBenchmark::benchmarkContainer<MutexQueue<int>>(
        "MutexQueue",
        OPERATIONS,
        2,
        2);

    return 0;
}
#elif defined(TEST_CONFIGURATIONS_002)
int main()
{
	constexpr int OPERATIONS = 1'000'000;  // digit separator for readability from c++14

    struct TestConfiguration
    {
        int producerThreads;
        int consumerThreads;
    };

    // Fixed benchmark configurations
    constexpr std::array<TestConfiguration, 6> testConfigurations
    { {
        {1, 1},
        {2, 2},
        {4, 4},
        {8, 8},
        {1, 4},
        {4, 1}
    } };

    // Execute all benchmark configurations
    for (const auto& test : testConfigurations)
    {
        std::cout << "\n=====================================================\n";
        std::cout << "Configuration\n";
        std::cout << "  Producer threads : " << test.producerThreads << '\n';
        std::cout << "  Consumer threads : " << test.consumerThreads << '\n';
        std::cout << "  Operations       : " << OPERATIONS << '\n';
        std::cout << "=====================================================\n\n";

        // Benchmark the lock-free queue
        LockFreeBenchmark::benchmarkContainer<LockFreeQueue<int>>(
            "LockFreeQueue",
            OPERATIONS,
            test.producerThreads,
            test.consumerThreads);


        // Benchmark the mutex-protected queue
        LockFreeBenchmark::benchmarkContainer<MutexQueue<int>>(
            "MutexQueue",
            OPERATIONS,
            test.producerThreads,
            test.consumerThreads);
    }

    return 0;
}
#endif

