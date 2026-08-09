#pragma once

#include <chrono>
#include <future>
#include <functional>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>


template<typename T>
class AsyncTaskManager
{
private:

    std::vector<std::future<T>> activeTasks_;

    mutable std::mutex tasksMutex_;


public:

    // ============================================================
    // Submit async task
    // ============================================================

    template<typename Func, typename... Args>
    auto submitTask(
        Func&& func,
        Args&&... args) -> std::future<T>
    {
        auto task =
            std::make_shared<std::packaged_task<T()>>(
                std::bind(
                    std::forward<Func>(func),
                    std::forward<Args>(args)...
                )
            );


        std::future<T> future =
            task->get_future();


        // Execute in separate thread
        std::thread(
            [task]()
            {
                try
                {
                    (*task)();
                }
                catch (...)
                {
                    // Exception is automatically captured
                    // by packaged_task.
                }
            }
        ).detach();


        {
            std::lock_guard<std::mutex> lock(tasksMutex_);

            // The future is moved into the manager.
            // The manager becomes its owner.
            activeTasks_.push_back(
                std::move(future)
            );
        }


        return future;
    }


    // ============================================================
    // Wait for all tasks with timeout
    // ============================================================

    std::vector<T> waitForAll(
        std::chrono::milliseconds timeout =
        std::chrono::milliseconds(5000))
    {
        std::vector<T> results;


        std::lock_guard<std::mutex> lock(tasksMutex_);


        for (auto& future : activeTasks_)
        {
            try
            {
                if (future.wait_for(timeout)
                    == std::future_status::ready)
                {
                    // get() consumes the future and retrieves
                    // the result. It can only be called once.
                    results.push_back(
                        future.get()
                    );
                }
                else
                {
                    std::cout
                        << "Task timed out"
                        << std::endl;
                }
            }
            catch (const std::exception& e)
            {
                // future.get() rethrows exceptions generated
                // by the asynchronous task.
                std::cout
                    << "Task failed: "
                    << e.what()
                    << std::endl;
            }
        }


        // Remove all processed futures
        activeTasks_.clear();


        return results;
    }


    // ============================================================
    // Get completion status of all tasks
    // ============================================================

    struct TaskStatus
    {
        size_t completed = 0;
        size_t pending = 0;
        size_t failed = 0;
    };


    TaskStatus getStatus() const
    {
        TaskStatus status;


        std::lock_guard<std::mutex> lock(tasksMutex_);


        for (const auto& future : activeTasks_)
        {
            auto taskStatus =
                future.wait_for(
                    std::chrono::milliseconds(0)
                );


            if (taskStatus == std::future_status::ready)
            {
                status.completed++;
            }
            else
            {
                status.pending++;
            }
        }


        return status;
    }
};

