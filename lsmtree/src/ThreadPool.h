#pragma once
#include <condition_variable>
#include <mutex>
#include <vector>
#include <thread>
#include <queue>
#include <functional>

class ThreadPool{
    public:
        //批量拉起n个线程并让他们沉睡防止开销
        ThreadPool(size_t threads_):stop_(false) {
            workers_.reserve(threads_);
            for(size_t i = 0;i<threads_;i++){
                workers_.emplace_back(
                    [this](){WorkLoop();}
                );
            }
        }
        //前台向线程池投递任务
        void Enqueue(std::function<void()> task){
            {
                std::unique_lock<std::mutex> lock_(queue_mutex_);
                if(stop_) return ;
                tasks_.emplace(std::move(task));
                
            }
            cv_.notify_one();
        }
        //停止线程，唤醒剩余的线程，并且回收
        ~ThreadPool(){
            {
                std::unique_lock<std::mutex> lock_(queue_mutex_);
                stop_ = true;
            }
            cv_.notify_all();
            for(auto& worker : workers_){
                if(worker.joinable()){
                    worker.join();
                }
            }
        }

        ThreadPool(const ThreadPool&) = delete;
        ThreadPool(ThreadPool&&) = delete;
        ThreadPool& operator=(const ThreadPool&) = delete;
        ThreadPool& operator=(ThreadPool&&) = delete;

    private:
        std::vector<std::thread> workers_;
        std::queue<std::function<void()>> tasks_;
        std::mutex queue_mutex_;
        std::condition_variable cv_;
        bool stop_;

        void WorkLoop(){
            std::function<void()> task;
            while(true){
                {
                    std::unique_lock<std::mutex> lock_(queue_mutex_);
                    cv_.wait(lock_,[this](){ return stop_ || !tasks_.empty();});
                    if(stop_ && tasks_.empty()){
                        return ;
                    }
                    task = std::move(tasks_.front());
                    tasks_.pop();
                }
                task();
            }
        }

};