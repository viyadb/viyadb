#ifndef VIYA_UTIL_SCHEDULE_H_
#define VIYA_UTIL_SCHEDULE_H_

#include <atomic>
#include <chrono>
#include <future>

namespace viya {
namespace util {

class Later {
	public:
		template <class Func>
    Later(uint64_t after_ms, Func&& callback) {
      std::thread([after_ms, callback]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(after_ms));
        callback();
      }).detach();
    }
};

class Repeat {
  public:
		template <class Func>
    Repeat(uint64_t every_ms, Func&& callback):running_(true) {
      thread_ = std::thread([every_ms, callback, this]() {
        do {
          std::this_thread::sleep_for(std::chrono::milliseconds(every_ms));
          if (running_) {
            callback();
          }
        } while (running_);
      });
    }

    ~Repeat() {
      running_ = false;
      thread_.join();
    }

  private:
    std::atomic<bool> running_;
    std::thread thread_;
};

class Always {
  public:
		template <class Func>
    Always(Func&& callback):running_(true) {
      thread_ = std::thread([callback, this]() {
        while (running_) {
          callback();
        }
      });
    }

    ~Always() {
      running_ = false;
      thread_.join();
    }

  private:
    std::atomic<bool> running_;
    std::thread thread_;
};

}}

#endif // VIYA_UTIL_SCHEDULE_H_
