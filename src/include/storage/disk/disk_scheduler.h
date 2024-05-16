
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.h
//
// Identification: src/include/storage/disk/disk_scheduler.h
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <condition_variable>
#include <cstddef>
#include <future>  // NOLINT
#include <mutex>
#include <optional>
#include <queue>
#include <thread>  // NOLINT
#include <thread>
#include <type_traits>
#include <utility>

#include "common/channel.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

/**
 * @brief Represents a Write or Read request for the DiskManager to execute.
 */
struct DiskRequest {
  /** Flag indicating whether the request is a write or a read. */
  bool is_write_;

  /**
   *  Pointer to the start of the memory location where a page is either:
   *   1. being read into from disk (on a read).
   *   2. being written out to disk (on a write).
   */
  char *data_;

  /** ID of the page being read from / written to disk. */
  page_id_t page_id_;

  /** Callback used to signal to the request issuer when the request has been completed. */
  std::promise<bool> callback_;
};

// class IOThreadPool {
//  public:
//   explicit IOThreadPool(size_t n) : stop_(false) {
//     workers_.reserve(n);
//     for (size_t i = 0; i < n; ++i) {
//       workers_.emplace_back([this]() {
//         while (true) {
//           {
//             std::unique_lock lock(queue_mtx_);
//             condition.wait(lock, [this] { return !tasks_.empty() || stop_; });
//             if (stop_) {
//               return;
//             }
            
//           }
//         }
//       });
//     }
//   }

//   void Enqueue();

//   ~IOThreadPool() {
//     {
//       std::unique_lock lock(queue_mtx_);
//       stop_ = true;
//     }
//     condition.notify_all();
//     for (auto &worker : workers_) {
//       worker.join();
//     }
//   }

//  private:
//   bool stop_;
//   std::mutex queue_mtx_;
//   std::condition_variable condition;
//   Channel<std::optional<DiskRequest>> tasks_;
//   std::vector<std::thread> workers_;
// };

/**
 * @brief The DiskScheduler schedules disk read and write operations.
 *
 * A request is scheduled by calling DiskScheduler::Schedule() with an appropriate DiskRequest object. The scheduler
 * maintains a background worker thread that processes the scheduled requests using the disk manager. The background
 * thread is created in the DiskScheduler constructor and joined in its destructor.
 */
class DiskScheduler {
 public:
  explicit DiskScheduler(DiskManager *disk_manager);
  ~DiskScheduler();

  /**
   * TODO(P1): Add implementation
   *
   * @brief Schedules a request for the DiskManager to execute.
   *
   * @param r The request to be scheduled.
   */
  void Schedule(DiskRequest r);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Background worker thread function that processes scheduled requests.
   *
   * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
   * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
   */
  void StartWorkerThread();

  using DiskSchedulerPromise = std::promise<bool>;

  /**
   * @brief Create a Promise object. If you want to implement your own version of promise, you can change this function
   * so that our test cases can use your promise implementation.
   *
   * @return std::promise<bool>
   */
  auto CreatePromise() -> DiskSchedulerPromise { return {}; }

 private:
  /** Pointer to the disk manager. */
  DiskManager *disk_manager_ __attribute__((__unused__));
  /** A shared queue to concurrently schedule and process requests. When the DiskScheduler's destructor is called,
   * `std::nullopt` is put into the queue to signal to the background thread to stop execution. */
  Channel<std::optional<DiskRequest>> request_queue_;
  /** The background thread responsible for issuing scheduled requests to the disk manager. */
  std::optional<std::thread> background_thread_;
};
}  // namespace bustub
