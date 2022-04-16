#pragma once

#include "thread"
#include "sys/mman.h"
#include "sys/syscall.h"
#include "unistd.h"

#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "rocksdb/metadata.h"

#include "workloadwrapper.h"

#include <sys/time.h>
#include <cstring>

namespace ycsbc{

#define TIME_NOW (std::chrono::high_resolution_clock::now())
#define TIME_DURATION(start, end) (std::chrono::duration<double>((end)-(start)).count() * 1000 * 1000)
typedef std::chrono::high_resolution_clock::time_point TimePoint;

#if defined(__GNUC__) && __GNUC__ >= 4
	#define LIKELY(x)	 (__builtin_expect((x), 1))
	#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
	#define LIKELY(x)   (x)
	#define UNLIKELY(x) (x)
#endif

inline void *AllocBuffer(size_t capacity){
    void *buffer = mmap(nullptr, capacity, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    assert(buffer != MAP_FAILED);
    return buffer;
}

inline void DeallocBuffer(void *buffer, size_t capacity){
    assert(munmap(buffer, capacity) == 0);
}

class RocksDBClient{
	public:
		WorkloadProxy *workload_proxy_;
		KVStore* db_;

		const int loader_threads_;
		const uint64_t load_num_;
		const int worker_threads_;
		const uint64_t request_num_;
		double tput_;
		const double warmup_rate_ = 0.3;

		WorkloadWrapper *workload_wrapper_ = nullptr;

		double wal_time_;
		double wait_time_;
		double complete_memtable_time_;
		double block_read_time_;
		double write_delay_time_;
		double write_memtable_time_;
		std::mutex mutex_;
		uint64_t submit_time_;
		uint64_t warmup_finished_ = 0;

		struct TimeRecord{
		  private:
			double *data_;
			const uint64_t capacity_;
			uint64_t size_;
			bool sorted;
		  public:
			TimeRecord(uint64_t capacity):
				data_(nullptr),
				capacity_(capacity),
				size_(0),
				sorted(false){
					data_ = (double *)AllocBuffer(capacity_ * sizeof(double));
					assert(data_ != nullptr);
			}
			~TimeRecord(){
				assert(size_ <= capacity_);
				if(data_ != nullptr)
					DeallocBuffer(data_, capacity_ * sizeof(double));
			}
			void Insert(double time){
				data_[size_++] = time;
				assert(size_ <= capacity_);
				sorted = false;
			}
			double *Data(){
				return data_;
			}
			void Join(TimeRecord* time){
				assert(data_ != nullptr);
				assert(time->Data() != nullptr);
				uint64_t pos = __sync_fetch_and_add(&size_, time->Size());
				assert(size_ <= capacity_);
				memcpy(data_ + pos, time->Data(), sizeof(double) * time->Size());
				sorted = false;
			}
			double Sum(){
				assert(data_ != nullptr);
				return std::accumulate(data_, data_ + size_, 0.0);
			}
			uint64_t Size(){
				return size_;
			}
			double Tail(double f){
				assert(data_ != nullptr);
				if(!sorted){
					std::sort(data_, data_ + size_);
					sorted = true;
				}
				return data_[(uint64_t)(size_ * f)];
			}
		};

		TimeRecord *request_time_  = nullptr;
		TimeRecord *read_time_  = nullptr;
		TimeRecord *update_time_ = nullptr;
		TimeRecord *iops_ = nullptr;
		std::atomic<uint64_t> total_finished_requests_;
		std::atomic<uint64_t> total_write_latency;
		std::atomic<uint64_t> total_read_latency;
		std::atomic<uint64_t> write_finished;
		std::atomic<uint64_t> read_finished;


		TimeRecord *insert_failed_ = nullptr;

	public:
		int id_;
		RocksDBClient(WorkloadProxy *workload_proxy, int loader_threads, uint64_t load_num, int worker_threads,
                      uint64_t request_num, int id, KVStore* db):
				workload_proxy_(workload_proxy),
				db_(db),
				loader_threads_(loader_threads),
				load_num_(load_num),
				worker_threads_(worker_threads),
				request_num_(request_num),
				total_finished_requests_(0),
				total_write_latency(0),
				total_read_latency(0),
				write_finished(0),
				read_finished(0),
				id_(id){
			Reset();
			fflush(stdout);	
		}

		~RocksDBClient(){
			if(request_time_ != nullptr)
				delete request_time_;
			if(read_time_ != nullptr)
				delete read_time_;
			if(update_time_ != nullptr)
				delete update_time_;
			if(workload_wrapper_ != nullptr){
				delete workload_wrapper_;
			}
		}
		void Load();
		void Work();
		void Warmup();

	public:
		inline void SetAffinity(int coreid){
			coreid = coreid % sysconf(_SC_NPROCESSORS_ONLN);
			printf("client coreid: %d\n", coreid);
			cpu_set_t mask;
			CPU_ZERO(&mask);
			CPU_SET(coreid, &mask);
			int rc = sched_setaffinity(syscall(__NR_gettid), sizeof(mask), &mask);
			assert(rc == 0);
		}

		static std::string GetDayTime(){
			const int kBufferSize = 100;
			char buffer[kBufferSize];
			struct timeval now_tv;
			gettimeofday(&now_tv, nullptr);
			const time_t seconds = now_tv.tv_sec;
			struct tm t;
			localtime_r(&seconds, &t);
			snprintf(buffer, kBufferSize,
				        "%04d/%02d/%02d-%02d:%02d:%02d.%06d",
		            t.tm_year + 1900,
		            t.tm_mon + 1,
		            t.tm_mday,
		            t.tm_hour,
		            t.tm_min,
		            t.tm_sec,
		            static_cast<int>(now_tv.tv_usec));
			return std::string(buffer);
		}

		void RocksDBWorker(uint64_t num, int coreid, bool is_warmup, bool is_master);
		void RocksdDBLoader(uint64_t num, int coreid);
		void Reset();
		void PrintArgs();
};

}
