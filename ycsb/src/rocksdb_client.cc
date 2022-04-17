#include "rocksdb_client.h"
#include "algorithm"
#include "math.h"
#include <iostream>
#include <functional>
#include <unordered_map>
#include <pthread.h>

namespace ycsbc{

static constexpr int maxClients = 512;
static std::unordered_map<std::string, uint64_t> hashMap;
static pthread_spinlock_t lock;

void RocksDBClient::Load(){
	Reset();
	pthread_spin_init(&lock, PTHREAD_PROCESS_PRIVATE);

	assert(request_time_ == nullptr);
	int base_coreid = 16; /*options_.spandb_worker_num + 
					  options_.env->GetBgThreadCores(rocksdb::Env::LOW) + 
					  options_.env->GetBgThreadCores(rocksdb::Env::HIGH);
*/
	// assert(!options_.enable_spdklogging);
	uint64_t num = load_num_ / loader_threads_;
	std::vector<std::thread> threads;
	auto fn = std::bind(&RocksDBClient::RocksdDBLoader, this, 
						std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	auto start = TIME_NOW;
	for(int i=0; i<loader_threads_; i++){
		if(i == loader_threads_ - 1)
			num = num + load_num_ % loader_threads_;
		threads.emplace_back(fn, num, (base_coreid + i), i);
	}
	for(auto &t : threads)
		t.join();
	double time = TIME_DURATION(start, TIME_NOW);

	printf("==================================================================\n");
	PrintArgs();
	printf("Load %ld requests in %.3lf seconds.\n", load_num_, time/1000/1000);
	printf("==================================================================\n");
	std::this_thread::sleep_for(std::chrono::seconds(5));
}

void RocksDBClient::Work(){
	Reset();
	assert(request_time_ == nullptr);
	assert(read_time_ == nullptr);
	assert(update_time_ == nullptr);
	request_time_ = new TimeRecord(request_num_ + 1);
	read_time_ = new TimeRecord(request_num_ + 1);
	update_time_ = new TimeRecord(request_num_ + 1);

	int base_coreid = 16; /*options_.spandb_worker_num + 
					  options_.env->GetBgThreadCores(rocksdb::Env::LOW) + 
					  options_.env->GetBgThreadCores(rocksdb::Env::HIGH);
*/
	if(workload_wrapper_ == nullptr){
		workload_wrapper_ = new WorkloadWrapper(workload_proxy_, request_num_, false);
	}

	uint64_t num = request_num_ / worker_threads_;
	std::vector<std::thread> threads;
	std::function< void(uint64_t, int, bool, bool, int)> fn;
/*	if(options_.enable_spdklogging){
		fn = std::bind(&RocksDBClient::SpanDBWorker, this, 
						std::placeholders::_1, std::placeholders::_2,
						std::placeholders::_3, std::placeholders::_4);
	}else{*/
		fn = std::bind(&RocksDBClient::RocksDBWorker, this, 
		   			    std::placeholders::_1, std::placeholders::_2,
		   			    std::placeholders::_3, std::placeholders::_4, std::placeholders::_5);
	//}
	printf("start time: %s\n", GetDayTime().c_str());
	auto start = TIME_NOW;
	for(int i=0; i<worker_threads_; i++){
		if(i == worker_threads_ - 1)
			num = num + request_num_ % worker_threads_;
		threads.emplace_back(fn, num, (base_coreid + worker_threads_ * id_ + i), false, i==0, i);
	}
	for(auto &t : threads)
		t.join();
	double time = TIME_DURATION(start, TIME_NOW);
	printf("end time: %s\n", GetDayTime().c_str());

	fflush(stdout);

	assert(request_time_->Size() == request_num_);
	printf("==================================================================\n");
	PrintArgs();
	printf("WAL sync time per request: %.3lf us\n", wal_time_/request_num_);
	printf("Wait time: %.3lf us\n", wait_time_/request_num_);
	// printf("Complete wait time: %.3lf us\n", complete_memtable_time_/request_num_);
	printf("Write delay time: %.3lf us\n", write_delay_time_/request_num_);
	printf("Write memtable time: %.3lf\n", write_memtable_time_/request_num_);
	printf("Finish %ld requests in %.3lf seconds.\n", request_num_, time/1000/1000);
	if(read_time_->Size() != 0){
		printf("read num: %ld, read avg latency: %.3lf us, read median latency: %.3lf us\n", 
				read_time_->Size(), read_time_->Sum()/read_time_->Size(), read_time_->Tail(0.50));
		printf("read P999: %.3lf us, P99: %.3lf us, P95: %.3lf us, P90: %.3lf us, P75: %.3lf us\n",
				read_time_->Tail(0.999), read_time_->Tail(0.99), read_time_->Tail(0.95),
				read_time_->Tail(0.90), read_time_->Tail(0.75));
	}else{
		printf("read num: 0, read avg latency: 0 us, read median latency: 0 us\n");
		printf("read P999: 0 us, P99: 0 us, P95: 0 us, P90: 0 us, P75: 0 us\n");
	}
	if(update_time_->Size() != 0){
		printf("update num: %ld, update avg latency: %.3lf us, update median latency: %.3lf us\n", 
			    update_time_->Size(), update_time_->Sum()/update_time_->Size(), update_time_->Tail(0.50));
		printf("update P999: %.3lf us, P99: %.3lf us, P95: %.3lfus, P90: %.3lf us, P75: %.3lf us\n",
				update_time_->Tail(0.999), update_time_->Tail(0.99), update_time_->Tail(0.95),
				update_time_->Tail(0.90), update_time_->Tail(0.75));
	}else{
		printf("update num: 0, update avg latency: 0 us, update median latency: 0 us\n");
		printf("update P999: 0 us, P99: 0 us, P95: 0 us, P90: 0 us, P75: 0 us\n");
	}
	printf("Work latency: %.3lf us\n", request_time_->Sum()/request_time_->Size());
	printf("Work IOPS: %.3lf K\n", request_num_/time*1000*1000/1000);
	tput_ = request_num_/time*1000*1000/1000;
	printf("Work median latency: %.3lf us\n", request_time_->Tail(0.5));
	printf("Work P999: %.3lfus, P99: %.3lfus, P95: %.3lfus, P90: %.3lfus, P75: %.3lfus\n",
			request_time_->Tail(0.999), request_time_->Tail(0.99), request_time_->Tail(0.95),
		    request_time_->Tail(0.90), request_time_->Tail(0.75));
	printf("==================================================================\n");
	fflush(stdout);
}

void RocksDBClient::Warmup(){
	int base_coreid = 16; /*options_.spandb_worker_num + 
					  options_.env->GetBgThreadCores(rocksdb::Env::LOW) + 
					  options_.env->GetBgThreadCores(rocksdb::Env::HIGH);*/
	Reset();
	if(workload_wrapper_ == nullptr){
		workload_wrapper_ = new WorkloadWrapper(workload_proxy_, (uint64_t)request_num_ * (1+warmup_rate_) + 1, false);
	}

	auto start = TIME_NOW;
	const uint64_t warmup_num = floor(request_num_ * warmup_rate_);
	const uint64_t num = warmup_num / worker_threads_;
	printf("Start warmup (%ld)...\n", num*worker_threads_);
	std::vector<std::thread> threads;
	std::function< void(uint64_t, int, bool, bool, int)> fn;
	printf("warmup start: %s\n", GetDayTime().c_str());
/*	if(options_.enable_spdklogging){
		fn = std::bind(&RocksDBClient::SpanDBWorker, this, 
						std::placeholders::_1, std::placeholders::_2,
						std::placeholders::_3, std::placeholders::_3);
	}else{*/
		fn = std::bind(&RocksDBClient::RocksDBWorker, this, 
		   			    std::placeholders::_1, std::placeholders::_2,
		   			    std::placeholders::_3, std::placeholders::_4, std::placeholders::_5);
	//}
	for(int i=0; i<worker_threads_; i++){
		threads.emplace_back(fn, num, base_coreid + worker_threads_ * id_ + i, true, i==0, i);
	}
	for(auto &t : threads)
		t.join();
	double time = TIME_DURATION(start, TIME_NOW);
	printf("warmup finish: %s\n", GetDayTime().c_str());
	printf("Warmup complete %ld requests in %.3lf seconds.\n", num*worker_threads_, time/1000/1000);
}

void RocksDBClient::RocksDBWorker(uint64_t num, int coreid, bool is_warmup, bool is_master, int id){
	// SetAffinity(coreid);

	TimeRecord request_time(num + 1);
	TimeRecord read_time(num + 1);
	TimeRecord update_time(num + 1);

	if(!is_warmup && is_master){
		printf("starting requests...\n");
	}

	char w_value[1024 * 128]; // TODO: change to a buffer
	char r_value[1024 * 128];
	for(uint64_t i=0; i<num; i++){
		WorkloadWrapper::Request *req = workload_wrapper_->GetNextRequest();
		ycsbc::Operation opt = req->Type();
		assert(req != nullptr);
		auto start = TIME_NOW;
		uint64_t offset;
		if(opt == READ){
			offset = hashMap.find(req->Key())->second;
			db_->Read(r_value, offset, 256);
			// db_->Get(read_options_, req->Key(), &r_value);
		}else if(opt == UPDATE){
			offset = hashMap.find(req->Key())->second;
			db_->Write(w_value, offset, 256);
			// ERR(db_->Put(write_options_, req->Key(), /*std::string(req->Length(), 'a')*/ w_value));
		}
		else if(opt == INSERT){
			offset = ((uint64_t)rand() % (1UL << 17)) << 8;
			pthread_spin_lock(&lock);
			hashMap.insert(std::pair<std::string, uint64_t>(req->Key(), offset));
			pthread_spin_unlock(&lock);
			db_->Write(w_value, offset, 256);
			// ERR(db_->Put(write_options_, req->Key(), /*std::string(req->Length(), 'a')*/ w_value));
		}else if(opt == READMODIFYWRITE){
			offset = hashMap.find(req->Key())->second;
			db_->Read(r_value, offset, 256);
			db_->Write(w_value, offset, 256);
			// db_->Get(read_options_, req->Key(), &r_value);
			// ERR(db_->Get(read_options_, req->Key(), &r_value));
			// ERR(db_->Put(write_options_, req->Key(), w_value));
		}else if(opt == SCAN){
//			rocksdb::Iterator* iter = db_->NewIterator(read_options_);
//			iter->Seek(req->Key());
//			for (int i = 0; i < req->Length() && iter->Valid(); i++) {
//				// Do something with it->key() and it->value().
//        		iter->Next();
//    		}
//    		ERR(iter->status());
//    		delete iter;
		}
		else{
			throw utils::Exception("Operation request is not recognized!");
		}
		double time =  TIME_DURATION(start, TIME_NOW);
		request_time.Insert(time);
		if(opt == READ || opt == SCAN){
			read_time.Insert(time);
			total_read_latency.fetch_add((uint64_t)time);
			read_finished.fetch_add(1);
		}else if(opt == UPDATE || opt == INSERT || opt == READMODIFYWRITE){
			update_time.Insert(time);
			total_write_latency.fetch_add((uint64_t)time);
			write_finished.fetch_add(1);
		}else{
			assert(0);
		}
		total_finished_requests_.fetch_add(1);
	}

	if(is_warmup)
		return ;

	mutex_.lock();
	request_time_->Join(&request_time);
	read_time_->Join(&read_time);
	update_time_->Join(&update_time);
	mutex_.unlock();
}

void RocksDBClient::RocksdDBLoader(uint64_t num, int coreid, int id){
	char w_value[128 * 1024];
	for(uint64_t i=0; i<num; i++){
		uint64_t offset = ((uint64_t)rand() % (1UL << 17)) << 8;
		std::string table;
		std::string key;
		std::vector<ycsbc::CoreWorkload::KVPair> values;
		workload_proxy_->LoadInsertArgs(table, key, values);
		assert(values.size() == 1);
		for (ycsbc::CoreWorkload::KVPair &field_pair : values) {
			std::string value = field_pair.second;
			db_->Write(w_value, offset, 256);
			pthread_spin_lock(&lock);
			hashMap.insert(std::pair<std::string, uint64_t>(key, offset));
			pthread_spin_unlock(&lock);
			// ERR(db_->Put(write_options_, key, value));
		}
	}
}

void RocksDBClient::Reset(){
	wait_time_ = wal_time_ = 0;
	complete_memtable_time_ = 0;
	block_read_time_ = 0;
	write_delay_time_ = 0;
	write_memtable_time_ = 0;
	submit_time_ = 0;
	total_finished_requests_.store(0);
}

void RocksDBClient::PrintArgs(){
/*	printf("-----------configuration------------\n");
	if(options_.ssdlogging_type != "spdk"){
		printf("Clients: %d\n", worker_threads_);
	}else{
		printf("Clients: %d * %d\n", worker_threads_, async_num_);
		printf("Loggers: %d (%d)\n", options_.logging_server_num, options_.ssdlogging_num);
		printf("Workers: %d\n", options_.spandb_worker_num);
	}
	printf("Auto configure: %s\n", options_.auto_config ? "true" : "false");
	printf("Dynamic moving: %s\n", options_.dynamic_moving ? "true" : "false");
	printf("Max read queue: %d\n", options_.max_read_que_length);
	printf("WAL: %d, fsync: %d\n", !(write_options_.disableWAL), write_options_.sync);
	printf("Max level: %d\n", options_.max_level);
	printf("Data: %s\n", data_dir_.c_str());
	if(write_options_.disableWAL){
		printf("WAL: nologging\n");
	}else if(options_.ssdlogging_type == "spdk"){
		printf("WAL: %s\n", options_.ssdlogging_path.c_str());
	}else{
		printf("WAL: %s\n", options_.wal_dir.c_str());
	}
	printf("Max_write_buffer_number: %d\n", options_.max_write_buffer_number);
	printf("Max_background_jobs: %d\n", options_.max_background_jobs);
	printf("High-priority backgd threds: %d\n", options_.env->GetBackgroundThreads(rocksdb::Env::HIGH));
	printf("Low-priority backgd threds: %d\n", options_.env->GetBackgroundThreads(rocksdb::Env::LOW));
	printf("Max_subcompactions: %d\n", options_.max_subcompactions);
	if(options_.write_buffer_size >= (1ull << 30)){
		printf("Write_buffer_size: %.3lf GB\n", options_.write_buffer_size * 1.0/(1ull << 30));
	}else{
		printf("Write_buffer_size: %.3lf MB\n", options_.write_buffer_size * 1.0/(1ull << 20));
	}
	printf("-------------------------------------\n");
	printf("write done by self: %ld\n", options_.statistics->getTickerCount(rocksdb::WRITE_DONE_BY_SELF));
	printf("WAL sync num: %ld\n", options_.statistics->getTickerCount(rocksdb::WAL_FILE_SYNCED));
	printf("WAL write: %.3lf GB\n", options_.statistics->getTickerCount(rocksdb::WAL_FILE_BYTES)*1.0/(1ull << 30));
	printf("compaction read: %.3lf GB, compaction write: %.3lf GB\n",
			options_.statistics->getTickerCount(rocksdb::COMPACT_READ_BYTES)*1.0/(1ull << 30),
			options_.statistics->getTickerCount(rocksdb::COMPACT_WRITE_BYTES)*1.0/(1ull << 30));
	printf("flush write: %.3lf GB\n",
			options_.statistics->getTickerCount(rocksdb::FLUSH_WRITE_BYTES)*1.0/(1ull << 30));
	printf("flush time: %.3lf s\n",
			options_.statistics->getTickerCount(rocksdb::FLUSH_TIME)*1.0/1000000);
	fflush(stdout);*/
}

}

