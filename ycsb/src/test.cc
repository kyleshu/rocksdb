#include "rocksdb_client.h"
#include "iostream"
#include "cmath"
#include <sys/vfs.h>
#include <memory>

void ParseCommandLine(int argc, const char *argv[], utils::Properties &props);
void PrintWorkload(const char* filename);

void* run_test(void* args) {
	ycsbc::RocksDBClient* rocksdb_client = (ycsbc::RocksDBClient*) args;
	// rocksdb_client->SetAffinity(rocksdb_client->id_);
	rocksdb_client->Load();
	rocksdb_client->Warmup();
	rocksdb_client->Work();
	return NULL;
}

int main(const int argc, const char *argv[]){
	utils::Properties props;
	ParseCommandLine(argc, argv, props);

//	ycsbc::CoreWorkload wl;
//	wl.Init(props);
//	ycsbc::WorkloadProxy wp(&wl);

	const int client_num = stoi(props.GetProperty("client_num")); // 1 for each instance
	const uint64_t load_num = stoull(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
	const uint64_t requests_num = stoull(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
	//const std::string log_dir = props.GetProperty("log_dir"); // no use
	//const std::string data_dir = props.GetProperty("data_dir"); // no use
	//const int is_load = stoi(props.GetProperty("is_load")); // always load
	//const std::string dbname = props.GetProperty("dbname"); // no use
	//const std::string db_bak = props.GetProperty("db_bak"); // no use
	const std::string config_path = props.GetProperty("config_path"); // important
	const std::string bdev_name = props.GetProperty("bdev_name"); // important
	const int num_instance = stoi(props.GetProperty("num_instance")); // 2 should be sufficient

	ycsbc::CoreWorkload instance_wls[num_instance];
	ycsbc::WorkloadProxy* instance_wps[num_instance];
	ycsbc::RocksDBClient* clients[num_instance];
	for(int i = 0; i < num_instance; ++i) {
		instance_wls[i].Init(props);
		instance_wps[i] = new ycsbc::WorkloadProxy(&instance_wls[i]);
	}
	//===================common-setting==========
//	auto cache = rocksdb::NewLRUCache(1 * 1024 * 1024 * 1024);
//	rocksdb::BlockBasedTableOptions bbt_opts;
//	bbt_opts.block_size = 32 * 1024;
//	bbt_opts.block_cache = cache;
//	bbt_opts.flush_block_policy_factory.reset(new rocksdb::FlushBlockBySizePolicyFactory());
	
//	rocksdb::Options options;
//	rocksdb::WriteOptions write_options;
//	rocksdb::ReadOptions read_options;
//	options.table_factory.reset(NewBlockBasedTableFactory(bbt_opts));
//	options.allow_concurrent_memtable_write = true;
//	options.recycle_log_file_num = false;
//	options.allow_2pc = false;
//	options.compression = rocksdb::kNoCompression;
//	options.max_open_files = 500000;
//	options.wal_dir = log_dir;
//	options.bytes_per_sync = 128 * 1024;
//	write_options.sync = true;
//	write_options.disableWAL = false;
//	if(is_load == 1){
//		// write_options.sync = false;
//		// write_options.disableWAL = true;
//		options.error_if_exists = false;
//		options.create_if_missing = true;
//	}else{
//		options.error_if_exists = false;
//		options.create_if_missing = false;
//	}
//	options.statistics = rocksdb::CreateDBStatistics();
//	options.max_total_wal_size =  1 * (1ull << 25); // wal size
//	options.write_buffer_size = 1 * (1ull << 25);   // write buffer size
//	std::string db = data_dir; //"/users/kyleshu/data";

	// std::string spdk_name = "/users/kyleshu/git/dRaid/src/rocksdb/rocksdb.json";
	// std::string spdk_bdev = "Nvme0n1";
	std::string spdk_name = config_path; //"/users/kyleshu/git/dRaid/raid_config/raid5.json";
	std::string spdk_bdev = bdev_name; //"Raid0";
	auto db = rocksdb::NewSpdkKVStore(config_path, bdev_name);
//	options.env = env;
//	/*options.auto_config = true;
//	options.dynamic_moving = true;
//	if(dbname == "spandb" && !options.auto_config){
//		env->SetBgThreadCores(2, rocksdb::Env::HIGH);
//		env->SetBgThreadCores(6, rocksdb::Env::LOW);
//	}*/
//	env->SetBackgroundThreads(2, rocksdb::Env::HIGH);
//	env->SetBackgroundThreads(6, rocksdb::Env::LOW);
//	options.max_background_jobs = 8;
//	options.max_subcompactions = 4;
//	options.max_write_buffer_number = 4;
//	// options.topfs_cache_size = 90; //20GB

//	if(is_load == 0 || is_load == 1){
//		printf("empty the existing data folder\n");
//		for(int i = 0; i < num_instance; ++i)
//			system(("rm " + data_dir + "/instance" + std::to_string(i) + "/*").c_str());
//	}
//
//	if(dbname == "rocksdb"){
//		printf("empty the existing log folder\n");
//		for(int i = 0; i < num_instance; ++i)
//			system(("rm " + log_dir + "/instance" + std::to_string(i) + "/*").c_str());
//	}
//	if(is_load == 0){
//		printf("loading database from %s to %s \n", db_bak.c_str(), data_dir.c_str());
//		system(("cp " + db_bak + "/*" + " " + data_dir + "/").c_str());
//		printf("loading finished\n");
//	}

  	//===================DB=======================================
//  	printf("dbname: %s\n", dbname.c_str());
//  	const int async_num = 50;
/*	if(dbname == "rocksdb"){
		options.auto_config = false;
	}else if(dbname == "spandb"){
		int core_num = 40;
		if(core_num > sysconf(_SC_NPROCESSORS_ONLN))
			core_num = sysconf(_SC_NPROCESSORS_ONLN);
		std::string pcie_addr = "trtype:PCIe " + options.wal_dir;
		options.wal_dir = data_dir;
		options.enable_spdklogging = true;
		options.ssdlogging_type = "spdk";
		options.spdk_recovery = false;
		options.wal_dir = data_dir;
		options.lo_path = data_dir;
		options.max_level = 4;
		options.l0_queue_num = 20;
		options.max_compaction_bytes = 64ull<<20;
		options.ssdlogging_path = pcie_addr;
		options.max_read_que_length = 2;
		options.ssdlogging_num = 6;
		options.logging_server_num = 1;
		// options.lo_env = rocksdb::NewSpdkEnv(rocksdb::Env::Default(), pcie_addr, options, is_load);
		options.spandb_worker_num = core_num - env->GetBgThreadCores(rocksdb::Env::HIGH) 
		 							   		 - env->GetBgThreadCores(rocksdb::Env::LOW)
		 							   		 - client_num;
	}else{
		printf("Please choose the correct db (rocksdb or spandb)\n");
		exit(0);
	}
*/
	pthread_t client_thread[num_instance];
//	system("sync;echo 3 > /proc/sys/vm/drop_caches");
//	fflush(stdout);
//	printf("--------------memory usage----------------\n");
//	fflush(stdout);
//	system("free -h");
//	fflush(stdout);
//	printf("------------------------------------------\n");
//	fflush(stdout);
	for(int i = 0; i < num_instance; ++i) {
		printf("instance %d\n",i);
//		rocksdb::Options instance_options(options);
//		instance_options.wal_dir = log_dir + "/instance" + std::to_string(i);
		clients[i] = new ycsbc::RocksDBClient(instance_wps[i], client_num, load_num, client_num, requests_num, i, db); // TODO: only take instance_wps[i], bdev wrapper, range of space, client_num, load_num, requests_num as input
		pthread_create(&client_thread[i], NULL, run_test, clients[i]);
	}

	double tput = 0.0;
	double work_avg_latency  = 0.0;
	for(int i = 0; i < num_instance; ++i) {
		pthread_join(client_thread[i], NULL);
		tput += clients[i]->tput_;
		work_avg_latency += clients[i]->request_time_->Sum() / clients[i]->request_time_->Size();
	}
	printf("total tput: %.3lf K, work latency: %.3lf us\n", tput, work_avg_latency / num_instance);
	for(int i = 0; i < num_instance; ++i) {
		delete instance_wps[i];
		delete clients[i];
	}
/*	if(dbname == "spandb"){
		delete options.lo_env;
	}
*/	fflush(stdout);
	return 0;
}

void ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
	if(argc != 11){
		printf("usage: <workload_file> <client_num> <data_dir> <log_dir> <is_load> <dbname> <db_bak> <config_path> <bdev_name> <num_instace>\n");
		exit(0);
	}
	// workload file
	std::ifstream input(argv[1]);
	try {
		props.Load(input);
	} catch (const std::string &message) {
		printf("%s\n", message.c_str());
		exit(0);
	}
	input.close();
	PrintWorkload(argv[1]);
	props.SetProperty("client_num", argv[2]);
	//props.SetProperty("data_dir", argv[3]);
	//props.SetProperty("log_dir", argv[4]);
	//props.SetProperty("is_load", argv[5]);
	//props.SetProperty("dbname", argv[6]);
	//props.SetProperty("db_bak", argv[7]);
	props.SetProperty("config_path", argv[8]);
	props.SetProperty("bdev_name", argv[9]);
	props.SetProperty("num_instance", argv[10]);
}

void PrintWorkload(const char* filename){
	FILE *file = fopen(filename, "r");
	char line[201];
	fgets(line,200,file);
	printf("==================Workload=================\n");
	printf("%s\n", filename);
	while(!feof(file)){
		std::string s = std::string(line);
		if(s.find("#") != 0 && s != "\n" && s!=""){
			printf("%s", s.c_str());
		}
		fgets(line,200,file);
	}
	fclose(file);
	printf("==========================================\n");
	fflush(stdout);
}
