//
// Created by ruihong on 5/25/22.
//
#include "DSMContainer.h"
int kReadRatio;
int kThreadCount;
int kComputeNodeCount;
int kMemoryNodeCount;
bool table_scan;
void parse_args(int argc, char *argv[]) {
    if (argc != 6) {
        printf("Usage: ./benchmark kComputeNodeCount kMemoryNodeCount kReadRatio kThreadCount tablescan\n");
        exit(-1);
    }

    kComputeNodeCount = atoi(argv[1]);
    kMemoryNodeCount = atoi(argv[2]);
    kReadRatio = atoi(argv[3]);
    kThreadCount = atoi(argv[4]);
    int scan_number = atoi(argv[5]);
    if(scan_number == 0)
        table_scan = false;
    else
        table_scan = true;

    printf("kComputeNodeCount %d, kMemoryNodeCount %d, kReadRatio %d, kThreadCount %d, tablescan %d\n", kComputeNodeCount,
           kMemoryNodeCount, kReadRatio, kThreadCount, scan_number);
}
int main(int argc,char* argv[])
{
    parse_args(argc, argv);
    DSMConfig conf;
    conf.ComputeNodeNum = kComputeNodeCount;
    conf.MemoryNodeNum = kMemoryNodeCount;
    ThreadConnection *thCon[MAX_APP_THREAD];
    DirectoryConnection *dirCon[NR_DIRECTORY];
//    DSMConfig conf;
    auto keeper = new DSMContainer(thCon, dirCon, conf, conf.ComputeNodeNum, conf.MemoryNodeNum);

    keeper->initialization();
    while(1){}
}