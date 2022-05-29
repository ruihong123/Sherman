//
// Created by ruihong on 5/25/22.
//
#include "DSMContainer.h"
int kReadRatio;
int kThreadCount;
int kComputeNodeCount;
int kMemoryNodeCount;
void parse_args(int argc, char *argv[]) {
    if (argc != 4) {
        printf("Usage: ./benchmark kComputeNodeCount kReadRatio kThreadCount\n");
        exit(-1);
    }

    kComputeNodeCount = atoi(argv[1]);
    kMemoryNodeCount = atoi(argv[2]);
    kReadRatio = atoi(argv[3]);
    kThreadCount = atoi(argv[4]);

    printf("kComputeNodeCount %d, kReadRatio %d, kThreadCount %d\n", kComputeNodeCount,
           kReadRatio, kThreadCount);
}
int main(int argc,char* argv[])
{
    parse_args(argc, argv);
    DSMConfig config;
    config.ComputeNodeNum = kComputeNodeCount;
    config.MemoryNodeNum = kComputeNodeCount;
    ThreadConnection *thCon[MAX_APP_THREAD];
    DirectoryConnection *dirCon[NR_DIRECTORY];
    DSMConfig conf;
    auto keeper = new DSMContainer(thCon, dirCon, conf, conf.ComputeNodeNum);

    keeper->initialization();
    while(1){}
}