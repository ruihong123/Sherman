//
// Created by ruihong on 5/25/22.
//
#include "DSMContainer.h"
int kReadRatio;
int kThreadCount;
int kNodeCount;
void parse_args(int argc, char *argv[]) {
    if (argc != 4) {
        printf("Usage: ./benchmark kNodeCount kReadRatio kThreadCount\n");
        exit(-1);
    }

    kNodeCount = atoi(argv[1]);
    kReadRatio = atoi(argv[2]);
    kThreadCount = atoi(argv[3]);

    printf("kNodeCount %d, kReadRatio %d, kThreadCount %d\n", kNodeCount,
           kReadRatio, kThreadCount);
}
int main(int argc,char* argv[])
{
    parse_args(argc, argv);
    DSMConfig config;
    config.ComputeNodeNum = kNodeCount;
    ThreadConnection *thCon[MAX_APP_THREAD];
    DirectoryConnection *dirCon[NR_DIRECTORY];
    DSMConfig conf;
    auto keeper = new DSMContainer(thCon, dirCon, conf, conf.ComputeNodeNum);

    keeper->initialization();
    while(1){}
}