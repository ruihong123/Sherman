//
// Created by ruihong on 5/21/22.
//

#ifndef SHERMAN_DSMCONTAINER_H
#define SHERMAN_DSMCONTAINER_H

#include <vector>
#include "DirectoryConnection.h"
#include "Connection.h"
#include "Keeper.h"

struct ThreadConnection;
struct DirectoryConnection;
struct CacheAgentConnection;
struct RemoteConnection;



class DSMContainer : public Keeper  {
private:
    static const char *OK;
    static const char *ServerPrefix;
    uint64_t baseAddr;
    ThreadConnection **thCon;
    DirectoryConnection **dirCon;
    RemoteConnection *remoteCon;
//    RemoteConnection *remoteInfo;
    ExchangeMeta localMeta;

    std::vector<std::string> serverList;

    std::string setKey(uint16_t remoteID) {
        return std::to_string(getMyNodeID()) + "M" + "-" + std::to_string(remoteID) + "C";
    }

    std::string getKey(uint16_t remoteID) {
        return std::to_string(remoteID) + "C" + "-" + std::to_string(getMyNodeID()) + "M";
    }
    void initRDMAConnection_Compute();
    void initLocalMeta();
    void connectMySelf();
    void initRouteRule();

    void setDataToRemote(uint16_t remoteID);
    void setDataFromRemote(uint16_t remoteID, ExchangeMeta *remoteMeta);

protected:
    virtual bool connectNode(uint16_t remoteID) override;
    virtual void serverConnect() override;
    virtual void serverEnter() override;
public:
    DSMContainer(ThreadConnection **thCon, DirectoryConnection **dirCon,
                 const DSMConfig &conf, uint32_t maxServer = 12)
    : Keeper(maxServer), thCon(thCon), dirCon(dirCon) {

        remoteCon = new RemoteConnection[conf.ComputeNodeNum];

        baseAddr = (uint64_t)hugePageAlloc(conf.dsmSize * define::GB);
        assert(baseAddr != 0);
//        baseAddr = reinterpret_cast<uint64_t>(malloc(1024 * 1024 * 1024));
        for (uint64_t i = baseAddr; i < baseAddr + conf.dsmSize * define::GB;
             i += 2 * define::MB) {
            *(char *)i = 0;
        }
        memset((char *)baseAddr, 0, define::kChunkSize);

        for (int i = 0; i < NR_DIRECTORY; ++i) {
            // the connection below just create the queue pair for DSM. also initialize the DSM memory region
            // in this machine.
            dirCon[i] =
                    new DirectoryConnection(i, (void *)baseAddr, conf.dsmSize * define::GB,
                                            conf.MemoryNodeNum, remoteCon);
        }
        initLocalMeta();

        if (!connectMemcached()) {
            return;
        }
    }
    void initialization(){
        serverEnter();

        serverConnect();
//        connectMySelf();

        initRouteRule();
    }

    ~DSMContainer() { disconnectMemcached(); }
    void barrier(const std::string &barrierKey);
    uint64_t sum(const std::string &sum_key, uint64_t value);

};


#endif //SHERMAN_DSMCONTAINER_H
