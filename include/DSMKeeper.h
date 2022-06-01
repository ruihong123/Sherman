#ifndef __LINEAR_KEEPER__H__
#define __LINEAR_KEEPER__H__

#include <vector>

#include "Keeper.h"

struct ThreadConnection;
struct DirectoryConnection;
struct CacheAgentConnection;
struct RemoteConnection;



class DSMKeeper : public Keeper {

private:
  static const char *OK;
  static const char *ServerPrefix;

  ThreadConnection **thCon;
  DirectoryConnection **dirCon;
  RemoteConnection *remoteCon;

  ExchangeMeta localMeta;

  std::vector<std::string> serverList;

    std::string setKey(uint16_t remoteID) {
        return std::to_string(getMyNodeID()) + "C" + "-" + std::to_string(remoteID) + "M";
    }

    std::string getKey(uint16_t remoteID) {
        return std::to_string(remoteID) + "M" + "-" + std::to_string(getMyNodeID()) + "C";
    }

  void initLocalMeta_Compute();
  void initLocalMeta_Memory();
  void connectMySelf();
  void initRouteRule();

  void setDataToRemote(uint16_t remoteID);
  void setDataFromRemote(uint16_t remoteID, ExchangeMeta *remoteMeta);

protected:
  virtual bool connectNode(uint16_t remoteID) override;
  virtual void serverConnect() override;
  virtual void serverEnter() override;
public:
  DSMKeeper(ThreadConnection **thCon, DirectoryConnection **dirCon, RemoteConnection *remoteCon,
            uint32_t maxMemoryServer = 12, uint32_t maxComputeServer = 12)
      : Keeper(maxComputeServer, maxMemoryServer), thCon(thCon), dirCon(dirCon),
        remoteCon(remoteCon) {

      initLocalMeta_Compute();

    if (!connectMemcached()) {
      return;
    }

  }
  void initialization(){
      serverEnter();

      serverConnect();
//      connectMySelf();

      initRouteRule();
  }

  ~DSMKeeper() { disconnectMemcached(); }
  void barrier(const std::string &barrierKey);
  uint64_t sum(const std::string &sum_key, uint64_t value);
};

#endif
