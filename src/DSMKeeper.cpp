#include "DSMKeeper.h"

#include "Connection.h"

const char *DSMKeeper::OK = "OK";
const char *DSMKeeper::ServerPrefix = "SPre";
//TODO: seperate into initLocalMeta_memroy and initLocalMeta_compute
void DSMKeeper::initLocalMeta_Compute() {
    //What is the difference between dsmPool and dsmMR Answer dsmPool is the pointer of dsmMR
//  localMeta.dsmBase = (uint64_t)dirCon[0]->dsmPool;
//  localMeta.lockBase = (uint64_t)dirCon[0]->lockPool;
  localMeta.cacheBase = (uint64_t)thCon[0]->cachePool;
  localMeta.node_type = Compute;

  // per thread APP
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    localMeta.appTh[i].lid = thCon[i]->ctx.lid;
    localMeta.appTh[i].rKey = thCon[i]->cacheMR->rkey;
    memcpy((char *)localMeta.appTh[i].gid, (char *)(&thCon[i]->ctx.gid),
           16 * sizeof(uint8_t));

    localMeta.appUdQpn[i] = thCon[i]->message->getQPN();
      uint8_t* p = localMeta.appTh[i].gid;
      fprintf(stdout,
              "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ",
              p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10],
              p[11], p[12], p[13], p[14], p[15]);
      printf("Put lid : 0x%x, qpn : 0x%x\n", localMeta.appTh[i].lid, localMeta.appUdQpn[i]);
  }


  // per thread DIR
//  for (int i = 0; i < NR_DIRECTORY; ++i) {
//    localMeta.dirTh[i].lid = dirCon[i]->ctx.lid;
//    localMeta.dirTh[i].rKey = dirCon[i]->dsmMR->rkey;
//    localMeta.dirTh[i].lock_rkey = dirCon[i]->lockMR->rkey;
//    memcpy((char *)localMeta.dirTh[i].gid, (char *)(&dirCon[i]->ctx.gid),
//           16 * sizeof(uint8_t));
//
//    localMeta.dirUdQpn[i] = dirCon[i]->message->getQPN();
//  }

}
void DSMKeeper::serverEnter() {
    memcached_return rc;
    uint64_t serverNum;

    while (true) {
        rc = memcached_increment(memc, COMPUTE_NUM_KEY, strlen(COMPUTE_NUM_KEY), 1,
                                 &serverNum);
        if (rc == MEMCACHED_SUCCESS) {

            myNodeID = serverNum - 1;

            printf("I am servers %d [%s]\n", myNodeID, getIP());
            return;
        }
        fprintf(stderr, "Server %d Counld't incr value and get ID: %s, retry...\n",
                myNodeID, memcached_strerror(memc, rc));
        usleep(10000);
    }
}
void DSMKeeper::serverConnect() {

    size_t l;
    uint32_t flags;
    memcached_return rc;

    while (curServer < maxServer) {
        char *serverNumStr = memcached_get(memc, MEMORY_NUM_KEY,
                                           strlen(MEMORY_NUM_KEY), &l, &flags, &rc);
        if (rc != MEMCACHED_SUCCESS) {
            fprintf(stderr, "Server %d Counld't get serverNum: %s, retry\n", myNodeID,
                    memcached_strerror(memc, rc));
            continue;
        }
        uint32_t serverNum = atoi(serverNumStr);
        free(serverNumStr);

        // /connect server K
        for (size_t k = curServer; k < serverNum; ++k) {
//            if (k != myNodeID) {
                connectNode(k);
                printf("I connect server %zu\n", k);
//            }
        }
        curServer = serverNum;
    }
}
bool DSMKeeper::connectNode(uint16_t remoteID) {
    //Actually the function below can be moved out side connectNOde, making it executed exactly
    // once.
  setDataToRemote(remoteID);

  std::string setK = setKey(remoteID);
  memSet(setK.c_str(), setK.size(), (char *)(&localMeta), sizeof(localMeta));

  std::string getK = getKey(remoteID);
  ExchangeMeta *remoteMeta = (ExchangeMeta *)memGet(getK.c_str(), getK.size());
  if (remoteMeta->node_type == Memory)
    setDataFromRemote(remoteID, remoteMeta);

  free(remoteMeta);
  return true;
}

void DSMKeeper::setDataToRemote(uint16_t remoteID) {
//  for (int i = 0; i < NR_DIRECTORY; ++i) {
//    auto &c = dirCon[i];
//
//    for (int k = 0; k < MAX_APP_THREAD; ++k) {
//      localMeta.dirRcQpn2app[i][k] = c->data2app[k][remoteID]->qp_num;
//    }
//  }

  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    auto &c = thCon[i];
    for (int k = 0; k < NR_DIRECTORY; ++k) {
      localMeta.appRcQpn2dir[i][k] = c->data[k][remoteID]->qp_num;
    }
  
  }
}

void DSMKeeper::setDataFromRemote(uint16_t remoteID, ExchangeMeta *remoteMeta) {
  for (int i = 0; i < NR_DIRECTORY; ++i) {
    auto &c = dirCon[i];

    for (int k = 0; k < MAX_APP_THREAD; ++k) {
      auto &qp = c->data2app[k][remoteID];

      assert(qp->qp_type == IBV_QPT_RC);
      modifyQPtoInit(qp, &c->ctx);
      modifyQPtoRTR(qp, remoteMeta->appRcQpn2dir[k][i],
                    remoteMeta->appTh[k].lid, remoteMeta->appTh[k].gid,
                    &c->ctx);
      modifyQPtoRTS(qp);
    }
  }

//  for (int i = 0; i < MAX_APP_THREAD; ++i) {
//    auto &c = thCon[i];
//    for (int k = 0; k < NR_DIRECTORY; ++k) {
//      auto &qp = c->data[k][remoteID];
//
//      assert(qp->qp_type == IBV_QPT_RC);
//      modifyQPtoInit(qp, &c->ctx);
//      modifyQPtoRTR(qp, remoteMeta->dirRcQpn2app[k][i],
//                    remoteMeta->dirTh[k].lid, remoteMeta->dirTh[k].gid,
//                    &c->ctx);
//      modifyQPtoRTS(qp);
//    }
//  }

  auto &info = remoteCon[remoteID];
  info.dsmBase = remoteMeta->dsmBase;
//  info.cacheBase = remoteMeta->cacheBase;
  info.lockBase = remoteMeta->lockBase;

  for (int i = 0; i < NR_DIRECTORY; ++i) {
    info.dsmRKey[i] = remoteMeta->dirTh[i].rKey;
    info.lockRKey[i] = remoteMeta->dirTh[i].lock_rkey;
    info.dirMessageQPN[i] = remoteMeta->dirUdQpn[i];
    uint8_t* p = remoteMeta->dirTh[i].gid;
    fprintf(stdout,
              "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ",
              p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10],
              p[11], p[12], p[13], p[14], p[15]);
    printf("Received lid : 0x%x, qpn : 0x%x\n", remoteMeta->dirTh[i].lid, info.dirMessageQPN[i]);
    for (int k = 0; k < MAX_APP_THREAD; ++k) {
      struct ibv_ah_attr ahAttr;
      fillAhAttr(&ahAttr, remoteMeta->dirTh[i].lid, remoteMeta->dirTh[i].gid,
                 &thCon[k]->ctx);
      info.appToDirAh[k][i] = ibv_create_ah(thCon[k]->ctx.pd, &ahAttr);

      assert(info.appToDirAh[k][i]);
    }
  }


//  for (int i = 0; i < MAX_APP_THREAD; ++i) {
//    info.appRKey[i] = remoteMeta->appTh[i].rKey;
//    info.appMessageQPN[i] = remoteMeta->appUdQpn[i];
//
//    for (int k = 0; k < NR_DIRECTORY; ++k) {
//      struct ibv_ah_attr ahAttr;
//      fillAhAttr(&ahAttr, remoteMeta->appTh[i].lid, remoteMeta->appTh[i].gid,
//                 &dirCon[k]->ctx);
//      info.dirToAppAh[k][i] = ibv_create_ah(dirCon[k]->ctx.pd, &ahAttr);
//
//      assert(info.dirToAppAh[k][i]);
//    }
//  }
}

void DSMKeeper::connectMySelf() {
  setDataToRemote(getMyNodeID());
  setDataFromRemote(getMyNodeID(), &localMeta);
}

void DSMKeeper::initRouteRule() {

  std::string k =
      std::string(ServerPrefix) + std::to_string(this->getMyNodeID());
  memSet(k.c_str(), k.size(), getMyIP().c_str(), getMyIP().size());
}

void DSMKeeper::barrier(const std::string &barrierKey) {

  std::string key = std::string("barrier-") + barrierKey;
  if (this->getMyNodeID() == 0) {
    memSet(key.c_str(), key.size(), "0", 1);
  }
  memFetchAndAdd(key.c_str(), key.size());
  while (true) {
    uint64_t v = std::stoull(memGet(key.c_str(), key.size()));
    if (v == this->getServerNR()) {
      return;
    }
  }
}

uint64_t DSMKeeper::sum(const std::string &sum_key, uint64_t value) {
  std::string key_prefix = std::string("sum-") + sum_key;

  std::string key = key_prefix + std::to_string(this->getMyNodeID());
  memSet(key.c_str(), key.size(), (char *)&value, sizeof(value));

  uint64_t ret = 0;
  for (int i = 0; i < this->getServerNR(); ++i) {
    key = key_prefix + std::to_string(i);
    ret += *(uint64_t *)memGet(key.c_str(), key.size());
  }

  return ret;
}
