#ifndef _COORDINATOR_HH_
#define _COORDINATOR_HH_

#include <algorithm>
#include <fstream>
#include <iostream> 
#include <map>
#include <set>
#include <thread>
#include <unordered_map>
#include <sstream>
#include <string>

#include <assert.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <math.h>

#include "Config.hh"
#include "Socket.hh"

extern "C"{
#include "Jerasure/jerasure.h"
#include "Jerasure/galois.h"
#include "Jerasure/reed_sol.h"
#include "Jerasure/cauchy.h"
}

#define DEBUG_COORD 0
#define PEER_NODE_CMD 1
#define PROXY_NODE_CMD 2
#define UPPER_INFI 9999
#define print(value) std::cout << value.c_str() << " = " << value << std::endl

using namespace std;

class Coordinator{
  protected:
    Config* _conf; 
    int _ecK;
    int _ecM;
    int _ecN;
    int _coeffi;
    int _stripe_num;
    int _peer_node_num;
    int _rack_num;
    int _num_rebuilt_chunks;
    int _num_repair_chnk_round;
    int _balance_steps;
    int _balance_dev;
    size_t _chunk_size;
    size_t _packet_size;
    double _cross_rack_bdwh;
    double _intra_rack_bdwh;
    double _disk_bdwh;

    int* _rsEncMat;
    int* _placement;
    int* _related_stripes;
    int* _chnk_num_rack;
    int* _sort_rack_index;
    int* _cross_rack_traf;

    // for parseLog
    map<unsigned int, int> _ip2idx;
    map<size_t, string> _chunkid2addr; // the map between the global chunk id and the logical address on the node
    map<string, set<pair<unsigned int, string>>> _blk2Stripe;
    map<string,string> _blkName2stripeName;

    // for repair processing
    void init(void);
    void gene_rndm_placement(int, int, int*, int);
    void readEncMat(int*);
    void getRepairSol(int, int, int*, int*, int*);
    int* getDecodeCoeff(int*, int*, int);
    string ip2Str(unsigned int) const;

    // for doProcess 
    void QuickSort_index(int*, int*, int, int);
    void display(int, int, int*);
    void parseLog(void);
    void initCommand(string[], string[], int, int, int*, int*, int*, int*);
    void sendCommand(string[], int);
    void recvACK(int*, int);
    void doProcess(int, int, int*, int*);

    void RecordLoad(int*, int*, int*, int);
    void ClusterSRBalance(int, int);
    void ClusterSRInit(int, int*, int*, int*);
    void ClusterSROptimize(int, int*, int*, int*, int*, int*, int*, int);
    void ClusterSRSubChunkDownload(int, int, int*, int*, int*, int*, int*);
    int ClusterSRSubChunkUpload(int, int, int*, int*, int*, int*, int*);

    void RandSol(int, int*, int*);
    void CARSol(int, int*, int*);
    int calSum(int*, int);

  public:
    Coordinator(Config*);
    void preprocess(int, int);
    // calculate traffic 
    int ClusterSR(int);
    int CAR(int);
    int RandRepair(int);
    void freeGlobal(void);
};

#endif
