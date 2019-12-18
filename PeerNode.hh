#ifndef _PEERNODE_HH_
#define _PEERNODE_HH_

#include <iostream>
#include <map>
#include <thread>
#include <queue>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <assert.h>
#include <fcntl.h>
#include <dirent.h>
#include <net/if.h>
#include <netinet/in.h>
#include <net/if_arp.h>

#include "Socket.hh"
#include "Config.hh"

extern "C"{
#include "Jerasure/jerasure.h"
#include "Jerasure/galois.h"
#include "Jerasure/reed_sol.h"
#include "Jerasure/cauchy.h"
}

#define DEBUG_PEERNODE true
#define PARTIAL_REPAIR 0 
#define FULL_REPAIR    1

using namespace std;

class PeerNode{
  protected:

    // erasure coding parameters
    size_t _chunk_size;
    size_t _packet_size;
    size_t _meta_size;
    int _rack_num;
    int _peer_node_num;
    int _ecK;

    // ip addr and data path
    unsigned int _coordinator_ip;
    string _data_path;

    // repair functions
    string findChunkAbsPath(char*, char*);
    void partialEncodeSendData(char*, int, char*, char*, int*);
    void readData(char*, string, int*);
    void aggrDataSendData(char*, char*, int, int*, char*, int);
    void getLocalIP(char*);
    int  getRackID(Config*, char*);


  public:

    PeerNode(Config*); 
    void recvData(int, int, char*, int);
    void sendData(string, char*, int, char*, int);
    void sendData_debug(string, int, int, int);
    void paraRecvData(int, string, int);
    void parseCommand(string, string&, string&, string&, string&, string&, string&);
    void aggrDataWriteData(string, char*, int, size_t, int*, int);
    void writeData(string, size_t, char*, size_t, int);
    string ip2Str(unsigned int);
    int getVal(string);
    void commitACK(void);
    void initChar(char**, int, int);
    void sendQueueData(queue<string>, vector<pair<string,string> >, vector<pair<string,string> >, vector<pair<string,string> >, vector<pair<string,string> >);
    void repairData(vector<string>, vector<pair<string, string> >, vector<pair<string, int> >, vector<pair<string, string> >, int, int);
    void proxyForward(char*, char*, int*, int*, int, int); 
};
#endif
