#ifndef _CONFIG_HH_
#define _CONFIG_HH_

#include <algorithm>
#include <iostream>
#include <map>
#include <string>
#include <queue>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/fcntl.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "Util/tinyxml2.h"

#define ROLE_LEN 2
#define ACK_LEN 3

#define BLK_NAME_LEN 24
#define STRIPE_NAME_LEN 4 
#define NEXT_IP_LEN 10
#define COEFF_LEN 3

#define RECV_UNIT_CMD_LEN BLK_NAME_LEN+STRIPE_NAME_LEN

using namespace tinyxml2; 

class Config{
  public: 
    
    // variables
    int _ecK; 
    int _ecN;
    int _peer_node_num;
    int _rack_num;
    int _balance_steps;
    int _balance_dev;
    size_t _chunk_size;  //in MB
    size_t _meta_size;  //in MB
    size_t _packet_size; //in MB 
    size_t _stripe_num;
    double _cross_rack_bdwh;
    double _intra_rack_bdwh;
    double _disk_bdwh;

    std::vector<unsigned int> _peerNodeIPs;
    std::vector<unsigned int> _proxyIPs;
	std::string _localDataPath;
    unsigned int _coordinatorIP;
    unsigned int _localIP;

    // functions
    Config(std::string confFile);
    void display();
};

#endif
