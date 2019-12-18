#include <iostream>
#include <string> 
#include "PeerNode.hh"

using namespace std;

int main(int argc, char**argv){
    
    Config* conf = new Config("metadata/config.xml");
    Socket* sock = new Socket();
    PeerNode* pn = new PeerNode(conf);

    // two threads: one for sending data, and the other for receiving data
    thread doThreads[2]; 

    int index;
    int ecK = pn->getVal("_ecK");
    int chunk_size = pn->getVal("_chunk_size");
    int packet_size = pn->getVal("_packet_size");
    int packet_num = chunk_size/packet_size;
    int if_first;

    char* recv_cmd;
    string role;
    string blk_name;
    string stripe_name;
    string coeff;
    string next_ip;
    string proxy_ip;
    
    // some freq use memory
    int* cur_conn = (int*)malloc(sizeof(int)*(ecK+1));
    int* mark_recv = (int*)malloc(sizeof(int)*ecK*packet_num);
    char* total_recv_data = (char*)malloc(sizeof(char)*ecK*chunk_size);
    int recv_chunk_num;
    int sum_chunk_num;

    // define cmd len
    int cmd_unit_len = ROLE_LEN + BLK_NAME_LEN + STRIPE_NAME_LEN + COEFF_LEN + NEXT_IP_LEN + NEXT_IP_LEN;
    char* one_cmd = (char*)malloc(sizeof(char)*cmd_unit_len);

    // define a queues with chunk_name; 
    queue<string> send_queue;

    // global vectors
    vector<string> recv_vector;
    vector<pair<string, string> > sendBlkName2stripeName;
    vector<pair<string, string> > sendBlkName2coeff;
    vector<pair<string, string> > sendBlkName2nextIP; 
    vector<pair<string, string> > sendBlkName2proxyIP; 
    vector<pair<string, string> > recvStripeName2chunkName;
    vector<pair<string, string> > recvStripeName2nextIP;
    vector<pair<string, int> >    recvStripeName2recvBlkNum;

    // define flag for multi-threading
    int send_flag;
    int recv_flag;
    int rs_flag; 
    int rack_dl_cnt = 0;
    while(1){

	// we should use multi-thread to listen commands as one node may receive two commands in a repair round
        cout << "PeerNode: listen command ..." << endl;
        recv_cmd = sock->recvCommand(PN_RECV_CMD_PORT, cmd_unit_len);
	if(recv_cmd == NULL)
	    continue;

        // define variables in cmd
        string role;
        string chunk_name;
        string stripe_name;
        string coeff;
        string next_ip;
        string ntwk_next_ip;
        string ntwk_proxy_ip;
        int cmd_id;

        // init flag
        send_flag = 0;
        recv_flag = 0;
        rs_flag = 0;

        // init maps
        sendBlkName2stripeName.clear();
        sendBlkName2coeff.clear();
        sendBlkName2nextIP.clear();
        sendBlkName2proxyIP.clear();
        recvStripeName2chunkName.clear();
        recvStripeName2recvBlkNum.clear();
        recvStripeName2nextIP.clear();
        recv_vector.clear();

        while(!send_queue.empty())
            send_queue.pop();

        index = 0;
        cmd_id=0;
        recv_chunk_num=0;
        sum_chunk_num = 0;
        if_first = 1;
        // we process the command first and then use two threads for sending and receiving data
        while(recv_cmd[index]!='\0'){
            // copy each command
            memcpy(one_cmd, recv_cmd+index, cmd_unit_len);
            index += cmd_unit_len;
    
            cout << "parse cmd ..." << endl;
            pn->parseCommand(one_cmd, role, chunk_name, stripe_name, coeff, next_ip, proxy_ip); 

            cout << "====== recv cmd ======" << endl; 
	    cout << "cmd_id = " << cmd_id << endl;
            cout << role        << endl;
            cout << chunk_name  << endl;
            cout << stripe_name << endl;
            cout << coeff       << endl;
            cout << next_ip     << endl;
            cout << proxy_ip     << endl;

            cmd_id++;
            ntwk_next_ip = pn->ip2Str(atoi((char*)next_ip.c_str()));
            ntwk_proxy_ip = pn->ip2Str(atoi((char*)proxy_ip.c_str()));

            if(if_first){
                string tmp_mark = chunk_name.substr(0,2);
                rack_dl_cnt = atoi((char*)tmp_mark.c_str());
                if_first = 0;
            }

            cout << "network_next_ip = " << ntwk_next_ip << endl;
            cout << "network_proxy_ip = " << ntwk_proxy_ip << endl;
            cout << "rack_dl_cnt = " << rack_dl_cnt;
            cout << "=======================" << endl;
            // if need to send data only, then keep the information in a send_queue
            if(role == "SO"){
                // update the info
                send_queue.push(chunk_name);
                sendBlkName2stripeName.push_back(make_pair(chunk_name, stripe_name));
                sendBlkName2coeff.push_back(make_pair(chunk_name, coeff));
                sendBlkName2nextIP.push_back(make_pair(chunk_name, ntwk_next_ip)); 
                sendBlkName2proxyIP.push_back(make_pair(chunk_name, ntwk_proxy_ip)); 
                send_flag = 1;
            }

            else if (role == "RO"){
                recv_chunk_num = atoi(coeff.c_str());
                sum_chunk_num += recv_chunk_num;
                cout << "recv_chunk_num = " << recv_chunk_num << endl;
                cout << "sum_chunk_num = " << sum_chunk_num << endl;
                recv_vector.push_back(stripe_name);
                recvStripeName2chunkName.push_back(make_pair(stripe_name, chunk_name));
                recvStripeName2recvBlkNum.push_back(make_pair(stripe_name, recv_chunk_num));
                recv_flag = 1;
            }

            // add send and recv code
            else if(role == "RS"){
                recv_chunk_num = atoi(coeff.c_str());
                sum_chunk_num += recv_chunk_num;
                cout << "recv_chunk_num = " << recv_chunk_num << endl;
                cout << "sum_chunk_num = " << sum_chunk_num << endl;
                cout << "debug: rack_dl_cnt = " << rack_dl_cnt;
                recv_vector.push_back(stripe_name);
                recvStripeName2recvBlkNum.push_back(make_pair(stripe_name, recv_chunk_num));
                recvStripeName2nextIP.push_back(make_pair(stripe_name, ntwk_next_ip));
                rs_flag = 1;
            }
        }

        // multi-thread for sending and receiving data
        if(send_flag){
           doThreads[0] = thread(&PeerNode::sendQueueData, pn, send_queue, sendBlkName2stripeName, sendBlkName2coeff, sendBlkName2nextIP, sendBlkName2proxyIP);
        }

        if(recv_flag){
            doThreads[1] = thread(&PeerNode::repairData, pn, recv_vector, recvStripeName2chunkName, recvStripeName2recvBlkNum, recvStripeName2nextIP, sum_chunk_num, FULL_REPAIR);
        }

        if(rs_flag){
            pn->repairData(recv_vector, recvStripeName2chunkName, recvStripeName2recvBlkNum, recvStripeName2nextIP, sum_chunk_num, rack_dl_cnt); 
        }

        if(send_flag)
            doThreads[0].join();
        if(recv_flag)
            doThreads[1].join();
        free(recv_cmd);

        if(recv_flag)
            pn->commitACK();
    }

    free(mark_recv);
    free(cur_conn);
    free(total_recv_data);
    free(one_cmd);
    delete conf;
    delete sock;
    delete pn;
}
