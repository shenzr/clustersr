#include "PeerNode.hh"

// read the information from the config file
PeerNode::PeerNode(Config* conf){
    _chunk_size = conf->_chunk_size;
    _packet_size = conf->_packet_size;
    _meta_size = conf->_meta_size;
    _ecK = conf->_ecK;
    _data_path = conf->_localDataPath;
    _coordinator_ip = conf->_coordinatorIP;
    _rack_num = conf->_rack_num;
    _peer_node_num = conf->_peer_node_num;
    cout << "data_path = " << _data_path << endl;

}

// init char array
void PeerNode::initChar(char** array, int len, int num){

    for(int i=0; i<num; i++)
    array[i] = (char*)malloc(sizeof(char)*len);
}

// parse and extract the information from the command
void PeerNode::parseCommand(string one_cmd, string& role, string& blk_name, string& stripe_name, string& coeff, string& next_ip, string& proxy_ip){

    size_t cur_pos;
    role = one_cmd.substr(0, ROLE_LEN);

    cur_pos = ROLE_LEN;
    blk_name = one_cmd.substr(cur_pos, BLK_NAME_LEN);
    
    cur_pos += BLK_NAME_LEN;
    stripe_name = one_cmd.substr(cur_pos, STRIPE_NAME_LEN);
    
    cur_pos += STRIPE_NAME_LEN;
    coeff = one_cmd.substr(cur_pos, COEFF_LEN);
    
    cur_pos += COEFF_LEN;
    next_ip = one_cmd.substr(cur_pos, NEXT_IP_LEN);
    
    cur_pos += NEXT_IP_LEN;
    proxy_ip = one_cmd.substr(cur_pos, NEXT_IP_LEN);
}

// transform a ip address to string
string PeerNode::ip2Str(unsigned int ip){

    string retVal;
    retVal += to_string(ip & 0xff);
    retVal += '.';
    
    retVal += to_string((ip >> 8) & 0xff);
    retVal +='.';

    retVal += to_string((ip >> 16) & 0xff);
    retVal +='.';

    retVal += to_string((ip >> 24) & 0xff);
    return retVal;
}


// partial encode and send data 
void PeerNode::partialEncodeSendData(char* stripe_name, int coeff, char* data_buff, char* next_ip, int* mark_index){

    cout << "coeff = " <<  coeff << endl;
    // partial encode data
    Socket* sock = new Socket();
    char* pse_buff = (char*)malloc(sizeof(char)*_packet_size);
    int num_packets = _chunk_size/_packet_size;

    // init the client socket info and connect to the server
    int client_socket = sock->initClient(0);

    struct sockaddr_in server_addr;
    bzero(&server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PN_RECV_DATA_PORT);
    if(inet_aton(next_ip, &server_addr.sin_addr) == 0)
        perror("inet_aton fails");
   
    cout << "next_ip = " << next_ip << endl;
    while(connect(client_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0);

    // use a socket to send data multiple times
    int sent_count = 0;
    int ret;

    // we send the stripe name first 
    ret = write(client_socket, stripe_name, STRIPE_NAME_LEN);
    assert(ret == STRIPE_NAME_LEN);
    while(sent_count < num_packets){

        if(mark_index[sent_count] == 0)
           sleep(0.001);

        if(mark_index[sent_count] == 1){
 
            if(coeff!=-1){
                galois_w08_region_multiply(data_buff+sent_count*_packet_size, coeff, _packet_size, pse_buff, 0);

                size_t sent_len = 0;
                while(sent_len < _packet_size){
                    ret =  write(client_socket, pse_buff + sent_len, _packet_size - sent_len);
                    sent_len += ret;
                }  
            }
            else{
                size_t sent_len = 0;
                while(sent_len < _packet_size){
                    ret =  write(client_socket, data_buff + sent_len, _packet_size - sent_len);
                    sent_len += ret;
                }  
            }

           sent_count++;
           //gettimeofday(&ed_tm, NULL);
           //printf("encode_send_data_time = %.2lf\n", ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000);
            
        }
    }
    ret = close(client_socket);
    if(ret == -1){
        cout << "ERR: close socket" << endl;
        exit(1);
    }
    delete sock;
    free(pse_buff);
}

// read data from the data path 
void PeerNode::readData(char* buff, string blk_abs_path, int* mark_index){

    struct timeval bg_tm, ed_tm;
    gettimeofday(&bg_tm, NULL);

    int fd = open((char*)blk_abs_path.c_str(), O_RDONLY);
    if(fd<0)
        perror("open_file_fails");
    
    size_t read_size = 0;
    size_t ret;
    int read_count = 0;
    size_t rd_pkt_len;
    while(read_size < _chunk_size){

        rd_pkt_len = 0;
        while(rd_pkt_len < _packet_size){
            ret = read(fd, buff + read_count*_packet_size + rd_pkt_len, _packet_size - rd_pkt_len);
            rd_pkt_len += ret;
        }
        assert(rd_pkt_len == _packet_size);

        read_size += _packet_size;
        lseek(fd, read_size, SEEK_SET);
        mark_index[read_count++] = 1;
    }

    close(fd);

    gettimeofday(&ed_tm, NULL);
    cout << "read_data_time = " << ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000 << endl;
}

void PeerNode::proxyForward(char* recv_stripe_vector, char* dest_ip_vector, int* cur_recv_vector, int* conn, int expect_recv_num, int start_port_num){

    cout << "======> enter proxyForward" << endl; 

    thread recvprxthrd;
    thread sendprxthrd; 

    Socket* sock = new Socket();
    int packet_num;
    int i;
    string dest_ip_str;

    packet_num = _chunk_size/_packet_size;
    char stripe_name[STRIPE_NAME_LEN];
    char dest_ip[NEXT_IP_LEN];
    char* recv_data_buff = (char*)malloc(sizeof(char)*_chunk_size);
    int* mark_recv_pkt = (int*)malloc(sizeof(int)*packet_num);

    for(i=0; i<expect_recv_num; i++){
		printf("cur_recv_vector[i]=%d\n", cur_recv_vector[i]);
        cout << "debug: i = " << i << endl;
        if(cur_recv_vector[i]==0){
            i--;
            sleep(0.01);
        }
        else{
            strncpy(stripe_name, recv_stripe_vector+i*STRIPE_NAME_LEN, STRIPE_NAME_LEN);
            strncpy(dest_ip, dest_ip_vector+i*NEXT_IP_LEN, NEXT_IP_LEN);
            dest_ip_str = ip2Str(atoi(dest_ip));
            memset(mark_recv_pkt, 0, sizeof(int)*packet_num);
            cout << "***** proxy forward: cur_index = " << i << endl;
            cout << "***** proxy forward: expect_num = " << expect_recv_num << endl;
            cout << "***** proxy forward: stripe_name = " << stripe_name << endl;
            cout << "***** proxy forward: dest_ip = " << dest_ip_str << endl;
            recvprxthrd = thread(&Socket::recvData, sock, _chunk_size, conn[i], recv_data_buff, 0, mark_recv_pkt, packet_num, _packet_size);
            sendprxthrd = thread(&PeerNode::aggrDataSendData, this, (char*)dest_ip_str.c_str(), recv_data_buff, 1, mark_recv_pkt, stripe_name, start_port_num+i);
            recvprxthrd.join();
            sendprxthrd.join();
            cout << "***** proxy forward: send data complete " << endl;
        }
    } 

    delete sock;
    free(recv_data_buff);
    free(mark_recv_pkt);
}



void PeerNode::repairData(vector<string> recv_stripe_vector, vector<pair<string, string> > recvStripeName2chunkName, 
                          vector<pair<string, int> > recvStripeName2recvBlkNum, vector<pair<string, string> > recvStripeName2nextIP, 
                          int recv_chk_num, int flag){

    cout << "======> enter repairData" << endl; 
    char stripe_name[STRIPE_NAME_LEN+1];
    char dest_ip[NEXT_IP_LEN+1]; 
    int rpr_stripe_num;
    int buff_index;
    int stripe_index;
    int conn_index;
    int i;
    int ret;
    string ntwk_dest_ip;
    string dest_ip_str;

    Config* conf = new Config("metadata/config.xml");

    rpr_stripe_num = recv_stripe_vector.size();
    cout << "rpr_stripe_num = " << rpr_stripe_num << endl;
    for(i=0; i<rpr_stripe_num; i++)
        cout << "rpr_stripe = " << recv_stripe_vector[i] << endl;
    cout<< endl;

    vector<pair<string, string> >::iterator strp_it;
    vector<pair<string, string> >::iterator chnk_it;
    vector<pair<string, int> >::iterator chunk_num_it;

    thread dothrds[recv_chk_num+rpr_stripe_num];
    thread proxyfwdthrd;

    int packet_num = _chunk_size/_packet_size;
    int* expect_recv_cnt = (int*)malloc(sizeof(int)*rpr_stripe_num);
    int* cur_recv_cnt = (int*)malloc(sizeof(int)*rpr_stripe_num);

    // allocate the size for received data
    char** recv_data_buff = (char**)malloc(sizeof(char*)*rpr_stripe_num);
    int** mark_recv_pkt = (int**)malloc(sizeof(int*)*rpr_stripe_num);

    string chunk_name[rpr_stripe_num];
    string next_ip[rpr_stripe_num];

    // get the number of received chunks for each repaired stripe
    stripe_index = 0;
    for(chunk_num_it = recvStripeName2recvBlkNum.begin(); chunk_num_it!=recvStripeName2recvBlkNum.end(); chunk_num_it++){
        ret = posix_memalign((void**)&recv_data_buff[stripe_index], getpagesize(), sizeof(char)*_chunk_size*chunk_num_it->second);
        mark_recv_pkt[stripe_index] = (int*)malloc(sizeof(int)*chunk_num_it->second*packet_num);
        expect_recv_cnt[stripe_index] = chunk_num_it->second;
        cout << stripe_index << "-th stripe receives " << chunk_num_it->second << " chunks" << endl;
        stripe_index++;
    }
    for(i=0; i<rpr_stripe_num; i++){
        memset(mark_recv_pkt[i], 0, sizeof(int)*expect_recv_cnt[i]*packet_num);
    }
    assert(stripe_index == rpr_stripe_num);


    // get each repaired chunk name
    stripe_index = 0;
    for(chnk_it = recvStripeName2chunkName.begin(); chnk_it!=recvStripeName2chunkName.end(); chnk_it++){
        chunk_name[stripe_index] = chnk_it->second;
        stripe_index++;
    }

    // get the next ip addr for partial repair (proxy node)
    stripe_index = 0;
    for(chnk_it = recvStripeName2nextIP.begin(); chnk_it!=recvStripeName2nextIP.end(); chnk_it++){
        next_ip[stripe_index] = chnk_it->second;
        stripe_index++;
    }

    for(i=0; i<rpr_stripe_num; i++){
        cout << "next_ip[" << i << "] = " << next_ip[i] << endl;
    }

    // for proxy to forward data
    int* proxy_connfd = (int*)malloc(sizeof(int)*flag);
    int* proxy_recv_vector = (int*)malloc(sizeof(int)*flag);;
    char* proxy_fwd_stripe_vecotr = (char*)malloc(sizeof(char)*flag*STRIPE_NAME_LEN);
    char* proxy_fwd_destip_vector = (char*)malloc(sizeof(char)*flag*NEXT_IP_LEN);
    memset(proxy_recv_vector, 0 ,sizeof(int)*flag);

    // for socket connections
    Socket* sock = new Socket();
    int* connfd = (int*)malloc(sizeof(int)*recv_chk_num);
    int server_socket = sock->initServer(PN_RECV_DATA_PORT);
    struct sockaddr_in sender_addr;
    socklen_t length = sizeof(sender_addr);

    if(listen(server_socket, 100) == -1)
        perror("listen fails");

    memset(cur_recv_cnt, 0, sizeof(int)*rpr_stripe_num);

    int fwd_cnt = 0;
    int cur_conn;
    conn_index=0;
    
    while(1){
       
        cur_conn = accept(server_socket, (struct sockaddr*)&sender_addr, &length);
        cout << "--recv_connection_from " << inet_ntoa(sender_addr.sin_addr) << endl;

        // read four bytes for determining the stripe name
        ret = read(cur_conn, stripe_name, STRIPE_NAME_LEN);
        assert(ret == STRIPE_NAME_LEN);
        stripe_name[STRIPE_NAME_LEN] = '\0';
        cout << "recv_stripe_name = " << stripe_name << endl;

        // check if comes from a proxy 
        for(i=0; i<_rack_num; i++){
            if(inet_addr(inet_ntoa(sender_addr.sin_addr)) == conf->_proxyIPs[i])
                break;
        }

        if(i<_rack_num && flag != FULL_REPAIR){
            ret = read(cur_conn, dest_ip, NEXT_IP_LEN);
            assert(ret == NEXT_IP_LEN);
            dest_ip[NEXT_IP_LEN] = '\0';
            dest_ip_str = ip2Str(atoi(dest_ip));
            cout << "* from proxy" << endl;
            cout << "orig_dest_ip = " << dest_ip_str << endl;
            cout << "fwd_cnt = " << fwd_cnt << endl;
 
            proxy_connfd[fwd_cnt] = cur_conn;
            proxy_recv_vector[fwd_cnt] = 1;
            strncpy(proxy_fwd_stripe_vecotr+fwd_cnt*STRIPE_NAME_LEN, stripe_name, STRIPE_NAME_LEN);
            strncpy(proxy_fwd_destip_vector+fwd_cnt*NEXT_IP_LEN, dest_ip, NEXT_IP_LEN);

            cout << "proxy_recv_vector :" << endl;
            for(int j=0; j<flag; j++){
				cout << proxy_recv_vector[j] << " ";
			}
			cout << endl;

            if(fwd_cnt==0){
                proxyfwdthrd = thread(&PeerNode::proxyForward, this, proxy_fwd_stripe_vecotr, proxy_fwd_destip_vector, proxy_recv_vector, proxy_connfd, flag, rpr_stripe_num);
            }

            fwd_cnt++;
            if((conn_index == recv_chk_num) && (fwd_cnt == flag))
                break;
            else  
                continue;
        }

        connfd[conn_index] = cur_conn;
        // check the number of recv chunks of that stripe
        // get the stripe_index
        for(i=0; i<rpr_stripe_num; i++){
            cout << "recv_stripe_vector[" << i<< "] = " << recv_stripe_vector[i] << endl;
            if(recv_stripe_vector[i] == string(stripe_name)){
                stripe_index = i;
                break;
            }
        }
        cout << "stripe_index = " << stripe_index << endl;
        assert(i<rpr_stripe_num);
        
        // get buff_index
        buff_index = cur_recv_cnt[stripe_index];
        cout << "buff_index = " << buff_index << endl;
        cout << "conn_index = " << conn_index << endl;
        
        // read the data by using multi-threading
        dothrds[conn_index] = thread(&Socket::recvData, sock, _chunk_size, connfd[conn_index], 
                                    recv_data_buff[stripe_index] + buff_index*_chunk_size, buff_index, 
                                    mark_recv_pkt[stripe_index], packet_num, _packet_size);

        // if all the associated connections have been established, then aggregate and write data 
        cur_recv_cnt[stripe_index]++;

        cout << "send_stripe_name = " << stripe_name << endl;
        if((cur_recv_cnt[stripe_index] == expect_recv_cnt[stripe_index]) && (flag == FULL_REPAIR))
            dothrds[recv_chk_num+stripe_index] = thread(&PeerNode::aggrDataWriteData, this, 
                                                        chunk_name[stripe_index], recv_data_buff[stripe_index], 
                                                        expect_recv_cnt[stripe_index], _chunk_size, 
                                                        mark_recv_pkt[stripe_index], DATA_CHUNK);

        else if((cur_recv_cnt[stripe_index] == expect_recv_cnt[stripe_index]) && (flag != FULL_REPAIR)){
            int record_index = stripe_index;
            dothrds[recv_chk_num+record_index] = thread(&PeerNode::aggrDataSendData, this, 
                                                        (char*)next_ip[record_index].c_str(), recv_data_buff[record_index], 
                                                        expect_recv_cnt[record_index], mark_recv_pkt[record_index], (char*)recv_stripe_vector[record_index].c_str(), record_index);
        }
            
        conn_index++;
        if((conn_index == recv_chk_num) && (flag == FULL_REPAIR))
            break;
        else if((conn_index == recv_chk_num) && (fwd_cnt == flag))
            break;
    }
    
    // join threads
    for(i=0; i<recv_chk_num+rpr_stripe_num; i++)
        dothrds[i].join();

    if(fwd_cnt>0)
        proxyfwdthrd.join();

    // close the socket info
    close(server_socket);
    for(i=0; i<recv_chk_num; i++)
        close(connfd[i]);
    free(connfd);

    for(i=0; i<flag; i++)
        close(proxy_connfd[i]);

    // free data buff
    for(i=0; i<rpr_stripe_num; i++){
        free(mark_recv_pkt[i]);
        free(recv_data_buff[i]);
    }
    free(mark_recv_pkt);
    free(recv_data_buff);   
    free(proxy_fwd_destip_vector);
    free(proxy_fwd_stripe_vecotr);
    free(proxy_recv_vector);
    free(proxy_connfd);
    free(cur_recv_cnt);
    free(expect_recv_cnt);
    delete sock;
    delete conf;
}

void PeerNode::sendQueueData(queue<string> send_queue, vector<pair<string, string> > sendBlkName2stripeName, 
                             vector<pair<string, string> > sendBlkName2coeff, vector<pair<string, string> > sendBlkName2nextIP, 
                             vector<pair<string, string> > sendBlkName2proxyIP){

    // pop the chunk in the queue and send it
    cout << "=======> enter sendQueueData" << endl;
    string chunk_name;
    string stripe_name;
    string next_ip;
    string proxy_ip;
    string coeff;

    char this_stripe[STRIPE_NAME_LEN+1];
    
    vector<pair<string, string> >::iterator ip_it;
    vector<pair<string, string> >::iterator strp_it;
    vector<pair<string, string> >::iterator coeff_it;
    vector<pair<string, string> >::iterator proxy_it;

    // send the chunks in the queue until the queue is empty
    while(1){

        cout << "---debug 1" << endl;
        chunk_name = send_queue.front();
        cout << "---debug chunk_name = " << chunk_name << endl;
        send_queue.pop();

        cout << "---debug 2" << endl;
        for(strp_it = sendBlkName2stripeName.begin(); strp_it != sendBlkName2stripeName.end(); strp_it++){
            if(strp_it->first == chunk_name)
                break;
        }
        stripe_name = strp_it->second;

        cout << "---debug 3" << endl;
        for(int i=0; i<STRIPE_NAME_LEN; i++)
            this_stripe[i] = stripe_name[i];
        this_stripe[STRIPE_NAME_LEN] = '\0';

        cout << "---debug 4" << endl;
        for(ip_it = sendBlkName2nextIP.begin(); ip_it != sendBlkName2nextIP.end(); ip_it++){
            if(ip_it->first == chunk_name)
                break;
        }
        next_ip = ip_it->second;

        cout << "---debug 5" << endl;
        for(proxy_it = sendBlkName2proxyIP.begin(); proxy_it != sendBlkName2proxyIP.end(); proxy_it++){
            if(proxy_it->first == chunk_name)
                break;
        }
        proxy_ip = proxy_it->second;

        cout << "---debug 6" << endl;
        for(coeff_it = sendBlkName2coeff.begin(); coeff_it != sendBlkName2coeff.end(); coeff_it++){
            if(coeff_it->first == chunk_name)
                break;
        }
        coeff = coeff_it->second;

        // send the data 
        // we should send the stripe_name with the data
        cout << "chunk_name  = " << chunk_name << endl;
        cout << "stripe = "      << this_stripe << endl;
        cout << "(int)coeff = "  << atoi(coeff.c_str()) << endl;
        cout << "proxy_ip = "    << proxy_ip.c_str() << endl;
        
        sendData(chunk_name, (char*)stripe_name.c_str(), atoi(coeff.c_str()), (char*)proxy_ip.c_str(), DATA_CHUNK); 
        if(send_queue.size()==0)
            break;
    }    
}

// send data 
void PeerNode::sendData(string blk_name, char* stripe_name, int coeff, char* next_ip, int flag){
   
    struct timeval bg_tm, ed_tm;
    gettimeofday(&bg_tm, NULL);

    // get absolute path
    string blk_abs_path;
    string parent_dir;

    size_t sent_size;
    if(flag == DATA_CHUNK)
        sent_size = _chunk_size;
    else 
        sent_size = _meta_size;

    //parent_dir = findChunkAbsPath((char*)blk_name.c_str(), (char*)_data_path.c_str());
    //blk_abs_path = parent_dir + string("/") + blk_name;
    blk_abs_path = string("/root/datafile");
    cout << "blk_abs_path = " << blk_abs_path << endl;

    // multi-thread: 1) read data; 2) send data 
    // read data
    int num_packets;
    num_packets = sent_size/_packet_size;

    char* buff;
    size_t ret;

    // if there are more packets, then we use multiple threads
    if(flag == DATA_CHUNK){

        ret = posix_memalign((void**)&buff, getpagesize(), sizeof(char)*sent_size);
        if(ret){
            cout << "ERR: posix_memalign" << endl;
            exit(1);
         }

        int* mark_read = (int*)malloc(sizeof(int)*num_packets);
        memset(mark_read, 0, sizeof(int)*num_packets);

        thread dothrds[2];
        dothrds[0] = thread(&PeerNode::readData, this, buff, blk_abs_path, mark_read);
        dothrds[1] = thread(&PeerNode::partialEncodeSendData, this, stripe_name, coeff, buff, next_ip, mark_read);
                                             
        dothrds[0].join();
        dothrds[1].join();
       
        //cout << "debug: thread ends" << endl; 
        free(mark_read); 
    }

    //if it is the metadata chunk 
    else {
        //read the data
        int fd = open((char*)blk_abs_path.c_str(), O_RDWR);
        buff = (char*)malloc(sizeof(char)*sent_size);

        ret = read(fd, buff, sent_size);
        if(ret!=sent_size){
            cout << "ret = " << ret << endl;
            exit(1);
        } 
        // sent the data
        Socket* sock = new Socket();
        sock->sendData(buff, sent_size, next_ip, PN_RECV_DATA_PORT);
        close(fd);
        delete sock;
    }

    cout << "<--------sendData " << endl;
    free(buff);
    
    gettimeofday(&ed_tm, NULL);
    cout << "sendData time = " << ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000 << endl;

}

// write the data 
void PeerNode::writeData(string chunk_name, size_t offset, char* write_buff, size_t write_size, int flag){

    struct timeval bg_tm, ed_tm;
    gettimeofday(&bg_tm, NULL);

    // append the data, we put the repaired data on the subdir0
    size_t ret;
    string abs_lost_chunk_path; 
    //You can choose to place the repaired chunk at the default data path of HDFS 
    // abs_lost_chunk_path = _data_path + string("/subdir0/") + chunk_name;
    abs_lost_chunk_path = string("/root/") + chunk_name;

    int fd;
    if(flag == DATA_CHUNK)
        fd = open((char*)abs_lost_chunk_path.c_str(), O_RDWR | O_CREAT | O_SYNC, 0755);
    else 
        fd = open((char*)abs_lost_chunk_path.c_str(), O_RDWR | O_CREAT, 0755);

    lseek(fd, offset, SEEK_SET);
    
    ret = write(fd, write_buff, write_size);
    if(ret!=write_size)
        perror("write error");

    close(fd);

    //remove the write data
    if(remove((char*)abs_lost_chunk_path.c_str())!=0)
        perror("remove file");

    gettimeofday(&ed_tm, NULL);
    cout << "writeData time = " << ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000 << endl;
}

void PeerNode::getLocalIP(char* local_ip){

    int sock;
    struct sockaddr_in sin;
    struct ifreq ifr;

    sock = socket(AF_INET, SOCK_DGRAM, 0);

    strncpy(ifr.ifr_name, "eth0", IFNAMSIZ);
    ifr.ifr_name[IFNAMSIZ-1]=0;

    if(ioctl(sock, SIOCGIFADDR, &ifr)<0)
        perror("ioctl");

    memcpy(&sin, &ifr.ifr_addr, sizeof(sin));

    close(sock);
    strcpy(local_ip, inet_ntoa(sin.sin_addr));
}

int PeerNode::getRackID(Config* conf, char* ip){
    int i;
    int num_node_rack = _peer_node_num/_rack_num; 
    for(i=0; i<_peer_node_num; i++){
        if(inet_addr(ip) == conf->_peerNodeIPs[i])
            break;
    }
    if(i<_peer_node_num)
        return i/num_node_rack;
    
    for(i=0; i<_rack_num; i++){
        if(inet_addr(ip) == conf->_proxyIPs[i])
            break;
    }
    assert(i<_rack_num);
    return i;
}

// aggregate the data and send the data
void PeerNode::aggrDataSendData(char* next_ip, char* recv_buff, int recv_chunk_num, int* mark_recv, char* stripe_name, int port_num){

    cout << "======> Enter aggrDataSendData " << endl;
    cout << "---debug: stripe_name = " << stripe_name << endl;
    cout << "---debug: next_ip = " << next_ip << endl;
    struct timeval bg_tm, ed_tm;
    gettimeofday(&bg_tm, NULL);

    int i;
    int packet_num = _chunk_size/_packet_size;
    int sum = 0;
    int tmp_ret;
    size_t ret;
    size_t sent_len;
    char* repair_buff = (char*)malloc(sizeof(char)*_packet_size);;

    // check the rack ids of the local_ip and next_ip
    Config* conf = new Config("metadata/config.xml");
    char local_ip[20];
    char dest_ip[20];
    int this_rack, next_rack;
    string dest_ip_str = "";

    getLocalIP(local_ip);
    cout << "local_ip = " << local_ip << endl;
    this_rack = getRackID(conf, local_ip);
    next_rack = getRackID(conf, next_ip);

    cout << "this_rack = " << this_rack << endl;
    cout << "next_rack = " << next_rack << endl;

    if(this_rack!=next_rack){
        // next_ip is the next proxy ip
        strcpy(dest_ip, next_ip);
        next_ip = (char*)ip2Str(conf->_proxyIPs[next_rack]).c_str();
        cout << "debug: next_rack = " << next_rack << endl;
        cout << "debug: next_ip = " << next_ip << endl;
    }   

    cout << "next_ip = " << next_ip << endl;

    // init the client socket info and connect to the server
    Socket* sock = new Socket();
    int client_socket = sock->initClient(port_num);
    // set server_addr info
    struct sockaddr_in server_addr;
    bzero(&server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PN_RECV_DATA_PORT);
    if(inet_aton(next_ip, &server_addr.sin_addr) == 0)
        perror("inet_aton fails");

    while(connect(client_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0);

    // we send the stripe name first 
    ret = write(client_socket, stripe_name, STRIPE_NAME_LEN);
    assert(ret == STRIPE_NAME_LEN);

    // if the next_ip is not in the same rack, then send the dest_ip
    string tmp_dest_ip_str = to_string(inet_addr(dest_ip));
    if(this_rack!=next_rack){
        for(i=(int)tmp_dest_ip_str.length(); i<NEXT_IP_LEN; i++)
            dest_ip_str+="0";
        dest_ip_str += tmp_dest_ip_str;  
        ret = write(client_socket, (char*)dest_ip_str.c_str(), NEXT_IP_LEN);
    }

    // aggregate the data and send it
    for(i=0; i<packet_num; i++){
        sum = 0;
        for(int j=0; j<recv_chunk_num; j++)
            if(mark_recv[j*packet_num+i])
                sum++;
        
        // if the first packet of all the chunks is not received
        if(sum!=recv_chunk_num){
            i--;
            sleep(0.001);
        }
        // aggregate the packets and send the packet 
        else {
            char* tmp_pos;
            tmp_pos = sock->aggrData(recv_buff, repair_buff, recv_chunk_num, _chunk_size, i, _packet_size);

            sent_len = 0;
            while(sent_len < _packet_size){
                ret =  write(client_socket, tmp_pos + sent_len, _packet_size - sent_len);
                sent_len += ret;
            }  
        }
    }
    
    free(repair_buff);
    delete(sock);
    delete(conf);

    tmp_ret = close(client_socket);
    if(tmp_ret == -1){
        cout << "ERR: close socket" << endl;
        exit(1);
    }
    // remove the file after write completes 
    //if(remove((char*)abs_lost_chunk_path.c_str()) != 0) --------> mark at Mar. 06
        //perror("remove file"); --------------> mark at Mar.06

   gettimeofday(&ed_tm, NULL);
   cout << "aggr_data_send_data_time = " << ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000 << endl;
}


// aggregate the data and write the data
void PeerNode::aggrDataWriteData(string chunk_name, char* recv_buff, int recv_chunk_num, size_t recv_size, int* mark_recv, int flag){

    struct timeval bg_tm, ed_tm;
    gettimeofday(&bg_tm, NULL);

    // we have to differentiate data chunk and metadata chunk
    if((recv_chunk_num >= 1) && (recv_size == _chunk_size)){

        size_t ret;
        int fd;
        char* repair_buff;
        // string abs_lost_chunk_path = _data_path + string("/subdir0/") + chunk_name;
        // string abs_lost_chunk_path = string("/home/ncsgroup/zrshen/") + chunk_name;
        // -----> modify zrshen, 2019/10/10
        string abs_lost_chunk_path = string("/root/") + chunk_name; 
        fd = open((char*)abs_lost_chunk_path.c_str(), O_RDWR | O_CREAT, 0755);
        cout << "abs_lost_chunk_path = " << abs_lost_chunk_path << endl;
        if(fd<0){
            cout << "ERR: open file" << endl;
            exit(1);
        }

        int tmp_ret = posix_memalign((void**)&repair_buff, getpagesize(), sizeof(char)*_packet_size);
        if(tmp_ret){
            cout << "ERR: posix_memalign" << endl;
            exit(1);         
        }
        char* tmp_pos;
        Socket* sock = new Socket();

        int packet_num = _chunk_size/_packet_size;
        int sum = 0;
        int i;
        for(i=0; i<packet_num; i++){
            sum = 0;
            for(int j=0; j<recv_chunk_num; j++)
                if(mark_recv[j*packet_num+i])
                    sum++;
            
            // if the first packet of all the chunks is not received
            if(sum!=recv_chunk_num){
                i--;
                sleep(0.001);
            }
            // aggregate the packets and write the packet            
            else {
                tmp_pos = sock->aggrData(recv_buff, repair_buff, recv_chunk_num, recv_size, i, _packet_size);
                //writeData(chunk_name, i*_packet_size, tmp_pos, _packet_size, flag);               

                lseek(fd, i*_packet_size, SEEK_SET);
                ret = write(fd, tmp_pos, _packet_size);
                if(ret!=_packet_size)
                    perror("write error");
                
                //cout << "aggregate: mark_recv, check packet_id = " << i << endl;
                //for(int p=0; p<recv_chunk_num; p++){
                //    for(int q=0; q<packet_num; q++)
                //        printf("%d ", mark_recv[p*packet_num+q]);
                //printf("\n");
                //}    
            }
        }
        
        free(repair_buff);
        close(fd);
        delete sock;

        // remove the file after write completes 
        if(remove((char*)abs_lost_chunk_path.c_str()) != 0)
            perror("remove file");
    }

    // we have to further consider the writes for multiple metadata chunks
    else if(recv_size!=_chunk_size){
        writeData(chunk_name, 0, recv_buff, recv_size, flag);
    }
   gettimeofday(&ed_tm, NULL);
   cout << "write_data_time = " << ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000 << endl;
}

// parallel receive data 
void PeerNode::paraRecvData(int recv_chunk_num, string lost_chunk_name, int flag){

    struct timeval bg_tm, ed_tm;
    gettimeofday(&bg_tm, NULL);

    cout << "--------> paraRecvData, recv_chunk_num =  " << recv_chunk_num << endl;
    size_t recv_size;
    char* recv_buff;
    int ret;
    if(flag == DATA_CHUNK){
        recv_size = _chunk_size;
        ret = posix_memalign((void**)&recv_buff, getpagesize(), sizeof(char)*recv_chunk_num*recv_size);
        if(ret){
            cout << "ERR: posix_memalign" << endl;
            exit(1);
        }    
    }
    else{ 
        recv_size = _meta_size;
        recv_buff = (char*)malloc(sizeof(char)*recv_size*recv_chunk_num);
    }
    
    // mark the recv data 
    int packet_num = recv_size/_packet_size;
    int* mark_recv = NULL;

    if(flag == DATA_CHUNK){
        mark_recv = (int*)malloc(sizeof(int)*recv_chunk_num*packet_num);
        memset(mark_recv, 0, sizeof(int)*recv_chunk_num*packet_num);
    }

    // recv the data    
    Socket* sock = new Socket();
    thread dothrds[2];

    // multiple threads: recv data and write data
    dothrds[0] = thread(&Socket::paraRecvData, sock, PN_RECV_DATA_PORT, recv_size, recv_buff, recv_chunk_num, mark_recv, packet_num, _packet_size, flag);
    dothrds[1] = thread(&PeerNode::aggrDataWriteData, this, lost_chunk_name, recv_buff, recv_chunk_num, recv_size, mark_recv, flag);

    dothrds[0].join();
    dothrds[1].join();
    
    free(recv_buff);
    delete(sock);

    if(flag == DATA_CHUNK)
        free(mark_recv);

    cout << "<-------- paraRecvData" << endl;

    gettimeofday(&ed_tm, NULL);
    cout << "recvData time = " << ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000 << endl;
}

// commit ack
void PeerNode::commitACK(){

    // commit the ack
    char ack[ACK_LEN+1]="ACK";
    Socket* sock_ack = new Socket();
    cout << "coordinator ip = " << (char*)ip2Str(_coordinator_ip).c_str() << endl; 
    cout << "CD_RECV_ACK_PORT = " << CD_RECV_ACK_PORT << endl; 
    sock_ack->sendData(ack, ACK_LEN+1, (char*)ip2Str(_coordinator_ip).c_str(), CD_RECV_ACK_PORT);
    delete sock_ack;
    cout << "--------commit finishes--------" << endl;

}

// find the absolute path of a given data chunk
string PeerNode::findChunkAbsPath(char* chunk_name, char* parent_dir){

    DIR* dir = opendir(parent_dir);
    assert(dir!=NULL);

    string want_dir;
    string child_dir;
    struct dirent *d_ent;
    
    while((d_ent = readdir(dir))!=NULL){

        if((strcmp(d_ent->d_name,".")==0) || (strcmp(d_ent->d_name,"..")==0))
            continue;

        if((d_ent->d_type == 8) && strcmp(d_ent->d_name, chunk_name)==0){
            return parent_dir;
        }

        else if(d_ent->d_type == 4){
            child_dir = parent_dir + string("/") + string(d_ent->d_name);
            want_dir = findChunkAbsPath(chunk_name, (char*)child_dir.c_str());
            if(want_dir != ""){
                closedir(dir);
                return want_dir;
            }
        }
     }

     closedir(dir);
     return "";
}

// get values
int PeerNode::getVal(string req){

    if(req == "_ecK")
        return _ecK;

    else if(req == "_chunk_size")
        return _chunk_size;

    else if(req == "_packet_size")
        return _packet_size;

    else {
        cout << "ERR: input " << endl;
        exit(1);
    }
}

