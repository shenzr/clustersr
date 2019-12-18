#include "Coordinator.hh"

Coordinator::Coordinator(Config* conf){
    _conf = conf;
    init();
}

void Coordinator::display(int len, int width, int* array){
    for(int i=0; i<width; i++){
        for(int j=0; j<len; j++)
            printf("%d\t", array[i*len+j]);
        printf("\n");
    }
    printf("\n");
}

void Coordinator::init(void){

    _ecK = _conf->_ecK;
    _ecN = _conf->_ecN;
    _ecM = _ecN - _ecK;
    _stripe_num = _conf->_stripe_num;
    _peer_node_num = _conf->_peer_node_num;
    _chunk_size = _conf->_chunk_size;
    _packet_size = _conf->_packet_size;
    _rack_num = _conf->_rack_num;
    _balance_steps = _conf->_balance_steps;
    _balance_dev = _conf->_balance_dev;
    _disk_bdwh = _conf->_disk_bdwh;
    _cross_rack_bdwh = _conf->_cross_rack_bdwh;
    _intra_rack_bdwh = _conf->_intra_rack_bdwh;
    // temporarily assume that the number of chunks to be repaired relates to the number of racks
    _num_repair_chnk_round = 5*_conf->_rack_num;
    // read the placement
    _placement = (int*)malloc(sizeof(int)*_stripe_num*_ecN); 

    // read the metadata from HDFS
    // parseLog();

    
       ifstream readin("metadata/placement");
       for(int i=0; i<_stripe_num; i++)
       for(int j=0; j<_ecN; j++){
       readin >> _placement[i*_ecN+j];
       }
       readin.close();
     

    printf("_placement:\n");
    for(int i=0; i<_stripe_num; i++){
        for(int j=0; j<_ecN; j++)
            printf("%d ", _placement[i*_ecN+j]);
        printf("\n");          
    }
    printf("\n");
}


/* sort the array and move the corresponding index to the new position */
void Coordinator::QuickSort_index(int* data, int* index,int left, int right){
    int temp = data[left];
    int p = left;
    int temp_value=index[left];
    int i = left, j = right;

    while (i <= j){
        while (data[j] <= temp && j >= p)
            j--;
        if(j >= p) {
            data[p] = data[j];
            index[p]=index[j];
            p = j;
        }

        while (data[i] >= temp && i <= p)
            i++;
        if (i <= p){
            data[p] = data[i];
            index[p]=index[i];
            p = i;
        }
    }

    data[p] = temp;
    index[p]=temp_value;

    if(p - left > 1)
        QuickSort_index(data, index, left, p-1);
    if(right - p > 1)
        QuickSort_index(data, index, p+1, right);

}

void Coordinator::preprocess(int fail_node, int specified_repair_num){

    std::cout << "----enter: preprocess" << std::endl;
    // determine the number of repaired chunks  
    int i,j; 
    int index;

    _num_rebuilt_chunks=0;  
    for(i=0; i<_stripe_num; i++){
        for(j=0; j<_ecN; j++){
            if(_placement[i*_ecN+j]==fail_node){
                _num_rebuilt_chunks++;
                break;
            }
        }
    }

    if(_num_rebuilt_chunks < specified_repair_num){
        printf("ERR: _num_rebuilt_chunks < specified_repair_num\n");
        printf("_num_rebuilt_chunks = %d\n", _num_rebuilt_chunks);
        exit(1);
    }

    _num_rebuilt_chunks = specified_repair_num;
    index=0;
    // record the stripe id that comprises the failed chunk 
    _related_stripes=(int*)malloc(sizeof(int)*_num_rebuilt_chunks);
    for(i=0; i<_stripe_num; i++){
        for(j=0; j<_ecN; j++){  
            if(_placement[i*_ecN+j]==fail_node){
                _related_stripes[index++]=i;
                    break;
            }
        }
        if(index == _num_rebuilt_chunks)
            break;
    }

    // count the number of chunks in each rack for each stripe 
    int node_num_rack;
    int fail_stripe;
    int node_id;

    node_num_rack = _peer_node_num/_rack_num;
    _chnk_num_rack = (int*)malloc(sizeof(int)*_rack_num*_num_rebuilt_chunks);
    memset(_chnk_num_rack, 0, sizeof(int)*_rack_num*_num_rebuilt_chunks);

    printf("node_num=%d\n",_peer_node_num);
    printf("rack_num=%d\n",_rack_num);
    printf("node_num_rack=%d\n", node_num_rack);
    // calculate the number of remaining chunks in each rack
    for(i=0; i<_num_rebuilt_chunks; i++){
        fail_stripe = _related_stripes[i];
        for(j=0; j<_ecN; j++){ 
            node_id = _placement[fail_stripe*_ecN+j];
            if(node_id == fail_node)
                continue;

            _chnk_num_rack[i*_rack_num+node_id/node_num_rack]++;
            //assert(_chnk_num_rack[i*_rack_num+node_id/node_num_rack]!=(_ecM+1));
        }            
    }

#if DEBUG_COORD
        printf("before sort: _chnk_num_rack:\n");
        display(_rack_num, _num_rebuilt_chunks, _chnk_num_rack);
#endif

    // sort the racks 
    _sort_rack_index = (int*)malloc(sizeof(int)*_rack_num*_num_rebuilt_chunks);
    for(i=0; i<_num_rebuilt_chunks; i++){
        // initialize
        for(j=0; j<_rack_num; j++)
            _sort_rack_index[i*_rack_num+j]=j;
        // sort the racks 
        QuickSort_index(_chnk_num_rack+_rack_num*i, _sort_rack_index+_rack_num*i, 0, _rack_num-1);
    }

    printf("after sort: _chnk_num_rack:\n");
    display(_rack_num, _num_rebuilt_chunks, _chnk_num_rack);
    printf("after sort: _sort_rack_index:\n");
    display(_rack_num, _num_rebuilt_chunks, _sort_rack_index);
}

// get decoding coefficient 
int* Coordinator::getDecodeCoeff(int* remain_chunks, int* complete_enc_mat, int repair_chunk_id){

    int i;
    int remain_chunk_id;
    //cout << "complete_matrix:" << std::endl;
    //display(_ecK, _ecN, complete_enc_mat);

    //get remaining matrix
    int* remain_mat = (int*)malloc(sizeof(int)*_ecK*_ecK);
    for(i=0; i<_ecK; i++){
        remain_chunk_id = remain_chunks[i];
        memcpy((char*)remain_mat + i*_ecK*sizeof(int), (char*)complete_enc_mat + remain_chunk_id*_ecK*sizeof(int), sizeof(int)*_ecK);
    }

    cout << "remain_matrix:" << std::endl;
    display(_ecK, _ecK, remain_mat);

    //get inverted matrix
    int *invert_mat = (int*)malloc(sizeof(int)*_ecK*_ecK);
    jerasure_invert_matrix(remain_mat, invert_mat, _ecK, 8);

    //get coeff 
    return jerasure_matrix_multiply(complete_enc_mat + repair_chunk_id*_ecK, invert_mat, 1, _ecK, _ecK, _ecK, 8);
}

void Coordinator::readEncMat(int* complete_matrix){

    // get the cauchy encoding matrix
    memset(complete_matrix, 0, sizeof(int)*_ecK*_ecN);
    for(int i=0; i<_ecK; i++)
        complete_matrix[i*_ecK+i]=1;

    // get the filename of the encoding matrix 
    string enc_file_name = string("metadata/rsEncMat_") + to_string(_ecN) + string("_") + to_string(_ecK);
    // cout << "enc_file_name = " << enc_file_name << endl; 

    // read encoding matrix from the rsEncMat file
    ifstream readin(enc_file_name);
    for(int i=0; i<_ecN-_ecK; i++)
        for(int j=0; j<_ecK; j++){
            readin >> complete_matrix[(i+_ecK)*_ecK+j];
        }
    readin.close();

    cout << "complete_matrix:" << endl;
    display(_ecK, _ecN, complete_matrix);
}


/*  
 *  One node may receive two commands for receiving data and sending data
 */
void Coordinator::initCommand(string cmd[], string proxy_cmd[], int num_repair_chnk_round, int fail_node, int* rpr_sltns, int* rpr_chk_dist, int* rpr_chnk, int* enc_matrix){

    printf("=====> enter initCommand\n");
    int send_cnt;
    int glb_chk;
    int stripe;
    int cnt;
    int cur_coeff;
    int recv_cnt;
    int i, j, k;
    int num_node_per_rack; 
    int rack_id;
    int next_id;

    string chunk_name;
    string stripe_name;
    string recv_cmd;

    map<size_t, string>::iterator blk_it;
    map<string, string>::iterator strp_it;
    
    int* cmd_cnt = (int*)malloc(sizeof(int)*_peer_node_num);
    int* slct_k_chk = (int*)malloc(sizeof(int)*num_repair_chnk_round*_ecK);
    int* fail_lgc_id = (int*)malloc(sizeof(int)*num_repair_chnk_round);
    int* dest_node_id = (int*)malloc(sizeof(int)*num_repair_chnk_round);    
    int** decode_coeff = (int**)malloc(sizeof(int*)*num_repair_chnk_round);

    num_node_per_rack = _peer_node_num/_rack_num;
    memset(slct_k_chk, -1, sizeof(int)*num_repair_chnk_round*_ecK);
    // get the logical id of the failed chunks
    printf("=====fail_node=%d\n", fail_node);
    for(j=0; j<num_repair_chnk_round; j++){
        stripe = _related_stripes[rpr_chnk[j]];
        for(k=0; k<_ecN; k++){
            if(_placement[stripe*_ecN+k]==fail_node){
                fail_lgc_id[j] = k;
                break;
            }
        }
    }
    // record the destination
    memset(dest_node_id, -1, sizeof(int)*num_repair_chnk_round);
    for(i=0; i<num_repair_chnk_round; i++){
        for(j=0; j<_peer_node_num; j++){
            if(rpr_chk_dist[i*_peer_node_num+j]<-1){
                dest_node_id[i] = j;
                break;
            }
        }
    }
    //printf("fail_lgc_id:\n");
    //display(_ecK, 1, fail_lgc_id);
    // get the logical ids of the selected k chunks
    for(i=0; i<num_repair_chnk_round; i++){
        cnt = 0;
        for(j=0; j<_peer_node_num; j++){
            if(rpr_chk_dist[i*_peer_node_num+j]>=0){
                slct_k_chk[i*_ecK+cnt] = rpr_chk_dist[i*_peer_node_num+j];
                cnt++;
                if(cnt == _ecK)
                    break;
            }
        }
        printf("slct_k_chk\n");
        display(_ecK, 1, slct_k_chk+i*_ecK);
        assert(j!=_peer_node_num);
        decode_coeff[i] = getDecodeCoeff(slct_k_chk+i*_ecK, enc_matrix, fail_lgc_id[i]); 
    }

    printf("decode_coeff:\n");
    for(i=0; i<num_repair_chnk_round; i++){
        display(_ecK, 1, decode_coeff[i]);
    }

    // count chunks
    for(i=0; i<_peer_node_num; i++)
        cmd_cnt[i] = 0;
    for(i=0; i<_peer_node_num; i++){
        printf("\n-----node = %d------\n", i);
        for(j=0; j<num_repair_chnk_round; j++){
            stripe = _related_stripes[rpr_chnk[j]];
            // for sending data
            if(rpr_chk_dist[j*_peer_node_num+i]>=0){
                // reserve the first two bytes for storing the number of cmds
                if(cmd_cnt[i]==0)
                    cmd[i] += "00";
                cmd_cnt[i]++;
                cmd[i] += "SO";
                glb_chk = stripe*_ecN + rpr_chk_dist[j*_peer_node_num+i];

                // read the block name
                // -----> modify zrshen 2019/10/10 
                // --- blk_it = _chunkid2addr.find(glb_chk);
                // --- chunk_name = blk_it->second;
                // --- cout << "send chunk = " << blk_it->second << endl;
                chunk_name = to_string(glb_chk);

                assert(chunk_name.length()<=BLK_NAME_LEN);
                for(k=chunk_name.length(); k<BLK_NAME_LEN; k++)
                    cmd[i] += "0";
                cmd[i] += chunk_name;

                // read the stripe name 
                // -----> modify zrshen 2019/10/10 
                // ---- strp_it = _blkName2stripeName.find(chunk_name);
                // ---- stripe_name = strp_it->second;
                stripe_name = to_string(stripe);

                cout << "send stripe = " << stripe_name << endl;
                for(k=stripe_name.length(); k<STRIPE_NAME_LEN; k++)
                    cmd[i] += "0";
                cmd[i] += stripe_name;       

                // get the decoding coeff
                for(k=0; k<_ecK; k++){
                    if(slct_k_chk[j*_ecK+k] == rpr_chk_dist[j*_peer_node_num+i]){
                        cur_coeff = decode_coeff[j][k];
                        break;
                    }
                }
                for(k=0; k<COEFF_LEN-(int)(to_string(cur_coeff).length()); k++)
                    cmd[i] += "0";
                cmd[i] += to_string(cur_coeff);

		// get the next ip addr
                next_id = dest_node_id[j];
		cout << "peerNodeIP = " << _conf->_peerNodeIPs[next_id] << endl;
                for(int h=0; h<NEXT_IP_LEN-(int)(to_string(_conf->_peerNodeIPs[next_id]).length()); h++)
                    cmd[i]+="0";
                cmd[i] += to_string(_conf->_peerNodeIPs[next_id]);

                // get the proxy ip addr
                rack_id = i/num_node_per_rack;
		cout << "proxy_IP = " << _conf->_proxyIPs[rack_id] << endl;

                for(int h=0; h<NEXT_IP_LEN-(int)(to_string(_conf->_proxyIPs[rack_id]).length()); h++)
                    cmd[i] += "0";
                cmd[i] += to_string(_conf->_proxyIPs[rack_id]); 
                send_cnt++;
            }
            // for receiving data
            if(rpr_chk_dist[j*_peer_node_num+i]<-1){
                // reserve two bytes for storing the number of cmds
                if(cmd_cnt[i]==0)
                    cmd[i] += "00";
                cmd_cnt[i]++;

                recv_cnt = (-1)*rpr_chk_dist[j*_peer_node_num+i];
                if(recv_cnt >=100){
                    printf("ERR: receive chunks is larger than 100!\n");
                    exit(1);
                }
                // init recv_cmd
                recv_cmd = "RO";
                // get the logical identity of the failed chunk
                glb_chk = stripe*_ecN + fail_lgc_id[j];

                // read the block name
                // -------> modify zrshen, 2019/10/10
                // ---- blk_it = _chunkid2addr.find(glb_chk);
                // ---- chunk_name = blk_it->second;
                // ---- assert(chunk_name.length()<=BLK_NAME_LEN);
                chunk_name = to_string(glb_chk);

                for(k=chunk_name.length(); k<BLK_NAME_LEN; k++)
                    recv_cmd += "0";
                recv_cmd += chunk_name;

                // read the stripe name
                // -------> modify zrshen, 2019/10/10
                // ---- strp_it = _blkName2stripeName.find(chunk_name);
                // ---- stripe_name = strp_it->second;
                stripe_name = to_string(stripe);

                for(k=stripe_name.length(); k<STRIPE_NAME_LEN; k++)
                    recv_cmd += "0";
                recv_cmd += stripe_name;

                // record the number of recv chunks
                for(k=0; k<COEFF_LEN-2; k++)
                    recv_cmd += "0";
                if(recv_cnt < 10)
                    recv_cmd += "0";
                recv_cmd += to_string(recv_cnt);

                // add dummy values to the fields of next_ip and proxy_ip
                for(k=0; k<2*NEXT_IP_LEN; k++)
                    recv_cmd += "0";

                cmd[i] += recv_cmd;
            }
        }
    // insert the cmd_cnt for each node
    if(cmd_cnt[i]==0)
        continue;
    string cnt_str;
    if(cmd_cnt[i]<10)
        cnt_str = "0";
    cnt_str += to_string(cmd_cnt[i]);
    cmd[i].replace(0, 2, cnt_str);
    }

    cout << "send command to peer nodes:" << endl;    
    for(i=0; i<_peer_node_num; i++)
        cout << cmd[i] << endl;

    // record the stripe
    string repair_stripe[num_repair_chnk_round];
    string tmp_stripe_name;
    for(i=0; i<num_repair_chnk_round; i++){
        for(j=0; j<_peer_node_num; j++){

            stripe = _related_stripes[rpr_chnk[i]];
            if(rpr_chk_dist[i*_peer_node_num+j]>=0){

                glb_chk = stripe*_ecN + rpr_chk_dist[i*_peer_node_num+j];
                // read the block name
                // -----> modify zrshen, 2019/10/10
                // ---- blk_it = _chunkid2addr.find(glb_chk);
                // ---- chunk_name = blk_it->second;
                chunk_name = to_string(glb_chk);

                // read the stripe name 
                // -----> modify zrshen, 2019/10/10
                // ---- strp_it = _blkName2stripeName.find(chunk_name);
                // ---- repair_stripe[i] = strp_it->second;
                stripe_name="";
                tmp_stripe_name = to_string(stripe);
                for(k=tmp_stripe_name.length(); k<STRIPE_NAME_LEN; k++)
                    stripe_name += "0";
                stripe_name += tmp_stripe_name;
                
                repair_stripe[i] = stripe_name;
                break;
            }
        }
    }

    // init commands for proxy nodes
    // add the proxy command initiation
    string tmp_cmd;
    int proxy_cnt[_rack_num];
    int in_rack_num[_rack_num];
    int rack_dl_cnt[_rack_num];

    memset(rack_dl_cnt, 0, sizeof(int)*_rack_num);
    memset(in_rack_num, 0, sizeof(int)*_rack_num);
    memset(proxy_cnt, 0, sizeof(int)*_rack_num);
    for(i=0; i<num_repair_chnk_round; i++){
        for(j=0; j<_rack_num; j++){
            if(rpr_sltns[i*_rack_num+j]<0){
                rack_dl_cnt[j] += rpr_sltns[i*_rack_num+j]*(-1);
                break;
            }
        }
    }
    cout << "rack_dl_cnt:" << endl;
    display(_rack_num, 1, rack_dl_cnt);


    // generate each proxy cmd
    cout << " num_node_per_rack = " << num_node_per_rack << endl;
    for(i=0; i<num_repair_chnk_round; i++){

        memset(in_rack_num, 0, sizeof(int)*_rack_num);
        for(j=0; j<_peer_node_num; j++){
            if(rpr_chk_dist[i*_peer_node_num+j]>=0){
                rack_id = j/num_node_per_rack;
                in_rack_num[rack_id]++; 
            }
        }
        cout << "in_rack_num:" << endl;
        display(_rack_num, 1, in_rack_num);

        // calculate the cross-rack download traffic

        for(j=0; j<_rack_num; j++){

            if(in_rack_num[j]==0)
                continue;
            cout << "j = " << j << endl;
            cout << "in_rack_num " << in_rack_num[j] << endl;
            if(proxy_cnt[j]==0)
                proxy_cmd[j] = "00";

            // get the role
            tmp_cmd = "RS";
            for(k=0; k<BLK_NAME_LEN; k++)
                tmp_cmd += "0";

            // get the stripe id
            tmp_cmd += repair_stripe[i];

            // use the remaining bits reserved for coeff to record  the number of intra-rack chunks to be received
            for(k=0; k<COEFF_LEN-(int)(to_string(in_rack_num[j]).length()); k++)
                tmp_cmd += "0";
            tmp_cmd += to_string(in_rack_num[j]);

            // get the next_ip
            next_id = dest_node_id[i];
            cout << "peerNodeIP = " << _conf->_peerNodeIPs[next_id] << endl;
            for(k=0; k<NEXT_IP_LEN-(int)(to_string(_conf->_peerNodeIPs[next_id]).length()); k++)
                tmp_cmd += "0";
	    tmp_cmd += to_string(_conf->_peerNodeIPs[next_id]);

            // get the proxy ip
            for(k=0; k<NEXT_IP_LEN; k++)
                tmp_cmd += "0";

            proxy_cmd[j] += tmp_cmd;
        }

        // increase the cmd count
        for(j=0; j<_rack_num; j++){
            if(in_rack_num[j]>0)
                proxy_cnt[j]++;
        }
    }

    for(i=0; i<_rack_num; i++){
        if(proxy_cnt[i]==0)
            continue;

        string tmp_str;
        if(proxy_cnt[i]<10)
            tmp_str = "0";
        tmp_str += to_string(proxy_cnt[i]);
        proxy_cmd[i].replace(0, 2, tmp_str); 
    }

    // add the rack download info to the cmds
    string dl_cmd;
    for(i=0; i<_rack_num; i++){
        if(rack_dl_cnt[i]<10)
            dl_cmd = "0";
        else 
            dl_cmd = "";
        dl_cmd += to_string(rack_dl_cnt[i]);
        proxy_cmd[i].replace(4, 2, dl_cmd);
    }

    cout << "proxy command to proxy nodes:" << endl;    
    for(i=0; i<_rack_num; i++)
        cout << proxy_cmd[i] << endl;

    for(i=0; i<num_repair_chnk_round; i++)
        free(decode_coeff[i]);
    free(decode_coeff);
    free(slct_k_chk);
    free(fail_lgc_id);
    free(cmd_cnt);
    free(dest_node_id);
}

void Coordinator::getRepairSol(int fail_node, int num_repair_chnk_round, int* rpr_chnk, int* rpr_sltns, int* rpr_chk_dist){

    int i,j,k;
    int h;
    int stripe;
    int join_node;
    int dest_rack;
    int dest_node;
    int node_num_rack;
    int read_cnt;
    int rack;
    int if_find;

    int mark_read[_rack_num];

    dest_rack = -1;
    node_num_rack = _peer_node_num/_rack_num;
    // establish the chunk retrieval
    for(i=0; i<num_repair_chnk_round; i++){
        read_cnt = 0;
        stripe = _related_stripes[rpr_chnk[i]];
        printf("i = %d\n", i);
        printf("global_stripe = %d\n", stripe);
        printf("local_stripe = %d\n", rpr_chnk[i]);
        printf("node_num_rack = %d\n", node_num_rack);

        // find at least a chunk for each involved rack
        memset(mark_read, -1 ,sizeof(int)*_rack_num);
        for(j=0; j<_rack_num; j++){
            if(rpr_sltns[i*_rack_num+j]>0){
                for(k=0; k<_ecN; k++){
                        
                    join_node = _placement[stripe*_ecN+k];
                    if(join_node == fail_node)
                        continue; 
                    rack = join_node/node_num_rack;
                    if(rack == j){
                        rpr_chk_dist[i*_peer_node_num+join_node] = k;
                        mark_read[rack] = k; 
                        read_cnt++;
                        break;
                    }
                }
            }
        }
        // record the remaining read chunks
        for(k=0; k<_ecN; k++){
            // check if the chunk has bee read in the last step
            for(h=0; h<_rack_num; h++){
                if(mark_read[h] == k)
                    break;
            }
            if(h<_rack_num)
                continue;
            join_node = _placement[stripe*_ecN+k];
            if(join_node == fail_node)
                continue;
            rack = join_node/node_num_rack;
            if(rpr_sltns[i*_rack_num+rack]!=0){
                printf("--k=%d\n",k);
                printf("--join_node=%d\n", join_node);
                printf("--rack=%d\n", rack);
                printf("--read_cnt=%d\n", read_cnt);
                rpr_chk_dist[i*_peer_node_num+join_node]=k; 
                read_cnt++;
                if(read_cnt==_ecK)
                    break;
            }
        }
        assert(k<_ecN);

        // record the repair node
        for(k=0; k<_rack_num; k++){
            if(rpr_sltns[i*_rack_num+k]<0){
                dest_rack=k;
                break;
            }
        }

        dest_node = -1;
        for(j=dest_rack*node_num_rack; j<(dest_rack+1)*node_num_rack; j++){
            for(k=0; k<_ecN; k++){
                if(_placement[stripe*_ecN+k]==j)
                    break;
            }
            if(k==_ecN){
                printf("dest_node = %d\n", j);
                dest_node = j;
                break;
            }
        }
        assert(dest_node!=-1);

        // check if reading data from the dest_rack
        if_find = 0;
        for(j=dest_rack*node_num_rack; j<(dest_rack+1)*node_num_rack; j++){
            if(rpr_chk_dist[i*_peer_node_num+j]>=0){
                if_find = 1;
                break;
            }
        }
        if(if_find == 1)
            rpr_chk_dist[i*_peer_node_num+dest_node] = rpr_sltns[i*_rack_num+dest_rack] - 1;
        else 
            rpr_chk_dist[i*_peer_node_num+dest_node] = rpr_sltns[i*_rack_num+dest_rack];
    }
    printf("---debug: rpr_chk_dist:\n");
    display(_peer_node_num, num_repair_chnk_round, rpr_chk_dist);
}

// perform the repair
void Coordinator::doProcess(int fail_node, int num_repair_chnk_round, int*rpr_chnk, int* rpr_sltns){

    cout << "==========> a repair round starts" << endl;
    // we use positive value to denote the logical identity of a chunk in the corresponding stripe
    // and use negative value to denote the number of chunks to be downloaded across racks
    int* rpr_chk_dist=(int*)malloc(sizeof(int)*_peer_node_num*num_repair_chnk_round);
    int* enc_matrix = (int*)malloc(sizeof(int)*_ecN*_ecK);

    cout << "rpr_sltns:" << endl;
    display(_rack_num, num_repair_chnk_round, rpr_sltns);

    // get the chunk distribution
    memset(rpr_chk_dist, -1, sizeof(int)*_peer_node_num*num_repair_chnk_round);
    getRepairSol(fail_node, num_repair_chnk_round, rpr_chnk, rpr_sltns, rpr_chk_dist);

    // read the encoding matrix
    readEncMat(enc_matrix);

    // initialize the commands
    // SO: send_only;    RS: retrieve and send;      RO: retrieve only
    // format:
    string peer_cmd[_peer_node_num];
    string proxy_cmd[_rack_num];
    initCommand(peer_cmd, proxy_cmd, num_repair_chnk_round, fail_node, rpr_sltns, rpr_chk_dist, rpr_chnk, enc_matrix);

    // sendCommand
    sendCommand(proxy_cmd, PROXY_NODE_CMD);
    sendCommand(peer_cmd, PEER_NODE_CMD);
    recvACK(rpr_chk_dist, num_repair_chnk_round);
    sleep(0.01);
    cout << "<========== a repair round completes" << endl;

    free(rpr_chk_dist);
    free(enc_matrix);
}

void Coordinator::sendCommand(string cmd[], int flag){

    int i;
    Socket* sock = new Socket();

    if(flag == PROXY_NODE_CMD){
        // send the proxy commands
        for(i=0; i<_rack_num; i++){
            if(cmd[i]=="")
            continue;
            sock->sendData((char*)cmd[i].c_str(), cmd[i].length(), (char*)ip2Str(_conf->_proxyIPs[i]).c_str(), PN_RECV_CMD_PORT);
        }
    }

    // send the peernode commands
    else if (flag == PEER_NODE_CMD){
        for(i=0; i<_peer_node_num; i++){
            if(cmd[i]=="")
                continue;
            sock->sendData((char*)cmd[i].c_str(), cmd[i].length(), (char*)ip2Str(_conf->_peerNodeIPs[i]).c_str(), PN_RECV_CMD_PORT);
        }
    }

    delete sock;
}

string Coordinator::ip2Str(unsigned int ip) const {

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

void Coordinator::recvACK(int* rpr_chk_dist, int num_repair_chnk_round){

    int num_ack;
    int i, j;
    // count the number of receiver nodes
    num_ack = 0;
    for(i=0; i<_peer_node_num; i++){
        for(j=0; j<num_repair_chnk_round; j++){
            if(rpr_chk_dist[j*_peer_node_num+i]<-1){
                num_ack++;
                break;
            }
        }
    }

    // recv the ack
    char* recv_ack = (char*)malloc(sizeof(char)*(ACK_LEN+1)*num_ack);
    Socket* sock = new Socket();
    sock->paraRecvData(CD_RECV_ACK_PORT, ACK_LEN+1, recv_ack, num_ack, NULL, -1, -1, ACK_INFO);
    free(recv_ack);
    delete sock;
}
int Coordinator::RackSR(int fail_node){

    int cr_cnt;
    int i,j;
    int rd_chnk_cnt;
    int des_rack;
    int rack_cnt;

    _cross_rack_traf = (int*)malloc(sizeof(int)*_num_rebuilt_chunks);
    cr_cnt = 0; 
    for(i=0; i<_num_rebuilt_chunks; i++){

        rd_chnk_cnt = 0;
        rack_cnt = 0;
        des_rack = -1;
        // find the destination rack first
        for(j=0; j<_rack_num; j++){
            if(_chnk_num_rack[i*_rack_num+j]<_ecM){
                des_rack = _sort_rack_index[i*_rack_num+j];
                rd_chnk_cnt = _chnk_num_rack[i*_rack_num+j];
                break;
            }
        }
        // count the cross-rack traffic
        for(j=0; j<_rack_num; j++){

            if(_sort_rack_index[i*_rack_num+j] == des_rack)
                continue;

            rack_cnt++;
            rd_chnk_cnt += _chnk_num_rack[i*_rack_num+j];
            if(rd_chnk_cnt >= _ecK)
                break;
        }

        cr_cnt += rack_cnt;
        _cross_rack_traf[i] = rack_cnt;
        //printf("dest_rack = %d, ivl_rack_cnt = %d, cr_cnt = %d\n", dest_rack, ivl_rack_cnt, cr_cnt);
    }

    printf("_cross_rack_traf:\n");
    display(_num_rebuilt_chunks,1,_cross_rack_traf);
    printf("RackSR-CR = %d\n", cr_cnt);
    printf("===== RackSR: Balance CR =====\n");
    RackSRBalance(fail_node, _num_repair_chnk_round);
    
    free(_cross_rack_traf);
    return cr_cnt;
}

// record the upload and download traffic
void Coordinator::RecordLoad(int* rack_upload, int* rack_download, int* rpr_sltns, int num_repair_chnk_round){
    int i;
    int rk_id;

    memset(rack_upload, 0, sizeof(int)*_rack_num);
    memset(rack_download, 0, sizeof(int)*_rack_num);    
    for(i=0; i<num_repair_chnk_round; i++){
        for(rk_id=0; rk_id<_rack_num; rk_id++){
            if(rpr_sltns[i*_rack_num+rk_id]==1)
                rack_upload[rk_id]++;
            else if(rpr_sltns[i*_rack_num+rk_id]<0)
                rack_download[rk_id]+=(-1)*rpr_sltns[i*_rack_num+rk_id];
        }
    }
    printf("rack_upload:\n");
    display(_rack_num, 1, rack_upload);
    printf("rack_download:\n");
    display(_rack_num, 1, rack_download);
}

// this function will subtitude a selected chunk with a residual chunk to reduce the maximum upload traffic without increasing the maximum download traffic
// if can find a chunk for substitution, then return 1; else 0;
int Coordinator::RackSRSubChunkUpload(int num_repair_chnk_round, int num_remain_chnk, int* rpr_sltns, int* rpr_chnk, int* remain_rpr_chnk, int* rack_upload, int* rack_download){

    int i, j;
    int temp_chnk;
    int slct_chnk, slct_idx;
    int rd_cnt;
    int rk_id, rack_cnt;
    int cnddt_rk;
    int max_upload_rack;
    int max_upload, max_download;
    int if_find;
    int temp_cross_traf;
    int temp_max;
    int rplc_rpr_chnk_idx;

    int* temp_chnk_rpr_sltn = (int*)malloc(sizeof(int)*_rack_num);
    int* temp_rpr_sltns = (int*)malloc(sizeof(int)*num_repair_chnk_round*_rack_num);
    int* temp_rack_upload = (int*)malloc(sizeof(int)*_rack_num);
    int* temp_rack_download = (int*)malloc(sizeof(int)*_rack_num);

    max_upload_rack = distance(rack_upload, max_element(rack_upload, rack_upload+_rack_num));
    max_upload = *max_element(rack_upload, rack_upload+_rack_num);
    max_download = *max_element(rack_download, rack_download+_rack_num);
    slct_idx = -1;
    slct_chnk = -1;

    // find a chunk 
    for(i=0; i<num_repair_chnk_round; i++){
        if(rpr_sltns[i*_rack_num+max_upload_rack]==1){
            break;
        }
    }
    rplc_rpr_chnk_idx = i;

    printf("==== before replacement ====\n");
    printf("max_upload = %d, max_download = %d\n", max_upload, max_download);
    printf("rplc_rpr_chnk_idx = %d\n", rplc_rpr_chnk_idx);
    printf("old chunk's solution:\n");
    display(_rack_num, 1, rpr_sltns+rplc_rpr_chnk_idx*_rack_num);

    // generate temp variables
    memcpy(temp_rpr_sltns, rpr_sltns, sizeof(int)*num_repair_chnk_round*_rack_num);
    memset(temp_rpr_sltns+rplc_rpr_chnk_idx*_rack_num, 0, sizeof(int)*_rack_num);
    printf("temp_rack_upload and temp_rack_download:\n");
    RecordLoad(temp_rack_upload, temp_rack_download, temp_rpr_sltns, num_repair_chnk_round);

    // find a solution from the candidate chunks that does not read data from the heaviest uploaded rack
    for(i=0; i<num_remain_chnk; i++){

        temp_chnk = remain_rpr_chnk[i];
        rd_cnt = 0;
        rack_cnt = 0;
        cnddt_rk = -1;
        temp_max = max_download;
        memset(temp_chnk_rpr_sltn, 0, sizeof(int)*_rack_num);

        // check if can find a valid solution: 1) find read chunks 
        if_find = 0;
        temp_cross_traf = -1;
        for(j=0; j<_rack_num; j++){
            rk_id = _sort_rack_index[temp_chnk*_rack_num+j];
            // the new solution should not increase the max_upload
            printf("rk_id = %d\n", rk_id);
            if(temp_rack_upload[rk_id]+1 >= max_upload)
                continue;

            rd_cnt += _chnk_num_rack[temp_chnk*_rack_num+j];
            rack_cnt++;
            temp_chnk_rpr_sltn[rk_id] = 1;

            printf("rd_cnt = %d, rack_cnt = %d\n", rd_cnt, rack_cnt);
            if(rd_cnt>=_ecK)
                break; 
        }
        // if cannot find sufficient data when choosing this remaining chunk
        if(j>=_rack_num)
            continue; 
        printf("### final read cnt = %d\n", rd_cnt);

        // check if can find a valid solution: 2) find destination rack 
        temp_max = max_download;
        for(j=0; j<_rack_num; j++){
            rk_id = _sort_rack_index[temp_chnk*_rack_num+j];
            if((temp_chnk_rpr_sltn[rk_id]==1) && (_chnk_num_rack[temp_chnk*_rack_num+j]<_ecM)){
                if(temp_rack_download[rk_id]+rack_cnt-1 <= temp_max){
                    cnddt_rk = rk_id;
                    temp_max = temp_rack_download[rk_id]+rack_cnt-1;
                    if_find = 1;;
                }
            }
        }
        if(if_find == 1){
            temp_chnk_rpr_sltn[cnddt_rk] = (-1)*(rack_cnt-1);
            temp_cross_traf = rack_cnt-1;
        }
        else if(if_find == 0){
            temp_max = max_download;
            for(j=0; j<_rack_num; j++){
                rk_id = _sort_rack_index[temp_chnk*_rack_num+j];
                if((temp_chnk_rpr_sltn[rk_id]==0) && (_chnk_num_rack[temp_chnk*_rack_num+j]<_ecM) && (temp_rack_download[rk_id]+rack_cnt <= temp_max)){
                        cnddt_rk = rk_id;
                        temp_max = temp_rack_download[rk_id]+rack_cnt;
                        if_find = 1;
                }
            }
            if(if_find == 1){
                temp_chnk_rpr_sltn[cnddt_rk] = (-1)*rack_cnt;
                temp_cross_traf = rack_cnt;
            }
        }

        printf("if_find = %d, temp_cross_traf = %d, _cross_rack_traf[%d] = %d\n", if_find, temp_cross_traf, temp_chnk, _cross_rack_traf[temp_chnk]);
        // if it is a valid solution, then reduce the upload traffic, then replace a chunk
        if(temp_cross_traf == _cross_rack_traf[temp_chnk]){
            slct_chnk = temp_chnk;
            slct_idx = i; 
            break;
        }
    }

    printf("slct_idx = %d, slct_chnk = %d\n", slct_idx, slct_chnk);
    if(i>=num_remain_chnk)
        return 0;

    // perform swapping 
    printf("new chunk's solution:\n");
    display(_rack_num, 1, temp_chnk_rpr_sltn);

    temp_chnk = rpr_chnk[rplc_rpr_chnk_idx];
    rpr_chnk[rplc_rpr_chnk_idx] = slct_chnk;
    remain_rpr_chnk[slct_idx] = temp_chnk;
    memcpy(rpr_sltns+rplc_rpr_chnk_idx*_rack_num, temp_chnk_rpr_sltn, sizeof(int)*_rack_num);

    printf("==== after replacement ====\n");
    printf("temp_rack_upload and temp_rack_download:\n");
    RecordLoad(temp_rack_upload, temp_rack_download, rpr_sltns, num_repair_chnk_round);

    free(temp_chnk_rpr_sltn);
    free(temp_rpr_sltns);
    free(temp_rack_upload);
    free(temp_rack_download);
    return 1;
}

// this function will subtitude a selected chunk with a residual chunk to reduce the maximum download traffic without increasing the maximum upload traffic
void Coordinator::RackSRSubChunkDownload(int num_repair_chnk_round, int num_remain_chnk, int* rpr_sltns, int* rpr_chnk, int* remain_rpr_chnk, int* rack_upload, int* rack_download){
    int i,j;
    int temp_rpr_chnk, temp_rpr_chnk_idx;
    int rplc_rpr_chnk_idx;
    int max_download, max_download_rack;
    int max_upload;
    int temp_des_rack;
    int cur_rcd_cnt;
    int rk_cnt;
    int temp_max;
    int traffic;
    int temp, temp_rack;
    int rk_id; 
    int expect_traffic;

    max_download = *max_element(rack_download, rack_download+_rack_num);
    max_download_rack = distance(rack_download, max_element(rack_download, rack_download+_rack_num));
    max_upload = *max_element(rack_upload, rack_upload+_rack_num);
    int* temp_chnk_rpr_sltn = (int*)malloc(sizeof(int)*_rack_num);
    int* temp_rpr_sltns = (int*)malloc(sizeof(int)*num_repair_chnk_round*_rack_num);
    int* temp_rack_upload = (int*)malloc(sizeof(int)*_rack_num);
    int* temp_rack_download = (int*)malloc(sizeof(int)*_rack_num);

    // find a repaired chunk to be replaced, it should read chunks from the most downloaded rack
    memset(temp_rack_upload, 0, sizeof(int)*_rack_num);
    memset(temp_rack_download, 0, sizeof(int)*_rack_num);
    memcpy(temp_rpr_sltns, rpr_sltns, sizeof(int)*num_repair_chnk_round*_rack_num);

    for(i=0; i<num_repair_chnk_round; i++){
        if(rpr_sltns[i*_rack_num+max_download_rack]<0)
            break;
    }    
    rplc_rpr_chnk_idx = i;

    printf("rplc_rpr_chnk_idx = %d, and its solution is: \n", rplc_rpr_chnk_idx);
    display(_rack_num, 1, rpr_sltns+rplc_rpr_chnk_idx*_rack_num);
    // update the temp solution, rack_upload, and rack_download
    memset(temp_rpr_sltns+rplc_rpr_chnk_idx*_rack_num, 0, sizeof(int)*_rack_num);
    RecordLoad(temp_rack_upload, temp_rack_download, temp_rpr_sltns, num_repair_chnk_round);
    printf("temp_rack_upload:\n");
    display(_rack_num, 1, temp_rack_upload); 
    printf("temp_rack_download:\n");
    display(_rack_num, 1, temp_rack_download); 

    // find a candidate chunk 
    for(j=0; j<num_remain_chnk; j++){
        temp_rpr_chnk = remain_rpr_chnk[j];
        temp_rpr_chnk_idx = j;
        if(temp_rpr_chnk == -1)
            break;
        printf("temp_rpr_chnk_idx = %d\n", j);
        printf("temp_rpr_chnk = %d\n", temp_rpr_chnk);
        // find its read distribution and it should not read data from the most uploaded rack
        cur_rcd_cnt = 0;
        rk_cnt = 0;
        temp_max = max_download;
        temp_des_rack = -1;
        traffic = -1;
        expect_traffic = _cross_rack_traf[temp_rpr_chnk];
        memset(temp_chnk_rpr_sltn, 0, sizeof(int)*_rack_num);

        for(rk_id=0; rk_id<_rack_num; rk_id++){

            temp_rack = _sort_rack_index[temp_rpr_chnk*_rack_num+rk_id];
            if(temp_rack_upload[temp_rack]+1>max_upload)
                continue;

            cur_rcd_cnt += _chnk_num_rack[temp_rpr_chnk*_rack_num+rk_id];
            temp_chnk_rpr_sltn[temp_rack] = 1;
            rk_cnt++;

            // if the read rack can be a destination rack
            if((_chnk_num_rack[temp_rpr_chnk*_rack_num+rk_id]<_ecM) && (temp_rack_download[temp_rack]+expect_traffic < temp_max)){
                if(temp_rack_download[temp_rack]+expect_traffic<temp_max){
                    temp_max = temp_rack_download[temp_rack]+expect_traffic;
                    temp_des_rack = temp_rack;
                }                
            }

            if(cur_rcd_cnt>=_ecK)
                break;    
        }

        if(cur_rcd_cnt<_ecK)
            continue;

        // calculate the cross-rack repair traffic for the candidate chunk
        if(temp_des_rack != -1)
                traffic = rk_cnt-1;
        else{
            // if not find a destination rack, then find one
            temp_max = max_download;
            for(i=0; i<_rack_num; i++){
                temp_rack = _sort_rack_index[temp_rpr_chnk*_rack_num+i];
                if((_chnk_num_rack[temp_rpr_chnk*_rack_num+i]<_ecM) && (temp_rack_download[temp_rack]+expect_traffic < temp_max)){
                    if(temp_rack_download[temp_rack]+expect_traffic<temp_max){
                        temp_max = temp_rack_download[temp_rack]+expect_traffic;
                        temp_des_rack = temp_rack;
                    }
                }
            }
            traffic = rk_cnt;
        }
        
        // if cannot find a dest rack
        if(temp_des_rack == -1)
            continue;

        printf("traffic = %d, expect_traffic = %d\n", traffic, expect_traffic);
        // check if the solution is a valid solution
        if(_cross_rack_traf[temp_rpr_chnk] == traffic){
            printf("temp_des_rack = %d\n", temp_des_rack);
            printf("-----Before replacement-----\n");
            printf("rpr_chnk:\n");
            display(num_repair_chnk_round, 1, rpr_chnk);
            // update the solution 
            temp_chnk_rpr_sltn[temp_des_rack] = (-1)*traffic;
            // perform replacement
            temp = rpr_chnk[rplc_rpr_chnk_idx];
            rpr_chnk[rplc_rpr_chnk_idx] = remain_rpr_chnk[temp_rpr_chnk_idx];
            remain_rpr_chnk[temp_rpr_chnk_idx] = temp;
            // copy the solution
            printf("-----After replacement-----\n");
            printf("rpr_chnk:\n");
            display(num_repair_chnk_round, 1, rpr_chnk);
            printf("temp_chnk_rpr_sltn:\n");
            display(_rack_num, 1, temp_chnk_rpr_sltn);
            memcpy(rpr_sltns+rplc_rpr_chnk_idx*_rack_num, temp_chnk_rpr_sltn, sizeof(int)*_rack_num);
            break;
        }
    }
    free(temp_chnk_rpr_sltn);
    free(temp_rpr_sltns);
    free(temp_rack_upload);
    free(temp_rack_download);
}

void Coordinator::RackSROptimize(int num_repair_chnk_round, int* rpr_chnk_rnd, int* rack_upload, int* rack_download, int* rpr_sltns, int* des_rack_round, int* remain_rpr_chnk, int num_remain_chnk){

    int j;
    int max_upload, max_download;
    int cur_rd_cnt, temp_cur_rd_cnt;
    int tmp_rk; 
    int max_upload_rack;
    int max_download_rack;
    int chk_id;
    int cnddt_dl_rk, cnddt_ul_rk;
    int des_rack_idx;
    int rpr_chnk_id;
    int tmp_max;
    int cnddt_chk;
    int rd_cnt = 0;
    int i, k;
    int stripe;
    int node_num_rack;

    des_rack_idx = -1;
    cnddt_chk = -1;
    node_num_rack = _peer_node_num/_rack_num;
    max_upload = *max_element(rack_upload, rack_upload+_rack_num);
    max_download = *max_element(rack_download, rack_download+_rack_num);
    printf("max_upload = %d, max_download = %d\n", max_upload, max_download);

    // if max_upload > max_download, find another valid solution
    cnddt_dl_rk = -1;
    cnddt_ul_rk = -1;   
    if(max_upload >= max_download){
        printf("----optimize upload\n");
        tmp_max = max_upload;
        max_upload_rack = distance(rack_upload, max_element(rack_upload, rack_upload+_rack_num));
        printf("max_upload = %d --\n", max_upload);
        printf("max_upload_rack = %d\n", max_upload_rack);

        for(chk_id=0; chk_id<num_repair_chnk_round; chk_id++){

            // find another valid solution that reads data from a new rack rather than the most-uploaded rack
            rpr_chnk_id = rpr_chnk_rnd[chk_id];
            assert(rpr_chnk_id != -1);

            // find the index of the download rack of that chunk and check if the repair reads data from the destination rack  
            j=0;
            while(_sort_rack_index[rpr_chnk_id*_rack_num+j]!=des_rack_round[chk_id]) j++;
            des_rack_idx = j;

            if(rpr_sltns[chk_id*_rack_num+max_upload_rack]==1){

                printf("cnddt_changed_chnk = %d\n", chk_id);    
                cur_rd_cnt = 0;
                // calculate the existing chunks read for recovery without the maximum upload rack
                for(j=0; j<_rack_num; j++){

                    tmp_rk = _sort_rack_index[rpr_chnk_id*_rack_num+j];
                    if(tmp_rk == max_upload_rack){
                        printf("minus %d chunks\n", _chnk_num_rack[rpr_chnk_id*_rack_num+j]);
                            continue;
                    }

                    if(rpr_sltns[chk_id*_rack_num+tmp_rk] == 0)
                        continue;

                    // if((rpr_sltns[chk_id*_rack_num+tmp_rk] > 1) && (if_des_rd_dt == 0))
                    //    continue;

                    cur_rd_cnt += _chnk_num_rack[rpr_chnk_id*_rack_num+j];
                }
                printf("cur_rd_cnt = %d\n", cur_rd_cnt);

                // find the candidate rack with the least upload traffic
                for(j=0; j<_rack_num; j++){

                    // the new read rack should not have the most upload traffic
                    tmp_rk = _sort_rack_index[rpr_chnk_id*_rack_num+j];
                    if(rack_upload[tmp_rk]+1 > max_upload)
                        continue;

                    // if find a rack that is not accessed in the selected solution
                    // then read its data and check if the number of read chunks is enough
                    if(rpr_sltns[chk_id*_rack_num+tmp_rk] == 0){

                        temp_cur_rd_cnt = cur_rd_cnt+_chnk_num_rack[rpr_chnk_id*_rack_num+j];
                        if(temp_cur_rd_cnt < _ecK)
                            continue;
                        printf("temp_cur_rd_cnt = %d\n", temp_cur_rd_cnt);     
                        if(rack_upload[tmp_rk]<tmp_max){
                            tmp_max = rack_upload[tmp_rk];
                            cnddt_ul_rk = tmp_rk;
                            cnddt_chk = chk_id;
                        }
                    }
                }
            }
        }
        if(cnddt_ul_rk!=-1){
            printf("Change %d-th chunk's repair:\n", cnddt_chk);
            printf("--original upload rpr_sltn:\n");
            display(_rack_num, 1, rpr_sltns+cnddt_chk*_rack_num); 

            rpr_sltns[cnddt_chk*_rack_num+max_upload_rack] = 0;
            rpr_sltns[cnddt_chk*_rack_num+cnddt_ul_rk] = 1;

            printf("new upload rpr_sltn:\n");
            display(_rack_num, 1, rpr_sltns+cnddt_chk*_rack_num); 
        }
        else{
            printf("----no upload optimization, call substitution\n");
            int ret = RackSRSubChunkUpload(num_repair_chnk_round, num_remain_chnk, rpr_sltns, rpr_chnk_rnd, remain_rpr_chnk, rack_upload, rack_download);
            if(ret == 0){
                printf("Cannot find upload substitution!\n");
            }
        }
    }

    // if the download traffic is the most 
    else {
        tmp_max = max_download;
        max_download_rack = distance(rack_download, max_element(rack_download, rack_download+_rack_num));
        printf("----optimize download\n");
        printf("-- max_download_rack = %d\n", max_download_rack);

        // scan the chunks to be repaired
        for(chk_id=0; chk_id<num_repair_chnk_round; chk_id++){

            rpr_chnk_id = rpr_chnk_rnd[chk_id];
            if(rpr_chnk_id == -1) break;

            // find another solution that has a different destination rack with the same download traffic 
            if(rpr_sltns[chk_id*_rack_num+max_download_rack] < 0){

                // find the index of the download rack of that chunk 
                j=0;
                while(_sort_rack_index[rpr_chnk_id*_rack_num+j]!=des_rack_round[chk_id]) j++;
                des_rack_idx = j;

                printf("-chk_id = %d\n", chk_id);
                printf("-original_des_rack_idx = %d\n", des_rack_idx);
                printf("-original_des_rack = %d\n", _sort_rack_index[rpr_chnk_id*_rack_num+des_rack_idx]);

                // calculate the read chunks except those from the destination rack
                rd_cnt = 0;
                for(i=0; i<_rack_num; i++){
                    tmp_rk = _sort_rack_index[rpr_chnk_id*_rack_num+i];
                    if((tmp_rk == max_download_rack) || (rpr_sltns[chk_id*_rack_num+tmp_rk] == 0))
                        continue;
                    rd_cnt += _chnk_num_rack[rpr_chnk_id*_rack_num+i];
                }

                // find another destination rack
                for(i=0; i<_rack_num; i++){
                    tmp_rk = _sort_rack_index[rpr_chnk_id*_rack_num+i];
                    if(tmp_rk == max_download_rack)
                            continue;

                    // check if the candidate download rack has available download nodes
                    stripe = _related_stripes[rpr_chnk_id];
                    for(j=tmp_rk*node_num_rack; j<(tmp_rk+1)*node_num_rack; j++){
                        for(k=0; k<_ecN; k++){
                            if(_placement[stripe*_ecN+k] == j)
                                break;
                        }
                        if(k>=_ecN) break;
                    }
                    if(j>=(tmp_rk+1)*node_num_rack)
                        continue;
                    
                    printf("---debug: rpr_chnk_id = %d\n", rpr_chnk_id);
                    printf("---debug: global_stripe = %d\n", stripe);
                    printf("---debug: tmp_rk = %d\n", tmp_rk);
                    //printf("tmp_rk = %d\n", tmp_rk);
                    //printf("rd_cnt = %d\n", rd_cnt);
                    //printf("_chnk_num_rack[rpr_chnk_id*_rack_num+i] = %d\n", _chnk_num_rack[rpr_chnk_id*_rack_num+i]);
                    if(rd_cnt+_chnk_num_rack[rpr_chnk_id*_rack_num+i]>=_ecK){
                        if(rack_download[tmp_rk] + _cross_rack_traf[rpr_chnk_id] < tmp_max){
                            tmp_max = rack_download[tmp_rk] + _cross_rack_traf[rpr_chnk_id];
                            cnddt_dl_rk = tmp_rk;
                            cnddt_chk = chk_id;
                            printf("**find!\n");
                            printf("-chk_id = %d\n", chk_id);
                            printf("tmp_rk = %d\n", tmp_rk);
                            printf("tmp_max = %d\n\n", tmp_max);
                        }
                    }
                }
            }
        }
        if(cnddt_dl_rk!=-1){
            printf("Change %d-th chunk's repair:\n", cnddt_chk);
            printf("--original download rpr_sltn:\n");
            display(_rack_num, 1, rpr_sltns+cnddt_chk*_rack_num); 

            if(rpr_sltns[cnddt_chk*_rack_num+cnddt_dl_rk]==1)    
                rpr_sltns[cnddt_chk*_rack_num+max_download_rack] = 1;
            else
                rpr_sltns[cnddt_chk*_rack_num+max_download_rack] = 0;

            rpr_sltns[cnddt_chk*_rack_num+cnddt_dl_rk] = (-1)*_cross_rack_traf[rpr_chnk_rnd[cnddt_chk]];
            printf("!!cnddt_dl_rk = %d\n", cnddt_dl_rk); 
            printf("--new rpr_sltn:\n");
            display(_rack_num, 1, rpr_sltns+cnddt_chk*_rack_num); 
        }
        else{
            printf("!!!! no optimization, call SubDownload\n");
            RackSRSubChunkDownload(num_repair_chnk_round, num_remain_chnk, rpr_sltns, rpr_chnk_rnd, remain_rpr_chnk, rack_upload, rack_download);
        }
    }   
}

void Coordinator::RackSRInit(int num_repair_chnk_round, int* rpr_chnk, int* rpr_sltns, int* des_rack_round){
    int i;
    int j;
    int rd_chnk_cnt;
    int rpr_chnk_id;
    int rd_rk_cnt;

    // printf("------before init: rpr_sltns:------\n");
    // display(_rack_num, num_repair_chnk_round, rpr_sltns);
    // random generate repair solutions 
    for(i=0; i<num_repair_chnk_round; i++){

        // for the last repair round
        if(rpr_chnk[i] == -1)
            break;

        // we first determine the destination rack 
        rpr_chnk_id = rpr_chnk[i]; 
        for(j=0; j<_rack_num; j++){

            if(_chnk_num_rack[rpr_chnk_id*_rack_num+j] < _ecM){
                des_rack_round[i] = _sort_rack_index[rpr_chnk_id*_rack_num+j];
                break;
            }
        }

        // generate a repair solution for each failed chunk
        rd_chnk_cnt = _chnk_num_rack[rpr_chnk_id*_rack_num+j];
        rd_rk_cnt = 0;
        for(j=0; j<_rack_num; j++){

            if(_sort_rack_index[rpr_chnk_id*_rack_num+j] == des_rack_round[i])
                continue;
            // identify the racks for reading data
            rd_chnk_cnt += _chnk_num_rack[rpr_chnk_id*_rack_num+j];
            rpr_sltns[i*_rack_num+_sort_rack_index[rpr_chnk_id*_rack_num+j]]=1;
            rd_rk_cnt++;

            if(rd_chnk_cnt >= _ecK){
                // update the destination rack in the repair solutions
                rpr_sltns[i*_rack_num+des_rack_round[i]] = (-1)*rd_rk_cnt;
                break;
            }
        }
    }
    // balance the repair solutions
    // find the most loaded link
    printf("------after init: rpr_sltns:------\n");
    display(_rack_num, num_repair_chnk_round, rpr_sltns);
}

// generate random solution for random repair 
void Coordinator::RandSol(int num_repair_chnk_round, int* rpr_chnk, int* rpr_sltns){

    printf("====> enter RandSol:\n");
    int i;
    int j;
    int rpr_chnk_id;
    int des_rack, des_rack_idx;
    int read_rack, read_rack_idx;
    int temp;
    int rand_idx;
    int cr_cnt, read_cnt;

    int* index = (int*)malloc(sizeof(int)*(_rack_num-1));
    // random generate repair solutions 
    srand(time(0));
    for(i=0; i<num_repair_chnk_round; i++){

        // for the last repair round
        if(rpr_chnk[i] == -1)
            break;

        // random select a destination rack 
        rpr_chnk_id = rpr_chnk[i]; 
        while(1){
            des_rack_idx = rand()%_rack_num;
            des_rack = _sort_rack_index[rpr_chnk_id*_rack_num+des_rack_idx];
            if(_chnk_num_rack[rpr_chnk_id*_rack_num+des_rack_idx] < _ecM){
                break;
            }
        }
        printf("des_rack_idx = %d\n", des_rack_idx);   
        printf("des_rack = %d\n", des_rack);   

        // select racks for reading data, the data in the destination rack will be read by default
        for(j=0; j<_rack_num; j++){
            if(j==des_rack_idx)
                continue;
            if(j<des_rack_idx)
                index[j] = j;
            else 
                index[j-1] = j;
        }
        printf("index:\n");
        display(_rack_num-1, 1, index);
        temp = _rack_num-1;
        cr_cnt = 0;
        read_cnt = _chnk_num_rack[rpr_chnk_id*_rack_num+des_rack_idx];
        while(1){
            rand_idx = rand()%temp;
            read_rack_idx = index[rand_idx];
            if(_chnk_num_rack[rpr_chnk_id*_rack_num+read_rack_idx]==0)
                continue;
            read_cnt += _chnk_num_rack[rpr_chnk_id*_rack_num+read_rack_idx];
            read_rack = _sort_rack_index[rpr_chnk_id*_rack_num+read_rack_idx];
            rpr_sltns[i*_rack_num+read_rack]=1;
            cr_cnt ++;

            printf("cr_cnt = %d, read_cnt = %d\n", cr_cnt, read_cnt);
            // update index
            index[rand_idx] = index[temp-1];
            temp--;

            if(read_cnt >= _ecK)
                break;
        }        

        // update the solution
        rpr_sltns[i*_rack_num+des_rack] = (-1)*cr_cnt;        
    }
    printf("------after init: rpr_sltns:------\n");
    display(_rack_num, num_repair_chnk_round, rpr_sltns);

    free(index);
}
int Coordinator::calSum(int* array, int len){
    int ret = 0;
    for(int i=0; i<len; i++)
        ret+=array[i];

    return ret;
}

void Coordinator::RackSRBalance(int fail_node, int num_repair_chnk_round){
    int i;
    int num_round;
    int rnd_cnt;
    int num_remain_chnk;
    int rndm_slct;
    int temp;
    int total_cr_traf;
    int max_upload, max_download, max;
    double aver_cr_traf;
    double gap;

    // for random selection
    int* index = (int*)malloc(sizeof(int)*_num_rebuilt_chunks);
    for(i=0; i<_num_rebuilt_chunks; i++)
            index[i] = i;

    // record the indices of repaired chunks
    int* rpr_chnk = (int*)malloc(sizeof(int)*num_repair_chnk_round);
    // record the rpeair solutsion for each failed chunk
    // we use "1" to denote the racks for retrieving data and use "-x" to denote the destination rack
    // where "x" is the number of chunks downloaded over the core-to-rack link
    int* rpr_sltns = (int*)malloc(sizeof(int)*_rack_num*num_repair_chnk_round);
    int* rack_upload = (int*)malloc(sizeof(int)*_rack_num);
    int* rack_download = (int*)malloc(sizeof(int)*_rack_num);
    int* des_rack_round = (int*)malloc(sizeof(int)*num_repair_chnk_round);
    num_round = (int)(ceil(_num_rebuilt_chunks*1.0/num_repair_chnk_round));

    printf("----before repair------\n");
    printf("--_num_rebuilt_chunks = %d\n", _num_rebuilt_chunks);
    printf("--num_repair_chnk_round = %d\n", num_repair_chnk_round);
    printf("--num_round = %d\n", num_round);
    printf("------------------\n");

    srand(time(0));
    for(rnd_cnt=0; rnd_cnt<num_round; rnd_cnt++){

        memset(rpr_chnk, -1, sizeof(int)*num_repair_chnk_round);
        memset(des_rack_round, -1, sizeof(int)*num_repair_chnk_round);
        memset(rpr_sltns, 0, sizeof(int)*_rack_num*num_repair_chnk_round);

        // update number of remaining chunks
        num_remain_chnk = _num_rebuilt_chunks-rnd_cnt*num_repair_chnk_round;
        printf("num_remain_chnk = %d\n", num_remain_chnk);

        // generate the rpr_chnk
        temp = num_remain_chnk;
        if(num_remain_chnk<num_repair_chnk_round)
            num_repair_chnk_round=num_remain_chnk;
        for(i=0; i<num_repair_chnk_round; i++){
            // random select
            rndm_slct = rand()%temp;
            rpr_chnk[i] = index[rndm_slct];
            index[rndm_slct] = index[temp-1];
            temp--;
        }

        // initialize a repair solution 
        // we assume that num_repair_chnk_round = _rack_num
        printf("%d-th round, rpr_chnk:\n", rnd_cnt);
        display(num_repair_chnk_round, 1, rpr_chnk);
        printf("\n");
        RackSRInit(num_repair_chnk_round, rpr_chnk, rpr_sltns, des_rack_round);
        // record the upload and download traffic
        RecordLoad(rack_upload, rack_download, rpr_sltns, num_repair_chnk_round);
        // optimize the repair solution
        for(int step = 0; step < _balance_steps; step++){
            printf("==== optimize step = %d ========\n", step);
            RackSROptimize(num_repair_chnk_round, rpr_chnk, rack_upload, rack_download, rpr_sltns, des_rack_round, index, temp);
            RecordLoad(rack_upload, rack_download, rpr_sltns, num_repair_chnk_round);
            // if the balancing ratio is small than a given deviation, then break
            total_cr_traf = calSum(rack_upload, _rack_num);
            printf("total_cr_traf = %d\n", total_cr_traf);

            max_upload = *max_element(rack_upload, rack_upload+_rack_num);
            max_download = *max_element(rack_download, rack_download+_rack_num);
            max = max_upload>max_download?max_upload:max_download;
            printf("max_upload = %d, max_download = %d, max = %d\n", max_upload, max_download, max);

            aver_cr_traf = total_cr_traf*1.0/_rack_num;
            gap = (max-aver_cr_traf)*1.0/aver_cr_traf;
            printf("aver_cr_traf = %.2lf, gap = %.2lf\n", aver_cr_traf, gap);
            if(gap <= (_balance_dev*1.0/100))
                break;
        }
        // do process
        doProcess(fail_node, num_repair_chnk_round, rpr_chnk, rpr_sltns);
    }

    free(index);
    free(rpr_chnk);
    free(rpr_sltns);
    free(rack_upload);
    free(rack_download);
    free(des_rack_round);
}

// generate the repair solution for CAR
// we randomly select a destination rack and use CAR to get the repair solution with the least cross-rack traffic
void Coordinator::CARSol(int num_repair_chnk_round, int* rpr_chnk, int* rpr_sltns){
    
    int rpr_chnk_id;
    int i, j;
    int des_rack;
    int des_rack_idx;
    int rd_cnt;
    int cr_cnt;
    int rack_id;
    vector<int> cddt_des;

    // initializea repair solutions
    for(i=0; i<num_repair_chnk_round; i++){

        // for the last repair round
        if(rpr_chnk[i] == -1)
            break;

        // randomly select a destination rack 
        rpr_chnk_id = rpr_chnk[i]; 
        while(1){
            des_rack_idx = rand()%_rack_num;
            des_rack = _sort_rack_index[rpr_chnk_id*_rack_num+des_rack_idx];
            if(_chnk_num_rack[rpr_chnk_id*_rack_num+des_rack_idx] < _ecM){
                break;
            }
        }
        printf("\ndes_rack_idx = %d\n", des_rack_idx);   
        printf("des_rack = %d\n", des_rack);   
        
        // select racks for reading k chunks 
        cr_cnt = 0;
        rd_cnt = _chnk_num_rack[rpr_chnk_id*_rack_num+des_rack_idx];
        printf("rd_cnt = %d\n", rd_cnt);
        for(j=0; j<_rack_num; j++){

            if(j==des_rack_idx) continue;

            rack_id = _sort_rack_index[rpr_chnk_id*_rack_num+j];
            rd_cnt += _chnk_num_rack[rpr_chnk_id*_rack_num+j];
            rpr_sltns[i*_rack_num+rack_id]=1;
            cr_cnt ++;
                
            printf("rack_id = %d\n", rack_id);
            printf("rd_cnt = %d\n", rd_cnt); 
            if(rd_cnt >=_ecK)
                break;
        }
        rpr_sltns[i*_rack_num+des_rack] = (-1)*cr_cnt;
    }

    printf("car: initial solution:\n");
    display(_rack_num, num_repair_chnk_round, rpr_sltns);
    
    // balance the solutions for the uploading traffic
    int* upld = (int*)malloc(sizeof(int)*_rack_num);
    int max_traf = -1;
    int max_rack = -1;
    int if_find;
    int cur_rack;

    while(1){

        memset(upld, 0, sizeof(int)*_rack_num);
        // get the uploading traffic
        for(i=0; i<num_repair_chnk_round; i++){
            for(j=0; j<_rack_num; j++){
                if(rpr_sltns[i*_rack_num+j]==1){
                    upld[j]++;

                }
            }
        }
        printf("car upload traffic:\n");
        display(_rack_num, 1, upld);

        // balance the solution 
        for(i=0; i<_rack_num; i++){
            if(upld[i] > max_traf){
                max_traf = upld[i];
                max_rack = i;
            }
        }

        printf("max_traf = %d\n", max_traf);
        printf("max_rack = %d\n", max_rack);

        // perform solution swapping
        if_find = 0;
        for(i=0; i<num_repair_chnk_round; i++){
            if(rpr_sltns[i*_rack_num+max_rack] != 1)
                continue;

            // calculate the read chunks except the heaviest rack
            rd_cnt = 0;
            rpr_chnk_id = rpr_chnk[i];
            for(j=0; j<_rack_num; j++){
                cur_rack = _sort_rack_index[rpr_chnk_id*_rack_num+j];
                if(cur_rack == max_rack)
                    continue;

                if(rpr_sltns[i*_rack_num+cur_rack] !=0){
                    rd_cnt += _chnk_num_rack[rpr_chnk_id*_rack_num+j];
                }
            }

            // check if can perform swapping
            for(j=0; j<_rack_num; j++){
                cur_rack = _sort_rack_index[rpr_chnk_id*_rack_num+j];
                if((rpr_sltns[i*_rack_num+cur_rack]==0) && (upld[max_rack]-upld[cur_rack] >= 2)){

                    // perform swapping
                    if(rd_cnt + _chnk_num_rack[rpr_chnk_id*_rack_num+j] >= _ecK){

                        printf("rd_cnt = %d\n", rd_cnt);
                        printf("swap_cnt = %d\n", _chnk_num_rack[rpr_chnk_id*_rack_num+j]);
                        rpr_sltns[i*_rack_num+max_rack] = 0; 
                        rpr_sltns[i*_rack_num+cur_rack] = 1;
                        if_find = 1;
                        break;
                    }
                }
            }
            printf("if_find = %d\n", if_find);
            if(if_find==1)
                break;
        }

        // if we cannot perform swapping
        if(if_find==0)
            break;
    }
    
    printf("car: balanced solution:\n");
    display(_rack_num, num_repair_chnk_round, rpr_sltns);

    free(upld);
}



// for CAR, we assume that the dedicated node resides in the same rack of the failed node 
int Coordinator::CAR(int fail_node){

    int i;
    int temp;
    int rndm_slct;
    int num_round;
    int sum;
    int rnd_cnt;
    int num_repair_chnk_round = _num_repair_chnk_round;
    int num_remain_chnk;

    // for random selection
    int* index = (int*)malloc(sizeof(int)*_num_rebuilt_chunks);
    // record the indices of repaired chunks
    int* rpr_chnk = (int*)malloc(sizeof(int)*num_repair_chnk_round);
    // record the rpeair solutsion for each failed chunk
    // we use "1" to denote the racks for retrieving data and use "-x" to denote the destination rack
    // where "x" is the number of chunks downloaded over the core-to-rack link
    // the rack that is not accessed is marked as 0
    int* rpr_sltns = (int*)malloc(sizeof(int)*_rack_num*num_repair_chnk_round);
    int* rack_upload = (int*)malloc(sizeof(int)*_rack_num);
    int* rack_download = (int*)malloc(sizeof(int)*_rack_num);
    num_round = (int)(ceil(_num_rebuilt_chunks*1.0/num_repair_chnk_round));

    printf("---- CAR ------\n");
    printf("--_num_rebuilt_chunks = %d\n", _num_rebuilt_chunks);
    printf("--num_repair_chnk_round = %d\n", num_repair_chnk_round);
    printf("--num_round = %d\n", num_round);
    printf("------------------\n");

    for(i=0; i<_num_rebuilt_chunks; i++)
        index[i] = i;

    srand(time(0));
    sum = 0;
    for(rnd_cnt=0; rnd_cnt<num_round; rnd_cnt++){

        memset(rpr_chnk, -1, sizeof(int)*num_repair_chnk_round);
        memset(rpr_sltns, 0, sizeof(int)*_rack_num*num_repair_chnk_round);

        // update number of remaining chunks
        num_remain_chnk = _num_rebuilt_chunks-rnd_cnt*num_repair_chnk_round;
        // generate the rpr_chnk
        temp = num_remain_chnk;
        if(num_remain_chnk<num_repair_chnk_round)
            num_repair_chnk_round=num_remain_chnk;
        printf("num_remain_chnk = %d\n", num_remain_chnk);

        for(i=0; i<num_repair_chnk_round; i++){
            // random select
            rndm_slct = rand()%temp;
            rpr_chnk[i] = index[rndm_slct];
            index[rndm_slct] = index[temp-1];
            temp--;
        }

        // initialize a repair solution 
        printf("%d-th round, CAR rpr_chnk:\n", rnd_cnt);
        display(num_repair_chnk_round, 1, rpr_chnk);
        printf("\n");
        CARSol(num_repair_chnk_round, rpr_chnk, rpr_sltns);
        // record the upload and download traffic
        RecordLoad(rack_upload, rack_download, rpr_sltns, num_repair_chnk_round);
        sum += (-1)*calSum(rack_download, _rack_num);
        // perform repair
        doProcess(fail_node, num_repair_chnk_round, rpr_chnk, rpr_sltns); 
    } 

    free(index);
    free(rpr_chnk);
    free(rpr_sltns);
    free(rack_upload);
    free(rack_download);

    return sum;

}

int Coordinator::RandRepair(int fail_node){

    int i;
    int num_round;
    int rnd_cnt;
    int num_remain_chnk;
    int rndm_slct;
    int temp;
    int num_repair_chnk_round = _num_repair_chnk_round;
    int sum;

    // for random selection
    int* index = (int*)malloc(sizeof(int)*_num_rebuilt_chunks);
    // record the indices of repaired chunks
    int* rpr_chnk = (int*)malloc(sizeof(int)*num_repair_chnk_round);
    // record the rpeair solutsion for each failed chunk
    // we use "1" to denote the racks for retrieving data and use "x" to denote the destination rack
    // where "x" is the number of chunks downloaded over the core-to-rack link
    int* rpr_sltns = (int*)malloc(sizeof(int)*_rack_num*num_repair_chnk_round);
    int* rack_upload = (int*)malloc(sizeof(int)*_rack_num);
    int* rack_download = (int*)malloc(sizeof(int)*_rack_num);
    num_round = (int)(ceil(_num_rebuilt_chunks*1.0/num_repair_chnk_round));

    printf("----random repair------\n");
    printf("--_num_rebuilt_chunks = %d\n", _num_rebuilt_chunks);
    printf("--num_repair_chnk_round = %d\n", num_repair_chnk_round);
    printf("--num_round = %d\n", num_round);
    printf("------------------\n");

    for(i=0; i<_num_rebuilt_chunks; i++)
        index[i] = i;

    srand(time(0));
    sum = 0;
    for(rnd_cnt=0; rnd_cnt<num_round; rnd_cnt++){

        memset(rpr_chnk, -1, sizeof(int)*num_repair_chnk_round);
        memset(rpr_sltns, 0, sizeof(int)*_rack_num*num_repair_chnk_round);

        // update number of remaining chunks
        num_remain_chnk = _num_rebuilt_chunks-rnd_cnt*num_repair_chnk_round;
        printf("num_remain_chnk = %d\n", num_remain_chnk);

        // generate the rpr_chnk
        temp = num_remain_chnk;
        if(num_remain_chnk<num_repair_chnk_round)
            num_repair_chnk_round=num_remain_chnk;
        for(i=0; i<num_repair_chnk_round; i++){
            // random select
            rndm_slct = rand()%temp;
            rpr_chnk[i] = index[rndm_slct];
            index[rndm_slct] = index[temp-1];
            temp--;
        }

        // initialize a repair solution 
        // we assume that num_repair_chnk_round = _rack_num
        printf("%d-th round, rpr_chnk:\n", rnd_cnt);
        display(num_repair_chnk_round, 1, rpr_chnk);
        printf("\n");
        RandSol(num_repair_chnk_round, rpr_chnk, rpr_sltns);
        // record the upload and download traffic
        RecordLoad(rack_upload, rack_download, rpr_sltns, num_repair_chnk_round);
        sum += calSum(rack_download, _rack_num);
        // perform repair
        doProcess(fail_node, num_repair_chnk_round, rpr_chnk, rpr_sltns); 
    } 

    free(index);
    free(rpr_chnk);
    free(rpr_sltns);
    free(rack_upload);
    free(rack_download);

    return sum;
}

// read the stripe information from the NameNode of HDFS 
void Coordinator::parseLog(){

    cout << "---parseLog" << endl;  
    // get the metadatabase by using hdfs fsck 
    // struct timeVal tv1, tv2;
    /////////////  read from NameNode //////////////
    string cmdResult;
    // string cmdFsck("hdfs fsck / -files -blocks -locations");
    // FILE* pipe = popen(cmdFsck.c_str(), "r");
    // if(!pipe) 
    //     cerr << "ERROR when using hdfs fsck" << endl;
    // char cmdBuffer[256];
    // while(!feof(pipe))
    // {
    //     if(fgets(cmdBuffer, 256, pipe) != NULL)
    //     {
    //         cmdResult += cmdBuffer;
    //     }
    // }
    // pclose(pipe);
    // / //////////////////////////////////////////////
     
    
    ////////////  read hdfs metadata ////////////////// 
    // string cmdResult;
    // FILE* fp = fopen("ec_hdfs_metadata", "r");
    // char cmdBuffer[256];
    // while(!feof(fp)){
    //     if(fgets(cmdBuffer, 256, fp)!=NULL)
    //         cmdResult += cmdBuffer;
    // }
    // cout << "Get the Metadata successfully" << endl;
    // cout << cmdResult << endl;
    /////// the result is stored in cmdResult as a string ///////////////

    //start to parse cmdResult
    //length of stripeID: 29
    //length of blkName: 24
    set<string> blks;    //the set of blks
    set<string> stripeSet;
    set<string> tempBlkSet;
    map<string, set<string>> blk2Stripe;
    map<string, vector<string>> recoveree;
    map<string, set<string>> stripe2Blk;
    //map<string, unsigned int> blk2Ip;
    _blk2Stripe.clear();
    _stripe_num = 0;

    string stripeId, ipAdd, blkName, stripeName;
    size_t currentPos, endPos, ipLength;
    size_t tmp_pos;

    /* Attention: as we assume that the HDFS client will create the file named /ec_test in our test, we will use the string "/ec_test" 
       as a marker to parse the output text returned by the NameNode. If you create a folder with a different name in your test, then 
       you should replace the "/ec_test" with the name of the folder used in your test */
    size_t strpPos = cmdResult.find("/ec_test");
    size_t startPos = cmdResult.find("blk_", strpPos);
    while(true){
        if(startPos == string::npos){
            break;
        }
        stripeId = cmdResult.substr(startPos, 29);
        tmp_pos = stripeId.find("_",4);
        stripeName = stripeId.substr(tmp_pos+1, 4);

        //stripeSet.insert(stripeId);
        cout << "Find the stripe: " << stripeId << endl;
        cout << "stripeName: " << stripeName << endl;
        for (int i = 0; i < _ecN; i++){
            cout << "i = " << i << endl;
            currentPos = cmdResult.find("blk_", startPos+29);
            //get the block index
            blkName = cmdResult.substr(currentPos, 24);
            cout << "blkName: " << blkName << endl;
            currentPos = cmdResult.find("[", currentPos);
            endPos = cmdResult.find(":", currentPos);
            ipLength = endPos - currentPos - 1;
            //get the ip_address of this block
            ipAdd = cmdResult.substr(currentPos + 1, ipLength);
            cout << "ip_address: " << ipAdd << endl; 
            startPos = endPos;
            //update _placement
            vector <unsigned int>::iterator find_pos = find(_conf->_peerNodeIPs.begin(), _conf->_peerNodeIPs.end(), inet_addr(ipAdd.c_str()));
            if(distance(_conf->_peerNodeIPs.begin(), find_pos) >= _peer_node_num){
                cout << "ERR: " << ipAdd << endl;
                exit(1);
            }
            _placement[_stripe_num*_ecN+i] = distance(_conf->_peerNodeIPs.begin(), find_pos);
            //update chunkid2addr
            size_t global_chunk_id = _stripe_num*_ecN+i;
            _chunkid2addr.insert(pair<size_t, string>(global_chunk_id, blkName));
            _blkName2stripeName.insert(pair<string,string>(blkName,stripeName));
            //blk2Ip.insert(make_pair(blkName, str2Ip(ipAdd)));
            blks.insert(blkName);
            tempBlkSet.insert(blkName);                 
        }
        // /ec_test is the folder created for storing erasure coded data 
        // ------------> strpPos = cmdResult.find("/ec_test", startPos);
        strpPos = cmdResult.find("BP-", startPos);
        startPos = cmdResult.find("blk_", strpPos);
        stripe2Blk.insert(make_pair(stripeId, tempBlkSet));
        tempBlkSet.clear();
        _stripe_num++;
    }

    cout << "placement:" << endl;
    display(_ecN, _stripe_num, _placement);

    cout << "global_chunk_id -> block_name" << endl;
    map<size_t, string>::const_iterator it;
    for(it=_chunkid2addr.begin(); it!=_chunkid2addr.end(); it++)
        cout << it->first << "==>" << it->second << std::endl;

    cout << "_stripe_num = " << _stripe_num << endl;
}


void Coordinator::freeGlobal(void){
    // free the FINAL global memory
    free(_placement);
    free(_related_stripes);
    free(_chnk_num_rack);
    free(_sort_rack_index);
}
