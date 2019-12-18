#include <iostream>
#include <thread>
#include <vector>

#include "Coordinator.hh"

using namespace std; 

int main(int argc, char** argv){

    if(argc != 4){
        std::cout << "Err: ./RackSRCoordinator (num_repair_chunk) (fail_node_id) (repair_scheme)" << std::endl;
        exit(1);
    }
    
    Config* conf = new Config("metadata/config.xml");

    int racksr_cnt; 
    int car_cnt;
    int rndm_cnt;
    int repair_chnk_num = atoi(argv[1]);
    int fail_node = atoi(argv[2]);

    racksr_cnt=0; 
    car_cnt=0;
    rndm_cnt=0;
        
    printf("===== fail_node = %d\n", fail_node);
    Coordinator *coord = new Coordinator(conf);
    coord->preprocess(fail_node, repair_chnk_num);
   
    if(strcmp(argv[3], "racksr") == 0) 
        racksr_cnt = coord->RackSR(fail_node);
    else if(strcmp(argv[3], "car") == 0) 
        car_cnt = coord->CAR(fail_node);
    else if(strcmp(argv[3], "rr") == 0)
        rndm_cnt = coord->RandRepair(fail_node);
    else{
        printf("ERR: repair_scheme\n");
        exit(1);
    } 

    printf("racksr_cnt = %.2lf\n", racksr_cnt*1.0);
    printf("car_cnt = %.2lf\n", car_cnt*1.0);
    printf("rndm_cnt = %.2lf\n", rndm_cnt*1.0);

    coord-> freeGlobal();
    delete coord;
    delete conf;
    return 0;

}
