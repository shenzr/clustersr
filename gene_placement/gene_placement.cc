// This program takes two parameters as input: $node_num and $rack_num, and generates a random placement 

#include <iostream>
#include <fstream>

#include "config.h"

extern "C" {
#include "common.h"
}

using namespace std; 

int main(int argc, char* argv[]){

    if(argc!=3){

		printf("ERR: ./gene_placement (node_num) (rack_num)\n");
		exit(1);
		
    	}

    int node_num = atoi(argv[1]);
    int rack_num = atoi(argv[2]);

    if(node_num%rack_num!=0){
        printf("ERR: node_num mod rack_num!=0\n");
        exit(1);
    }
    printf("node_num=%d, rack_num=%d, stripe_num = %d\n", node_num, rack_num, stripe_num);

    int* placement=(int*)malloc(sizeof(int)*(erasure_k+erasure_m)*stripe_num);
	gene_rndm_plcement(erasure_k+erasure_m, stripe_num, placement, node_num, rack_num);
    
	// write the placement 
	ofstream output("../metadata/placement");
	int i,j;

	for(i=0; i<stripe_num; i++){
	    for(j=0; j<erasure_k+erasure_m; j++)
			output <<placement[i*(erasure_k+erasure_m)+j] << " ";

		output<<endl;
		}

	free(placement);	
}
