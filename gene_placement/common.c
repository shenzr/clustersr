#define _GNU_SOURCE 

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <memory.h>

#include "config.h"

double max(double a, double b){

	return a>b?a:b;

}

void print_matrix(int len, int width, int* matrix){

	int i,j;

	for(i=0; i<width; i++){
		for(j=0; j<len; j++)
			printf("%d ", matrix[i*len+j]);
		printf("\n");
		}

}

void gene_rndm_plcement(int len, int width, int* placement, int node_num, int rack_num){

	//generate random distribution 
	int i,j; 
	int chnk_id;
    int rack_id;
	int rndm_nd_id;
	int remain_node_num;
    int node_per_rack;

	srand(time(0));
    node_per_rack = node_num/rack_num;

    int* chunk_num_rack = (int*)malloc(sizeof(int)*rack_num);
    int* node_id = (int*)malloc(sizeof(int)*node_num);
 
    for(i=0; i<stripe_num; i++){

        memset(chunk_num_rack, 0, sizeof(int)*rack_num);
	    for(j=0; j<node_num; j++)
	        node_id[j]=j;

	    remain_node_num=node_num;

	    for(chnk_id=0; chnk_id<erasure_k+erasure_m; chnk_id++){

           // select a random node id
	       rndm_nd_id=rand()%remain_node_num;

           // determine its rack and check the number of chunks
           rack_id = node_id[rndm_nd_id]/node_per_rack;
           if(chunk_num_rack[rack_id]>=erasure_m){
               chnk_id--; 
               continue;
           }

	       // update the node_id and remain_node_num
	       placement[i*(erasure_k+erasure_m)+chnk_id]=node_id[rndm_nd_id];
           chunk_num_rack[rack_id]++;
	       node_id[rndm_nd_id]=node_id[remain_node_num-1];
	       remain_node_num--;
		}
        //print_matrix(rack_num, 1, chunk_num_rack);
	}

	// print the placement
	//print_matrix(erasure_k+erasure_m, stripe_num, placement);
    free(chunk_num_rack);
    free(node_id);

}
