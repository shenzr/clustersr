#ifndef _COMMON_H
#define _COMMON_H
extern double max(double a, double b);
extern void print_matrix(int32_t len, int32_t width, int32_t* matrix);
extern void gene_rndm_plcement(int32_t len, int32_t width, int32_t* placement, int node_num, int rack_num);
#endif
