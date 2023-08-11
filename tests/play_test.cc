#include <iostream>
#include "rhm_wrapper.h"
#include "trhm_wrapper.h"
#include "util.h"
using namespace std;

int main(int argc, char **argv) {
    std::cout<<"RobinHood Hashmap"<<std::endl;
    int key_bits = 16;
    int rem_bits = 16;
    uint64_t key = 0xffff;
    g_trhm_init(1<<key_bits, key_bits + rem_bits , 0);
    key = 0xffff;
    for (int i=0; i < 255; i++) {
        g_trhm_insert(key << rem_bits | i, 0);
    }
    qf_dump_block(&g_trobinhood_hashmap, 0xffff / 64);
    qf_dump_block(&g_trobinhood_hashmap, 0xffff / 64 + 1);
    qf_dump_block(&g_trobinhood_hashmap, 0xffff / 64 + 2);
    qf_dump_block(&g_trobinhood_hashmap, 0xffff / 64 + 3);
    qf_dump_block(&g_trobinhood_hashmap, 0xffff / 64 + 4);
    qf_dump_block(&g_trobinhood_hashmap, 0xffff / 64 + 5);
    qf_dump_block(&g_trobinhood_hashmap, 0xffff / 64 + 6);

    g_rhm_init(1<<key_bits, key_bits + rem_bits, 0);
    for (int i=0; i < 256; i++) {
         g_rhm_insert(key << rem_bits | i, 0);
    }
    qf_dump_block(&g_robinhood_hashmap, 0xffff / 64);
    qf_dump_block(&g_robinhood_hashmap, 0xffff / 64 + 1);
    qf_dump_block(&g_robinhood_hashmap, 0xffff / 64 + 2);
    qf_dump_block(&g_robinhood_hashmap, 0xffff / 64 + 3);
    qf_dump_block(&g_robinhood_hashmap, 0xffff / 64 + 4);
    qf_dump_block(&g_robinhood_hashmap, 0xffff / 64 + 5);
    qf_dump_block(&g_robinhood_hashmap, 0xffff / 64 + 6);
    std::cout<<run_end(&g_robinhood_hashmap, key) - key<< "\n";

}

