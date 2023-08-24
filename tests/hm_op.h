#ifndef HM_OP_H
#define HM_OP_H

#include <iostream>
#include <fstream>
#include <cassert>
#include <vector>

using namespace std;

#define INSERT 0
#define DELETE 1
#define LOOKUP 2

struct hm_op {
  int op;
  uint64_t key;
  uint64_t value;
};

void load_ops(std::string replay_filepath, int *key_bits, int *quotient_bits,
              int *value_bits, std::vector<hm_op> &ops) {
  ifstream ifs;
  ifs.open(replay_filepath);
  ifs >> (*key_bits) >> (*quotient_bits) >> (*value_bits);
  uint64_t num_keys;
  ifs >> num_keys;
  for (int i = 0; i < num_keys; i++) {
    hm_op op;
    ifs >> op.op >> op.key >> op.value;
    ops.push_back(op);
  }
  ifs.close();
}

void write_ops(std::string replay_filepath, int key_bits, int quotient_bits,
               int value_bits, std::vector<hm_op> &ops) {
  ofstream ofs;
  ofs.open(replay_filepath);
  ofs << key_bits << " " << quotient_bits << " " << value_bits << std::endl;
  ofs << ops.size() << std::endl;
  for (auto op : ops) {
    ofs << op.op << " " << op.key << " " << op.value << std::endl;
  }
  ofs.close();
}

#endif
