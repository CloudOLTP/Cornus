#include "scheduler.h"
#include "matrix.h"

class DLDetectScheduler : public Scheduler {
public:
  DLDetectScheduler(Workload * wl, int maxSteps) : Scheduler(wl, maxSteps) {
    scheduler_name = "DLDetect Scheduler";
    memset(txn_states, 0, sizeof(int) * numTxns);
    history = new int * [maxSteps];
    for (int i = 0; i < maxSteps; i ++) {
      history[i] = new int[numTxns];
      memset(history[i], 0, sizeof(int) * numTxns);
    }
  }
  void visualize() {
    int width = 6;
    cout << "DLDetect Schedule:" << endl;
    if (runtime == maxSteps) {
      cout << "Schedule not found!" << endl;
      //return;
    }
    cout << setw(width) << "";
    for (int i = 0; i < numTxns; i++) {
      cout << setw(width - 1) << "T" << i;
    }
    cout << endl << endl;

    for (int step = 0; step < runtime; step ++) {
      cout << setw(width) << step;
      for (int i = 0; i < numTxns; i ++) {
        int a = history[step][i];
        if (a == length)
          cout << setw(width) << "";
        else {
          if (a == -1) cout << setw(width) << "A";
          else {
            cout << setw(width - 3) << a;
            cout << "-";
            Access &access = workload->txns[i].accesses[ a ];
            std::string type = access.isWrite? "W" : "R";
            cout << type << access.key;
          }
        }
      }
      cout << endl;
    }
  }
  void schedule() {
    int curr_step = 0;
    while (!all_txns_done()) {
      for (int i = 0; i < numTxns; i ++) {
        if (txn_states[i] == length) continue; // skip committed transactions
        cpu_cycles ++;
        int step = txn_states[i];
        Access &access = workload->txns[i].accesses[ txn_states[i] ];
        int aborted_txn_list[numTxns];
        memset(aborted_txn_list, 0, sizeof(int) * numTxns);
        int ret = locktable.tryLock(access.key, i, access.isWrite? 2:1, txn_states, aborted_txn_list);
        if (ret == 1) {
          txn_states[i] ++;
          if (txn_states[i] == length) // txn commits
            locktable.unlock_all(i);
        } else if (ret == 0) {  //abort
          for (int k = 0; k < numTxns; k ++) {
            if (aborted_txn_list[k]) {
              num_aborts ++;
              locktable.unlock_all(k);
              txn_states[k] = 0;
              history[curr_step][k] = -1;
            }
          }
        } else { // wait
          num_waits ++;
          assert(ret == 2);
        }
      }
      curr_step ++;
      runtime = curr_step;
      if (curr_step == maxSteps)
        break;
      memcpy(history[curr_step], txn_states, sizeof(int) * numTxns);
    }
  }
private:
  class LockTable {
  public:
    LockTable() {
      memset(table, 0, sizeof(int) * tableSize * numTxns);
    }
    bool conflict(int type1, int type2) {
      return type1 + type2 >= 3;
    }
    // return value:
    // 0: failure
    // 1: success
    // 2: wait
    // Policy: abort the txn with the least forward progress, when equal, abort
    // the one with larger txn id.
    // Assuming instantaneous deadlock detection
    int tryLock(int key, int txn_id, int type, int * txn_states, int * aborted_txn_list) {
      int ret = 1;
      if (table[key][txn_id] != 0)
        return mat.is_txn_waiting(txn_id)? 2 : 1;

      int txns_in_cycle[numTxns];
      memset(txns_in_cycle, 0, sizeof(int) * numTxns);
      bool abort = false;
      for (int i = 0; i < numTxns; i ++) {
        if (conflict(table[key][i], type)) {
          ret = 2;
          mat.set(txn_id, i);
          while (mat.has_cycle(numTxns, txns_in_cycle)) {
            abort = true;
            int txn_with_min_effort;
            int min_effort = length;
            for (int k = 0; k < numTxns; k ++) {
              if (txns_in_cycle[k]
                  && (txn_states[k] < min_effort
                      || (txn_states[k] == min_effort
                          && k > txn_with_min_effort))) {
                txn_with_min_effort = k;
                min_effort = txn_states[k];
              }
            }
            aborted_txn_list[txn_with_min_effort] = 1;
            mat.clear(txn_with_min_effort);
            memset(txns_in_cycle, 0, sizeof(int) * numTxns);
          }
        }
      }
      table[key][txn_id] = type;
      if (abort)
        return 0;
      return ret;
    }
    void unlock_all(int txn_id) {
      for (int i = 0; i < tableSize; i ++) {
        table[i][txn_id] = 0;
        mat.clear(txn_id);
      }
    }
  private:
    // 0: no lock
    // 1: shared lock
    // 2: exclusive lock
    int table[tableSize][numTxns];
    Matrix mat;
  };

  bool all_txns_done() {
    for (int i = 0; i < numTxns; i++)
      if (txn_states[i] != length)
        return false;
    return true;
  };
  // 0 <= value < L: the current step of the txn
  // L: the txn has committed.
  // if value == -1, the txn just aborted.
  int txn_states[numTxns];
  int ** history; //[maxSteps][numTxns];
  LockTable locktable;
};

