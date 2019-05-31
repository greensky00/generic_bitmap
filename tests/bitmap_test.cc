#include "test_common.h"

#include "generic_bitmap.h"

#include <atomic>
#include <thread>
#include <unordered_set>
#include <vector>

int basic_verification(size_t num) {
    GenericBitmap my_bitmap(num);

    std::unordered_set<size_t> set_list;
    for (size_t ii=0; ii<num/2; ++ii) {
        size_t idx = rand() % num;
        set_list.insert(idx);
        my_bitmap.set(idx, true);
    }

    for (size_t ii=0; ii<num; ++ii) {
        auto entry = set_list.find(ii);
        if (entry == set_list.end()) {
            CHK_FALSE( my_bitmap.get(ii) );
        } else {
            CHK_TRUE( my_bitmap.get(ii) );
        }
    }
    return 0;
}

int single_thread_compare(size_t num) {
    std::vector<bool> vector_bitmap(num, false);

    TestSuite::Timer tt;
    for (size_t ii=0; ii<num; ++ii) {
        bool val = (rand() % 2 == 1) ? true : false;
        size_t idx = rand() % num;
        vector_bitmap[idx] = val;
    }
    TestSuite::_msg("vector: %zu us\n", tt.getTimeUs());

    GenericBitmap my_bitmap(num);
    tt.reset();
    for (size_t ii=0; ii<num; ++ii) {
        bool val = (rand() % 2 == 1) ? true : false;
        size_t idx = rand() % num;
        my_bitmap.set(idx, val);
    }
    TestSuite::_msg("my bitmap: %zu us\n", tt.getTimeUs());

    return 0;
}

struct MtArgs : TestSuite::ThreadArgs {
    enum Operation {
        READER = 0,
        WRITER = 1
    };
    enum Type {
        VECTOR = 0,
        MY_BITMAP = 1,
    };

    std::vector<bool>* ptrVector;
    GenericBitmap* ptrMyBitmap;
    Operation op;
    Type type;
    size_t num;
    std::atomic<bool>* stopSignal;
    std::atomic<uint64_t>* numReads;
    std::atomic<uint64_t>* numWrites;
    std::unordered_set<uint64_t>* setList;
};

int worker(TestSuite::ThreadArgs* t_args) {
    MtArgs* args = static_cast<MtArgs*>(t_args);
    while ( !(*args->stopSignal) ) {
        size_t idx = rand() % args->num;

        switch (args->op) {
        case MtArgs::READER: {
            bool val = false; (void)val;
            if (args->type == MtArgs::VECTOR) val = (*args->ptrVector)[idx];
            else val = args->ptrMyBitmap->get(idx);
            args->numReads->fetch_add(1);
            break; }

        case MtArgs::WRITER: {
            bool val = (rand() % 2 == 1) ? true : false;
            if (args->type == MtArgs::VECTOR) (*args->ptrVector)[idx] = val;
            else args->ptrMyBitmap->set(idx, val);
            args->numWrites->fetch_add(1);
            break; }
        }
    }
    return 0;
}

int multi_thread_compare(size_t num) {
    std::vector<bool> vector_bitmap(num, false);
    GenericBitmap my_bitmap(num);

    for (MtArgs::Type base_type: {MtArgs::VECTOR, MtArgs::MY_BITMAP}) {
        std::vector<TestSuite::ThreadHolder> holders(12);
        std::vector<MtArgs> args(12);
        std::atomic<uint64_t> num_reads(0);
        std::atomic<uint64_t> num_writes(0);
        std::atomic<bool> stop_signal(false);
        TestSuite::Timer tt;
        for (size_t ii=0; ii<12; ++ii) {
            TestSuite::ThreadHolder& hh = holders[ii];
            MtArgs& aa = args[ii];
            aa.ptrVector = &vector_bitmap;
            aa.ptrMyBitmap = &my_bitmap;
            aa.op = (ii == 0) ? MtArgs::WRITER : MtArgs::READER;
            aa.num = num;
            aa.type = base_type;
            aa.stopSignal = &stop_signal;
            aa.numReads = &num_reads;
            aa.numWrites = &num_writes;
            hh.spawn(&aa, worker, nullptr);
        }
        TestSuite::sleep_sec(3, "running..");
        stop_signal = true;
        for (size_t ii=0; ii<12; ++ii) {
            TestSuite::ThreadHolder& hh = holders[ii];
            hh.join();
        }
        TestSuite::_msg("%s: %zu reads %zu writes\n",
                        (base_type == MtArgs::VECTOR) ? "VECTOR" : "MY BITMAP",
                        num_reads.load(),
                        num_writes.load());
    }

    return 0;
}

int mt_verifier(TestSuite::ThreadArgs* t_args) {
    MtArgs* args = static_cast<MtArgs*>(t_args);
    while ( !(*args->stopSignal) ) {

        switch (args->op) {
        case MtArgs::READER: {
            size_t idx = rand() % args->num;
            while (idx % 3 == 0) idx = rand() % args->num;

            bool val = false; (void)val;

            if (args->type == MtArgs::VECTOR) val = (*args->ptrVector)[idx];
            else val = args->ptrMyBitmap->get(idx);

            auto entry = args->setList->find(idx);
            if (entry == args->setList->end()) {
                CHK_FALSE(val);
            } else {
                CHK_TRUE(val);
            }
            break; }

        case MtArgs::WRITER: {
            size_t idx = (rand() % (args->num / 3)) * 3;
            while (idx >= args->num) idx = 0;

            bool val = (rand() % 2 == 1) ? true : false;

            if (args->type == MtArgs::VECTOR) (*args->ptrVector)[idx] = val;
            else args->ptrMyBitmap->set(idx, val);
            //TestSuite::_msg("%zu %d, ", idx, val);
            break; }
        }
    }
    return 0;
}

int mt_race_test(size_t duration) {
    size_t num = 64;
    size_t num_threads = 12;
    std::vector<bool> vector_bitmap(num, false);
    GenericBitmap my_bitmap(num);
    std::unordered_set<uint64_t> set_list;

    // Randomly set.
    for (size_t ii=0; ii<num; ++ii) {
        if (rand() % 2 == 0) {
            vector_bitmap[ii] = true;
            my_bitmap.set(ii, true);
            set_list.insert(ii);
        }
    }

    for (MtArgs::Type base_type: {MtArgs::VECTOR, MtArgs::MY_BITMAP}) {
        std::vector<TestSuite::ThreadHolder> holders(num_threads);
        std::vector<MtArgs> args(num_threads);
        std::atomic<uint64_t> num_reads(0);
        std::atomic<uint64_t> num_writes(0);
        std::atomic<bool> stop_signal(false);
        TestSuite::Timer tt;
        for (size_t ii=0; ii<num_threads; ++ii) {
            TestSuite::ThreadHolder& hh = holders[ii];
            MtArgs& aa = args[ii];
            aa.ptrVector = &vector_bitmap;
            aa.ptrMyBitmap = &my_bitmap;
            aa.setList = &set_list;
            aa.op = (ii == 0) ? MtArgs::WRITER : MtArgs::READER;
            aa.num = num;
            aa.type = base_type;
            aa.stopSignal = &stop_signal;
            aa.numReads = &num_reads;
            aa.numWrites = &num_writes;
            hh.spawn(&aa, mt_verifier, nullptr);
        }
        TestSuite::sleep_sec(duration, "checking..");
        stop_signal = true;
        for (size_t ii=0; ii<num_threads; ++ii) {
            TestSuite::ThreadHolder& hh = holders[ii];
            hh.join();
            CHK_Z(hh.getResult());
        }
    }

    return 0;
}

int dump_load_test(size_t num) {
    GenericBitmap my_bitmap(num);

    std::unordered_set<size_t> set_list;
    for (size_t ii=0; ii<num/2; ++ii) {
        size_t idx = rand() % num;
        set_list.insert(idx);
        my_bitmap.set(idx, true);
    }

    GenericBitmap cloned_bitmap(my_bitmap.getPtr(),
                                my_bitmap.getMemorySize(),
                                my_bitmap.size());

    for (size_t ii=0; ii<num; ++ii) {
        auto entry = set_list.find(ii);
        if (entry == set_list.end()) {
            CHK_FALSE( cloned_bitmap.get(ii) );
        } else {
            CHK_TRUE( cloned_bitmap.get(ii) );
        }
    }
    return 0;
}

int dump_move_test(size_t num) {
    GenericBitmap my_bitmap(num);

    std::unordered_set<size_t> set_list;
    for (size_t ii=0; ii<num/2; ++ii) {
        size_t idx = rand() % num;
        set_list.insert(idx);
        my_bitmap.set(idx, true);
    }

    size_t buf_len = my_bitmap.getMemorySize();
    void* tmp_buf = malloc(buf_len);
    memcpy(tmp_buf, my_bitmap.getPtr(), buf_len);

    GenericBitmap new_bitmap(0);
    new_bitmap.moveFrom(tmp_buf, buf_len, num);

    for (size_t ii=0; ii<num; ++ii) {
        auto entry = set_list.find(ii);
        if (entry == set_list.end()) {
            CHK_FALSE( new_bitmap.get(ii) );
        } else {
            CHK_TRUE( new_bitmap.get(ii) );
        }
    }
    return 0;
}

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);;

    ts.options.printTestMessage = true;

    ts.doTest("basic verification",
              basic_verification,
              TestRange<size_t>({1000000}));

    ts.doTest("single thread comparison",
              single_thread_compare,
              TestRange<size_t>({10000000}));

    ts.doTest("multi thread comparison",
              multi_thread_compare,
              TestRange<size_t>({10000000}));

    ts.doTest("multi thread race test",
              mt_race_test,
              TestRange<size_t>({10}));

    ts.doTest("dump load test",
              dump_load_test,
              TestRange<size_t>({1000000}));

    ts.doTest("dump move test",
              dump_move_test,
              TestRange<size_t>({1000000}));

    return 0;
}
