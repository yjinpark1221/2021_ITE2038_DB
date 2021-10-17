#define MAINTEST
#include "../db/include/file.h"
#include "../db/include/page.h"
#include "../db/include/yjinbpt.h"
#include "../db/src/file.cc"
#include "../db/src/page.cc"
#include "../db/src/yjinbpt.cc"
#include <string>
#include <cassert>
#include <string>
#include <stdio.h>
#include <vector>
#include <set>
int main(){

    puts("START");
    bool inTree[1000001];
    key__t n = 10000;
    table_t fd = file_open_database_file("tmp.db");
    assert(fd > 0);
    std::vector<key__t> randomVec;
    for (key__t i = 0; i <= n; ++i) {
        randomVec.push_back(i);
        inTree[i]= 0;
    }
    // random_shuffle(randomVec.begin(), randomVec.end());
    for (key__t i : randomVec) {
        if (i % 1000 == 0) printf("inserting %d\n", i);
        char val[] = "a\0b\0\0c\0d\0e\0f\0a\0b\0\0c\0d\0e\0f\0a\0b\0\0c\0d\0e\0f\0a\0b\0\0c\0d\0e\0f\0a\0b\0\0c\0d\0e\0f\0";
        int ret = db_insert(fd, i, val, 50);
        assert(ret == 0);
        inTree[i] = 1;
        //ASSERT_FALSE(ret) << "[insert " << i << " / ret = " << ret << "]\n";
    }
    puts(""); puts(""); puts("");

    random_shuffle(randomVec.begin(), randomVec.end());
    for (key__t key : randomVec) {
        if (key % 1000 == 0) printf("finding %d\n", key);
        char ret_val[8000] = { 0, };
        u16_t val_size;

        int ret = db_find(fd, key, ret_val, &val_size);
        if (!inTree[key]) {
            assert(ret);
        }
        else {
            assert(val_size == 50);
            assert(ret == 0);
            if (key % 1000 == 0){
                std::cout << "[find " << key << " / ret = " << ret << " / size = " << val_size << " / val = ";
                for (int j = 0; j < val_size; ++j) {
                    std::cout << ret_val[j];
                }
                std::cout << "]\n";
            }
        }
    }
    puts(""); puts(""); puts("");

    random_shuffle(randomVec.begin(), randomVec.end());
    for (key__t key : randomVec) {
        if (key % 1000 == 0) printf("deleting %d\n", key);
        int ret = db_delete(fd, key);
        inTree[key] = 0;
        assert(ret == 0);
        //  << "[delete " << key << " / ret = " << ret << "]\n";
    }
    puts(""); puts(""); puts("");

    random_shuffle(randomVec.begin(), randomVec.end());
    for (key__t key : randomVec) {
        if (key % 1000 == 0) printf("finding %d\n", key);
        char ret_val[8000] = { 0, };
        u16_t val_size;

        int ret = db_find(fd, key, ret_val, &val_size);
        if (!inTree[key]) {
            assert(ret);
        }
        else {
            assert(val_size == 50);
            assert(ret == 0);
            if (key % 1000 == 0){
                std::cout << "[find " << key << " / ret = " << ret << " / size = " << val_size << " / val = ";
                for (int j = 0; j < val_size; ++j) {
                    std::cout << ret_val[j];
                }
                std::cout << "]\n";
            }
        }
    }
    puts(""); puts(""); puts("");

    random_shuffle(randomVec.begin(), randomVec.end());
    for (key__t key : randomVec) {
        if (key % 1000 == 0) printf("inserting %d\n", key);
        char val[] = "012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";
        int ret = db_insert(fd, key, val, 112);
        inTree[key] = 1;

        assert(ret == 0);
        // ASSERT_FALSE(ret) << "[insert " << key << " / ret = " << ret << "]\n";
    }
    puts(""); puts(""); puts("");

    random_shuffle(randomVec.begin(), randomVec.end());
    for (key__t key : randomVec) {
        if (key % 1000 == 0) printf("finding %d\n", key);
        char ret_val[8000] = { 0, };
        u16_t val_size;

        int ret = db_find(fd, key, ret_val, &val_size);
        if (!inTree[key]) {
            assert(ret);
        }
        else {
            assert(val_size == 112);
            assert(ret == 0);
            if (key % 1000 == 0){
                std::cout << "[find " << key << " / ret = " << ret << " / size = " << val_size << " / val = ";
                for (int j = 0; j < val_size; ++j) {
                    std::cout << ret_val[j];
                }
                std::cout << "]\n";
            }
        }
    }
    puts(""); puts(""); puts("");

    remove("tmp.db");
}