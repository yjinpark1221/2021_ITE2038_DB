#include <gtest/gtest.h>
#include "yjinbpt.h"
#include <string>

TEST(DeleteTest, freePageTest) {
#include <cassert>
#include <string>
#include <stdio.h>
#include <vector>
#include <set>



    puts("START");
    int n = 10000;
    table_t fd = file_open_database_file("deleteFreeTest.db");
    ASSERT_TRUE(fd > 0);

    std::set<key__t> inTree;

    for (key__t key = -n; key <= n; ++key) {
        char val[] = "01234567890123456789012345678901234567890123456789";
        int ret = db_insert(fd, key, val, 50);
        inTree.insert(key);
        ASSERT_FALSE(ret) << "[insert " << key << " / ret = " << ret << "]\n";
    }
    puts(""); puts(""); puts("");


    for (key__t key = -n; key <= n; ++key) {
        char ret_val[8000] = { 0, };
        u16_t val_size;

        int ret = db_find(fd, key, ret_val, &val_size);
        if (inTree.find(key) == inTree.end()) {
            ASSERT_TRUE(ret);
        }
        else {
            ASSERT_EQ(val_size, 50);
            ASSERT_FALSE(ret) << "[find " << key << " / ret = " << ret << " / size = " << val_size << "]\n";
        }
    }
    puts(""); puts(""); puts(""); 

    for (key__t key = -n; key <= n; ++key) {
        char val[] = "01234567890123456789012345678901234567890123456789";
        int ret = db_delete(fd, key);
        ASSERT_FALSE(ret) <<  "[delete " << key << " / ret = " << ret << "]\n";
    }
    puts(""); puts(""); puts(""); 


    for (key__t key = -n; key <= n; ++key) {
        char ret_val[8000] = { 0, };
        u16_t val_size;

        int ret = db_find(fd, i, ret_val, &val_size);
        ASSERT_FALSE(ret) << "[find " << i << " / ret = " << ret << " / val = " << ret_val << " / size = " << val_size << "\n";
        ASSERT_EQ(val_size, 50);
    }
    puts(""); puts(""); puts(""); puts(""); puts("");


    std::vector<pagenum_t> v = file_get_free_list(fd);
    for (pagenum_t i = 0; i < 2560; ++i) {
        ASSERT_TRUE(inVec(v, i));
    }

    remove("deleteFreeTest.db");
}

TEST(DeleteTest, randomNullTest) {
#include <string>
#include <cassert>
#include <string>
#include <stdio.h>
#include <vector>
#include <set>

    puts("START");
    bool inTree[1000001];
    key__t n = 10000;
    table_t fd = file_open_database_file("randomDelete.db");
    ASSERT_TRUE(fd > 0);
    std::vector<key__t> randomVec;
    for (key__t i = 0; i <= n; ++i) {
        randomVec.push_back(i);
        inTree[i]= 0;
    }

    //random insert
    random_shuffle(randomVec.begin(), randomVec.end());
    for (key__t i : randomVec) {
        char val[] = "a\0b\0\0c\0d\0e\0f\0a\0b\0\0c\0d\0e\0f\0a\0b\0\0c\0d\0e\0f\0a\0b\0\0c\0d\0e\0f\0a\0b\0\0c\0d\0e\0f\0";
        int ret = db_insert(fd, i, val, 50);
        inTree[i] = 1;
        ASSERT_FALSE(ret) << "[insert " << i << " / ret = " << ret << "]\n";
    }
    puts(""); puts(""); puts("");

    for (key__t key : randomVec) {
        char ret_val[8000] = { 0, };
        u16_t val_size;

        int ret = db_find(fd, key, ret_val, &val_size);
        if (!inTree[key]) {
            ASSERT_TRUE(ret);
        }
        else {
            ASSERT_EQ(val_size, 50);
            ASSERT_FALSE(ret) << "[find " << key << " / ret = " << ret << " / size = " << val_size << " / val = ";
        }
    }
    puts(""); puts(""); puts("");

    //random delete
    random_shuffle(randomVec.begin(), randomVec.end());
    for (int i = 0; i < n / 3; ++i) {
        key__t key = randomVec[i];
        int ret = db_delete(fd, key);
        inTree[key] = 0;
        ASSERT_FALSE(ret) << "[delete " << key << " / ret = " << ret << "]\n";
    }
    puts(""); puts(""); puts("");

    random_shuffle(randomVec.begin(), randomVec.end());
    for (key__t key : randomVec) {
        char ret_val[8000] = { 0, };
        u16_t val_size;

        int ret = db_find(fd, key, ret_val, &val_size);
        if (!inTree[key]) {
            ASSERT_TRUE(ret);
        }
        else {
            ASSERT_EQ(val_size, 50);
            ASSERT_FALSE(ret) << "[find " << key << " / ret = " << ret << " / size = " << val_size << " / val = ";
        }
    }
    puts(""); puts(""); puts("");

    remove("randomDelete.db");
}

TEST(DeleteTest, randomAllTest) {
    #include <set>
int main(){

    puts("START");
    bool inTree[10001];
    key__t n = 10000;
    table_t fd = file_open_database_file("randomAll.db");
    ASSERT_TRUE(fd > 0);
    std::vector<key__t> randomVec;
    for (key__t i = 0; i <= n; ++i) {
        randomVec.push_back(i);
        inTree[i]= 0;
    }
    random_shuffle(randomVec.begin(), randomVec.end());
    for (key__t i : randomVec) {
        char val[] = "a\0b\0\0c\0d\0e\0f\0a\0b\0\0c\0d\0e\0f\0a\0b\0\0c\0d\0e\0f\0a\0b\0\0c\0d\0e\0f\0a\0b\0\0c\0d\0e\0f\0";
        int ret = db_insert(fd, i, val, 50);
        inTree[i] = 1;
        ASSERT_FALSE(ret) << "[insert " << i << " / ret = " << ret << "]\n";
    }
    puts(""); puts(""); puts("");

    random_shuffle(randomVec.begin(), randomVec.end());
    for (key__t key : randomVec) {
        char ret_val[8000] = { 0, };
        u16_t val_size;

        int ret = db_find(fd, key, ret_val, &val_size);
        if (!inTree[key]) {
            ASSERT_TRUE(ret);
        }
        else {
            ASSERT_TRUE(val_size == 50);
            ASSERT_TRUE(ret == 0) << "[find " << key << " / ret = " << ret << " / size = " << val_size << " / val = ";
        }
    }
    puts(""); puts(""); puts("");

    random_shuffle(randomVec.begin(), randomVec.end());
    for (key__t key : randomVec) {
        int ret = db_delete(fd, key);
        inTree[key] = 0;
        ASSERT_TRUE(ret == 0) << "[delete " << key << " / ret = " << ret << "]\n";
    }
    puts(""); puts(""); puts("");

    random_shuffle(randomVec.begin(), randomVec.end());
    for (key__t key : randomVec) {
        char ret_val[8000] = { 0, };
        u16_t val_size;

        int ret = db_find(fd, key, ret_val, &val_size);
        if (!inTree[key]) {
            ASSERT_TRUE(ret);
        }
        else {
            ASSERT_TRUE(val_size == 50);
            ASSERT_TRUE(ret == 0) << "[find " << key << " / ret = " << ret << " / size = " << val_size << " / val = ";
        }
    }
    puts(""); puts(""); puts("");

    random_shuffle(randomVec.begin(), randomVec.end());
    for (key__t key : randomVec) {
        char val[] = "012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";
        int ret = db_insert(fd, key, val, 112);
        inTree[key] = 1;

        ASSERT_FALSE(ret) << "[insert " << key << " / ret = " << ret << "]\n";
    }
    puts(""); puts(""); puts("");


    random_shuffle(randomVec.begin(), randomVec.end());
    for (key__t key : randomVec) {
        char ret_val[8000] = { 0, };
        u16_t val_size;

        int ret = db_find(fd, key, ret_val, &val_size);
        if (!inTree[key]) {
            ASSERT_TRUE(ret);
        }
        else {
            ASSERT_TRUE(val_size == 50);
            ASSERT_TRUE(ret == 0) << "[find " << key << " / ret = " << ret << " / size = " << val_size << " / val = ";
        }
    }
    puts(""); puts(""); puts("");

    remove("randomAll.db");
}