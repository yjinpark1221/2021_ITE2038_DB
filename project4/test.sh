rm -rf build
cmake -S . -B build
cmake --build build
cd build/bin
./test_lock_table
cd ../..