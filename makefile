fptree: fptree.cpp
	g++ -Wall -std=c++17 -lpmem -lpmemobj -o fptree fptree.cpp -mavx512f -mavx512vl -mavx512bw -mavx512dq -mavx512cd -mrtm -ltbb

# fptree: fptree.cpp
# 	g++ -Wall -std=c++17 -lpmem -lpmemobj -o fptree fptree.cpp -mavx512f -mavx512vl -mavx512bw -mavx512dq -mavx512cd -mrtm -ltbb

inspector: inspector.cpp
	g++ -Wall -std=c++17 -lpmem -lpmemobj -o inspector inspector.cpp fptree.cpp -mavx512f -mavx512vl -mavx512bw -mavx512dq -mavx512cd -mrtm -ltbb -pthread

clean:
	rm -f *.o fptree inspector test_pool



