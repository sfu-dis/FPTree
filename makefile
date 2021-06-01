fptree: fptree.cpp
	g++ -Wall -std=c++17 -lpmem -lpmemobj -o fptree fptree.cpp

clean:
	rm -f *.o fptree test_pool