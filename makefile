fptree: fptree.cpp
<<<<<<< HEAD
	g++ -std=c++11 -o fptree fptree.cpp

clean:
	rm -f *.o fptree
=======
	g++ -Wall -std=c++17 -lpmem -lpmemobj -o fptree fptree.cpp

clean:
	rm -f *.o fptree test_pool
>>>>>>> dram-fptree
