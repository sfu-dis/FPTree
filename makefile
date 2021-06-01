fptree: fptree.cpp
	g++ -std=c++17 -o fptree fptree.cpp -mavx512f -mavx512vl -mavx512bw -mavx512dq -mavx512cd

clean:
	rm -f *.o fptree
