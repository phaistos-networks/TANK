all:  libsnappy.a

libsnappy.a: snappy.o snappy-sinksource.o snappy-stubs-internal.o
	ar rc libsnappy.a snappy.o snappy-sinksource.o snappy-stubs-internal.o


.o : .cpp

clean:
	rm -f *.o

.PHONY: clean
	



	
