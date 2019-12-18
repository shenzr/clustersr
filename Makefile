CC = g++ -std=c++11
CLIBS = -pthread
CFLAGS = -g -Wall -O2 -lm -lrt
OBJ_DIR = obj
UTIL_DIR = Util
Jerasure_Dir = Jerasure
JERASURE_LIB = Jerasure/jerasure.o Jerasure/galois.o Jerasure/reed_sol.o Jerasure/cauchy.o 
all: jerasure tinyxml2.o Config.o Coordinator.o PeerNode.o ClusterSRPeerNode ClusterSRCoordinator

jerasure: 
	cd Jerasure/ && make && cd ..

tinyxml2.o: Util/tinyxml2.cpp Util/tinyxml2.h
	$(CC) $(CFLAGS) -c $<  

Config.o: Config.cc tinyxml2.o
	$(CC) $(CFLAGS) -c $<

Socket.o: Socket.cc 
	$(CC) $(CFLAGS) -c $<

Coordinator.o: Coordinator.cc Config.o Socket.o
	$(CC) $(CFLAGS) -c $< 

PeerNode.o: PeerNode.cc Socket.o
	$(CC) $(CFLAGS) -c $< 

ClusterSRPeerNode: ClusterSRPeerNode.cc PeerNode.o Config.o Socket.o tinyxml2.o
	$(CC) $(CFLAGS) -o $@ $^ $(CLIBS) $(JERASURE_LIB)

ClusterSRCoordinator: ClusterSRCoordinator.cc Coordinator.o Config.o Socket.o tinyxml2.o
	$(CC) $(CFLAGS) -o $@ $^ $(CLIBS) $(JERASURE_LIB)

clean:
	rm ClusterSRCoordinator ClusterSRPeerNode *.o Jerasure/*.o
