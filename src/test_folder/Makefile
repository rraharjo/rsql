INCL = -I ../header
LIB = ../src/*.cpp
LEAK = leak_test
BNODETEST = bnode_test
BTREETEST = btree_test

buildleak:
	g++ -g $(LEAK).cpp -o $(LEAK).exe $(INCL) $(LIB)
buildbtree:
	g++ -g $(BTREETEST).cpp -o $(BTREETEST).exe $(INCL) $(LIB)
buildbnode:
	g++ -g $(BNODETEST).cpp -o $(BNODETEST).exe $(INCL) $(LIB)
build:
	make buildbtree buildbnode buildleak

cleandb:
	rm *.rsql
cleanleak:
	rm $(LEAK).exe
cleanreadtree:
	rm $(LEAK).exe
cleanbtree:
	rm $(BTREETEST).exe
cleanbnode:
	rm $(BNODETEST).exe
clean:
	make cleanreadtree cleanbnode cleanbtree cleandb

runleak:
	valgrind ./$(LEAK).exe
runbtree:
	./$(BTREETEST).exe
runbnode:
	./$(BNODETEST).exe
run:
	make runbtree runbnode runleak
