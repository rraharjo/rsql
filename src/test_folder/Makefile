INCL = -I ../header
LIB = ../src/*.cpp
LEAK = leak_test
BNODETEST = bnode_test
BTREETEST = btree_test
TABLETEST = table_test
DBTEST = db_test
TIMETEST = time_test
DBFOLDER = /home/raharjo/project/database/rsql

buildleak:
	g++ -g $(LEAK).cpp -o $(LEAK).exe $(INCL) $(LIB)
buildbtree:
	g++ -g $(BTREETEST).cpp -o $(BTREETEST).exe $(INCL) $(LIB)
buildbnode:
	g++ -g $(BNODETEST).cpp -o $(BNODETEST).exe $(INCL) $(LIB)
buildtable:
	g++ -g $(TABLETEST).cpp -o $(TABLETEST).exe $(INCL) $(LIB)
builddb:
	g++ -g $(DBTEST).cpp -o $(DBTEST).exe $(INCL) $(LIB)
buildtime:
	g++ $(TIMETEST).cpp -o $(TIMETEST).exe $(INCL) $(LIB)
build:
	make buildbtree buildbnode buildtable builddb buildleak

cleancachefolder:
	rm -rf $(DBFOLDER)/*
cleanthiscache:
	rm *.rsql
cleanleak:
	rm $(LEAK).exe
cleanreadtree:
	rm $(LEAK).exe
cleanbtree:
	rm $(BTREETEST).exe
cleanbnode:
	rm $(BNODETEST).exe
cleantable:
	rm $(TABLETEST).exe
cleandb:
	rm $(DBTEST).exe
cleantime:
	rm $(TIMETEST).exe
clean:
	make cleanreadtree cleanbnode cleanbtree cleantable cleandb cleanthiscache cleancachefolder

runleak:
	valgrind ./$(LEAK).exe
runbtree:
	./$(BTREETEST).exe
runbnode:
	./$(BNODETEST).exe
runtable:
	./$(TABLETEST).exe
rundb:
	./$(DBTEST).exe
runtime:
	./$(TIMETEST).exe
run:
	make runbtree runbnode runtable rundb runleak
