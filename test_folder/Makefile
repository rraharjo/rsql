INCL = -I ../src/header
READTREE = read_tree

cleandb:
	rm *.rsql
readtree:
	g++ -g $(READTREE).cpp -o $(READTREE).exe $(INCL) ../src/src/*.cpp
cleanreadtree:
	rm $(READTREE).exe

clean:
	make cleanreadtree