INC_DIR = header
SRC_DIR = src
BIN_DIR = bin
OBJ_DIR = build
TEST_DIR = ./test_folder
CACHE_DIR = ../rsql
CXX = g++
CXXFLAGS = -g -Wall -std=c++17 -I$(INC_DIR)

BNODETEST = bnode_test
BTREETEST = btree_test
TABLETEST = table_test
DBTEST = db_test
TIMETEST = time_test
LEAKTEST = leak_test
COMPTEST = comparisons_test
TARGET = $(BIN_DIR)/main
BNODETARGET = $(BIN_DIR)/$(BNODETEST)
BTREETARGET = $(BIN_DIR)/$(BTREETEST)
TABLETARGET = $(BIN_DIR)/$(TABLETEST)
DBTARGET = $(BIN_DIR)/$(DBTEST)
TIMETARGET = $(BIN_DIR)/$(TIMETEST)
LEAKTARGET = $(BIN_DIR)/$(LEAKTEST)
COMPTARGET = $(BIN_DIR)/$(COMPTEST)

SRCS = $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(SRCS:$(SRC_DIR)/%.cpp=$(OBJ_DIR)/%.o)


all: $(TARGET)
buildobj: $(OBJS)

buildnodetest: $(BNODETARGET)
runnodetest:
	$(BNODETARGET).exe

buildtreetest: $(BTREETARGET)
runtreetest: 
	$(BTREETARGET).exe

buildtabletest: $(TABLETARGET)
runtabletest: 
	$(TABLETARGET).exe

builddbtest: $(DBTARGET)
rundbtest: 
	$(DBTARGET).exe

buildcomptest: $(COMPTARGET)
runcomptest:
	$(COMPTARGET).exe

buildtimetest: $(TIMETARGET)
runtimetest: 
	time $(TIMETARGET).exe

buildleaktest: $(LEAKTARGET)
runleaktest:
	valgrind $(LEAKTARGET).exe

buildtest: buildnodetest buildtreetest buildtabletest builddbtest buildcomptest buildtimetest
runtest: runnodetest runtreetest runtabletest rundbtest runcomptest

# Rule to build the target
$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) main.cpp -o $@.exe $^

$(BNODETARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) $(TEST_DIR)/$(BNODETEST).cpp -o $@.exe $^

$(BTREETARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) $(TEST_DIR)/$(BTREETEST).cpp -o $@.exe $^

$(TABLETARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) $(TEST_DIR)/$(TABLETEST).cpp -o $@.exe $^

$(DBTARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) $(TEST_DIR)/$(DBTEST).cpp -o $@.exe $^

$(COMPTARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) $(TEST_DIR)/$(COMPTEST).cpp -o $@.exe $^

$(TIMETARGET): $(OBJS)
	$(CXX) -std=c++17 -I$(INC_DIR) $(TEST_DIR)/$(TIMETEST).cpp -o $@.exe $^

$(LEAKTARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) $(TEST_DIR)/$(LEAKTEST).cpp -o $@.exe $^

# Rule to build object files
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean rule
clean:
	rm -rf $(OBJ_DIR)/* $(BIN_DIR)/*

cleancache:
	rm ./*.rsql ; rm -rf $(CACHE_DIR)/*
