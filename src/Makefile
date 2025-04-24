INC_DIR = ./header
SRC_DIR = ./src
BIN_DIR = ./bin
OBJ_DIR = ./build
CXX = g++
CXXFLAGS = -g -Wall -std=c++17 -I$(INC_DIR)
TARGET = $(BIN_DIR)/main

SRCS = $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(SRCS:$(SRC_DIR)/%.cpp=$(OBJ_DIR)/%.o)

all: $(TARGET)


# Rule to build the target
$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) main.cpp -o $@ $^

# Rule to build object files
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean rule
clean:
	rm -rf $(OBJ_DIR)/* $(BIN_DIR)/*
