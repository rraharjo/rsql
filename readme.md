Project: Database engine written in C++, with BTree as the building block. 
Structure:
1. each tree consists of multiple node, and tree information is stored inside tree.rsql file
2. tree.rsql format:
    the next 4 bytes            : the root node number
    the next 4 bytes            : the last node number
    the next 4 bytes            : the last column id number
    the next 4 bytes            : the tree_num
    the first 4 bytes = n       : how many columns there are
    the next 12 * n             : bytes are the columns information
3. each tree has the table structure which consists of the columns. Columns information is the following:
    first 4 bytes               : the column id
    next 4 bytes                : the type
    next 4 bytes                : the width
4. each node is stored in a single file with .rsql extension of format node_XX.rsql, where XX is the node number
    node_XX.rsql format:
    the first 4 bytes = i       : how many columns there are
    the next 12 * i bytes       : are the columns information
    
    the next 4 bytes = n        : how many items are stored
    the next k * n bytes        : the row data, k being the row size, which can be obtained from the columns information
    the next 4 * (n + 1) bytes  : the children node number.
    the last single byte        : determines whether the node is a leaf
5. new key inserted, if the new key equal to a key k where index of k is i, the new key is inserted to child with index i
6. table.rsql format
    first 256 bytes                 : table name
    next 4 bytes                    : number of column
    next n * (128 + 4 + 4) bytes    : column name and column index and tree number(n is the number of columns)
    next 4 bytes                    : primary tree num
    next 4 bytes                    : max tree num

Dependencies:
1. C++ Boost libraries: https://www.boost.org/

Reference: 
1. Thomas H. Cormen, Charles E. Leiserson, Ronald L. Rivest, Clifford Stein Introduction to Algorithms, Third Edition 2009

TODO:
    test optional index tree
    create a static function for tree: create_new_tree and load_existing_tree -> make constructor private
    modify constructor for table to receive columns, add new method to set primary key