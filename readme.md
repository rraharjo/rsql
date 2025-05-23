Project: Database engine written in C++, with BTree as the building block. 
Structure:
1. each tree consists of multiple node, and tree information is stored inside tree.rsql file
2. tree.rsql format:
    the next 4 bytes            : the root node number
    the next 4 bytes            : the last node number
    the next 4 bytes            : the last column id number
    the next 4 bytes            : the tree_num
    the next 1 byte             : determine if the key is unique
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
    next m * (128 + 4 + 4) bytes    : column name and column index and tree number(n is the number of columns)
    next 32 bytes                   : next default key value
    next 4 bytes                    : primary tree num
    next 4 bytes                    : max tree num
    next 4 bytes                    : number of composite trees
    next n * 12 bytes               : composite trees information (col_idx(4), col_idx(4), tree_num(4))

Dependencies:
1. C++ Boost libraries: https://www.boost.org/

Reference: 
1. Thomas H. Cormen, Charles E. Leiserson, Ronald L. Rivest, Clifford Stein Introduction to Algorithms, Third Edition 2009


TODO: build composite tree
    PKEY is always in big endian format
    create a flag in tree class whether it's a composite tree
        modify compare_key function inside node.cpp to have additional endianness parameter