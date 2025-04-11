Project: Database engine written in C++, with BTree as the building block. 
Structure:
1. each tree consists of multiple node, and tree information is stored inside tree.rsql file
2. tree.rsql format:
    the first 4 bytes = n : how many columns there are
    the next 12 * n bytes are the columns information
    the next 4 bytes is the root node number
    the next 4 bytes is the last node number
    the next 4 bytes is the last column id number
3. each tree has the table structure which consists of the columns. Columns information is the following:
    first 4 bytes is the column id
    next 4 bytes is the type
    next 4 bytes is the width
4. each node is stored in a single file with .rsql extension of format node_XX.rsql, where XX is the node number
    node_X.rsql format:
    the first 4 bytes = i : how many columns there are
    the next 12 * i bytes are the columns information
    
    the next 4 bytes = n : how many items are stored
    the next k * n bytes are the row data, k being the row size
    the next 4 * (n + 1) bytes are the children
    the last single byte determines if it's a leaf
5. new key inserted, if the new key equal to a key k where index of k is i, the new key is inserted to child with index i


Reference: Thomas H. Cormen, Charles E. Leiserson, Ronald L. Rivest, Clifford Stein Introduction to Algorithms, Third Edition  2009

TODO: reduce file I/O usage -> create a large buffer