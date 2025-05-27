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

Parser guide
1. keywords are case-insensitive, except for data type
2. list of suppported data type:
    UINT -> unsigned integer
    SINT -> signed integer
    CHAR -> array of character (string)
    DATE -> date
3. creating a database: 
    CREATE DATABASE $database_name
    Ex: CREATE DATABASE db_1
4. connect to a database:
    CONNECT DATABASE $database_name
    Ex: CONNECT DATABASE db_1
5. create a table (must be connected to a database to create a table):
    CREATE TABLE $table_name ($col_name_1 $data_type_1 $length_1) ($col_name_2 $data_type_2 $length_2)...
    Ex: CREATE TABLE table_1 (unsigned_int_col UINT 10) (date_col DATE)
    Note: when the data type is DATE, length should not be mentioned, it should just be ($col_name DATE)
6. insert row:
    INSERT INTO $table_name VALUES ($val_1, $val_2, ...), ($val_1, val_2, ...), ...
    Ex: INSERT INTO table_1 VALUES (10000, "2002-10-10"), (50000, 2002-10-10)
    Note: any leading/trailing space to the values will be removed. Use quotation marks would override this behavior.
7. search row:
    v1 = SELECT FROM $table_name
    v2 = SELECT FROM $table_name WHERE $primary_col $symbol $values AND ($col $symbol $values OR ($col $symbol $values AND $col $symbol %values))
    v3 = SELECT FROM $table_name WHERE ($col $symbol $values OR ($col $symbol $values AND $col $symbol %values))
    v4 = SELECT FROM $table_name WHERE $primary_col $symbol $values
    Note: the WHERE clause is optional - ignoring it would return everything on the table. The primary_col is also optional - this is the column used for indexed search - if this is not null, any additional condition must be connected with AND keyword. The additional conditions must be enclosed in parenthesis, and can be nested as many times as needed. In a single parenthesis, the connectors (AND/OR) must be the same - (a AND b OR c) is an invalid expression, but (a AND (b OR c)) is valid. 
8. delete row:
    DELETE FROM $table_name WHERE $primary_col $symbol $values AND ($col $symbol $values OR ($col $symbol $values AND $col $symbol %values))
    Note: The WHERE clause use the same rules as search
9. alter table add column
10. alter table remove column
11. alter table index column
12. exit: EXIT
    exit the program

Dependencies:
1. C++ Boost libraries: https://www.boost.org/

Reference: 
1. Thomas H. Cormen, Charles E. Leiserson, Ronald L. Rivest, Clifford Stein Introduction to Algorithms, Third Edition 2009