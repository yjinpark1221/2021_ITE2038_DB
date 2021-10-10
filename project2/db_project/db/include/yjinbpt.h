#ifndef __YJINBPT_H__
#define __YJINBPT_H__

// Uncomment the line below if you are compiling on Windows.
// #define WINDOWS
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <algorithm>
#include <queue>
#include "file.h"
#ifdef WINDOWS
#define bool char
#define false 0
#define true 1
#endif

// TYPES.

/* Type representing the record
 * to which a given key refers.
 * In a real B+ tree system, the
 * record would hold data (in a database)
 * or a file (in an operating system)
 * or some other information.
 * Users can rewrite this part of the code
 * to change the type and content
 * of the value field.
 */

/* Type representing a node in the B+ tree.
 * This type is general enough to serve for both
 * the leaf and the internal node.
 * The heart of the node is the array
 * of keys and the array of corresponding
 * pointers.  The relation between keys
 * and pointers differs between leaves and
 * internal nodes.  In a leaf, the index
 * of each key equals the index of its corresponding
 * pointer, with a maximum of order - 1 key-pointer
 * pairs.  The last pointer points to the
 * leaf to the right (or NULL in the case
 * of the rightmost leaf).
 * In an internal node, the first pointer
 * refers to lower nodes with keys less than
 * the smallest key in the keys array.  Then,
 * with indices i starting at 0, the pointer
 * at i + 1 points to the subtree with keys
 * greater than or equal to the key in this
 * node at index i.
 * The num_keys field is used to keep
 * track of the number of valid keys.
 * In an internal node, the number of valid
 * pointers is always num_keys + 1.
 * In a leaf, the number of valid pointers
 * to data is always num_keys.  The
 * last leaf pointer points to the next leaf.
 */

void print_leaves(table_t fd, mnode_t* root );
int height(table_t fd, mnode_t* root);
int path_to_root(table_t fd, mnode_t& child);
pagenum_t get_root_page(table_t fd);
void print_tree(table_t fd);
pagenum_t find_leaf_page(table_t fd, key__t key);
bool cmp_slot(mslot_t a, mslot_t b);
int db_find(table_t fd, key__t key, char* ret_val, u16_t* val_size);


int cut(int length);
int cut_leaf(mleaf_t* leaf);
int get_left_index(minternal_t internal /* parent */, pagenum_t left);
int adjust(mleaf_t& leaf);
pagenum_t insert_into_leaf(table_t fd, pagenum_t pn, key__t key, std::string value);
pagenum_t insert_into_leaf_after_splitting(table_t fd, pagenum_t pn, key__t key, std::string value);
pagenum_t insert_into_node(table_t fd, pagenum_t pn, pagenum_t new_pn, 
        key__t key, pagenum_t parent_pn, int left_index);
pagenum_t insert_into_node_after_splitting(table_t fd, pagenum_t pn, pagenum_t new_pn, 
        key__t key, pagenum_t parent_pn, int left_index);
pagenum_t insert_into_parent(table_t fd, pagenum_t pn, pagenum_t new_pn, key__t new_key, pagenum_t parent);
pagenum_t insert_into_new_root(table_t fd, pagenum_t pn, pagenum_t new_pn, key__t key);
pagenum_t start_new_tree(table_t fd, key__t key, std::string value);
int db_insert(table_t table_id, key__t key, char * value, u16_t val_size);


int64_t open_table (char *pathname);
int db_delete (table_t table_id, key__t key);
int init_db ();
int shutdown_db();
// void enqueue( node * new_node );
// node * dequeue( void );
// int height( node * root );
// int path_to_root( node * root, node * child );
// void print_leaves( node * root );
// void print_tree( node * root );
// void find_and_print(node * root, int key, bool verbose); 
// void find_and_print_range(node * root, int range1, int range2, bool verbose); 
// int find_range( node * root, int key_start, int key_end, bool verbose,
//         int returned_keys[], void * returned_pointers[]); 
// node * find_leaf( node * root, int key, bool verbose );
// record * find( node * root, int key, bool verbose );
// int cut( int length );

// // Insertion.

// record * make_record(int value);
// node * make_node( void );
// node * make_leaf( void );
// int get_left_index(node * parent, node * left);
// node * insert_into_leaf( node * leaf, int key, record * pointer );
// node * insert_into_leaf_after_splitting(node * root, node * leaf, int key,
//                                         record * pointer);
// node * insert_into_node(node * root, node * parent, 
//         int left_index, int key, node * right);
// node * insert_into_node_after_splitting(node * root, node * parent,
//                                         int left_index,
//         int key, node * right);
// node * insert_into_parent(node * root, node * left, int key, node * right);
// node * insert_into_new_root(node * left, int key, node * right);
// node * start_new_tree(int key, record * pointer);
// node * insert( node * root, int key, int value );

// // Deletion.

// int get_neighbor_index( node * n );
// node * adjust_root(node * root);
// node * coalesce_nodes(node * root, node * n, node * neighbor,
//                       int neighbor_index, int k_prime);
// node * redistribute_nodes(node * root, node * n, node * neighbor,
//                           int neighbor_index,
//         int k_prime_index, int k_prime);
// node * delete_entry( node * root, node * n, int key, void * pointer );
// node * db_delete( node * root, int key );

// void destroy_tree_nodes(node * root);
// node * destroy_tree(node * root);

#endif /* __YJINBPT_H__*/
