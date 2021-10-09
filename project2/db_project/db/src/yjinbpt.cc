#include "yjinbpt.h"
#include <iostream>
int order = DEFAULT_ORDER;
std::queue<pagenum_t> queue;
#define INTERNAL_MAX_KEYS 248
bool verbose_output = false;
#define VERBOSE 0

// FUNCTION DEFINITIONS.

// 1. int64_t open_table (char *pathname);
// • Open existing data file using ‘pathname’ or create one if not existed.
// • If success, return the unique table id, which represents the own table in this database. Otherwise,
// return negative value.
table_t open_table(char* pathname) {
    return file_open_database_file(pathname);
}

//  2. int db_insert (int64_t table_id, int64_t key, char * value, uint16_t val_size);
// • Insert input ‘key/value’ (record) with its size to data file at the right place.
// • If success, return 0. Otherwise, return non-zero value.
int db_insert(table_t table_id, key__t key, char * value, u16_t val_size) {
    char tmpv[120];
    u16_t tmps;
    mleaf_t leaf;
    page_t page;

    if(VERBOSE) {
        print_tree(table_id);
        printf("\nwas tree\n\n");
    }
    if (db_find(table_id, key, tmpv, &tmps) == 0) { // find success -> db unchanged
        if (VERBOSE) printf("key %ld found,  %s\n", key, tmpv);
        return 1;                           // insert fail
    }
    if (VERBOSE)printf("key %ld not found\n", key);
    /* Create a new record for the
     * value.
     */
    memcpy(tmpv, value, val_size);
    tmpv[val_size] = 0;
    std::string svalue = tmpv;

    pagenum_t pn = get_root_page(table_id);

    /* Case: the tree does not exist yet.
     * Start a new tree.
     */

    if (pn == 0) {
        return start_new_tree(table_id, key, svalue);
    }

    /* Case: the tree already exists.
     * (Rest of function body.)
     */

    pagenum_t leaf_pn = find_leaf_page(table_id, key);
    file_read_page(table_id, leaf_pn, &page);
    leaf = page;

    if (leaf.free_space >= val_size + 12) {
        return insert_into_leaf(table_id, leaf_pn, leaf, key, svalue);
    }

    /* Case: leaf has room for key and pointer.
     */

    return insert_into_leaf_after_splitting(table_id, leaf_pn, leaf, key, svalue);
}

// 3. int db_find (int64_t table_id, int64_t key, char * ret_val, uint16_t * val_size);
// • Find the record containing input ‘key’.
// • If found matching ‘key’, store matched ‘value’ string in ret_val and matched ‘size’ in val_size.
// If success, return 0. Otherwise, return non-zero value.
// • The “caller” should allocate memory for a record structure (ret_val).
int db_find(table_t fd, key__t key, char* ret_val, u16_t* val_size) {
    int i = 0;
    page_t page;
    mleaf_t leaf;
    pagenum_t pn = find_leaf_page(fd, key);
    if (VERBOSE) std::cout << "db_find : find_leaf_page = " << pn << "\n";
    //Empty tree
    if (pn == 0) { // fail
        ret_val[0] = 0;
        val_size = 0;
        return 1;
    }
    file_read_page(fd, pn, &page);
    leaf = page;
    auto iter = std::lower_bound(leaf.slots.begin(), leaf.slots.end(), key);
    if (iter->key == key) { // success
        i = iter - leaf.slots.begin();
        memcpy(ret_val, leaf.values[i].c_str(), leaf.slots[i].size);
        ret_val[leaf.slots[i].size] = 0;
        *val_size = leaf.slots[i].size;
        return 0;
    }
    else { // fail
        ret_val[0] = 0;
        val_size = 0;
        return 1;
    }
}

// 4. int db_delete (int64_t table_id, int64_t key);
// • Find the matching record and delete it if found.
// • If success, return 0. Otherwise, return non-zero value.
int db_delete (table_t table_id, key__t key) {
    return 0;
}

// 5. int init_db ();
// • Initialize your database management system.
// • Initialize and allocate anything you need.
// • The total number of tables is less than 20.
// • If success, return 0. Otherwise, return non-zero value.
int init_db () {
    return 0;
}

// 6. int shutdown_db();
// • Shutdown your database management system.
// • Clean up everything.
// • If success, return 0. Otherwise, return non-zero value.
int shutdown_db() {
    return 0;
}

// OUTPUT AND UTILITIES


/* Prints the bottom row of keys
 * of the tree (with their respective
 * pointers, if the verbose_output flag is set.
 */
void print_leaves(table_t fd, mnode_t* root ) {
    int i;
    mnode_t c = *root;
    page_t cp;
    if (root == NULL) {
        printf("Empty tree.\n");
        return;
    }
    while (!c.is_leaf) {
        minternal_t leaf = cp;
        pagenum_t first_child = leaf.children[0];
        file_read_page(fd, first_child, &cp);
        c = cp;
    }
    mleaf_t leaf = cp;
    while (1) {
        for (i = 0; i < leaf.num_keys; i++) {
            if (VERBOSE)
                printf("%s ", leaf.values[i].c_str());
            printf("%ld ", leaf.slots[i].key);
        }
        if (VERBOSE)
            printf("%ld ", leaf.right_sibling);
        if (leaf.right_sibling != 0) {
            printf(" | ");
            file_read_page(fd, leaf.right_sibling, &cp);
            leaf = cp;
        }
        else
            break;
    }
    printf("\n");
}


/* Utility function to give the height
 * of the tree, which length in number of edges
 * of the path from the root to any leaf.
 */
int height(table_t fd, mnode_t* root) {
    int h = 0;
    if (root == NULL) return 0;
    if (root->is_leaf) return 1;
    minternal_t c = *(minternal_t*)root;
    page_t cp;
    while (!c.is_leaf) {
        pagenum_t first_child = c.children[0];
        file_read_page(fd, first_child, &cp);
        c = cp;
        h++;
    }
    return h;
}


/* Utility function to give the length in edges
 * of the path from any node to the root.
 */
int path_to_root(table_t fd, mnode_t& child) {
    int length = 0;
    mnode_t c = child;
    page_t cp;
    while(c.parent != 0) {
        file_read_page(fd, c.parent, &cp);
        c = cp; 
        length++;
    }
    return length;
}

pagenum_t get_root_page(table_t fd) {
    page_t page;
    file_read_page(fd, 0, &page);
    return ((pagenum_t*)(page.a))[16 / 8];
}

/* Prints the B+ tree in the command
 * line in level (rank) order, with the 
 * keys in each node and the '|' symbol
 * to separate nodes.
 * With the verbose_output flag set.
 * the values of the pointers corresponding
 * to the keys also appear next to their respective
 * keys, in hexadecimal notation.
 */
void print_tree(table_t fd) {
    pagenum_t pn = get_root_page(fd);
    page_t page;
    mnode_t node;
    mleaf_t leaf;
    minternal_t internal;
    int rank = 0;
    int new_rank = 0;
    if (pn == 0) {
        printf("Empty tree.\n");
        return;
    }
    queue = std::queue<pagenum_t>();
    queue.push(pn);
    while (!queue.empty()) {
        pn = queue.front();
        queue.pop();
        file_read_page(fd, pn, &page);
        internal = page;
        if (internal.parent != 0) {
            pagenum_t first_child = internal.children[0];
            if (pn == first_child) {
                new_rank = path_to_root(fd, internal);
                if (new_rank != rank) {
                    rank = new_rank;
                    printf("\n");
                }
            }
        }
        node = page;
        if (!node.is_leaf) {
            internal = page;
            queue.push(internal.first_child);
            for (int i = 0; i < internal.num_keys; ++i) {
                printf("%ld ", internal.keys[i]);
                queue.push(internal.children[i]);
            }
        }
        else {
            leaf = page;
            for (int i = 0; i < leaf.num_keys; ++i) {
                printf("%ld ", leaf.slots[i].key);
            }
        }
        printf("| ");
    }
    printf("\n");
}


/* Traces the path from the root to a leaf, searching
 * by key.  Displays information about the path
 * if the verbose flag is set.
 * Returns the leaf containing the given key.
 */
pagenum_t find_leaf_page(table_t fd, key__t key) {
    pagenum_t pn  = get_root_page(fd); // page_t ALLOCATED
    if (pn == 0) {
        if (VERBOSE) 
            printf("Empty tree.\n");
        return 0;
    }
    
    mnode_t c;
    minternal_t internal;
    mleaf_t leaf;
    page_t page;
    file_read_page(fd, pn, &page);
    c = page;
    while (!c.is_leaf) {
        internal = page;
        auto iter = std::upper_bound(internal.keys.begin(), internal.keys.end(), key);
        int i = iter - internal.keys.begin();
        if (i == 0) pn = internal.first_child;
        else pn = internal.children[i - 1];
        file_read_page(fd, pn, &page);
        c = page;
    }
    return pn;
}


/* Finds and returns the record to which
 * a key refers.
 */
bool cmp_slot(mslot_t a, mslot_t b) {
    return a.key < b.key;
}


/* Finds the appropriate place to
 * split a node that is too big into two.
 */
int cut(int length) {
    return (length + 1) / 2; // ceil(length / 2)
}

int cut_leaf(mleaf_t* leaf) {
    int i = 0;
    for (int sum = 0; i < leaf->num_keys; ++i) {
        sum += leaf->slots[i].size + 12;
        if (sum >= 1984) break;
    }
    return i;
}


// INSERTION


/* Helper function used in insert_into_parent
 * to find the index of the parent's pointer to 
 * the node to the left of the key to be inserted.
 */
int get_left_index(minternal_t internal /* parent */, pagenum_t left) {
    for (int i = 0; i < internal.num_keys; ++i) {
        if (internal.children[i] == left) return i;
    }
    return -1;
}

/* Inserts a new pointer to a record and its corresponding
 * key into a leaf.
 * Returns the altered leaf page number.
 */
pagenum_t insert_into_leaf(table_t fd, pagenum_t pn, mleaf_t& leaf, key__t key, std::string value) {

    int i, insertion_point;
    mslot_t tmpslot(key);
    auto iter = std::upper_bound(leaf.slots.begin(), leaf.slots.end(), tmpslot);
    insertion_point = iter - leaf.slots.begin(); // first key index to move
    leaf.slots.push_back(tmpslot);
    leaf.values.push_back(""); // make space
    for (i = leaf.num_keys; i > insertion_point; --i) {
        leaf.slots[i] = leaf.slots[i - 1];
        leaf.values[i] = leaf.values[i - 1];
        leaf.slots[i].offset -= value.size();
        leaf.values[i] = leaf.values[i - 1];
    }

    leaf.slots[insertion_point].key = key;
    leaf.slots[insertion_point].size = value.size();
    leaf.slots[insertion_point].offset = leaf.slots[insertion_point - 1].offset - value.size();
    leaf.values[insertion_point] = value;

    leaf.num_keys++;
    leaf.free_space -= value.size() + 12;

    page_t page = leaf;
    file_write_page(fd, pn, &page);
    return 0;
}


/* Inserts a new key and pointer
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
pagenum_t insert_into_leaf_after_splitting(table_t fd, pagenum_t pn, mleaf_t leaf, key__t key, std::string value) {
    pagenum_t new_pn = file_alloc_page(fd);
    int i, insertion_point;
    int split_point = cut_leaf(&leaf);
    mleaf_t new_leaf;
    new_leaf.parent = leaf.parent;
    key__t cmp_key = leaf.slots[split_point].key;
//  int offset_diff = PAGE_SIZE - leaf.slots[split_point - 1].size;
    for (int i = split_point; i < leaf.num_keys; ++i) {
        new_leaf.push_back(leaf.slots[i].key, leaf.values[i]);
    }
    for (int i = split_point; i < leaf.num_keys; ++i) {
        leaf.pop_back();
    }

    /* Case : insert into original leaf
    */
    if (key < cmp_key) {
        page_t page = new_leaf;
        file_write_page(fd, new_pn, &page);
        insert_into_leaf(fd, pn, leaf, key, value);
    }

    /* Case : insert into new leaf
     */
    else {
        page_t page = leaf;
        file_write_page(fd, pn, &page);
        insert_into_leaf(fd, new_pn, new_leaf, key, value);
    }

    return insert_into_parent(fd, pn, new_pn, new_leaf.slots[0].key, leaf.parent);
}

/* Inserts a new key and pointer to a node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
pagenum_t insert_into_node(table_t fd, pagenum_t pn, pagenum_t new_pn, 
        key__t key, pagenum_t parent_pn, int left_index) {
    page_t page;
    file_read_page(fd, parent_pn, &page);
    minternal_t parent = page;
    parent.keys.push_back(-1);
    parent.children.push_back(-1);
    for (int i = parent.num_keys; i > left_index + 1; --i) {
        parent.keys[i + 1] = parent.keys[i];
        parent.children[i + 1] = parent.children[i];
    }
    parent.keys[left_index + 1] = key;
    parent.children[left_index + 1] = new_pn;
    ++parent.num_keys;
    page = parent;
    file_write_page(fd, parent_pn, &page);
    return 0;
}

/* Inserts a new key and pointer to a node
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 */
pagenum_t insert_into_node_after_splitting(table_t fd, pagenum_t pn, pagenum_t new_pn, 
        key__t key, pagenum_t parent_pn, int left_index) {
    page_t page;
    file_read_page(fd, parent_pn, &page);
    minternal_t internal = page;
    
    /* First create a temporary set of keys and pointers
     * to hold everything in order, including
     * the new key and pointer, inserted in their
     * correct places. 
     * Then create a new node and copy half of the 
     * keys and pointers to the old node and
     * the other half to the new.
     */
    std::vector<key__t> temp_keys = internal.keys;
    std::vector<pagenum_t> temp_children = internal.children;
    temp_keys.push_back(-1);
    temp_children.push_back(-1);
    for (int i = internal.num_keys; i > left_index; --i) {
        temp_keys[i + 1] = temp_keys[i];
        temp_children[i + 1] = temp_children[i];
    }
    temp_keys[left_index + 1] = key;
    temp_children[left_index + 1] = new_pn;
        
    /* Create the new node and copy
     * half the keys and pointers to the
     * old and half to the new.
     */
    pagenum_t new_internal_pn = file_alloc_page(fd);
    key__t new_key = temp_keys[124];
    minternal_t new_internal, original_internal;
    new_internal.parent = internal.parent;
    new_internal.first_child = temp_children[124];
    new_internal.num_keys = 0;
    internal.num_keys = 0;
    internal.keys.clear();
    internal.children.clear();
    for (int i = 0; i < 124; ++i) {
        internal.push_back(temp_keys[i], temp_children[i]);
        new_internal.push_back(temp_keys[i + 125], temp_children[i + 125]);
    }

    page = internal;
    file_write_page(fd, parent_pn, &page);

    page = new_internal;
    file_write_page(fd, new_internal_pn, &page);

    /* Change the parent page number of children of new page
     */
    pagenum_t child_pn = new_internal.first_child;
    file_read_page(fd, child_pn, &page);
    
    ((pagenum_t*)(page.a))[0] = new_pn;
    file_write_page(fd, child_pn, &page);

    for (int i = 0; i < new_internal.num_keys; ++i) {
        child_pn = new_internal.children[i];
        file_read_page(fd, child_pn, &page);

        ((pagenum_t*)(page.a))[0] = new_pn;
        file_write_page(fd, child_pn, &page);
    }

    /* Insert a new key into the parent of the two
     * nodes resulting from the split, with
     * the old node to the left and the new to the right.
     */
    return insert_into_parent(fd, parent_pn, new_pn, new_key, internal.parent);
}

/* Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
pagenum_t insert_into_parent(table_t fd, pagenum_t pn, pagenum_t new_pn, key__t new_key, pagenum_t parent) {

    /* Case: new root. */

    if (parent == 0) {
        return insert_into_new_root(fd, pn, new_pn);
    }
    
    /* Case: leaf or node. (Remainder of
     * function body.)  
     */
    page_t page;
    file_read_page(fd, parent, &page);
    minternal_t internal;

    /* Find the parent's pointer to the left 
     * node.
     */
    int left_index = get_left_index(internal, pn);

    /* Simple case: the new key fits into the node. 
     */
    if (internal.num_keys < INTERNAL_MAX_KEYS) {
        return insert_into_node(fd, pn, new_pn, new_key, parent, left_index);
    }

    /* Harder case:  split a node in order 
     * to preserve the B+ tree properties.
     */
    return insert_into_node_after_splitting(fd, pn, new_pn, new_key, parent, left_index);
}

/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
pagenum_t insert_into_new_root(table_t fd, pagenum_t pn, pagenum_t new_pn) {
    page_t op, np;
    file_read_page(fd, pn, &op);
    file_write_page(fd, new_pn, &np);
    mnode_t on = op;
    pagenum_t root_pn = file_alloc_page(fd);
    page_t rp;
    minternal_t root;
    root.num_keys = 1;
    root.parent = 0;
    root.first_child = pn;
    root.children.push_back(new_pn);

    if (on.is_leaf) {
        mleaf_t ol = op, nl = np;
        root.keys.push_back(nl.slots[0].key);
        ol.parent = root_pn;
        nl.parent = root_pn;
        nl.right_sibling = ol.right_sibling;
        ol.right_sibling = new_pn;
        op = ol;
        np = nl;
    }
    else {
        minternal_t oi = op, ni = np;
        root.keys.push_back(ni.keys[0]);
        oi.parent = root_pn;
        ni.parent = root_pn;
        op = oi;
        np = ni;
    }
    rp = root;
    file_write_page(fd, pn, &op);
    file_write_page(fd, new_pn, &np);
    file_write_page(fd, root_pn, &rp);
    return 0;
}

/* First insertion:
 * start a new tree.
 */
pagenum_t start_new_tree(table_t fd, key__t key, std::string value) {
    pagenum_t pn = file_alloc_page(fd);
    mleaf_t leaf(key, value);
    page_t page = leaf;
    file_write_page(fd, pn, &page);     // root

    file_read_page(fd, 0, &page);
    ((pagenum_t*)page.a)[2] = pn;
    file_write_page(fd, 0, &page);      // header
    return 0;
}


#if 0



// DELETION.

/* Utility function for deletion.  Retrieves
 * the index of a node's nearest neighbor (sibling)
 * to the left if one exists.  If not (the node
 * is the leftmost child), returns -1 to signify
 * this special case.
 */
int get_neighbor_index( node * n ) {

    int i;

    /* Return the index of the key to the left
     * of the pointer in the parent pointing
     * to n.  
     * If n is the leftmost child, this means
     * return -1.
     */
    for (i = 0; i <= n->parent->num_keys; i++)
        if (n->parent->pointers[i] == n)
            return i - 1;

    // Error state.
    printf("Search for nonexistent pointer to node in parent.\n");
    printf("Node:  %#lx\n", (unsigned long)n);
    exit(EXIT_FAILURE);
}


node * remove_entry_from_node(node * n, int key, node * pointer) {

    int i, num_pointers;

    // Remove the key and shift other keys accordingly.
    i = 0;
    while (n->keys[i] != key)
        i++;
    for (++i; i < n->num_keys; i++)
        n->keys[i - 1] = n->keys[i];

    // Remove the pointer and shift other pointers accordingly.
    // First determine number of pointers.
    num_pointers = n->is_leaf ? n->num_keys : n->num_keys + 1;
    i = 0;
    while (n->pointers[i] != pointer)
        i++;
    for (++i; i < num_pointers; i++)
        n->pointers[i - 1] = n->pointers[i];


    // One key fewer.
    n->num_keys--;

    // Set the other pointers to NULL for tidiness.
    // A leaf uses the last pointer to point to the next leaf.
    if (n->is_leaf)
        for (i = n->num_keys; i < order - 1; i++)
            n->pointers[i] = NULL;
    else
        for (i = n->num_keys + 1; i < order; i++)
            n->pointers[i] = NULL;

    return n;
}


node * adjust_root(node * root) {

    node * new_root;

    /* Case: nonempty root.
     * Key and pointer have already been deleted,
     * so nothing to be done.
     */

    if (root->num_keys > 0)
        return root;

    /* Case: empty root. 
     */

    // If it has a child, promote 
    // the first (only) child
    // as the new root.

    if (!root->is_leaf) {
        new_root = (node *)root->pointers[0];
        new_root->parent = NULL;
    }

    // If it is a leaf (has no children),
    // then the whole tree is empty.

    else
        new_root = NULL;

    free(root->keys);
    free(root->pointers);
    free(root);

    return new_root;
}


/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
node * coalesce_nodes(node * root, node * n, node * neighbor, int neighbor_index, int k_prime) {

    int i, j, neighbor_insertion_index, n_end;
    node * tmp;

    /* Swap neighbor with node if node is on the
     * extreme left and neighbor is to its right.
     */

    if (neighbor_index == -1) {
        tmp = n;
        n = neighbor;
        neighbor = tmp;
    }

    /* Starting point in the neighbor for copying
     * keys and pointers from n.
     * Recall that n and neighbor have swapped places
     * in the special case of n being a leftmost child.
     */

    neighbor_insertion_index = neighbor->num_keys;

    /* Case:  nonleaf node.
     * Append k_prime and the following pointer.
     * Append all pointers and keys from the neighbor.
     */

    if (!n->is_leaf) {

        /* Append k_prime.
         */

        neighbor->keys[neighbor_insertion_index] = k_prime;
        neighbor->num_keys++;


        n_end = n->num_keys;

        for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
            neighbor->keys[i] = n->keys[j];
            neighbor->pointers[i] = n->pointers[j];
            neighbor->num_keys++;
            n->num_keys--;
        }

        /* The number of pointers is always
         * one more than the number of keys.
         */

        neighbor->pointers[i] = n->pointers[j];

        /* All children must now point up to the same parent.
         */

        for (i = 0; i < neighbor->num_keys + 1; i++) {
            tmp = (node *)neighbor->pointers[i];
            tmp->parent = neighbor;
        }
    }

    /* In a leaf, append the keys and pointers of
     * n to the neighbor.
     * Set the neighbor's last pointer to point to
     * what had been n's right neighbor.
     */

    else {
        for (i = neighbor_insertion_index, j = 0; j < n->num_keys; i++, j++) {
            neighbor->keys[i] = n->keys[j];
            neighbor->pointers[i] = n->pointers[j];
            neighbor->num_keys++;
        }
        neighbor->pointers[order - 1] = n->pointers[order - 1];
    }

    root = delete_entry(root, n->parent, k_prime, n);
    free(n->keys);
    free(n->pointers);
    free(n); 
    return root;
}


/* Redistributes entries between two nodes when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small node's entries without exceeding the
 * maximum
 */
node * redistribute_nodes(node * root, node * n, node * neighbor, int neighbor_index, 
        int k_prime_index, int k_prime) {  

    int i;
    node * tmp;

    /* Case: n has a neighbor to the left. 
     * Pull the neighbor's last key-pointer pair over
     * from the neighbor's right end to n's left end.
     */

    if (neighbor_index != -1) {
        if (!n->is_leaf)
            n->pointers[n->num_keys + 1] = n->pointers[n->num_keys];
        for (i = n->num_keys; i > 0; i--) {
            n->keys[i] = n->keys[i - 1];
            n->pointers[i] = n->pointers[i - 1];
        }
        if (!n->is_leaf) {
            n->pointers[0] = neighbor->pointers[neighbor->num_keys];
            tmp = (node *)n->pointers[0];
            tmp->parent = n;
            neighbor->pointers[neighbor->num_keys] = NULL;
            n->keys[0] = k_prime;
            n->parent->keys[k_prime_index] = neighbor->keys[neighbor->num_keys - 1];
        }
        else {
            n->pointers[0] = neighbor->pointers[neighbor->num_keys - 1];
            neighbor->pointers[neighbor->num_keys - 1] = NULL;
            n->keys[0] = neighbor->keys[neighbor->num_keys - 1];
            n->parent->keys[k_prime_index] = n->keys[0];
        }
    }

    /* Case: n is the leftmost child.
     * Take a key-pointer pair from the neighbor to the right.
     * Move the neighbor's leftmost key-pointer pair
     * to n's rightmost position.
     */

    else {  
        if (n->is_leaf) {
            n->keys[n->num_keys] = neighbor->keys[0];
            n->pointers[n->num_keys] = neighbor->pointers[0];
            n->parent->keys[k_prime_index] = neighbor->keys[1];
        }
        else {
            n->keys[n->num_keys] = k_prime;
            n->pointers[n->num_keys + 1] = neighbor->pointers[0];
            tmp = (node *)n->pointers[n->num_keys + 1];
            tmp->parent = n;
            n->parent->keys[k_prime_index] = neighbor->keys[0];
        }
        for (i = 0; i < neighbor->num_keys - 1; i++) {
            neighbor->keys[i] = neighbor->keys[i + 1];
            neighbor->pointers[i] = neighbor->pointers[i + 1];
        }
        if (!n->is_leaf)
            neighbor->pointers[i] = neighbor->pointers[i + 1];
    }

    /* n now has one more key and one more pointer;
     * the neighbor has one fewer of each.
     */

    n->num_keys++;
    neighbor->num_keys--;

    return root;
}


/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */
node * delete_entry( node * root, node * n, int key, void * pointer ) {

    int min_keys;
    node * neighbor;
    int neighbor_index;
    int k_prime_index, k_prime;
    int capacity;

    // Remove key and pointer from node.

    n = remove_entry_from_node(n, key, (node *)pointer);

    /* Case:  deletion from the root. 
     */

    if (n == root) 
        return adjust_root(root);


    /* Case:  deletion from a node below the root.
     * (Rest of function body.)
     */

    /* Determine minimum allowable size of node,
     * to be preserved after deletion.
     */

    min_keys = n->is_leaf ? cut(order - 1) : cut(order) - 1;

    /* Case:  node stays at or above minimum.
     * (The simple case.)
     */

    if (n->num_keys >= min_keys)
        return root;

    /* Case:  node falls below minimum.
     * Either coalescence or redistribution
     * is needed.
     */

    /* Find the appropriate neighbor node with which
     * to coalesce.
     * Also find the key (k_prime) in the parent
     * between the pointer to node n and the pointer
     * to the neighbor.
     */

    neighbor_index = get_neighbor_index( n );
    k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
    k_prime = n->parent->keys[k_prime_index];
    neighbor = neighbor_index == -1 ? (node *)n->parent->pointers[1] : 
        (node *)n->parent->pointers[neighbor_index];

    capacity = n->is_leaf ? order : order - 1;

    /* Coalescence. */

    if (neighbor->num_keys + n->num_keys < capacity)
        return coalesce_nodes(root, n, neighbor, neighbor_index, k_prime);

    /* Redistribution. */

    else
        return redistribute_nodes(root, n, neighbor, neighbor_index, k_prime_index, k_prime);
}



/* Master deletion function.
 */
node * db_delete(node * root, int key) {

    node * key_leaf;
    record * key_record;

    key_record = find(root, key, false);
    key_leaf = find_leaf(root, key, false);
    if (key_record != NULL && key_leaf != NULL) {
        root = delete_entry(root, key_leaf, key, key_record);
        free(key_record);
    }
    return root;
}


void destroy_tree_nodes(node * root) {
    int i;
    if (root->is_leaf)
        for (i = 0; i < root->num_keys; i++)
            free(root->pointers[i]);
    else
        for (i = 0; i < root->num_keys + 1; i++)
            destroy_tree_nodes((node *)root->pointers[i]);
    free(root->pointers);
    free(root->keys);
    free(root);
}


node * destroy_tree(node * root) {
    destroy_tree_nodes(root);
    return NULL;
}

#endif