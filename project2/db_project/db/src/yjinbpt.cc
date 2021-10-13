// #include "../include/yjinbpt.h"
#include <cassert>
#include <iostream>

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
    char tmpv[123];
    u16_t tmps;
    mleaf_t leaf;
    page_t page;

    if (db_find(table_id, key, tmpv, &tmps) == 0) { // find success -> db unchanged
        return 1;                           // insert fail
    }
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
        return insert_into_leaf(table_id, leaf_pn, key, svalue);
    }
    /* Case: leaf has room for key and pointer.
     */
    return insert_into_leaf_after_splitting(table_id, leaf_pn, key, svalue);
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
    if (pn == 0) { // fail
        ret_val[0] = 0;
        *val_size = 0;
        return 1;
    }
    file_read_page(fd, pn, &page);
    leaf = page;
    auto iter = std::lower_bound(leaf.slots.begin(), leaf.slots.end(), key);
    if (iter != leaf.slots.end() && iter->key == key) { // success
        i = iter - leaf.slots.begin();
        memcpy(ret_val, leaf.values[i].c_str(), leaf.slots[i].size);
        ret_val[leaf.slots[i].size] = 0;
        *val_size = leaf.slots[i].size;
        return 0;
    }
    else { // fail
        ret_val[0] = 0;
        *val_size = 0;
        return 1;
    }
}


// 4. int db_delete (int64_t table_id, int64_t key);
// • Find the matching record and delete it if found.
// • If success, return 0. Otherwise, return non-zero value.
int db_delete (table_t table_id, key__t key) {
    char tmpv[123];
    u16_t tmps;
    pagenum_t leaf_pn = find_leaf_page(table_id, key);
    if (leaf_pn == 0 || db_find(table_id, key, tmpv, &tmps)) { // if the tree is empty or the key is nonexistent
        return 1; // deletion failure
    }
    return delete_entry(table_id, leaf_pn, key); // should be 0 (successful deletion)
}

// 5. int init_db ();
// • Initialize your database management system.
// • Initialize and allocate anything you need.
// • The total number of tables is less than 20.
// • If success, return 0. Otherwise, return non-zero value.
int init_db() {
    return 0;
}

// 6. int shutdown_db();
// • Shutdown your database management system.
// • Clean up everything.
// • If success, return 0. Otherwise, return non-zero value.
int shutdown_db() {
    file_close_database_file();
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
        // printf("Empty tree.\n");
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
            printf("%ld ", leaf.slots[i].key);
        }
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
    std::queue<pagenum_t> queue;
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
    queue.push(pn);
    while (!queue.empty()) {
        pn = queue.front();
        queue.pop();
        file_read_page(fd, pn, &page);
        node = page;
        pagenum_t parent_pn = node.parent;
        page_t parent_page;
        minternal_t parent;
        file_read_page(fd, parent_pn, &parent_page);
        parent = parent_page;

        if (pn == parent.first_child) {
            new_rank = path_to_root(fd, node);
            if (new_rank != rank) {
                rank = new_rank;
                printf("\n");
            }
        }
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
    pagenum_t pn = get_root_page(fd);
    if (pn == 0) {
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
    assert(i < leaf->num_keys);
    return i;
}

void print(mleaf_t& leaf) {
    // printf("printing leaf\n");
    // printf("parent %d\n", leaf.parent);
    // printf("numkeys %d\n", leaf.num_keys);
    // printf("free_space %d\n", leaf.free_space);
    // for (int i = 0; i < leaf.num_keys; ++i) {
    //     printf("slots[i] : %d %d %d\tvalues[i] : %s\n", leaf.slots[i].key, leaf.slots[i].size, leaf.slots[i].offset, leaf.values[i]);
    // }
}

void print(minternal_t& internal) {
    // printf("printing internal\n");
    // printf("parent %d\n", internal.parent);
    // printf("numkeys %d\n", internal.num_keys);
    // printf("first_child %d\n", internal.first_child);
    // for (int i = 0; i < internal.num_keys; ++i) {
    //     printf("keys[i] : %d \tchildren[i] : %d\n", internal.keys[i], internal.children[i]);
    // }

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
    assert(internal.first_child == left);
    return -1;
}

/* Inserts a new pointer to a record and its corresponding
 * key into a leaf.
 * Returns the altered leaf page number.
 */
pagenum_t insert_into_leaf(table_t fd, pagenum_t pn, key__t key, std::string value) {
    page_t leaf_page;
    file_read_page(fd, pn, &leaf_page);
    mleaf_t leaf = leaf_page;

    int i, insertion_point;
    mslot_t slot;
    slot.size = value.size();
    slot.key = key;
    auto iter = std::upper_bound(leaf.slots.begin(), leaf.slots.end(), slot);
    insertion_point = iter - leaf.slots.begin(); // first key index to move
    leaf.slots.insert(leaf.slots.begin() + insertion_point, slot);
    leaf.values.insert(leaf.values.begin() + insertion_point, value);
    leaf.num_keys++;
    adjust(leaf);
    leaf_page = leaf;
    file_write_page(fd, pn, &leaf_page);
    return 0;
}

int adjust(mleaf_t& leaf) {
    int offset = 4096;
    int used_space = 0;
    for (int i = 0; i < leaf.num_keys; ++i) {
        offset -= leaf.slots[i].size;
        used_space +=  leaf.slots[i].size + 12;
        leaf.slots[i].offset = offset;
    }
    leaf.free_space = 3968 - used_space;
    return used_space;
}

/* Inserts a new key and pointer
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
pagenum_t insert_into_leaf_after_splitting(table_t fd, pagenum_t pn, key__t key, std::string value) {
    pagenum_t new_pn = file_alloc_page(fd);
    mleaf_t leaf, new_leaf;
    page_t page;
    file_read_page(fd, pn, &page);
    leaf = page;

    new_leaf.right_sibling = leaf.right_sibling;
    leaf.right_sibling = new_pn;

    new_leaf.parent = leaf.parent;

    int split_point = cut_leaf(&leaf);
    key__t cmp_key = leaf.slots[split_point].key;
    new_leaf.is_leaf = 1;
    //split_point ~ num_keys - 1 -> num_keys  - split_point
    new_leaf.slots.reserve(leaf.num_keys - split_point);
    new_leaf.values.reserve(leaf.num_keys - split_point);
    new_leaf.slots.insert(new_leaf.slots.end(), leaf.slots.begin() + split_point, leaf.slots.end());
    new_leaf.values.insert(new_leaf.values.end(), leaf.values.begin() + split_point, leaf.values.end());
    new_leaf.num_keys = leaf.num_keys - split_point;

    leaf.num_keys = split_point;

// ---------------------------------------------------------------------------------- //
    leaf.slots.resize(split_point);
    leaf.values.resize(split_point);
// or //
    // leaf.slots.erase(leaf.slots.begin() + split_point, leaf.slots.end());
    // leaf.values.erase(leaf.values.begin() + split_point, leaf.values.end());
// ---------------------------------------------------------------------------------- //

    adjust(leaf);
    adjust(new_leaf);

    page = new_leaf;
    file_write_page(fd, new_pn, &page);

    page = leaf;
    file_write_page(fd, pn, &page);
    
    /* Case : insert into original leaf
    */
    if (key < cmp_key) {
        insert_into_leaf(fd, pn, key, value);
        file_read_page(fd, pn, &page);
        leaf = page;
    }
    /* Case : insert into new leaf
     */
    else {
        insert_into_leaf(fd, new_pn, key, value);
        file_read_page(fd, new_pn, &page);
        new_leaf = page;
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
    for (int i = parent.num_keys - 1; i > left_index; --i) {
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
    for (int i = internal.num_keys - 1; i > left_index; --i) {
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
    new_internal.is_leaf = 0;
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
    
    ((pagenum_t*)(page.a))[0] = new_internal_pn;
    file_write_page(fd, child_pn, &page);

    for (int i = 0; i < new_internal.num_keys; ++i) {
        child_pn = new_internal.children[i];
        file_read_page(fd, child_pn, &page);

        ((pagenum_t*)(page.a))[0] = new_internal_pn;
        file_write_page(fd, child_pn, &page);
    }

    /* Insert a new key into the parent of the two
     * nodes resulting from the split, with
     * the old node to the left and the new to the right.
     */
    return insert_into_parent(fd, parent_pn, new_internal_pn, new_key, internal.parent);
}

/* Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
pagenum_t insert_into_parent(table_t fd, pagenum_t pn, pagenum_t new_pn, key__t new_key, pagenum_t parent) {
    if (parent == 0) {
        return insert_into_new_root(fd, pn, new_pn, new_key);
    }
    /* Case: leaf or node. (Remainder of
     * function body.)  
     */
    page_t page;
    file_read_page(fd, parent, &page);
    minternal_t internal = page;

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
pagenum_t insert_into_new_root(table_t fd, pagenum_t pn, pagenum_t new_pn, key__t key) {
    page_t page, new_page;
    file_read_page(fd, pn, &page);
    file_read_page(fd, new_pn, &new_page);
    pagenum_t root_pn = file_alloc_page(fd);
    page_t root_page;
    minternal_t root;
    root.num_keys = 1;
    root.parent = 0;
    root.is_leaf = 0;

    root.first_child = pn;
    root.children.push_back(new_pn);
    mnode_t node = page;
    if (node.is_leaf) {
        mleaf_t leaf = page, new_leaf = new_page;
        root.keys.push_back(new_leaf.slots[0].key);
        leaf.parent = root_pn;
        new_leaf.parent = root_pn;
        new_leaf.right_sibling = leaf.right_sibling;
        leaf.right_sibling = new_pn;
        page = leaf;
        new_page = new_leaf;
    }
    else {
        minternal_t internal = page, new_internal = new_page;
        root.keys.push_back(key);
        internal.parent = root_pn;
        new_internal.parent = root_pn;
        page = internal;
        new_page = new_internal;
    }
    root_page = root;
    file_write_page(fd, pn, &page);
    file_write_page(fd, new_pn, &new_page);
    file_write_page(fd, root_pn, &root_page);
    page_t header_page;
    file_read_page(fd, 0, &header_page);
    ((pagenum_t*)header_page.a)[2] = root_pn;
    file_write_page(fd, 0, &header_page);      // header
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


// DELETION.

/* Utility function for deletion.  Retrieves
 * the index of a node's nearest neighbor (sibling)
 * to the left if one exists.  If not (the node
 * is the leftmost child), returns -1 to signify
 * this special case.
 */

// if pn is leftmost(first_child) -> -1
// else index
int get_index(table_t fd, pagenum_t pn) {
    /* Return the index of the key to the left
     * of the pointer in the parent pointing
     * to n.  
     * If n is the leftmost child, this means
     * return -1.
     */
    page_t page, parent_page;
    file_read_page(fd, pn, &page);
    mnode_t node;
    node = page;

    file_read_page(fd, node.parent, &parent_page);
    minternal_t internal;
    for (int i = 0; i <= internal.num_keys; ++i) {
        if (internal.children[i] == pn) {
            return i;
        }
    }
    if (internal.first_child == pn) {
        return -1;
    }
    perror("get_index failure");
    exit(0);
}


void remove_entry_from_node(table_t fd, pagenum_t pn, int index) {
    page_t page;
    file_read_page(fd, pn, &page);
    mnode_t node = page;
    if (node.is_leaf) {
        if (index < 0 || index >= node.num_keys) {
            perror("in remove_entry_from_node wrong leaf index");
            exit(0);
        }
        mleaf_t leaf = page;
        for (int i = index; i < leaf.num_keys - 1; ++i) {
            leaf.slots[i] = leaf.slots[i + 1];
            leaf.values[i] = leaf.values[i + 1];
        }
        leaf.slots.resize(leaf.num_keys - 1);
        leaf.values.resize(leaf.num_keys - 1);
        --leaf.num_keys;
        adjust(leaf); // adjust offset and free_space
        page = leaf;
        file_write_page(fd, pn, &page);
    }
    else {
        minternal_t internal = page;
        if (index < -1 || index > internal.num_keys) {
            perror("in remove_entry_from_node wrong internal index");
            exit(0);
        }
        if (index == -1) {
            internal.first_child = internal.children[0];
            ++index;
        }
        for (int i = index; i < internal.num_keys - 1; ++i) {
            internal.keys[i] = internal.keys[i + 1];
            internal.children[i] = internal.children[i + 1];
        }
        internal.keys.resize(internal.num_keys - 1);
        internal.children.resize(internal.num_keys - 1);
        --internal.num_keys;
        page = internal;
        file_write_page(fd, pn, &page);
    }
}


int adjust_root(table_t fd, pagenum_t pn) {
    page_t page;
    file_read_page(fd, pn, &page);
    mnode_t node;
    
    /* Case: nonempty root.
     * Key and pointer have already been deleted,
     * so nothing to be done.
     */
    if (node.num_keys > 0) {
        return 0;
    }

    /* Case: empty root. 
     */

// TODO : check file_free_page
    file_free_page(fd, pn);
    pagenum_t new_root_pn;

    // If it has a child, promote 
    // the first (only) child
    // as the new root.
    if (!node.is_leaf) {
        minternal_t internal = page;
        new_root_pn = internal.first_child;
        file_read_page(fd, new_root_pn, &page);
        ((pagenum_t*)page.a)[0] = 0;
        file_write_page(fd, new_root_pn, &page);
    }

    // If it is a leaf (has no children),
    // then the whole tree is empty.
    file_read_page(fd, 0, &page);
    ((pagenum_t*)page.a)[2] = new_root_pn;
    file_write_page(fd, 0, &page);

    return 0;
}

/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
int coalesce_nodes(table_t fd, pagenum_t pn, pagenum_t neighbor_pn, int index/*index of pn*/, int k_prime) {
    /* Swap neighbor with node if node is on the
     * extreme left and neighbor is to its right.
     * after this, neighbor is on the left of node
     */
    if (index == -1) {
        pagenum_t tmp = pn;
        pn = neighbor_pn;
        neighbor_pn = tmp;
    }

    page_t page, neighbor_page;
    file_read_page(fd, pn, &page);
    file_read_page(fd, pn, &neighbor_page);
    mnode_t node = page, neighbor = neighbor_page;

    /* Starting point in the neighbor for copying
     * keys and pointers from n.
     * Recall that n and neighbor have swapped places
     * in the special case of n being a leftmost child.
     */
    int insertion_point = neighbor.num_keys;

    /* Case:  nonleaf node.
     * Append k_prime and the following pointer.
     * Append all pointers and keys from the neighbor.
     */
    if (!node.is_leaf) {
        /* Append k_prime.
         */
        minternal_t internal = page, neighbor_internal = neighbor_page;
        neighbor_internal.keys.push_back(k_prime);
        neighbor_internal.children.push_back(internal.first_child);
        neighbor_internal.keys.insert(neighbor_internal.keys.end(), internal.keys.begin(), internal.keys.end());
        neighbor_internal.children.insert(neighbor_internal.children.end(), internal.children.begin(), internal.children.end());
        neighbor_page = neighbor_internal;
        file_write_page(fd, neighbor_pn, &neighbor_page);

        /* All children must now point up to the same parent.
         */
        for (int i = neighbor_internal.num_keys; i <= neighbor_internal.num_keys + internal.num_keys; ++i) {
            pagenum_t child_pn = neighbor_internal.children[i];
            page_t child_page;
            file_read_page(fd, child_pn, &child_page);
            ((pagenum_t*)child_page.a)[0] = neighbor_pn;
        }
    }

    /* In a leaf, append the keys and pointers of
     * n to the neighbor.
     * Set the neighbor's last pointer to point to
     * what had been n's right neighbor.
     */
    else {
        mleaf_t leaf = page, neighbor_leaf = neighbor_page;
        neighbor_leaf.right_sibling = leaf.right_sibling;
        for (int i = 0; i < leaf.num_keys; ++i) {
            neighbor_leaf.slots.push_back(leaf.slots[i]);
            neighbor_leaf.values.push_back(leaf.values[i]);
        }
        adjust(neighbor_leaf); // adjust offset and free_space
        neighbor_page = neighbor_leaf;
        file_write_page(fd, neighbor_pn, &neighbor_page);
    }
    file_free_page(fd, pn);
    return delete_entry(fd, neighbor.parent, k_prime);
}


/* Redistributes entries between two nodes when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small node's entries without exceeding the
 * maximum
 */
int redistribute_nodes(table_t fd, pagenum_t pn, pagenum_t neighbor_pn, int index, int k_prime_index, int k_prime) {
    /* Case: n is the leftmost child.
     * Take a key-pointer pair from the neighbor to the right.
     * Move the neighbor's leftmost key-pointer pair
     * to n's rightmost position.
     */
    page_t page, neighbor_page;
    file_read_page(fd, pn, &page);
    file_read_page(fd, neighbor_pn, &neighbor_page);
    mnode_t node = page;

    if (index == -1) {
        if (node.is_leaf) {
            mleaf_t leaf = page, neighbor_leaf = neighbor_page;
            leaf.slots.push_back(neighbor_leaf.slots[0]);
            leaf.values.push_back(neighbor_leaf.values[0]);
            neighbor_leaf.slots.erase(neighbor_leaf.slots.begin(), neighbor_leaf.slots.begin() + 1);
            neighbor_leaf.values.erase(neighbor_leaf.values.begin(), neighbor_leaf.values.begin() + 1);
            ++leaf.num_keys;
            --neighbor_leaf.num_keys;
            adjust(leaf);
            adjust(neighbor_leaf);
            
            page_t parent_page;
            file_read_page(fd, leaf.parent, &parent_page);
            minternal_t parent = parent_page;
            parent.keys[k_prime_index] = neighbor_leaf.slots[0].key;
            parent_page = parent;
            file_write_page(fd, leaf.parent, &parent_page);

            page = leaf;
            neighbor_page = neighbor_leaf;
        }
        else {
            minternal_t internal = page, neighbor_internal = neighbor_page;
            internal.keys.push_back(k_prime);
            internal.children.push_back(neighbor_internal.first_child);

            page_t parent_page;
            file_read_page(fd, internal.parent, &parent_page);
            minternal_t parent = parent_page;
            parent.keys[k_prime_index] = neighbor_internal.keys[0];
            parent_page = parent;
            file_write_page(fd, internal.parent, &parent_page);

            neighbor_internal.first_child = neighbor_internal.children[0];
            neighbor_internal.keys.erase(neighbor_internal.keys.begin(), neighbor_internal.keys.begin() + 1);
            neighbor_internal.children.erase(neighbor_internal.children.begin(), neighbor_internal.children.begin() + 1);
            --neighbor_internal.num_keys;
            ++internal.num_keys;

            pagenum_t child = internal.children[internal.num_keys - 1];
            page_t child_page;
            file_read_page(fd, child, &child_page);
            ((pagenum_t*)child_page.a)[0] = pn;
            file_write_page(fd, child, &child_page);
            
            page = internal;
            neighbor_page = neighbor_internal;
        }
    }

    /* Case: n has a neighbor to the left. 
     * Pull the neighbor's last key-pointer pair over
     * from the neighbor's right end to n's left end.
     */
    else {
        if (node.is_leaf) {
            mleaf_t leaf = page, neighbor_leaf = neighbor_page;
            leaf.slots.insert(leaf.slots.begin(), neighbor_leaf.slots[neighbor_leaf.num_keys - 1]);
            leaf.values.insert(leaf.values.begin(), neighbor_leaf.values[neighbor_leaf.num_keys - 1]);
            neighbor_leaf.slots.pop_back();
            neighbor_leaf.values.pop_back();
            ++leaf.num_keys;
            --neighbor_leaf.num_keys;
            adjust(leaf);
            adjust(neighbor_leaf);
            
            page_t parent_page;
            file_read_page(fd, leaf.parent, &parent_page);
            minternal_t parent = parent_page;
            parent.keys[k_prime_index] = leaf.slots[0].key;
            parent_page = parent;
            file_write_page(fd, leaf.parent, &parent_page);

            page = leaf;
            neighbor_page = neighbor_leaf;
        }
        else {
            minternal_t internal = page, neighbor_internal = neighbor_page;
            internal.keys.insert(internal.keys.begin(), k_prime);
            internal.children.insert(internal.children.begin(), internal.first_child);
            internal.first_child = neighbor_internal.children[neighbor_internal.num_keys - 1];

            page_t parent_page;
            file_read_page(fd, internal.parent, &parent_page);
            minternal_t parent = parent_page;
            parent.keys[k_prime_index] = neighbor_internal.keys[neighbor_internal.num_keys - 1];
            parent_page = parent;
            file_write_page(fd, internal.parent, &parent_page);

            neighbor_internal.keys.pop_back();
            neighbor_internal.children.pop_back();
            --neighbor_internal.num_keys;
            ++internal.num_keys;

            pagenum_t child = internal.first_child;
            page_t child_page;
            file_read_page(fd, child, &child_page);
            ((pagenum_t*)child_page.a)[0] = pn;
            file_write_page(fd, child, &child_page);

            page = internal;
            neighbor_page = neighbor_internal;
        }
    }
    file_write_page(fd, pn, &page);
    file_write_page(fd, pn, &neighbor_page);
    return 0;
}

/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */

int delete_entry(table_t fd, pagenum_t pn, key__t key) {
    // Remove key and pointer from node.
    remove_entry_from_node(fd, pn, key);

    /* Case:  deletion from the root. 
     */
    if (pn == get_root_page(fd)) {
        return adjust_root(fd, pn);
    }

    /* Case:  deletion from a node below the root.
     * (Rest of function body.)
     */

    /* Determine minimum allowable size of node,
     * to be preserved after deletion.
     */
     
     page_t page;
     file_read_page(fd, pn, &page);
     mnode_t node = page;

    if (node.is_leaf) {
        mleaf_t leaf = page;

        /* Case:  node stays at or above minimum.
        * (The simple case.)
        */
        if (leaf.free_space < LEAF_THRESHOLD) {
            return 0;
        }
    }
    else {
        mleaf_t leaf = page;

        /* Case:  node stays at or above minimum.
        * (The simple case.)
        */
        if (leaf.num_keys >= 124) {
            return 0;
        }
    }

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
    int index = get_index(fd, pn), neighbor_index, k_prime_index;
    if (index == -1) { // leftmost -> right neighbor
        neighbor_index = 0;
        k_prime_index = 0;
    }
    else { // left neighbor
        neighbor_index = index - 1;
        k_prime_index = index;
    }

    page_t parent_page;
    file_read_page(fd, node.parent, &parent_page);
    minternal_t parent = parent_page;
    pagenum_t neighbor_pn;
    key__t k_prime = parent.keys[k_prime_index];

    if (neighbor_index == -1) {
        neighbor_pn = parent.first_child;
    }
    else {
        neighbor_pn = parent.children[neighbor_index];
    }

    page_t neighbor_page;
    file_read_page(fd, neighbor_pn, &neighbor_page);
    mnode_t neighbor = neighbor_page;

    // leaf
    if (node.is_leaf) {
        mleaf_t neighbor_leaf = neighbor_page;
        mleaf_t leaf = page;

        /* Coalescence. */
        if (neighbor_leaf.free_space >= 3968 - leaf.free_space) {
            return coalesce_nodes(fd, pn, neighbor_pn, index, k_prime);
        }

        /* Redistribution. */
        else {
            return redistribute_nodes(fd, pn, neighbor_pn, neighbor_index, k_prime_index, k_prime);
        }
    }

    // internal
    /* Coalescence. */
    if (neighbor.num_keys + node.num_keys + 2 <= 249) {
        return coalesce_nodes(fd, pn, neighbor_pn, index, k_prime);
    }

    /* Redistribution. */
    else {
        return redistribute_nodes(fd, pn, neighbor_pn, neighbor_index, k_prime_index, k_prime);
    }
}


// void destroy_tree_nodes(node * root) {
//     int i;
//     if (root->is_leaf)
//         for (i = 0; i < root->num_keys; i++)
//             free(root->pointers[i]);
//     else
//         for (i = 0; i < root->num_keys + 1; i++)
//             destroy_tree_nodes((node *)root->pointers[i]);
//     free(root->pointers);
//     free(root->keys);
//     free(root);
// }


// node * destroy_tree(node * root) {
//     destroy_tree_nodes(root);
//     return NULL;
// }
