#include <stdint.h>
#ifndef ART_H
#define ART_H

#ifdef __cplusplus
extern "C" {
#endif

#define NODE4   1
#define NODE16  2
#define NODE48  3
#define NODE256 4

#define MAX_PREFIX_LEN 10

#if defined(__GNUC__) && !defined(__clang__)
# if __STDC_VERSION__ >= 199901L && 402 == (__GNUC__ * 100 + __GNUC_MINOR__)
#  define BROKEN_GCC_C99_INLINE
# endif
#endif

typedef int(*art_callback)(void *data, const unsigned char *key, uint32_t key_len, void *value);

typedef struct {
    uint32_t partial_len;
    uint8_t type;
    uint8_t num_children;
    unsigned char partial[MAX_PREFIX_LEN];
} art_node;

typedef struct {
    art_node n;
    unsigned char keys[4];
    art_node *children[4];
} art_node4;

typedef struct {
    art_node n;
    unsigned char keys[16];
    art_node *children[16];
} art_node16;

typedef struct {
    art_node n;
    unsigned char keys[256];
    art_node *children[48];
} art_node48;

typedef struct {
    art_node n;
    art_node *children[256];
} art_node256;

typedef struct {
    void *value;
    uint32_t key_len;
    unsigned char key[];
} art_leaf;

typedef struct {
    art_node *root;
    uint64_t size;
} art_tree;

int art_tree_init(art_tree *t);
int art_tree_destroy(art_tree *t);

#ifdef BROKEN_GCC_C99_INLINE
# define art_size(t) ((t)->size)
#else
inline uint64_t art_size(art_tree *t) {
    return t->size;
}
#endif

void* art_insert(art_tree *t, const unsigned char *key, int key_len, void *value);
void* art_insert_no_replace(art_tree *t, const unsigned char *key, int key_len, void *value);
void* art_delete(art_tree *t, const unsigned char *key, int key_len);
void* art_search(const art_tree *t, const unsigned char *key, int key_len);
art_leaf* art_minimum(art_tree *t);
art_leaf* art_maximum(art_tree *t);
int art_iter(art_tree *t, art_callback cb, void *data);
int art_iter_prefix(art_tree *t, const unsigned char *prefix, int prefix_len, art_callback cb, void *data);

#ifdef __cplusplus
}
#endif

#endif
