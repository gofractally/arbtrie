# Examples

Complete, runnable examples are in the [`examples/`](https://github.com/gofractally/arbtrie/tree/main/examples) directory. Build them with:

```bash
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -B build/release
cmake --build build/release --target example-basic_crud example-cursor_iteration \
    example-range_operations example-snapshots example-subtrees
```

---

## Basic CRUD

Create a database, insert/get/update/remove keys using transactions.

```cpp title="examples/basic_crud.cpp"
--8<-- "examples/basic_crud.cpp"
```

**Expected output:**
```
alice = engineer
dave not found
insert carol again: no
alice = senior engineer
bob   = (gone)
carol = manager
```

---

## Cursor Iteration

Iterate keys forward, backward, and by prefix using read-only cursors.

```cpp title="examples/cursor_iteration.cpp"
--8<-- "examples/cursor_iteration.cpp"
```

**Expected output:**
```
=== All keys (forward) ===
  fruit/apple = red
  fruit/banana = yellow
  fruit/cherry = red
  fruit/date = brown
  veggie/carrot = orange
  veggie/pea = green

=== All keys (reverse) ===
  veggie/pea
  veggie/carrot
  fruit/date
  fruit/cherry
  fruit/banana
  fruit/apple

=== Keys with prefix 'fruit/' ===
  fruit/apple
  fruit/banana
  fruit/cherry
  fruit/date

=== lower_bound('fruit/c') ===
  first key >= 'fruit/c': fruit/cherry
```

---

## Range Operations

O(log n) range counting and range deletion — no leaf scanning required.

```cpp title="examples/range_operations.cpp"
--8<-- "examples/range_operations.cpp"
```

**Expected output:**
```
Total keys:              10000
Keys in [01000, 02000):  1000
Removed in [05000, 06000): 1000
Total after removal:     9000
```

---

## Snapshots

Zero-cost snapshots via copy-on-write — O(1) to create, readers see a consistent point-in-time view while writers proceed independently.

```cpp title="examples/snapshots.cpp"
--8<-- "examples/snapshots.cpp"
```

**Expected output:**
```
=== Snapshot (before mutation) ===
  balance/alice = 1000
  balance/bob = 500

=== Current state (after mutation) ===
  balance/alice = 800
  balance/bob = 700
  balance/carol = 300
```

---

## Subtrees

Store entire trees as values — composable, hierarchical data with O(1) subtree operations.

```cpp title="examples/subtrees.cpp"
--8<-- "examples/subtrees.cpp"
```

**Expected output:**
```
=== metadata subtree ===
  created_by = admin
  engine = psitri
  schema_version = 3

=== top-level keys ===
  metadata = [subtree]
  users/alice = engineer
  users/bob = designer
```
