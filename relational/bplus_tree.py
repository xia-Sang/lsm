from typing import Any, List, Optional, Tuple

class BPlusNode:
    def __init__(self, is_leaf: bool = False, order: int = 4):
        self.is_leaf = is_leaf
        self.keys: List[Any] = []
        self.children: List[BPlusNode] = []
        self.next: Optional[BPlusNode] = None
        self.order = order

    def is_full(self) -> bool:
        return len(self.keys) >= self.order - 1

class BPlusTree:
    def __init__(self, order: int = 4):
        self.root = BPlusNode(is_leaf=True, order=order)
        self.order = order

    def insert(self, key: Any, value: Any):
        # If root is full, create new root
        if self.root.is_full():
            new_root = BPlusNode(is_leaf=False, order=self.order)
            new_root.children.append(self.root)
            self._split_child(new_root, 0)
            self.root = new_root

        self._insert_non_full(self.root, key, value)

    def _split_child(self, parent: BPlusNode, child_index: int):
        order = self.order
        child = parent.children[child_index]
        new_node = BPlusNode(is_leaf=child.is_leaf, order=order)

        # Handle leaf node split
        if child.is_leaf:
            mid = (order - 1) // 2
            new_node.keys = child.keys[mid:]
            child.keys = child.keys[:mid]
            
            # Update leaf node links
            new_node.next = child.next
            child.next = new_node
            
            # Insert the first key of new node into parent
            parent.keys.insert(child_index, new_node.keys[0])
            parent.children.insert(child_index + 1, new_node)
        else:
            # Handle internal node split
            mid = (order - 1) // 2
            new_node.keys = child.keys[mid + 1:]
            new_node.children = child.children[mid + 1:]
            
            parent.keys.insert(child_index, child.keys[mid])
            parent.children.insert(child_index + 1, new_node)
            
            child.keys = child.keys[:mid]
            child.children = child.children[:mid + 1]

    def _insert_non_full(self, node: BPlusNode, key: Any, value: Any):
        i = len(node.keys) - 1

        if node.is_leaf:
            while i >= 0 and key < node.keys[i]:
                i -= 1
            node.keys.insert(i + 1, (key, value))
        else:
            while i >= 0 and key < node.keys[i]:
                i -= 1
            i += 1
            
            if node.children[i].is_full():
                self._split_child(node, i)
                if key > node.keys[i]:
                    i += 1
                    
            self._insert_non_full(node.children[i], key, value)

    def search(self, key: Any) -> Optional[Any]:
        return self._search(self.root, key)

    def _search(self, node: BPlusNode, key: Any) -> Optional[Any]:
        i = 0
        while i < len(node.keys) and key > node.keys[i]:
            i += 1

        if node.is_leaf:
            if i < len(node.keys) and node.keys[i][0] == key:
                return node.keys[i][1]
            return None
        
        if i < len(node.keys) and key == node.keys[i]:
            i += 1
        return self._search(node.children[i], key)

    def range_search(self, start_key: Any, end_key: Any) -> List[Tuple[Any, Any]]:
        """Search for all key-value pairs within the given range."""
        result = []
        node = self._find_leaf(self.root, start_key)
        
        while node:
            for key, value in node.keys:
                if start_key <= key <= end_key:
                    result.append((key, value))
                elif key > end_key:
                    return result
            node = node.next
        return result

    def _find_leaf(self, node: BPlusNode, key: Any) -> BPlusNode:
        if node.is_leaf:
            return node
            
        i = 0
        while i < len(node.keys) and key >= node.keys[i]:
            i += 1
        return self._find_leaf(node.children[i], key)
