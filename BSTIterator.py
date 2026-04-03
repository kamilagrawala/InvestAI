class BSTIterator:

    def __init__(self, root):
        self.stack = []
        # Call your helper to "prime" the stack with the left-most path
        self._push_left(root)

    def _push_left(self, node):
        # Loop while node is not None:
        while node is not None:
            # Add node to stack
            self.stack.append(node)
            # Move to node.left
            node = node.left
        # Print after populating the stack
        print(f"stack {self.stack} len stack {len(self.stack)}\n")

    def hasNext(self):
        if len(self.stack) != 0:
            return True
        else:
            return False

    def next(self):
        