import functions

class Block:
    def __init__(self, operation=(None,None,None), hashpointer=None, nonce=""):
        self.operation = operation
        self.hashpointer = hashpointer
        self.nonce = nonce
        self.decided = False

    def __repr__(self):
        return f'[OP: {self.operation}, "HASHP: {self.hashpointer}", "NONCE: {self.nonce}", "DECIDED: {self.decided}]'

