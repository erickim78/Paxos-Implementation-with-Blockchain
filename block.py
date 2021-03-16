import functions
import pickle
import hashlib
import uuid

class Block:
    def __init__(self, operation=(None,None,None), hashpointer="", nonce=""):
        self.operation = operation
        self.hashpointer = hashpointer
        self.nonce = self.calculateNonce()
        self.decided = False

    def toBytes(self):
        rval = pickle.dumps( self.operation ) + pickle.dumps( self.hashpointer ) + pickle.dumps( self.nonce )
        return rval

    def calculateNonce(self):
        validNonce = ["0", "1", "2"]
        while True:
            rval = uuid.uuid4().hex
            if hashlib.sha256( pickle.dumps(self.operation) + pickle.dumps( rval ) ).hexdigest()[-1] in validNonce:
                return rval

        return

    def __repr__(self):
        return f'[OP: {self.operation}, "HASHP: {self.hashpointer}", "NONCE: {self.nonce}", "DECIDED: {self.decided}]'

