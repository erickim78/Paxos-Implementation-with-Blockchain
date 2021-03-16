import block
import hashlib
import pickle
import uuid

tempblock = block.Block( ("get","asdfsad","adfsdafsad") )
print( hashlib.sha256( tempblock.toBytes() ).hexdigest() )
print(tempblock)
print( hashlib.sha256( pickle.dumps(tempblock.operation) + pickle.dumps(tempblock.nonce) ).hexdigest() )

exit()

temp = hashlib.sha256()
temp.update( pickle.dumps(tempblock.operation) )
temp.update( pickle.dumps(tempblock.hashpointer))
temp.update( pickle.dumps(tempblock.nonce) )
print( temp.hexdigest() )

u = uuid.uuid4()
print( u.hex )
print( pickle.dumps(u) )

temp = hashlib.sha256( u.hex )
print(temp)