import traceback

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hbase import THBaseService
from hbase.ttypes import TPut, TColumnValue, TGet

transport = TTransport.TBufferedTransport(TSocket.TSocket('127.0.0.1', 40030))
protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
client = THBaseService.Client(protocol)

transport.open()

try:
    #tput = TPut(
    #    row=b'u003',
    #    columnValues=[
    #        TColumnValue(family=b'user', qualifier=b'age', value=b'16'),
    #    ]
    #)
    #client.put(b't_user', tput)
    
    tget = TGet(row=b'u001')
    tresult = client.get(b't_user', tget)
    #tresult = client.get('t-user', tget)
    for col in tresult.columnValues:
        print (col.qualifier, '=', col.value)
except Exception as e:
    traceback.print_exc()

transport.close()

#hbase(main):004:0> scan 't_user' 
#u001 column=role:rolename, timestamp=1553226766895, value=admin
#u001 column=user:username, timestamp=1553226764640, value=jevoncode
