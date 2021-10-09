
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
import pymodbus
from pymodbus.pdu import ModbusRequest
from pymodbus.transaction import ModbusRtuFramer


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


client = ModbusClient(method='rtu', port="COM9",stopbits = 1, timeout=3, bytesize = 8, parity = 'E', baudrate= 19200)
connection = client.connect()
print(connection)
rq = client.read_coils(20480,64,unit=0x01) #start address of input coil for PLC
rq = rq.bits
rr = [int(i) for i in rq]
rt = list(chunks(rr,8))
print(rt)



rq = client.read_coils(20736,64,unit=0x01) #start address of input coil for PLC
rq = rq.bits
rr = [int(i) for i in rq]
rt = list(chunks(rr,8))
print(rt)











rq = client.read_coils(24576,64,unit=0x01) #start address of output coil for PLC 
rq = rq.bits
rr = [int(i) for i in rq]
rt = list(chunks(rr,8))
print(rt)
client.close()

rq = client.read_coils(24832,64,unit=0x01) #start address of output coil for PLC expansion 1
rq = rq.bits
rr = [int(i) for i in rq]
rt = list(chunks(rr,8))
print(rt)