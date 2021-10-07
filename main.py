import pymodbus
from pymodbus.pdu import ModbusRequest
from pymodbus.client.sync import ModbusSerialClient as ModbusClient #initialize a serial RTU client instance
from pymodbus.transaction import ModbusRtuFramer
import logging
FORMAT = ('%(asctime)-15s %(threadName)-15s'
' %(levelname)-8s %(module)-15s:%(lineno)-8s %(message)s')
logging.basicConfig(format=FORMAT)
log = logging.getLogger()
log.setLevel(logging.DEBUG)





unit = 0x01
client= ModbusClient(method = "rtu", port="COM11",stopbits = 1, timeout=3, bytesize = 8, parity = 'E', baudrate= 19200)
connection = client.connect()
print(connection)
rq = client.write_coil(18432,1,unit=1)
rq = client.write_coil(18434,1,unit=1)
#rq = client.read_coils(18432,100,unit=1)
responce = rq.bits
print(responce)
client.close()
