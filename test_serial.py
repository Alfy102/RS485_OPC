
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
import pymodbus
from pymodbus.pdu import ModbusRequest
from pymodbus.transaction import ModbusRtuFramer

client = ModbusClient(method='rtu', port="COM7",stopbits = 1, timeout=3, bytesize = 8, parity = 'E', baudrate= 19200)
connection = client.connect()
print(connection)