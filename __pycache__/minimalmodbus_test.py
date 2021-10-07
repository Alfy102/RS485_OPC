import minimalmodbus
import serial
instrument = minimalmodbus.Instrument("COM8", 1) # port name, slave address (in decimal)
#instrument.serial.port # this is the serial port name
instrument.serial.baudrate = 192000 # Baud
instrument.serial.bytesize = 8
instrument.serial.parity = serial.PARITY_EVEN
instrument.serial.stopbits = 1
instrument.serial.timeout = 1 # seconds

temperature = instrument.read_register(0, 0)  # Registernumber, number of decimals
print(temperature)