import asyncio

async def plc_tcp_socket_read(ip_address, port_number,start_device,number_of_device):
    """
        :param ip_address: PLC IP Address
        :param port_number: PLC IP Address's port number
        :param start_device: starting relay/memory address of the PLC
        :param number_of_device: number of address to read fater the starting address
        :param mode: read or write mode

    """
    reader, writer = await asyncio.open_connection(ip_address, port_number)
    encapsulate = bytes(f"RDS {start_device} {number_of_device}\r\n","utf-8")
    writer.write(encapsulate)
    await writer.drain()
    recv_value = await reader.readuntil(separator=b'\r\n') 
    recv_value = recv_value.decode("UTF-8").split()
    recv_value = [int(recv_value[i]) for i in range(len(recv_value))]
    writer.close()
    return recv_value

async def plc_tcp_socket_write(ip_address, port_number,start_device,data_value):
    """
        :param ip_address: PLC IP Address
        :param port_number: PLC IP Address's port number
        :param start_device: starting relay/memory address of the PLC
        :param data_value: data to write
        :param mode: read or write mode

    """
    reader, writer = await asyncio.open_connection(ip_address, port_number)
    message = f"WR {start_device} {int(data_value)}\r\n"
    encapsulate = bytes(message,'utf-8')
    writer.write(encapsulate)
    await writer.drain()
    recv_value = await reader.readuntil(separator=b'\r\n') 
    recv_value = recv_value.decode("UTF-8").split()
    recv_value = [int(recv_value[i]) for i in range(len(recv_value))]
    writer.close()
    return recv_value



def serial_read_holding_registers(client,unit , start_address, number_of_address):
    rr = client.read_holding_registers(int(start_address), number_of_address, unit=unit)
    response = rr.registers
    return response


def serial_read_coils(client,unit , start_address, number_of_address):
    rr = client.read_holding_registers(int(start_address), number_of_address, unit=unit)
    response = rr.bits
    return response