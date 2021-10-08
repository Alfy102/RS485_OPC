import asyncio
from asyncua import ua, Server
from datetime import timedelta, datetime
from asyncua.server.history_sql import HistorySQLite
from pathlib import Path
import pandas as pd
import sqlite3
from io_layout_map import node_structure
import collections
import time
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
from pymodbus.client.asynchronous import schedulers
from comm_protocol import serial_read_holding_registers
from pymodbus.transaction import ModbusRtuFramer


class OpcServerThread(object):
    
    def __init__(self,client,current_file_path,endpoint,server_refresh_rate,uri,parent=None,**kwargs):
        self.plc_address=0x01
        self.file_path = current_file_path
        self.server = Server()
        self.endpoint = endpoint
        self.uri = uri
        self.namespace_index = 0
        self.server_refresh_rate = server_refresh_rate
        self.plc_clock_dict = {key:value for key,value in node_structure.items() if value['node_property']['category']=='plc_clock'}
        #delay of subscribtion time in ms. reducing this value will cause server lag.
        self.sub_time = 50
        self.hmi_sub = 10
        asyncio.run(self.opc_server(client))



    def get_node(self, node_id):
        node =  self.server.get_node(ua.NodeId(node_id, self.namespace_index))
        return node
    
    def ua_variant_data_type(self, data_type, data_value):
        if data_type == 'UInt16':
            ua_var = ua.Variant(int(data_value), ua.VariantType.UInt16)
        elif data_type == 'UInt32':
            ua_var = ua.Variant(int(data_value), ua.VariantType.UInt32)
        elif data_type == 'UInt64':    
            ua_var = ua.Variant(int(data_value), ua.VariantType.UInt64)
        elif data_type == 'String':
            ua_var = ua.Variant(str(data_value), ua.VariantType.String)
        elif data_type == 'Boolean':
            ua_var = ua.Variant(bool(data_value), ua.VariantType.Boolean)
        elif data_type == 'Float':
            ua_var = ua.Variant(float(data_value), ua.VariantType.Float)
            
        return ua_var

    def data_type_conversion(self, data_type, data_value):
        if data_type == 'UInt16':
            data_value = int(data_value)
        elif data_type == 'UInt32':
            data_value = int(data_value)
        elif data_type == 'UInt64':    
            data_value = int(data_value)
        elif data_type == 'String':
            data_value = str(data_value)
        elif data_type == 'Boolean':
            data_value = bool(data_value)
        elif data_type == 'Float':
            data_value = float(data_value)
        return data_value

    def checkTableExists(self,dbcon, tablename):
        dbcur = dbcon.cursor()
        dbcur.execute(f"SELECT * FROM sqlite_master WHERE type='table' AND name='{tablename}';")
        table = dbcur.fetchone()
        if table is not None:
            if tablename in table:
                dbcur.close()
                return True
        else:   
            dbcur.close()
            return False

    async def scan_loop_plc(self,client,io_dict):
        lead_data = io_dict[list(io_dict.keys())[0]]
        lead_device = lead_data['name']
        device_size = len(io_dict)
        #print(lead_device)
        #print(device_size)
        current_relay_list = serial_read_holding_registers(client,int(lead_device),device_size,self.plc_address)
        #current_memory = client.read_holding_registers(lead_device, device_size, unit=self.plc_address)
        #current_memory = client.read_holding_registers(16397, 6, unit=0x01)
        #current_relay_list = current_memory.registers
        
        #print(current_relay_list.registers)
        i=0
        for key,value in io_dict.items():
            node_id = key
            data_type = value['node_property']['data_type']
            asyncio.create_task(self.simple_write_to_opc(node_id, current_relay_list[i], data_type))
            i+=1

    async def simple_write_to_opc(self, node_id, data_value, data_type):
        node_id=self.get_node(node_id)
        self.source_time = datetime.now()
        data_value = ua.DataValue(self.ua_variant_data_type(data_type, data_value),SourceTimestamp=self.source_time, ServerTimestamp=self.source_time)
        await self.server.write_attribute_value(node_id.nodeid, data_value)

    async def node_creation(self,database_file,node_category_list):
        conn = sqlite3.connect(self.file_path.joinpath(database_file))
        for category in node_category_list:
                    server_obj = await self.server.nodes.objects.add_object(self.namespace_index, category)
                    for key, value in node_structure.items():
                        if value['node_property']['category']==category:
                            node_id, variable_name, data_type, rw_status, historizing = key, value['name'], value['node_property']['data_type'], value['node_property']['rw'],value['node_property']['history']
                            
                            if historizing==True and self.checkTableExists(conn, f"{self.namespace_index}_{node_id}"):
                                previous_data = pd.read_sql_query(f"SELECT Value FROM '{self.namespace_index}_{node_id}' ORDER BY _Id DESC LIMIT 1", conn)
                                if not previous_data.empty:
                                    previous_value = previous_data.iloc[0]['Value']
                                    initial_value = self.data_type_conversion(data_type, previous_value)
                                else:
                                    initial_value = value['node_property']['initial_value']
                            else:
                                initial_value = value['node_property']['initial_value']           
                            server_var = await server_obj.add_variable(ua.NodeId(node_id,self.namespace_index), str(variable_name), self.ua_variant_data_type(data_type,initial_value))
                            if rw_status:
                                await server_var.set_writable()
        conn.close()

    async def opc_server(self,client):

        database_file = "variable_history.sqlite3"
        #Configure server to use sqlite as history database (default is a simple memory dict)
        self.server.iserver.history_manager.set_storage(HistorySQLite(self.file_path.joinpath(database_file)))
        await self.server.init()

        #populate the server with the defined nodes imported from io_layout_map
        self.server.set_endpoint(f"opc.tcp://{self.endpoint}")
        
        self.namespace_index = await self.server.register_namespace(self.uri)

        
        node_category = [item['node_property']['category'] for item in node_structure.values()]
        node_category = list(set(node_category))
        await self.node_creation(database_file,node_category)
        

        plc_clock_dict = collections.OrderedDict(sorted(self.plc_clock_dict.items()))
        async with self.server:
            while True:
                #tic = time.time()
                await asyncio.sleep(self.server_refresh_rate)
                await self.scan_loop_plc(client,plc_clock_dict)
                #toc = time.time()
                #print(f"{toc - tic- self.server_refresh_rate :.9f}")

def main():
    uri = "PLC_Server"

    file_path = Path(__file__).parent.absolute()
    endpoint = "localhost:4840/gshopcua/server"
    server_refresh_rate = 0.001 

    #loop, client = ModbusClient(schedulers.ASYNC_IO, port="COM7",stopbits = 1, bytesize = 8, parity = 'E', baudrate= 19200,method='rtu')
    client = ModbusClient(method = "rtu", port="COM7",stopbits = 1, bytesize = 8, parity = 'E', baudrate= 19200)
    OpcServerThread(client,file_path,endpoint,server_refresh_rate,uri)

if __name__ == "__main__":
    main()