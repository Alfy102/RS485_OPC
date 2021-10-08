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
from pymodbus.client.asynchronous.serial import (AsyncModbusSerialClient as ModbusClient)
from pymodbus.client.asynchronous import schedulers
from comm_protocol import plc_tcp_socket_request
#io_dict standard dictionary: {variables_id:[device_ip, variables_ns, device_name, category_name,variable_name,0]}
#hmi_signal standard: (namespace, node_id, data_value)


class SubDeviceModeHandler(object):
    def __init__(self,mode_dict,mode_update):
        self.device_mode = mode_dict
        self.mode_update = mode_update
    async def datachange_notification(self, node, val, data):
        node_identifier = node.nodeid.Identifier
        asyncio.create_task(self.mode_update(node_identifier, val))

class OpcServerThread(object):
    def __init__(self,plc_address,current_file_path,endpoint,server_refresh_rate,uri,parent=None,**kwargs):
        self.client = ModbusClient(schedulers.ASYNC_IO, port="COM7",stopbits = 1, timeout=3, bytesize = 8, parity = 'E', baudrate= 19200)
        self.plc_ip_address=plc_address
        self.file_path = current_file_path
        self.server = Server()
        self.endpoint = endpoint
        self.uri = uri
        self.namespace_index = 0
        self.server_refresh_rate = server_refresh_rate
        #self.monitored_node = {key:value for key,value in node_structure.items() if value['node_property']['category']=='server_variables'}
        self.plc_clock_dict = {key:value for key,value in node_structure.items() if value['node_property']['category']=='plc_clock'}

        #delay of subscribtion time in ms. reducing this value will cause server lag.
        self.sub_time = 50
        self.hmi_sub = 10
        asyncio.run(self.opc_server())

    async def count_node(self,node_id,data_value, data_type):
        node = self.server.get_node(self.get_node(node_id)) 
        current_value = await node.read_value()
        new_value = current_value + data_value
        asyncio.create_task(self.simple_write_to_opc(node_id, new_value, data_type))
        return new_value

    def yield_calculation(self,in_value, out_value):
        if in_value == 0 or out_value==0:
            total_yield=0.0
        else:
            total_yield = (out_value/in_value)*100
            total_yield = round(total_yield, 2)       
        return total_yield

    async def mode_update(self,node_id, data_value):
        node_property = self.mode_dict[node_id]
        node_property['node_property']['initial_value'] = data_value
        node_property.update({'flag_time':datetime.now()})
        self.mode_dict.update({node_id:node_property})
        for key,value in self.lot_time_dict.items():
            if value['monitored_node']==node_id:
                if data_value == True:
                    node_id = self.get_node(key)
                    value['node_property']['initial_value']= await node_id.read_value()     
                self.lot_time_dict.update({key:value})
        for key,value in self.shift_time_dict.items():
            if value['monitored_node']==node_id and data_value == True:
                if data_value == True:
                    node_id = self.get_node(key)
                    value['node_property']['initial_value']= await node_id.read_value()         
                self.lot_time_dict.update({key:value})
 
    def timer_function(self, time_dict):
        for node_id,value in time_dict.items():
            corr_flag_node = value['monitored_node']
            if corr_flag_node != None:
                device_mode = self.mode_dict[corr_flag_node]['node_property']['initial_value']
                if device_mode == True:
                    data_type = value['node_property']['data_type']
                    flag_time = self.mode_dict[corr_flag_node]['flag_time']
                    delta_time = self.convert_string_to_time(value['node_property']['initial_value'])
                    duration = self.duration(flag_time, delta_time)
                    asyncio.create_task(self.simple_write_to_opc(node_id,duration,data_type))
    
    async def system_uptime(self):    
        lot_start_node = self.get_node(10054)
        lot_start_datetime = await lot_start_node.read_value()
        if lot_start_datetime != 'Null':
            uptime = self.uptime(lot_start_datetime)
            uptime = str(uptime).split('.')[0]
            asyncio.create_task(self.simple_write_to_opc(10044, uptime, 'String')) #write to lot_uptime

        shift_start_node = self.get_node(10055)
        shift_start_datetime = await shift_start_node.read_value()
        if shift_start_datetime != 'Null':
            uptime = self.uptime(shift_start_datetime)
            uptime = str(uptime).split('.')[0]
            asyncio.create_task(self.simple_write_to_opc(10040, uptime, 'String')) #write to shift_uptime

    def duration(self,start_time, delta_time):
        if isinstance(start_time, str):
            start_time = self.convert_string_to_time(start_time)
        if isinstance(delta_time, str):
            delta_time = self.convert_string_to_time(delta_time)
        duration = datetime.now() - start_time + delta_time
        return duration

    def uptime(self, start_datetime):
        if isinstance(start_datetime, str):
                start_time = self.convert_string_to_datetime(start_datetime)
        uptime = datetime.now() - start_time
        return uptime


    def convert_string_to_datetime(self,time_string):
        try:
            date_time = datetime.strptime(time_string,"%d.%m.%Y %H:%M:%S")
        except:
            date_time = datetime.strptime(time_string,"%d.%m.%Y %H:%M")
        return date_time

    def convert_string_to_time(self,time_string):
        try:
            delta_time = datetime.strptime(time_string,"%H:%M:%S.%f")
        except:
            delta_time = datetime.strptime(time_string,"%H:%M:%S")
        delta_time = timedelta(hours=delta_time.hour, minutes=delta_time.minute, seconds=delta_time.second, microseconds=delta_time.microsecond)
        return delta_time

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


    async def scan_loop_plc(self,io_dict):
        lead_data = io_dict[list(io_dict.keys())[0]]
        lead_device = lead_data['name']
        device_size = len(io_dict)
        current_relay_list = await plc_tcp_socket_request(self.plc_ip_address,self.port_number,lead_device,device_size,'read')
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

    async def opc_server(self):
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
                await self.scan_loop_plc(plc_clock_dict)
                #toc = time.time()
                #print(f"{toc - tic- self.server_refresh_rate :.9f}")

def main():
    uri = "PLC_Server"
    plc_address = 1
    file_path = Path(__file__).parent.absolute()
    endpoint = "localhost:4845/gshopcua/server"
    server_refresh_rate = 0.1
    OpcServerThread(plc_address,file_path,endpoint,server_refresh_rate,uri)

if __name__ == "__main__":
    main()