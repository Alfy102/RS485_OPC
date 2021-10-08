from datetime import timedelta
from asyncua.ua.uatypes import flatten_and_get_shape


node_structure = {

10010:{ 'name': '16397', #year
        'label_point':[],
        'node_property':{'data_type': 'UInt16', 'category': 'plc_clock','rw': 'r', 'history': False, 'initial_value': 0}},

10011:{ 'name': '16398', #month
        'label_point':[],
        'node_property':{'data_type': 'UInt16', 'category': 'plc_clock','rw': 'r', 'history': False, 'initial_value': 0}},

10012:{ 'name': '16399', #day
        'label_point':[],
        'node_property':{'data_type': 'UInt16', 'category': 'plc_clock','rw': 'r', 'history': False, 'initial_value': 0}},

10013:{ 'name': '16400', #hour
        'label_point':[],
        'node_property':{'data_type': 'UInt16', 'category': 'plc_clock','rw': 'r', 'history': False, 'initial_value': 0}},

10014:{ 'name': '16401', #minute
        'label_point':[],
        'node_property':{'data_type': 'UInt16', 'category': 'plc_clock','rw': 'r', 'history': False, 'initial_value': 0}},

10015:{ 'name': '16402', #second
        'label_point':[],
        'node_property':{'data_type': 'UInt16', 'category': 'plc_clock','rw': 'r', 'history': False, 'initial_value': 0}},



}
