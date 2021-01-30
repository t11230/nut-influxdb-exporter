#!/usr/bin/python
import os
import time
from typing import List, Optional

from influxdb import InfluxDBClient
from nut2 import PyNUTClient

# InfluxDB details
influx_dbname = os.getenv('INFLUXDB_DATABASE', 'nutupstest')  # type: str
influx_username = os.getenv('INFLUXDB_USER')  # type: Optional[str]
influx_password = os.getenv('INFLUXDB_PASSWORD')  # type: Optional[str]
influx_host = os.getenv('INFLUXDB_HOST', '127.0.0.1')  # type:str
influx_port = int(os.getenv('INFLUXDB_PORT', 8086))  # type: int

# NUT related variables
nut_host = os.getenv('NUT_HOST', '127.0.0.1')  # type:str
nut_port = int(os.getenv('NUT_PORT', 3493))  # type: int
nut_password = os.getenv('NUT_PASSWORD')  # type: Optional[str]
nut_username = os.getenv('NUT_USERNAME')  # type: Optional[str]
nut_watts = os.getenv('WATTS')  # type: Optional[str]

# Other vars
interval = float(os.getenv('INTERVAL', 21))  # type: float
ups_names = os.getenv('UPS_NAME', 'UPS').split(',')  # type: List[str]
verbose = os.getenv('VERBOSE', '').lower() in ['true', '1', 'y']  # type: bool

# Extra keys to remove from NUT data
remove_keys = ['driver.version.internal', 'driver.version.usb', 'ups.beeper.status', 'driver.name', 'battery.mfr.date']

# NUT keys that are considered tags and not measurements
tag_keys = ['battery.type', 'device.model', 'device.serial', 'driver.version', 'driver.version.data', 'device.mfr',
            'device.type', 'ups.mfr', 'ups.model', 'ups.productid', 'ups.serial', 'ups.vendorid']


def convert_to_type(s):
    """ A function to convert a str to either integer or float. If neither, it will return the str. """
    try:
        int_var = int(s)
        return int_var
    except ValueError:
        try:
            float_var = float(s)
            return float_var
        except ValueError:
            return s


def construct_object(data):
    """
    Constructs NUT data into  an object that can be sent directly to InfluxDB

    :param data: data received from NUT
    :return:
    """
    fields = {}
    tags = {'host': os.getenv('HOSTNAME', 'localhost')}

    for k, v in data.items():
        if k not in remove_keys:
            if k in tag_keys:
                tags[k] = v
            else:
                fields[k] = convert_to_type(v)

    watts = float(nut_watts if nut_watts else fields['ups.realpower.nominal'])
    fields['watts'] = watts * 0.01 * fields['ups.load']

    result = [
        {
            'measurement': 'ups_status',
            'fields': fields,
            'tags': tags
        }
    ]
    return result


def main():
    if verbose:
        print("INFLUXDB_PORT: ", influx_port)
        print("INFLUXDB_HOST: ", influx_host)
        print("INFLUXDB_DATABASE: ", influx_dbname)
        print("INFLUXDB_USER: ", influx_username)
        # Not really safe to just print it. Feel free to uncomment this if you really need it
        # print("INFLUXDB_PASSWORD: ", influx_password)

        print("NUT_HOST: ", nut_host)
        print("NUT_PORT: ", nut_port)
        print("NUT_USER: ", nut_username)
        # Same as above
        # print("NUT_PASS: ", nut_password)

        print("UPS_NAME", ups_names)
        print("INTERVAL: ", interval)
        print("VERBOSE: ", verbose)

    print("Connecting to InfluxDB host:{}, DB:{}".format(influx_host, influx_dbname))

    influx_client = InfluxDBClient(influx_host, influx_port, influx_username, influx_password, influx_dbname)
    influx_client.create_database(influx_dbname)

    print("Connected successfully to InfluxDB")

    print("Connecting to NUT host {}:{}".format(nut_host, nut_port))

    nut_client = PyNUTClient(host=nut_host, port=nut_port, login=nut_username, password=nut_password, debug=verbose)

    print("Connected successfully to NUT")

    # Main infinite loop: Get the data from NUT every interval and send it to InfluxDB.
    while True:
        for ups_name in ups_names:
            # Query NUT for data about this UPS
            ups_data = nut_client.list_vars(ups_name)

            # Convert NUT data format to InfluxDB format
            json_body = construct_object(ups_data)

            if verbose:
                print(json_body)

            # Write the latest data to InfluxDB
            write_result = influx_client.write_points(json_body)

            if verbose:
                print(write_result)

        time.sleep(interval)


if __name__ == '__main__':
    main()
