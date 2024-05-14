from yoctopuce.yocto_power import *
import csv
import time
import os

class Yocto:
    def __init__(self, device, query, partitions, fileformat):
        self.target = device
        self.query = query
        self.partitions = partitions
        self.fileformat = fileformat

    def usage(self):
        scriptname = os.path.basename(sys.argv[0])
        print("Usage:")
        print(scriptname + ' <serial_number>')
        print(scriptname + ' <logical_name>')
        print(scriptname + ' any ')
        sys.exit()

    def die(self, msg):
        sys.exit(msg + ' (check USB cable)')

    def YoctoConnect(self, queue):
        print("Yocto reading engery values!")
        errmsg = YRefParam()

        # Setup the API to use local USB devices
        if YAPI.RegisterHub("usb", errmsg) != YAPI.SUCCESS:
            sys.exit("init error" + errmsg.value)

        if self.target == 'any':
            # retreive any Power sensor
            sensor = YPower.FirstPower()
            if sensor is None:
                self.die('No module connected')
        else:
            sensor = YPower.FindPower(self.target + '.power')

        if not (sensor.isOnline()):
            self.die('device not connected')
        
        id = 1
        file_exist = 0 
        if os.path.isfile(f'./resultsvtwo/energy_{self.fileformat}.csv') == True:
            file_exist = 1

        with open(f'./resultsvtwo/energy_{self.fileformat}_testv4.csv', 'a', newline='') as file:
            writer = csv.writer(file)
            if file_exist == 0:
                writer.writerow(["id", "energy_values", "file_format", "query", "number_of_partitions", "time"])
            while sensor.isOnline():
                print(sensor.get_currentValue())
                try:
                    if str(queue.get_nowait()) == "finish":
                        break
                except:
                    writer.writerow([id, str(sensor.get_currentValue()), self.fileformat, self.query, self.partitions])
                    YAPI.Sleep(1000)
                    id += 1
        YAPI.FreeAPI()
        file.close()

