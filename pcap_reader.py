import json
import sys

from scapy.utils import RawPcapReader
from scapy.all import *


captured = rdpcap(sys.argv[1])
msg = ""
for pkt in captured:
    if Raw in pkt:
        msg += pkt[Raw].load.decode('utf-8')
        try:
            blob = json.loads(msg)
        except:
            continue

        blob['time'] = str(pkt.time)
        print(json.dumps(blob))
        msg = ""
