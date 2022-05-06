app_name = 'knot_gw'

import bleparser
import json

UNKNOWNS = []
TRIGGERS = []
DEVICES = dict()
STATS = {
    "dups":      0,
    "wrong_len": 0,
    "missing":   0,
    "payload_error": 0,
    "unknown":   0,
}

ble_parser = bleparser.BleParser()

def update_ble_entities(mac, name, temperature, humidity,
                        battery_percent, battery_v, d_rssi):
    if DEVICES[mac]['attributes_initialized']:
        if temperature != -100:
            state.set(DEVICES[mac]['temperature_entity'], temperature, rssi=d_rssi)
        if humidity != 0:
            state.set(DEVICES[mac]['humidity_entity'], humidity, rssi=d_rssi)
        if battery_percent != 0 and battery_v != 0:
            state.set(DEVICES[mac]['battery_entity'], battery_percent,
                          battery_voltage=battery_v, rssi=d_rssi)
    else:
        # if static attributes haven't been set, create them now
        log.info(f"Create entity attributes for {mac}/{name}")
        DEVICES[mac]['attributes_initialized'] = True

        temperature_attributes = {
            "friendly_name": f"{name} Temperature",
            "device_class": "temperature",
            "unit_of_measurement": "°C",
            "icon": "mdi:thermometer",
            "mac": mac,
            "rssi": d_rssi
        }
        state.set(DEVICES[mac]['temperature_entity'], temperature, temperature_attributes)        

        humidity_attributes = {
            "friendly_name": f"{name} Humidity",
            "device_class": "humidity",
            "unit_of_measurement": "%",
            "icon": "mdi:water-percent",
            "mac": mac,
            "rssi": d_rssi
        }
        state.set(DEVICES[mac]['humidity_entity'], humidity, humidity_attributes)
        
        if battery_percent != 0 and battery_v != 0:
            battery_attributes = {
                "friendly_name": f"{name} Battery",
                "device_class": "battery",
                "unit_of_measurement": "%",
                "icon": "mdi:battery-bluetooth-variant",
                "mac": mac,
                "rssi": d_rssi
            }
            state.set(DEVICES[mac]['battery_entity'], battery_percent, battery_attributes)
        


# called when an MQTT message is received on one of the subscribed topics.
def ble_message(**mqttmsg):
    """MQTT message from BLE relay"""
    
    # message looks like  { "mac": "A4:C1:38:DE:90:CE", "service_data": "18:1A", "len": 13, "data": "A4C138DE90CE00E3255A0BCE7B" }
    
    if not 'payload_obj' in mqttmsg:
        log.warning("payload not valid JSON")
        STATS['payload_error'] += 1
        return

    m = mqttmsg['payload_obj']
    #log.debug(json.dumps(m))

    for device in m['locs'][0]['tags']:
        log.debug(device)
        mac = device['id'].lower()
        if mac not in DEVICES:
            log.debug(f"MAC skipped {mac}")
            continue
        rssi = device['rssi']
        pdu = device['ed']['ad']
        # unclear if 26 is a sane value?
        if len(pdu) < 26:
            log.debug(f"PDU {pdu} length < 26, assume invalid")
        clean_mac = bytes(bytearray.fromhex(mac.replace(':', '')))
        log.debug(DEVICES[mac])
        if (DEVICES[mac]['device'] == "xiaomi"):
            res = bleparser.parse_xiaomi(ble_parser, bytes(bytearray.fromhex(pdu)), clean_mac, rssi)
            # packet sometimes is garbage
            if not res:
                log.warning(f'Xiaomi {mac} returned garbage')
                continue
            temp = -100
            hum = 0
            bat = 0
            volts = 0
            if 'temperature' in res:
                temp = res['temperature']
            if 'humidity' in res:
                hum = res['humidity']
            if 'battery' in res:
                bat = res['battery']
            if 'voltage' in res:
                volts = res['voltage']

            update_ble_entities(mac, DEVICES[mac]['name'], temp, hum, bat, volts, int(rssi))

# generate an MQTT topic trigger 
def mqttTrigger(topic):
    log.debug(f'Subscribing to topic {topic}')

    @mqtt_trigger(topic)
    def mqtt_message_fun(**kwargs):
        ble_message(**kwargs)

    return mqtt_message_fun

def initialize(cfg):
    import re

    for mac in cfg['devices']:
        name = cfg['devices'][mac]['name']
        device_type = cfg['devices'][mac]['device']
        # contruct an entity name, all lower case and replacing illegal characters with '_'
        ent_name = re.sub(r'[^0-9A-Za-z]', '_', name.lower())
        mac = mac.lower()
        DEVICES[mac] = {
            'temperature_entity':   f'sensor.ble_{ent_name}_temperature',
            'humidity_entity':      f'sensor.ble_{ent_name}_humidity',            
            'battery_entity':       f'sensor.ble_{ent_name}_battery',
            'attributes_initialized': False,
            'mac':                  mac,
            'name':                 name,
            'device':               device_type,
            'dups':                 0,
            'missing':              0
        }
        log.debug(f'config - {mac} as {ent_name} / {name}')

    # subscribe topics
    for topic in cfg['topics']:
        TRIGGERS.append(mqttTrigger(topic))


if 'apps' in pyscript.config and app_name in pyscript.config['apps']:
    initialize(pyscript.config['apps'][app_name])
else:
    logger.warning(f'No {app_name} configuration found')
