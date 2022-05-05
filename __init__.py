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
    log.debug(json.dumps(m))
    mac = m['locs'][0]['tags'][0]['id'].lower()
    rssi = m['locs'][0]['tags'][0]['rssi']

    data = m['locs'][0]['tags'][0]['ed']['ad']
    if len(data) < 26:
        STATS['wrong_len'] += 1
        return

    log.debug(f"DEVICES == {DEVICES}")
    if mac not in DEVICES:
        log.debug(f"MAC skipped {mac}")
        return

    h_mac = mac.replace(':', '')
    log.debug(f"mac is {mac}")
    log.debug(f"raw BLE PDU received {data}")
    h_mac = bytes(bytearray.fromhex(h_mac))
    res = bleparser.parse_xiaomi(ble_parser, bytes(bytearray.fromhex(data)), h_mac, rssi)

    # 0 is a reasonable temperature but -100 is not :)
    h_temp = -100
    h_humidity = 0
    h_battery_pct = 0
    h_battery_v = 0

    log.debug(f"res is {res}")

    if 'temperature' in res:
        h_temp = res['temperature']
    if 'humidity' in res:
        h_humidity = res['humidity']
    if 'battery' in res:
        h_battery_pct = res['battery']
    if 'voltage' in res:
        h_battery_v = res['voltage']

    h_rssi = int(rssi)

    log.debug(f'MAC: {mac} h_temp={h_temp:.1f} h_humidity={h_humidity}')
    update_ble_entities(mac, DEVICES[mac]['name'], h_temp, h_humidity, h_battery_pct, h_battery_v, h_rssi)

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
