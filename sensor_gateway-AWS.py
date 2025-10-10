import subprocess
import sys
import os
import asyncio
import logging
import signal
import json
import time
import aiohttp
from typing import Dict, Any, Optional

# --- EARLY LOGGING CONFIGURATION ---
log_format = "%(asctime)s %(levelname)s [%(name)s]: %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format, stream=sys.stdout)
logging.info(">>>> Global logging configuration applied <<<<")

# --- Configuration  ---
DEVICE_PATH = '/dev/ttyUSB0'
BAUDRATE = 115200
FLOW_CONTROL = None

# --- VERSION CONFIGURATION ---
GATEWAY_VERSION = "1.2.0"  # ✅ NEW VERSION WITH PING BROADCAST
PERMIT_JOIN_DURATION_ON_STARTUP = 180
REOPEN_JOIN_INTERVAL_SECONDS = 150

ESP32_H2_ENDPOINT_ID = 1
CUSTOM_CLUSTER_ID = 0xFC01

# ATTR_ID_ALL_CURRENTS_STRING Is the only one we need
ATTR_ID_ALL_CURRENTS_STRING = 0x0004

REPORTING_MIN_INTERVAL = 20
REPORTING_MAX_INTERVAL = 60
REPORTABLE_CHANGE_FOR_STRING = 0

AGGREGATION_WINDOW_SECONDS = 3.0

# --- IMPROVED CONFIGURATIONS FOR RECONNECTION HANDLING ---
DEVICE_RECONNECTION_TIMEOUT = 30
REPORTING_RECONFIGURE_DELAY = 5
MAX_REPORTING_RETRIES = 5
DEVICE_ACTIVITY_TIMEOUT = 90

# --- AWS API CONFIGURATION  ---
API_ENDPOINT_URL = "https://mknncrj1nj.execute-api.us-east-1.amazonaws.com/dev/data"
API_TIMEOUT_SECONDS = 15

# --- IMPROVED SHUTDOWN CONFIGURATION ---
SHUTDOWN_TIMEOUT = 8.0
GRACEFUL_SHUTDOWN_DELAY = 1.0

# ✅ NEW CONFIGURATION FOR PING BROADCAST
PING_BROADCAST_INTERVAL_SECONDS = 8  # Every 8 seconds

import zigpy.backups
import zigpy.config as zigpy_config
import zigpy.exceptions
import zigpy.device as zigpy_dev
import zigpy.endpoint as zigpy_ep
import zigpy.zcl.foundation as zcl_f
from zigpy.zcl import Cluster
import zigpy.types as t
import zigpy.zdo.types as zdo_types

try:
    from bellows.zigbee.application import ControllerApplication as BellowsApplication
except ImportError:
    logging.error("Error: The 'bellows' library is not installed.")
    BellowsApplication = None

# ✅ COMMANDS UPDATED ACCORDING TO SPECIFICATIONS 
CMD_COORDINATOR_PING_BROADCAST = 0x01  # Coordinator ping broadcast only
# CMD_PONG_RESPONSE removed - no longer used


# --- 🚀 INTEGRATED ASYNCHRONOUS FUNCTION FOR AWS SENDING  ---
async def send_data_to_aws_async(device_id: str, sensors_data: list, coordinator_ieee: str) -> bool:
    """
    Sends sensor data to AWS asynchronously.
    
    Args:
        device_id: Device ID (IEEE address)	
        sensors_data: List of sensors in the format [{"sid": "1", "current": 23.5}, ...]
        coordinator_ieee: IEEE address of the Zigbee coordinator
    
    Returns:
        bool: True if sending was successful, False otherwise
    """
    
    timestamp_actual = int(time.time())
    
    # Process and validate sensor data 
    processed_sensors = []
    for sensor_entry in sensors_data:
        try:
            sid = int(sensor_entry.get("sid"))
            current = float(sensor_entry.get("current"))
            processed_sensors.append({"sid": sid, "current": current})
        except (ValueError, TypeError) as e:
            logging.warning(f"⚠️  Invalid sensor entry {sensor_entry}: {e}. It will be omitted.")
            continue
    
    if not processed_sensors:
        logging.error("❌ No valid sensor data to send to AWS")
        return False
    
    # Prepare payload for AWS
    payload_dict = {
        "id": device_id,
        "timestamp": timestamp_actual,
        "coordinator_ieee": coordinator_ieee,
        "sensors": processed_sensors
    }
    
    # Professional startup logs	    
    # logging.info("🌐 ═══════════════════════════════════════════════════════════")
    logging.info("🚀 INITIATING DATA SEND TO AWS")
    logging.info(f"📡 Device: {device_id}")
    logging.info(f"🏢 Coordinador: {coordinator_ieee}")
    logging.info(f"📊 Sensors: {len(processed_sensors)} current readings")
    logging.info(f"⏰ Timestamp: {timestamp_actual} ({time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp_actual))})")
    
    # Show detailed data
    for i, sensor in enumerate(processed_sensors, 1):
        logging.info(f"   📈 Sensor {sensor['sid']}: {sensor['current']:.2f} A")
    
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=API_TIMEOUT_SECONDS)) as session:
            headers = {'Content-Type': 'application/json'}
            payload_json = json.dumps(payload_dict)
            
            logging.info("🔄 Sending data to AWS API Gateway...")
            logging.debug(f"🔍 URL: {API_ENDPOINT_URL}")
            logging.debug(f"🔍 Payload: {payload_json}")
            
            async with session.post(API_ENDPOINT_URL, headers=headers, data=payload_json) as response:
                response_text = await response.text()
                
                if response.status in [200, 201]:
                    # ✅ SUCCESS - Professional messages
                    logging.info("✅ ═══════════════════════════════════════════════════════════")
                    logging.info("🎉 DATA SENT SUCCESSFULLY TO AWS")
                    logging.info(f"📡 HTTP Status: {response.status}")
                    logging.info(f"🏢 Device {device_id} synchronized with the cloud")
                    logging.info(f"📊 {len(processed_sensors)} current readings stored")
                    logging.info("☁️ The data is now available in AWS")
                    logging.info("✅ ═══════════════════════════════════════════════════════════")
                    
                    if response_text:
                        logging.debug(f"📝 AWS Response: {response_text}")
                    
                    return True
                else:
                    # ❌  HTTP ERROR
                    logging.error("❌ ═══════════════════════════════════════════════════════════")
                    logging.error("🚨 ERROR IN SENDING TO AWS")
                    logging.error(f"📡 HTTP Status: {response.status}")
                    logging.error(f"🏢 Device: {device_id}")
                    logging.error(f"📝 Response: {response_text}")
                    logging.error("❌ ═══════════════════════════════════════════════════════════")
                    return False
                    
    except asyncio.TimeoutError:
        logging.error("❌ ═══════════════════════════════════════════════════════════")
        logging.error("⏰ TIMEOUT IN SENDING TO AWS")
        logging.error(f"🏢 Device: {device_id}")
        logging.error(f"⏱️ The request took more than {API_TIMEOUT_SECONDS} seconds")
        logging.error("🔄 The data will be retried on the next reading")
        logging.error("❌ ═══════════════════════════════════════════════════════════")
        return False
        
    except aiohttp.ClientError as e:
        logging.error("❌ ═══════════════════════════════════════════════════════════")
        logging.error("🌐 CONNECTION ERROR TO AWS")
        logging.error(f"🏢 Device: {device_id}")
        logging.error(f"🔌 Network error: {e}")
        logging.error("🔄 Verify internet connectivity")
        logging.error("❌ ═══════════════════════════════════════════════════════════")
        return False
        
    except Exception as e:
        logging.error("❌ ═══════════════════════════════════════════════════════════")
        logging.error("🚨 UNEXPECTED ERROR IN SENDING TO AWS")
        logging.error(f"🏢 Device: {device_id}")
        logging.error(f"⚠️  Error: {e}")
        logging.error("❌ ═══════════════════════════════════════════════════════════")
        return False


class CustomPowerSensorCluster(Cluster):
    cluster_id = CUSTOM_CLUSTER_ID

    attributes = {
        ATTR_ID_ALL_CURRENTS_STRING: ("all_currents_as_string", t.CharacterString),
    }

    # ✅ SIMPLIFIED COMMANDS - PING BROADCAST ONLY
    server_commands = {
        CMD_COORDINATOR_PING_BROADCAST: zcl_f.ZCLCommandDef(
            "coordinator_ping_broadcast", 
            schema={}, # El payload es vacío
            is_manufacturer_specific=False,
            direction=zcl_f.Direction.Client_to_Server,
        ),
    }

    # ✅ COMMANDS THAT THIS CLUSTER SENDS (as client)
    client_commands = {
        CMD_COORDINATOR_PING_BROADCAST: zcl_f.ZCLCommandDef(
            "coordinator_ping_broadcast", 
            schema={}, # payload is empty
            is_manufacturer_specific=False,
            direction=zcl_f.Direction.Client_to_Server,
        ),
    }

    # ✅ PING/PONG HANDLER REMOVED - NO LONGER USED
    # The coordinator only sends, it doesn't receive responses


# --- Custom cluster registration ---
if CUSTOM_CLUSTER_ID not in Cluster._registry:
    Cluster._registry[CUSTOM_CLUSTER_ID] = CustomPowerSensorCluster
    logging.info(f"✅ Cluster ID {CUSTOM_CLUSTER_ID:#06x} (CustomPowerSensorCluster) resgistered with PING BROADCAST v{GATEWAY_VERSION}.")
else:
    logging.info(f"Cluster ID {CUSTOM_CLUSTER_ID:#06x} (CustomPowerSensorCluster) was already in Cluster.registry.")


shutdown_event = asyncio.Event()

class SensorAttributeListener:
    def __init__(self, device_ieee: t.EUI64, owning_cluster: Cluster, coordinator_ieee: Optional[str]):
        self.device_ieee = device_ieee
        self.owning_cluster = owning_cluster
        self.coordinator_ieee_str = coordinator_ieee if coordinator_ieee is not None else "UNKNOWN_COORDINATOR"
        self._pending_send_data: Dict[int, float] = {}
        self._send_timer_handle: Optional[asyncio.TimerHandle] = None
        self._last_data_received: Optional[float] = None
        self._data_count = 0
        self.app_loop = asyncio.get_running_loop()

    async def _send_aggregated_data_to_aws(self, device_id: str, sensors_list_for_api: list, coordinator_ieee_to_send: str):
        """Sends aggregated data to AWS using the integrated asynchronous function."""
        if not sensors_list_for_api:
            logging.debug(f"No hay datos agregados para enviar para el dispositivo {device_id}.")
            return

        # Verificar si el shutdown no está en progreso
        if shutdown_event.is_set():
            logging.info(f"🚫 Envío a AWS cancelado para {device_id} - cierre en progreso")
            return

        logging.info(f"📤 Preparando envío de {len(sensors_list_for_api)} lecturas para dispositivo {device_id}")
        
        # Llamar a la función asíncrona integrada
        success = await send_data_to_aws_async(
            device_id=device_id,
            sensors_data=sensors_list_for_api,
            coordinator_ieee=coordinator_ieee_to_send
        )
        
        if success:
            logging.info(f"🎉 Datos del dispositivo {device_id} enviados exitosamente a AWS")
        else:
            logging.error(f"❌ Falló el envío de datos del dispositivo {device_id} a AWS")

    async def _flush_pending_data(self):
        self._send_timer_handle = None
        if not self._pending_send_data:
            return

        # Verificar si el shutdown no está en progreso
        if shutdown_event.is_set():
            logging.info("🚫 Flush de datos cancelado - cierre en progreso")
            return

        device_identifier_str = str(self.device_ieee)
        sensors_to_send_api = []
        data_to_process = self._pending_send_data.copy()
        self._pending_send_data.clear()

        logging.info(f"🔄 FLUSH: Procesando {len(data_to_process)} lecturas para {device_identifier_str} (Coord: {self.coordinator_ieee_str})")

        # Mapeo de IDs internos a IDs de sensor para la API
        attr_to_sensor_map = {
            1: ("1", "Sensor Corriente 1"),
            2: ("2", "Sensor Corriente 2"),
            3: ("3", "Sensor Corriente 3"),
        }

        for attr_id, value in data_to_process.items():
            if attr_id in attr_to_sensor_map:
                sensor_id_for_api, sensor_name_log = attr_to_sensor_map[attr_id]
                if isinstance(value, (float, int)):
                    logging.info(f"  📊 Agregando para envío: [{device_identifier_str}] {sensor_name_log} = {value:.2f} A")
                    sensors_to_send_api.append({"sid": sensor_id_for_api, "current": float(value)})
            else:
                logging.warning(f"  ⚠️  Omitiendo: Clave interna {attr_id} no mapeada para {device_identifier_str}.")

        if sensors_to_send_api:
            # Crear tarea asíncrona para envío a AWS
            asyncio.create_task(
                self._send_aggregated_data_to_aws(
                    device_id=device_identifier_str,
                    sensors_list_for_api=sensors_to_send_api,
                    coordinator_ieee_to_send=self.coordinator_ieee_str
                )
            )

    def cleanup(self):
        """Limpia recursos del listener."""
        if self._send_timer_handle:
            self._send_timer_handle.cancel()
            self._send_timer_handle = None

    def attribute_updated(self, attribute_id: int, value: Any, timestamp: Any):
        # Verificar si el shutdown no está en progreso
        if shutdown_event.is_set():
            return

        if self.owning_cluster.endpoint.device.ieee != self.device_ieee or self.owning_cluster.cluster_id != CUSTOM_CLUSTER_ID:
            return

        device_identifier_str = str(self.owning_cluster.endpoint.device.ieee)
        data_updated_for_flush = False

        if attribute_id == ATTR_ID_ALL_CURRENTS_STRING:
            if not isinstance(value, str):
                logging.warning(f"⚠️  Valor no string recibido para ATTR_ID_ALL_CURRENTS_STRING en {device_identifier_str}: {value} (tipo: {type(value)}). Se ignora.")
                return

            self._last_data_received = self.app_loop.time()
            self._data_count += 1

            logging.info(f"📡 PAYLOAD RECIBIDO #{self._data_count} [{device_identifier_str}]: '{value}'. Parseando...")
            
            try:
                # Intenta corregir el JSON si es necesario
                corrected_value = value.strip()
                if not corrected_value.startswith('['):
                    corrected_value = f"[{corrected_value}"
                if not corrected_value.endswith(']'):
                    corrected_value = f"{corrected_value}]"
                
                currents_list = json.loads(corrected_value)

                if isinstance(currents_list, list) and len(currents_list) == 3:
                    self._pending_send_data[1] = float(currents_list[0])
                    self._pending_send_data[2] = float(currents_list[1])
                    self._pending_send_data[3] = float(currents_list[2])
                    logging.info(f"  ✅ Parseado exitoso: S1={currents_list[0]:.2f}A, S2={currents_list[1]:.2f}A, S3={currents_list[2]:.2f}A")
                    data_updated_for_flush = True
                else:
                    logging.error(f"❌ Error parseando payload: '{corrected_value}' no es una lista de 3 elementos.")
            except (json.JSONDecodeError, ValueError, TypeError) as e:
                logging.error(f"❌ Error procesando payload: '{value}'. Error: {e}")
        else:
            return

        if data_updated_for_flush and not shutdown_event.is_set():
            if self._send_timer_handle:
                self._send_timer_handle.cancel()
            self._send_timer_handle = self.app_loop.call_later(
                AGGREGATION_WINDOW_SECONDS,
                lambda: asyncio.create_task(self._flush_pending_data())
            )


class MyEventListener:
    def __init__(self, app_controller):
        self._app = app_controller
        self._sensor_listeners: Dict[t.EUI64, SensorAttributeListener] = {}
        self.coordinator_ieee: Optional[t.EUI64] = None
        self._device_reporting_configured: Dict[t.EUI64, bool] = {}
        self._reconnection_tasks: Dict[t.EUI64, asyncio.Task] = {}

    def cleanup_all_listeners(self):
        """Clean all the listeners and tasks."""
        logging.info("🧹 Cleaning listeners y tasks...")
        
        # Cancel recognition tasks
        for device_ieee, task in self._reconnection_tasks.items():
            if not task.done():
                task.cancel()
                logging.debug(f"🚫 recognition task cancelled for{device_ieee}")
        self._reconnection_tasks.clear()
        
        # clean listeners
        for device_ieee, listener in self._sensor_listeners.items():
            listener.cleanup()
            logging.debug(f"🧹 Listener cleaned for {device_ieee}")
        self._sensor_listeners.clear()

    def device_joined(self, device: zigpy_dev.Device):
        if not shutdown_event.is_set():
            logging.info(f"🔗 JOINED DEVICE: {device.nwk:#06x} / {device.ieee}")

    def raw_device_initialized(self, device: zigpy_dev.Device):
        if not shutdown_event.is_set():
            logging.info(f"🔧 RAW DEVICE INITIALISED: {device.nwk:#06x} / {device.ieee}")

    def _is_reporting_response_successful(self, response):
        if response is None:
            return False
        if hasattr(response, 'status'):
            success = response.status == zcl_f.Status.SUCCESS
            if success: logging.info(f"    ✅ SUCCESS CONFIGURATION (direct status): {response.status}")
            return success
        if isinstance(response, tuple) and len(response) >= 1:
            return self._is_reporting_response_successful(response[1] if len(response)==2 else response[0])
        if hasattr(response, 'status_records') and response.status_records:
            return all(rec.status == zcl_f.Status.SUCCESS for rec in response.status_records)
        if isinstance(response, list):
            return all(self._is_reporting_response_successful(item) for item in response)
        logging.warning(f"    ❓ Uninterpretable response: {response} (type: {type(response)})")
        return False

    async def _configure_reporting_with_retry(self, device: zigpy_dev.Device, max_retries: int = MAX_REPORTING_RETRIES):
        """Attempts to configure reporting with retries."""
        for attempt in range(max_retries):
            if shutdown_event.is_set():
                logging.info(f"🚫 Configuration cancelled for {device.ieee} - shutdown in progress")
                return False
                
            try:
                if attempt > 0:
                    delay = REPORTING_RECONFIGURE_DELAY * (1.5 ** attempt)
                    logging.info(f"⏳ Waiting  {delay:.1f}s before attempt {attempt + 1}/{max_retries} for {device.ieee}")
                    await asyncio.sleep(delay)
                
                logging.info(f"🔄 ATTEMPT  {attempt + 1}/{max_retries} TO CONFIGURE {device.ieee}")
                
                success = await self.configure_device_reporting(device)
                if success:
                    logging.info(f"✅ Configuration SUCCESSFUL on attempt {attempt + 1} for {device.ieee}")
                    self._device_reporting_configured[device.ieee] = True
                    return True
                else:
                    logging.warning(f"❌ Attempt  {attempt + 1} failed for {device.ieee}")
                    
            except Exception as e:
                if not shutdown_event.is_set():
                    logging.error(f"❌ Error on configuration attempt {attempt + 1} for {device.ieee}: {e}")
        
        logging.error(f"❌ Configuration FAILED after {max_retries} attempts for {device.ieee}")
        return False

    async def _handle_device_connection(self, device: zigpy_dev.Device):
        """Handles a device connection (new or reconnection)."""
        device_ieee = device.ieee
        
        if device_ieee in self._reconnection_tasks:
            self._reconnection_tasks[device_ieee].cancel()

        async def connection_handler():
            try:
                if shutdown_event.is_set():
                    return
                    
                is_new = device_ieee not in self._device_reporting_configured
                logging.info(f"🔄 STARTING CONNECTION HANDLER FOR {'NEW' if is_new else 'EXISTING'} FOR {device_ieee}")
                await self._configure_reporting_with_retry(device)
            except asyncio.CancelledError:
                logging.info(f"🚫 Connection task cancelled for {device_ieee}")
            except Exception as e:
                if not shutdown_event.is_set():
                    logging.error(f"❌ Error in connection handling for {device_ieee}: {e}", exc_info=True)
            finally:
                self._reconnection_tasks.pop(device_ieee, None)
        
        task = asyncio.create_task(connection_handler())
        self._reconnection_tasks[device_ieee] = task

    async def configure_device_reporting(self, device: zigpy_dev.Device):
        if shutdown_event.is_set():
            return False
            
        try:
            logging.info(f"🔧 Starting reporting configuration for {device.ieee}")
            endpoint = device.endpoints.get(ESP32_H2_ENDPOINT_ID)
            if not endpoint:
                logging.warning(f"⚠️  Device  {device.ieee} does not have endpoint {ESP32_H2_ENDPOINT_ID}")
                return False

            custom_cluster = endpoint.in_clusters.get(CUSTOM_CLUSTER_ID)
            if not custom_cluster:
                logging.error(f"❌ Device  {device.ieee} DOES NOT HAVE cluster {CUSTOM_CLUSTER_ID:#06x}")
                return False

            coordinator = self._app.get_device(nwk=0x0000)
            dst_addr = zdo_types.MultiAddress()
            dst_addr.addrmode = t.AddrMode.IEEE
            dst_addr.ieee = coordinator.ieee
            dst_addr.endpoint = 1

            await device.zdo.Bind_req(device.ieee, endpoint.endpoint_id, CUSTOM_CLUSTER_ID, dst_addr)

            response = await custom_cluster.configure_reporting(
                ATTR_ID_ALL_CURRENTS_STRING,
                REPORTING_MIN_INTERVAL,
                REPORTING_MAX_INTERVAL,
                REPORTABLE_CHANGE_FOR_STRING,
            )
            
            success = self._is_reporting_response_successful(response)
            if success:
                if device.ieee not in self._sensor_listeners:
                    coord_ieee_str = str(self.coordinator_ieee) if self.coordinator_ieee else "UNKNOWN"
                    listener = SensorAttributeListener(device.ieee, custom_cluster, coord_ieee_str)
                    custom_cluster.add_listener(listener)
                    self._sensor_listeners[device.ieee] = listener
                logging.info(f"✅ Successful configuration and listener added for {device.ieee}")
                return True
            else:
                logging.error(f"❌ Reporting configuration failed for {device.ieee}")
                return False
        except Exception as e:
            if not shutdown_event.is_set():
                logging.error(f"❌ EException configuring reporting for {device.ieee}: {e}")
            return False

    def device_initialized(self, device: zigpy_dev.Device):
        if shutdown_event.is_set():
            return
            
        logging.info(f"🎉 DEVICE COMPLETELY INITIALIZED: {device}")
        
        if device.nwk == 0x0000:
            if not self.coordinator_ieee:
                self.coordinator_ieee = device.ieee
                logging.info(f"🏢 Coordinator IEEE set: {self.coordinator_ieee}")
            return
        
        asyncio.create_task(self._handle_device_connection(device))

    def device_left(self, device: zigpy_dev.Device):
        logging.warning(f"👋 DEVICE LEFT THE NETWORK: {device}")
        self._device_reporting_configured.pop(device.ieee, None)
        if device.ieee in self._reconnection_tasks:
            self._reconnection_tasks.pop(device.ieee).cancel()
        
        listener = self._sensor_listeners.pop(device.ieee, None)
        if listener:
            listener.cleanup()

    def connection_lost(self, exc: Exception):
        if shutdown_event.is_set():
            logging.info(f"🔌 Connection closed during orderly shutdown process: {exc}")
        else:
            logging.critical(f"💥 CONNECTION LOST unexpectedly with the coordinator: {exc}", exc_info=True)
            self._device_reporting_configured.clear()
            shutdown_event.set()


# ✅ NEW FUNCTION: SEND BROADCAST PING TO ALL DEVICES
async def send_ping_broadcast_to_all_devices(app_controller, shutdown_evt):
    """
    Sends periodic broadcast pings to all connected devices.
    General ping for everyone.
    """
    logging.info(f"🏓 Starting BROADCAST PING task every {PING_BROADCAST_INTERVAL_SECONDS} seconds")
    
    try:
        while not shutdown_evt.is_set():
            try:
                if app_controller.is_controller_running and not shutdown_evt.is_set():
                    # Get all connected devices (excluding coordinator)
                    connected_devices = []
                    for nwk, device in app_controller.devices.items():
                        if nwk != 0x0000 and device.status == zigpy_dev.Status.ENDPOINTS_INIT:  # Exclude coordinator
                            connected_devices.append(device)
                    
                    if connected_devices:
                        logging.info(f"📡 SENDING BROADCAST PING to {len(connected_devices)} connected devices...")
                        
                        for device in connected_devices:
                            try:
                                # Look for the correct endpoint and cluster
                                endpoint = device.endpoints.get(ESP32_H2_ENDPOINT_ID)
                                if endpoint:
                                    custom_cluster = endpoint.in_clusters.get(CUSTOM_CLUSTER_ID)
                                    if custom_cluster:
                                        # ✅ SEND BROADCAST PING
                                        await custom_cluster.command(
                                            CMD_COORDINATOR_PING_BROADCAST,
                                            manufacturer=None
                                        )
                                        logging.info(f"🏓 PING sent to {device.ieee} (NWK: 0x{device.nwk:04x})")
                                    else:
                                        logging.debug(f"⚠️  Device  {device.ieee} does not have custom cluster")
                                else:
                                    logging.debug(f"⚠️  Device  {device.ieee} does not have endpoint {ESP32_H2_ENDPOINT_ID}")
                                    
                            except Exception as e:
                                logging.warning(f"❌ Error sending ping to {device.ieee}: {e}")
                        
                        logging.info(f"✅ BROADCAST PING completed for {len(connected_devices)} devices")
                    else:
                        logging.debug("ℹ️  No connected devices to ping")
                        
                else:
                    logging.debug("⚠️  Controller is not ready to send pings")
                    
            except Exception as e:
                if not shutdown_evt.is_set():
                    logging.error(f"❌ Error in broadcast ping task: {e}")
            
            # Wait for the specified interval
            try:
                await asyncio.wait_for(shutdown_evt.wait(), timeout=PING_BROADCAST_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                pass  # Normal timeout, continue with the next ping
                
    except asyncio.CancelledError:
        logging.info("🚫 Broadcast ping task cancelled.")


async def periodic_permit_join_task(app_controller, shutdown_evt):
    logging.info("🔄 Starting periodic permit join task.")
    try:
        while not shutdown_evt.is_set():
            try:
                if app_controller.is_controller_running and not shutdown_evt.is_set():
                    logging.info(f"🚪 Opening the network for joining for {PERMIT_JOIN_DURATION_ON_STARTUP} seconds...")
                    await app_controller.permit(PERMIT_JOIN_DURATION_ON_STARTUP)
                    logging.info(f"✅ Network is open. Will reopen in {REOPEN_JOIN_INTERVAL_SECONDS}s")
                else:
                    logging.warning("⚠️  Controller is not ready, skipping network opening.")
            except Exception as e:
                if not shutdown_evt.is_set():
                    logging.error(f"❌ Error in periodic permit task: {e}")
            
            try:
                await asyncio.wait_for(shutdown_evt.wait(), timeout=REOPEN_JOIN_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                pass
    except asyncio.CancelledError:
        logging.info("🚫 Periodic permit join task cancelled.")


async def main():
    if BellowsApplication is None:
        return

    app: Optional[BellowsApplication] = None
    permit_join_task_handle: Optional[asyncio.Task] = None
    ping_broadcast_task_handle: Optional[asyncio.Task] = None  # ✅ New Task
    listener: Optional[MyEventListener] = None

    try:
        bellows_specific_config = {
            zigpy_config.CONF_DEVICE_PATH: DEVICE_PATH,
            zigpy_config.CONF_DEVICE_BAUDRATE: BAUDRATE,
        }
        if FLOW_CONTROL is not None:
            bellows_specific_config[zigpy_config.CONF_DEVICE_FLOW_CONTROL] = FLOW_CONTROL

        final_app_config = BellowsApplication.SCHEMA({
            zigpy_config.CONF_DEVICE: bellows_specific_config,
            zigpy_config.CONF_DATABASE: "zigbee.db",
            zigpy_config.CONF_NWK_BACKUP_ENABLED: True,
            zigpy_config.CONF_OTA: {
                zigpy_config.CONF_OTA_ENABLED: False,
                zigpy_config.CONF_OTA_PROVIDERS: []
            }
        })
        
        app = BellowsApplication(config=final_app_config)
        listener = MyEventListener(app_controller=app)
        app.add_listener(listener)

        await app.startup(auto_form=True)

        if app.state.node_info:
            logging.info(f"🏢 Coordinador IEEE: {app.state.node_info.ieee}, NWK: 0x{app.state.node_info.nwk:04x} [Gateway v{GATEWAY_VERSION}]")
        if app.state.network_info:
            logging.info(f"🌐 Network  EPID: {app.state.network_info.extended_pan_id}, PAN ID: 0x{app.state.network_info.pan_id:04x}, Channel: {app.state.network_info.channel}")

        permit_join_task_handle = asyncio.create_task(periodic_permit_join_task(app, shutdown_event))
        
        # ✅ START BROADCAST PING TASK
        ping_broadcast_task_handle = asyncio.create_task(send_ping_broadcast_to_all_devices(app, shutdown_event))
        
        logging.info("\n🚀 ═══════════════════════════════════════════════════════════")
        logging.info("🎉 ZIGBEE GATEWAY WITH AWS INTEGRATION AND BROADCAST PING STARTED")
        logging.info("📡 Waiting for current sensor data...")
        logging.info("🏓 Sending broadcast pings every 8 seconds to connected devices")
        logging.info("☁️  Data will be sent automatically to AWS")
        logging.info("⌨️  Press Ctrl+C to stop")
        logging.info("🚀 ═══════════════════════════════════════════════════════════\n")
        
        await shutdown_event.wait()

    except (KeyboardInterrupt, asyncio.CancelledError):
        logging.info("\n⏹️  Interruption detected. Initiating shutdown...")
    except Exception as e:
        logging.critical(f"💥  Fatal error in application: {type(e).__name__}: {e}", exc_info=True)
    finally:
        logging.info("🔄 Starting orderly shutdown process...")
        if not shutdown_event.is_set():
            shutdown_event.set()

        # Wait a moment for ongoing tasks to notice the shutdown
        await asyncio.sleep(GRACEFUL_SHUTDOWN_DELAY)

        # Clean up listeners and tasks first
        if listener:
            listener.cleanup_all_listeners()

        # ✅ CANCEL BROADCAST PING TASK
        if ping_broadcast_task_handle and not ping_broadcast_task_handle.done():
            ping_broadcast_task_handle.cancel()
            try:
                await asyncio.wait_for(ping_broadcast_task_handle, timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass

        # Cancel permit join task
        if permit_join_task_handle and not permit_join_task_handle.done():
            permit_join_task_handle.cancel()
            try:
                await asyncio.wait_for(permit_join_task_handle, timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass

        # Application shutdown with RESET restored
        if app and app.is_controller_running:
            try:
                logging.info("🚪 Closing permit join...")
                await asyncio.wait_for(app.permit(0), timeout=3.0)

                # ✅ RESTAURAR EL RESET - ES IMPORTANTE
                if app.state.node_info:
                    logging.info(f"🔄Sending 'RESET' command from coordinator {app.state.node_info.ieee}...")
                    try:
                        await asyncio.wait_for(app._ezsp.reset(), timeout=5.0)
                        await asyncio.sleep(2)  # Wait after reset
                        logging.info("✅ RESET sent successfully")
                    except Exception as reset_error:
                        logging.warning(f"⚠️  Error on RESET (normal during shutdown): {reset_error}")
                
            except (asyncio.TimeoutError, Exception) as e:
                logging.debug(f"⚠️  Error in shutdown operations: {e}")

        if app:
            try:
                logging.info("🔄 Shutting down Zigbee application...")
                await asyncio.wait_for(app.shutdown(), timeout=SHUTDOWN_TIMEOUT)
                logging.info("✅ Application shut down correctly")
            except asyncio.TimeoutError:
                logging.warning("⚠️  Timeout shutting down application - forcing close")
            except Exception as e:
                logging.debug(f"⚠️  Error shutting down application: {e}")

        logging.info("🏁 Script finished.")


def signal_handler_fn(sig, frame):
    logging.info(f"📡 Signal  {signal.Signals(sig).name} received, setting shutdown event...")
    if not shutdown_event.is_set():
        shutdown_event.set()

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler_fn)
    signal.signal(signal.SIGTERM, signal_handler_fn)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("🚫 Cierre forzado por el usuario.")
