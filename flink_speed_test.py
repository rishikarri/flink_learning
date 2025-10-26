"""
Flink example
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingProcessingTimeWindows, Time
from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types as FlinkTypes

# 1. SETUP THE EXECUTION ENVIRONMENT
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# 2. DEFINE THE DATA SOURCE
# Data with more high speed events for testing stateful processing
car_data = [
    ('sensor_A', 50),   # Normal
    ('sensor_B', 75),   # High speed #1
    ('sensor_A', 55),   # Normal
    ('sensor_B', 80),   # High speed #2 - should trigger alert
    ('sensor_C', 40),   # Low speed
    ('sensor_B', 85),   # High speed #3 - another alert
    ('sensor_A', 60),   # Normal
    ('sensor_C', 45),   # Low speed
    ('sensor_A', 78),   # High speed #1 for sensor_A
    ('sensor_A', 82),   # High speed #2 for sensor_A - should trigger alert
    ('sensor_B', 50),   # Normal - resets counter
    ('sensor_B', 90),   # High speed #1 again
]

# Create a data stream from the collection
ds = env.from_collection(
    collection=car_data,
    type_info=Types.TUPLE([Types.STRING(), Types.INT()])
)

print("=== STATEFUL FLINK STREAM PROCESSOR ===")

# 3. STATEFUL STREAM PROCESSOR
class HighSpeedTracker(KeyedProcessFunction):
    """Stateful processor that tracks consecutive high speeds per sensor"""
    
    def __init__(self):
        self.consecutive_high_speeds = None
        self.alert_threshold = 2  # Alert after 2 consecutive high speeds
    
    def open(self, runtime_context):
        # Initialize state for each sensor
        state_descriptor = ValueStateDescriptor("consecutive_high_speeds", FlinkTypes.INT())
        self.consecutive_high_speeds = runtime_context.get_state(state_descriptor)
    
    def process_element(self, value, ctx):
        sensor_id, speed = value[0], value[1]
        
        # Determine speed status
        if speed > 70:
            status = "HIGH_SPEED"
        elif speed < 50:
            status = "LOW_SPEED"
        else:
            status = "NORMAL"
        
        # Get current state
        current_count = self.consecutive_high_speeds.value()
        if current_count is None:
            current_count = 0
        
        # Update state based on speed
        if status == "HIGH_SPEED":
            current_count += 1
            self.consecutive_high_speeds.update(current_count)
            
            # Check for alert condition
            if current_count >= self.alert_threshold:
                alert_msg = f"🚨 ALERT: {sensor_id} has {current_count} consecutive high speeds! (Speed: {speed})"
                print(alert_msg)
                return (sensor_id, speed, status, f"ALERT_{current_count}")
            else:
                return (sensor_id, speed, status, f"WARNING_{current_count}")
        else:
            # Reset counter for normal/low speeds
            self.consecutive_high_speeds.update(0)
            return (sensor_id, speed, status, "NORMAL")

# 4. APPLY STREAM PROCESSING
print("1. All sensor data:")
ds.print()

print("\n2. Stateful processing - tracking consecutive high speeds:")
# Key by sensor_id for stateful processing
keyed_stream = ds.key_by(lambda x: x[0])
processed_stream = keyed_stream.process(HighSpeedTracker())
processed_stream.print()

# 5. EXECUTE
print("\n=== EXECUTING FLINK JOB ===")
env.execute("StatefulFlinkExample")

