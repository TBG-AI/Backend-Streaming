# src/config/time_config.py

import time
from datetime import datetime, timedelta

class TimeController:
    def __init__(self, 
                 start_simulation_time: datetime,
                 speed: float = 1.0):
        """
        :param start_simulation_time: The "simulated" time you want to start at.
        :param speed: How fast time proceeds relative to real time. 
                      1.0 means real speed, 2.0 is double speed, etc.
        """
        self.start_simulation_time = start_simulation_time
        self.speed = speed
        
        # Record the real system time when we start
        # so that we can calculate how much real time has passed.
        self.start_real_time = time.time()
        
    def now(self) -> datetime:
        """
        Returns the current simulated time.
        """
        # How many real seconds have passed since start_real_time?
        elapsed_real_seconds = time.time() - self.start_real_time
        
        # Multiply by speed to see how many 'simulated' seconds have passed.
        elapsed_simulated_seconds = elapsed_real_seconds * self.speed
        
        # Convert that to a timedelta
        delta = timedelta(seconds=elapsed_simulated_seconds)
        
        return self.start_simulation_time + delta
    
    def set_speed(self, new_speed: float):
        """
        Adjust the speed of the time flow. For instance, 
        set_speed(2.0) means from now on, time runs at 2x speed.
        """
        # First, figure out what time it is right now (before changing speed).
        current_sim_time = self.now()
        
        # Reset start points so that the new speed setting
        # begins from this moment in both real and simulated time.
        self.start_real_time = time.time()
        self.start_simulation_time = current_sim_time
        
        # Update speed
        self.speed = new_speed
    
    def jump_to(self, new_sim_time: datetime):
        """
        Jump the simulated clock to a specific new time (forward or backward).
        """
        self.start_simulation_time = new_sim_time
        self.start_real_time = time.time()
    
    def freeze(self):
        """
        Freeze the simulated clock. Time will not advance until 
        you call set_speed(...) with a non-zero speed.
        """
        self.set_speed(0.0)
    
    def unfreeze(self, speed: float = 1.0):
        """
        Resume the flow of time with the specified speed (default 1.0).
        """
        self.set_speed(speed)


class TimeConfig:
    """
    Basic configuration class. Reads from environment variables if set,
    otherwise uses defaults.
    """
    USE_REAL_TIME: bool = False
    SIMULATION_START_TIME: datetime = datetime(2024, 9, 28, 15, 00, 0)
    SIMULATION_SPEED: float = 500

    def __init__(self):
        # # Decide if we use real time or simulated time
        # self.USE_REAL_TIME = os.getenv("USE_REAL_TIME", "true").lower() == "true"
        
        # # Default simulated start time (in case we want to run in the 'past')
        # # e.g., "2020-01-01 00:00:00"
        # start_time_str = os.getenv("SIMULATION_START_TIME", "2020-01-01 00:00:00")
        # self.SIMULATION_START_TIME = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
        
        # # Speed factor
        # self.SIMULATION_SPEED = float(os.getenv("SIMULATION_SPEED", "1.0"))

        # Create our time controller or store a reference to real time
        if self.USE_REAL_TIME:
            # Real-time mode: basically the same as time_controller but starts "now" with speed=1
            self.time_controller = TimeController(datetime.now(), speed=1.0)
        else:
            # Use the configured start time & speed
            self.time_controller = TimeController(self.SIMULATION_START_TIME, self.SIMULATION_SPEED)

    def now(self):
        """
        Main method to get the 'current time' for the application.
        If USE_REAL_TIME is True, it's basically real time.
        Otherwise, it's the simulated time from TimeController.
        """
        return self.time_controller.now()

    def freeze_time(self):
        """Convenience method to freeze time if using TimeController."""
        if not self.USE_REAL_TIME:
            self.time_controller.freeze()

    def unfreeze_time(self):
        """Convenience method to unfreeze time if using TimeController."""
        if not self.USE_REAL_TIME:
            self.time_controller.unfreeze()

    def set_speed(self, speed: float):
        """Convenience method to set the speed if using TimeController."""
        if not self.USE_REAL_TIME:
            self.time_controller.set_speed(speed)

    def jump_to(self, new_time: datetime):
        """Jump the simulated time if not using real time."""
        if not self.USE_REAL_TIME:
            self.time_controller.jump_to(new_time)

# Create a single instance of the config that can be imported throughout the app
time_config = TimeConfig()