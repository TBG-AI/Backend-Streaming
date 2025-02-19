from datetime import datetime, timedelta
import logging
import pytz
from backend_streaming.providers.whoscored.domain.batch_schedule import BatchSchedule
from backend_streaming.deploy.monitoring.alerts.email_config import send_alert
from typing import List

class BatchScheduleMonitor:
    def __init__(self):
        self.logger = logging.getLogger('batch_monitor')

    def check_batch_schedule(self):
        """Check if batch_end is approaching"""
        current_batch = BatchSchedule.get_current()
        print(f"Current batch: {current_batch}")
        if not current_batch:
            self.alert("No active batch schedule found!")
            return

        # all scheduling is done in UTC time
        if current_batch.batch_end < datetime.now(pytz.UTC):
            self.alert(
                f"Batch Finished at {current_batch.batch_end}!\n"
                f"Action Required: Run schedule_batch.sh"
            )

    def alert(self, message: str):
        send_alert(message)
