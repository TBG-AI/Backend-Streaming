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
        """Check batch schedule status"""
        current_batch = BatchSchedule.get_current()
        subject = f"BATCH ALERTS"
        

        if not current_batch:
            send_alert(
                subject=subject,
                content="No active batch schedule found!"
            )
            return

        # Check if this is a new batch that we haven't notified about
        if not current_batch.notified:
            send_alert(
                subject=subject,
                content=f"New Batch Scheduled\n"
                f"Start Date: {current_batch.batch_start.strftime('%Y-%m-%d')}\n"
                f"End Date: {current_batch.batch_end.strftime('%Y-%m-%d')}\n"
                f"Days Window: {(current_batch.batch_end - current_batch.batch_start).days}"
            )
            # Mark as notified and save
            current_batch.notified = True
            current_batch.save()
