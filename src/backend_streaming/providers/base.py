import json

from abc import ABC, abstractmethod
from typing import Any, Tuple

class BaseProvider(ABC):
    @abstractmethod
    def __init__(self, game_id: int):
        pass
    
    @abstractmethod
    def is_finished(self) -> bool:
        pass
        
    @abstractmethod
    def get_live_events(self) -> Any:
        pass

    @abstractmethod
    def process_events(self, events: json) -> Tuple[int, int]:
        """
        Given json events, do the following:
            - parse into correct fields (refer to events table schema)
            - insert into events table and return start, end ids
        """
        pass
