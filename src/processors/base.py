from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from ..config import Config

class BaseProcessor(ABC):
    def __init__(self, config: Config):
        self.config = config

    @abstractmethod
    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single record and return the modified record.
        
        Args:
            record: The input record to process
            
        Returns:
            The processed record
        """
        pass

    def cleanup(self):
        """
        Optional cleanup method to be called when processing is complete.
        Useful for closing file handles, connections, etc.
        """
        pass 