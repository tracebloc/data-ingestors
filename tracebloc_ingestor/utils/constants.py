"""
Constants used throughout the application.
"""

# API Constants
API_TIMEOUT = 1500

# Intent Constants
class Intent:
    """
    Enumeration of supported intents for model training and classification.
    """
    TEST = "test"
    TRAIN = "train"
    
    @classmethod
    def get_all_intents(cls) -> list[str]:
        """
        Returns a list of all available intent values.
        """
        return [cls.TEST, cls.TRAIN]
        

# Data Categories
class DataCategory:
    """
    Enumeration of supported data categories for model training and classification.
    """
    IMAGE_CLASSIFICATION = "image_classification"
    OBJECT_DETECTION = "object_detection"
    KEYPOINT_DETECTION = "keypoint_detection"
    TEXT_CLASSIFICATION = "text_classification"
    TABULAR_CLASSIFICATION = "tabular_classification"

    @classmethod
    def get_all_categories(cls) -> list[str]:
        """
        Returns a list of all available category values.
        
        Returns:
            list[str]: List of all category values
        """
        return [
            cls.IMAGE_CLASSIFICATION,
            cls.OBJECT_DETECTION,
            cls.KEYPOINT_DETECTION,
            cls.TEXT_CLASSIFICATION,
            cls.TABULAR_CLASSIFICATION
        ]

    @classmethod
    def is_valid_category(cls, category: str) -> bool:
        """
        Check if a given category is valid.
        
        Args:
            category: The category string to validate
            
        Returns:
            bool: True if category is valid, False otherwise
        """
        return category in cls.get_all_categories() 