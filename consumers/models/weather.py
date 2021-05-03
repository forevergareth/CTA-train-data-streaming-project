"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""

        if  message.topic() == 'org.chicago.cta.weather.v1':
            weather_value = message.value()

            self.temperature = weather_value['temperature']
            self.status = weather_value['status']

            logger.debug(f"Current weather {self.temperature} - {self.status}")
