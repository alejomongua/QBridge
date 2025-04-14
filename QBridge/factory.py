from typing import TypedDict, Literal

from .rabbitmq_client import RabbitMQClient
from .base import QueueClient

class RabbitMQConfig(TypedDict):
    host: str
    username: str
    password: str
    queue_name: str

ServiceType = Literal['rabbitmq', 'sqs', 'zmq']

def get_queue_client(service_type: ServiceType, **kwargs: str) -> QueueClient:
    """
    Factory method to get a queue client based on service_type.
    
    Args:
        service_type (ServiceType): The type of queue service to use ('rabbitmq', 'sqs', 'zmq')
        kwargs (RabbitMQConfig): Configuration parameters for the RabbitMQ client containing:
            - host (str): The RabbitMQ server hostname
            - username (str): The RabbitMQ username
            - password (str): The RabbitMQ password
            - queue_name (str): The name of the queue to use
    
    Returns:
        QueueClient: An instance of a concrete QueueClient implementation

    Raises:
        ValueError: If the service_type is not supported
        NotImplementedError: If the service is planned but not yet implemented
        KeyError: If required configuration parameters are missing
    """
    if service_type.lower() == 'rabbitmq':
        # Validate all required RabbitMQ parameters are present
        required_params = {'host', 'username', 'password', 'queue_name'}
        if not all(param in kwargs for param in required_params):
            missing = required_params - kwargs.keys()
            raise KeyError(f"Missing required RabbitMQ parameters: {missing}")
            
        config: RabbitMQConfig = {
            'host': kwargs['host'],
            'username': kwargs['username'],
            'password': kwargs['password'],
            'queue_name': kwargs['queue_name']
        }
        return RabbitMQClient(**config)
    elif service_type.lower() == 'sqs':
        # Placeholder for SQSClient implementation
        raise NotImplementedError("SQS client not yet implemented")
    elif service_type.lower() == 'zmq':
        # Placeholder for ZMQClient implementation
        raise NotImplementedError("ZMQ client not yet implemented")
    else:
        raise ValueError("Unsupported messaging service type")
