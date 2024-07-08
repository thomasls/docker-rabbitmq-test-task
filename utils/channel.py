import pika, os

QUE_NAME = os.environ.get('QUE_NAME', 'process_msg')
AMQP_URL = os.environ.get('AMQP_URL', 'amqp://rabbitmq?connection_attempts=5&retry_delay=5')

# Get the RabbitMQ connection and channel 
def get_channel():
    # Parse AMQP_URL   
    params = pika.URLParameters(AMQP_URL)
    params.socket_timeout = 5

    connection = pika.BlockingConnection(params) # Connect to AMQP
    channel = connection.channel() # Start the channel
    channel.queue_declare(queue=QUE_NAME) # Declare the queue

    return channel, connection

# Publish the message on the channel
def publish_message(channel, msg):
    channel.basic_publish(exchange='', routing_key=QUE_NAME, body=msg)
