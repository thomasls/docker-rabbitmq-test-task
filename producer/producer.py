#
# A simple RabbitMQ producer
#
# This creates an exchange named "exchange", then publishes a message every 5 seconds.
#
import uuid
import json
import datetime;
from channel import get_channel, publish_message
from functools import partial
from logger import get_logger

# Delay between sending messages
DELAY = os.environ.get('DELAY', '5')

log = get_logger()

# Create a test message with message Id and timestamp
def create_test_message():
    return json.dumps({
        "message_id": str(uuid.uuid4()),
        "created_on": str(datetime.datetime.now())
    })

# --------------- MAIN METHOD --------------------------------------
def main():
    # Establish the RabbitMQ connection and get the channel
    _channel, _connection = get_channel()

    # Main loop...  This will run forever
    try:
        _connection.ioloop.start()
    except KeyboardInterrupt:
        # Close the connection
        _connection.close()
        _connection.ioloop.start()

    # Send the message to the queue
    publish_message(_channel, create_test_message())
    log.info("Message sent to consumer")

    """

    This function also registers itself as a timeout function, so the
    main :mod:`pika` loop will call this function again every 5 seconds.

    """
    _channel.connection.add_timeout(int(DELAY), partial(publish_message, _channel, create_test_message()))

if __name__ == '__main__':
    main()
    

