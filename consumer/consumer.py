#
# RabbitMQ consumer
#
import time
from utils.channel import QUE_NAME, get_channel
from utils.logger import get_logger

log = get_logger()

# Process the message received from the queue
def process_message(msg):
  log.info("Received message: " + str(msg))

  time.sleep(5) # delays for 5 seconds
  log.info("Processing finished")
  return;

# Callbackfunction which is called on incoming messages
def callback(ch, method, properties, body):
  process_message(body)

# --------------- MAIN METHOD --------------------------------------
def main():
  # Establish the RabbitMQ connection and get the channel
  _channel, _connection = get_channel()
      
  # Set up subscription on the queue
  _channel.basic_consume(QUE_NAME, callback, auto_ack=True)

  # Start consuming messages (blocks main thread)
  _channel.start_consuming()

  connection.close()

if __name__ == '__main__':
    main()