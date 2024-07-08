# RabbitMQ Producer-Consumer Sample Application
A simple Python application created as per the requirements in Python_Docker_RabbitMQ Test Task.pdf

This program runs [RabbitMQ](http://rabbitmq.com/) and two small Python programs (producer and consumer), 
all in a single Docker environment. The `producer` program publishes a message 
into RabbitMQ every 5 seconds; the `consumer` program logs every message it recevies to
its console.

## 🚀 Install

[Install Docker](https://docs.docker.com/engine/installation/), then
run `docker-compose up`. 

Once it's all up, you should see the producer and consumer logging messages on their console.  
You can also point a browser at http://localhost:15672/ (guest/guest) to see the RabbitMQ
management console.  

## 🚀 Development

Pre requisite: Python 3

After installing Python 3 (latest version), just run the following command at the root of this project:

```sh
virtualenv venv
. venv/bin/activate
pip install -r requirements.txt

```
## 🚀 Usage - Consumer
Just run the following command at the root of your project:

```sh
export AMQP_URL=amqp://localhost
python consumer/consumer.py

```
## ✨ Producer
Just run the following command at the root of your project:

```sh
export AMQP_URL=amqp://localhost
python producer/producer.py
```

The new producer will start sending messages all of which will appear on the consumer's output.

## 📝 Logs
Application logs can be found in this file 'test-task.log' in the project directory.

## 📝 License
This project is NOT licensed!! Use it on your will. Thank you!
