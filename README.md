# Text preprocessing Script

Script written in Python 3.6.5

## Requirements:

* Docker
* Docker-Compose

## How to run:

To run the script as a Docker service, clone this repo in your working directory and run the following commands:

Build the image:

``` $ docker-compose build ```

This will automatically download all dependencies and install the **NLTK** package (may take a minute). The image size is around 4gb.

Start service:

``` $ docker-compose up```

## How it works:

Running this services will launch the Preprocessing Script and the RabbitMQ.

The Preprocessing script will wait until RabbitMQ is running and then stablishes the connection and beging consuming messages.
Each message recived is in the format defined in the Wiki (Arquitectura de Sistema).

After preprocessing each document in the recived message, sends an http POST request to http://procesamiento:8000/ldamodel/
