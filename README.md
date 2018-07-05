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
