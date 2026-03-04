# navcanada-scraper

This project is a Python script that fetches upper winds data from the Nav Canada API, parses the data, and stores it in a MongoDB database.
It runs on a schedule every day at 02:00, 08:00, 14:00 and 20:00. This matches the new data update frequency of navcanada with some buffer to allow the API to be updated
It stores AM, PM and NIGHT data in a JSON object on the mongoDB.

It currently fetches data for the CYYU (Kapuskasing Airport, northen ontario) weather station and CYUL,CYTF,CYFC by default. To add or remove stations, simply add or remove the ICAO codes from the service's environment variables in the docker-compose.yml file

This was designed to run on a rpi4 running ubuntu server and docker. The official MongoDB does not support this hardware, thus I am using a unofficial mongo image: https://github.com/themattman/mongodb-raspberrypi-binaries?tab=readme-ov-file
How to use this custom image is described in the github repo linked

## Requirements

- Docker

## Setup

1. Clone the repository:
    ```sh
    git clone https://github.com/yourusername/navcanada-scraper.git
    cd navcanada-scraper
    ```

2. Start Docker containers:
    ```sh
    docker-compose up --build -d
    ```
