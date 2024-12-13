# navcanada-scraper

This project is a Python script that fetches upper winds data from the Nav Canada API, parses the data, and stores it in a MongoDB database.
It runs on a schedule every day at 06:00.
It stores AM and PM data in a JSON object on the mongoDB.

It currently fetches data for the CYYU (Kapuskasing Airport, northen ontario) weather station

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
