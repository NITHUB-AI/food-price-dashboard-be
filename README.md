# Food Price Dashboard Backend

## Introduction

This is the backend for the web app of the food price dashboard. It provides an API for the front-end to consume.

## Installation

To run this Flask app, first clone this repo

```bash
git clone https://github.com/NITHUB-AI/food-price-dashboard-be.git
```

After cloning the repo, navigate to the folder and create a virtual environment and activate it

```bash
cd food-price-dashboard-be
python3 -m venv env
source env/bin/activate
```

Then, install the dependencies

```bash
pip install -r requriements.txt
```

Remember to create a `.env` file to put in the environment variable before running the application. What the `.env` file should contain is defined in the `.env.example` file.

## Usage

The API endpoints are defined in the `app.py` file located in the `src` folder. To run the app:

```bash
flask --app src/app run
```

For auto-reloading,

```bash
flask --app src/app --debug run
```
