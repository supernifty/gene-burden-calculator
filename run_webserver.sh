#!/bin/bash
echo "Starting up Flask in debug mode on port 5000"
export FLASK_APP="main.py"
export FLASK_DEBUG=1
flask run --host 0.0.0.0
