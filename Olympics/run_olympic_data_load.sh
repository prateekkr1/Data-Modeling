#!/bin/bash

# Set the name for the virtual environment
VENV_NAME="olympic_env"

# Create a new virtual environment
python3 -m venv $VENV_NAME

# Activate the virtual environment
source $VENV_NAME/bin/activate

# Install requirements
pip install -r requirements.txt

# Run the Olympic data loader script
python olympic_data_loader.py

# Deactivate the virtual environment
deactivate

# Optional: Remove the virtual environment
# rm -rf $VENV_NAME

echo "Olympic data loading process completed."