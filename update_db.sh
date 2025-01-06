#!/bin/bash
echo "Uninstalling existing db package..."
pip uninstall -y db
echo "Installing fresh db package..."
pip install -r requirements.txt --no-cache-dir git+https://github.com/TBG-AI/Database.git@refactor#egg=db