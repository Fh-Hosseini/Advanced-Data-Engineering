#!/bin/bash
#python -m unittest ./project/tests.py
python -m unittest discover -s ./project -p 'tests.py'
