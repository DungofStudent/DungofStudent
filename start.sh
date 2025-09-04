#!/bin/bash
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
exec python crypto_research_bot_final.py
