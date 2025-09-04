#!/bin/bash
set -e

# update pip và wheel để chắc chắn không bị lỗi biên dịch
pip install --upgrade pip setuptools wheel

# cài dependencies
pip install -r requirements.txt

# chạy bot
exec python crypto_research_bot_final.py
