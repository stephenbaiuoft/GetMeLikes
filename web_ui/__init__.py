from flask import Flask

webapp = Flask(__name__)

# need from app import main to have the main function being copied here
from web_ui import web_host
