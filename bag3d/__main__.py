import sys
import os
import yaml
import logging

from bag3d import app

def main():
    here = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(here, 'logging.cfg'), 'r') as f:
        log_conf = yaml.safe_load(f)
    logging.config.dictConfig(log_conf)
    
    app.app(sys.argv, here)

if __name__ == '__main__':
    main()