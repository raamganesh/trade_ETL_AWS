"""
Running the Trade ETL application
"""
import logging
import logging.config

import yaml

def main():
    """
    Entry point to run the trade ETL job
    """

    #parsing the YAML file
    config_path = r"D:\doc\workspace\ETL\TradeETL\trade_ETL_AWS\configs\tradeETL_report1_config.yml"
    config = yaml.safe_load(open(config_path))
    
    # configure logging
    log_config = config['logging']
    logging.config.dictConfig( log_config )
    logger = logging.getLogger( __name__ )
    logger.info("testing logging !!")

if __name__ == '__main__':
    main()