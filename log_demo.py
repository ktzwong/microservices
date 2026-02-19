import logging 

logging.basicConfig(level=logging.INFO, filename='log_demo.log', filemode='w',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


# logging.debug("Debug"):
# logging.info("Info"):
# logging.warning("Warning"):
# logging.error("Error"):
# logging.critical("Critical"):


# Temperature = 2 

# logging.info('Temperture ={}'.format(Temperature))

# try:
#     1/0
# except ZeroDivisionError as e:
#     logging.exception(e)

logger = logging.getLogger(__name__)
handler = logging.FileHandler('log_demo_new.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

logger.info('Logging started')