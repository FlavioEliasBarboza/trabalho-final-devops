def grava_log(filename, msg):
    import logging

    log_format = '%(asctime)s:%(levelname)s:%(filename)s: %(message)s'
    logging.basicConfig(filename=filename,
                        filemode='w',
                        level=logging.DEBUG,
                        format=log_format)
    logger = logging.getLogger('root')


    logger.info(f'{msg}')