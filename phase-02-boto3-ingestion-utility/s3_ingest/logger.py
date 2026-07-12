import logging

def setup_logger(name="s3_ingest", log_file=r"C:\Users\hp\Desktop\retailflow-capstone\phase-02-boto3-ingestion-utility\app.log"):
    """Sets up a logger that records activity to both a file and the console."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers if initialized multiple times
    if not logger.handlers:
        logger.propagate = False
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')

        # 1. File Handler (Appends logs directly to app.log)
        file_handler = logging.FileHandler(log_file, mode='a')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # 2. Console Handler (Prints logs directly to your terminal screen)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger

# Create a single, shared logger instance for the package
logger = setup_logger()