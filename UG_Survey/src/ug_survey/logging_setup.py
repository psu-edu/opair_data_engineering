import logging
import pathlib

def setup_logging(name: str, log_dir: str, level: str = "INFO"):
    # default/normalize
    level = (level or "INFO").upper()
    lvl = getattr(logging, level, logging.INFO)

    log_path = pathlib.Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(lvl)

    # avoid duplicate handlers if called multiple times
    if not logger.handlers:
        fh = logging.FileHandler(log_path / f"{name.lower()}.log", encoding="utf-8")
        sh = logging.StreamHandler()
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
        fh.setFormatter(fmt); sh.setFormatter(fmt)
        logger.addHandler(fh); logger.addHandler(sh)

    return logger
