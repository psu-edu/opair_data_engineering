from .db import exec_scalar

def table_count(conn, three_part_name: str) -> int:
    return int(exec_scalar(conn, f"SELECT COUNT(*) FROM {three_part_name}") or 0)

def term_count(conn, three_part_name: str) -> int:
    return int(exec_scalar(conn, f"SELECT COUNT(DISTINCT term) FROM {three_part_name}") or 0)
