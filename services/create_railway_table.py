"""Cria a tabela eos_metrics no PostgreSQL Railway.

Uso:
  python -m services.create_railway_table
"""

from services.reporte_service import ensure_table_exists


if __name__ == "__main__":
    ensure_table_exists()
    print("Tabela eos_metrics validada/criada com sucesso no Railway.")
