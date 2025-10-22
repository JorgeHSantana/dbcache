class SchemaMismatchError(Exception):
    """Levanta quando o conjunto de colunas solicitado difere do schema salvo no cache."""
    pass
