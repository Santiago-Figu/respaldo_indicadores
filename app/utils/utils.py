from pathlib import Path


def find_dotenv():
    """
    Busca recursivamente el archivo .env en caso de que se necesite.
    Esta función solo la dejo como utilidad, 
    pero no es necesaria ya que se usa el BaseSettings
    """
    current = Path(__file__).resolve()
    while current != current.parent:
        if (current / '.env').exists():
            return current / '.env'
        current = current.parent
    return None