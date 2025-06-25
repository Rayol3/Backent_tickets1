import os

class Config:
    # Lista de URIs para failover dinámico (todas deben tener la misma estructura)
    DATABASE_URIS = [
        'mysql+pymysql://root:root@25.40.94.84:3307/venta_tickets?charset=utf8mb4&connect_timeout=3',
        'mysql+pymysql://root:root@25.40.94.84:3308/venta_tickets?charset=utf8mb4&connect_timeout=3',
        'mysql+pymysql://root:root@25.40.94.84:3310/venta_tickets?charset=utf8mb4&connect_timeout=3'
    ]

    # Configuración para binds (sedes y compradores)
    SQLALCHEMY_BINDS = {
        'db2': 'mysql+pymysql://root:root@25.40.94.84:3308/venta_tickets?charset=utf8mb4&connect_timeout=3',
        'db3': 'mysql+pymysql://root:root@25.40.94.84:3310/venta_tickets?charset=utf8mb4&connect_timeout=3'
    }

    # Configuraciones adicionales
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {
        'pool_pre_ping': True,
        'pool_recycle': 3600,
        'connect_args': {
            'connect_timeout': 10,
            'read_timeout': 30,
            'write_timeout': 30
        }
    }