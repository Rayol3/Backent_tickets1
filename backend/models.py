from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class Concierto(db.Model):
    __bind_key__ = None  # Base de datos principal (3307)
    __tablename__ = 'conciertos'
    
    id_concierto = db.Column(db.Integer, primary_key=True)
    id_sede = db.Column(db.Integer, nullable=False)
    artista = db.Column(db.String(100), nullable=False)
    fecha = db.Column(db.DateTime)
    
    def __repr__(self):
        return f'<Concierto {self.id_concierto}: {self.artista}>'

class Sede(db.Model):
    __bind_key__ = 'db2'  # Segunda base de datos (3308)
    __tablename__ = 'sedes'
    
    id_sede = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(50), nullable=False)
    
    def __repr__(self):
        return f'<Sede {self.id_sede}: {self.nombre}>'

class Comprador(db.Model):
    __bind_key__ = 'db3'  # Tercera base de datos (3310)
    __tablename__ = 'compradores'
    
    id_comprador = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(100), nullable=False)
    
    def __repr__(self):
        return f'<Comprador {self.id_comprador}: {self.nombre}>'

class Ticket(db.Model):
    __bind_key__ = None  # Misma DB que conciertos (3307)
    __tablename__ = 'tickets'
    
    id_ticket = db.Column(db.Integer, primary_key=True)
    id_concierto = db.Column(db.Integer, nullable=False)
    id_comprador = db.Column(db.Integer, nullable=False)
    asiento = db.Column(db.String(10), nullable=False)
    precio = db.Column(db.Numeric(8, 2), nullable=False)
    fecha_compra = db.Column(db.DateTime)
    
    def __repr__(self):
        return f'<Ticket {self.id_ticket}: Concierto {self.id_concierto}, Asiento {self.asiento}>'