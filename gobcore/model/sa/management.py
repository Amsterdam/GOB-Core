from sqlalchemy import Column, DateTime, Integer, JSON, String, Boolean
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Log(Base):
    __tablename__ = 'logs'

    logid = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    process_id = Column(String)
    source = Column(String)
    destination = Column(String)
    catalogue = Column(String)
    entity = Column(String)
    level = Column(String)
    name = Column(String)
    msgid = Column("id", String)
    msg = Column(String)
    data = Column(JSON)

    def __repr__(self):
        return f'<Msg {self.msg}>'


class Service(Base):
    __tablename__ = "services"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    is_alive = Column(Boolean)
    timestamp = Column(DateTime)

    def __repr__(self):
        return f'<Service {self.name}>'


class ServiceTask(Base):
    __tablename__ = "service_tasks"

    id = Column(Integer, primary_key=True)
    service_name = Column(String)
    name = Column(String)
    is_alive = Column(Boolean)

    def __repr__(self):
        return f'<ServiceTask {self.name}:{self.service_name}>'
