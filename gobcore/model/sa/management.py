"""Management

SQLAlchemy Management Models

"""
from sqlalchemy import Column, DateTime, Integer, JSON, String, Boolean
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Log(Base):
    """Log

    Class that holds GOB log messages

    """
    __tablename__ = 'logs'

    logid = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    process_id = Column(String)
    source = Column(String)
    application = Column(String)
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
    """Service

    Class that holds a GOB Service (e.g. Upload, Import, ...)
    """
    __tablename__ = "services"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    is_alive = Column(Boolean)
    timestamp = Column(DateTime)

    def __repr__(self):
        return f'<Service {self.name}>'


class ServiceTask(Base):
    """ServiceTask

    Class that holds a task within a GOB Service (e.g. Mainloop)

    """
    __tablename__ = "service_tasks"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    service_name = Column(String)
    is_alive = Column(Boolean)

    def __repr__(self):
        return f'<ServiceTask {self.service_name}:{self.name}>'
