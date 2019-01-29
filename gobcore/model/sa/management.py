"""Management

SQLAlchemy Management Models

"""
from sqlalchemy import Column, DateTime, Integer, JSON, String, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Log(Base):
    """Log

    Class that holds GOB log messages

    """
    __tablename__ = 'logs'

    logid = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    process_id = Column(String, index=True)
    source = Column(String, index=True)
    application = Column(String, index=True)
    destination = Column(String, index=True)
    catalogue = Column(String, index=True)
    entity = Column(String, index=True)
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

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    host = Column(String)
    pid = Column(Integer)
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
    service_id = Column(ForeignKey(Service.id))
    service_name = Column(String) # remove this later; https://github.com/Amsterdam/GOB-Core/issues/125

    is_alive = Column(Boolean)

    def __repr__(self):
        return f'<ServiceTask {self.service_name}:{self.name}>'
