"""Management

SQLAlchemy Management Models

"""
from sqlalchemy import Column, DateTime, Integer, JSON, ARRAY, String, Boolean, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Job(Base):
    """Job

    Class that holds GOB jobs

    Jobs are started by the workflow manager.
    The workflow manager controls and monitors the progress of jobs

    """
    __tablename__ = 'jobs'

    id = Column(Integer, primary_key=True, doc="Internal primary key", index=True)
    name = Column(String, doc="e.g. import.data/metingen.json.2020-01-20T12:43:18.005")
    type = Column(String, doc="import, export, ...", index=True)
    args = Column(ARRAY(String), doc="whatever was passed as argument when the job was started")
    start = Column(DateTime, doc="Time when the job was started", index=True)
    end = Column(DateTime, doc="Time when the job has ended", index=True)
    status = Column(String, doc="started, paused, waiting, ended, ...", index=True)

    def __repr__(self):
        return f'<Job {self.name}>'


class JobStep(Base):
    """Job

    Class that holds GOB job steps

    """
    __tablename__ = 'jobsteps'

    id = Column(Integer, primary_key=True, doc="Internal primary key", index=True)
    jobid = Column(ForeignKey(Job.id), index=True)
    name = Column(String, doc="compare, upload, enrich")
    start = Column(DateTime, doc="Time when the job step was started", index=True)
    end = Column(DateTime, doc="Time when the job step has ended", index=True)
    status = Column(String, doc="started, paused, waiting, ended, ...", index=True)

    def __repr__(self):
        return f'<Job {self.name}>'


class Log(Base):
    """Log

    Class that holds GOB log messages

    """
    __tablename__ = 'logs'

    logid = Column(Integer, primary_key=True, index=True)
    jobid = Column(ForeignKey(Job.id), index=True)
    stepid = Column(ForeignKey(JobStep.id), index=True)
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


Index("ix_logs_logid_desc", Log.logid.desc())


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


Index("ix_timestamp_desc", Service.timestamp.desc())


class ServiceTask(Base):
    """ServiceTask

    Class that holds a task within a GOB Service (e.g. Mainloop)

    """
    __tablename__ = "service_tasks"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    service_id = Column(ForeignKey(Service.id))
    service_name = Column(String)  # remove this later; https://github.com/Amsterdam/GOB-Core/issues/125

    is_alive = Column(Boolean)

    def __repr__(self):
        return f'<ServiceTask {self.service_name}:{self.name}>'
