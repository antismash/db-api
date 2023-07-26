"""Handler for calls involving the background job runner"""
import enum
import uuid

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.postgresql import JSONB

from .models import db


class JobType(enum.Enum):
    """The different available job types"""
    COMPARIPPSON = "comparippson"
    CLUSTERBLAST = "clusterblast"


class Job(db.Model):
    __tablename__ = "jobs"
    __table_args__ = {"schema": "asdb_jobs"}

    id = db.Column(db.Text, primary_key=True, server_default=db.FetchedValue())
    jobtype = db.Column(db.Text)
    status = db.Column(db.Text)
    runner = db.Column(db.Text)
    data = db.Column(JSONB)
    results = db.Column(JSONB)
    version = db.Column(db.Integer)


def dispatchBlast(jobtype: JobType, name: str, sequence: str) -> Job:
    """Dispatch a blast-style job"""
    if jobtype not in (JobType.CLUSTERBLAST, JobType.COMPARIPPSON):
        raise ValueError(f"job type ${jobtype} not supported")

    job_data = {
        "name": name,
        "sequence": sequence,
    }

    job = Job(
        id=str(uuid.uuid4()),
        jobtype=jobtype.value,
        status="pending",
        data=job_data,
        results={},
        version=1,
    )
    db.session.add(job)
    db.session.commit()

    return job
