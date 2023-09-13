"""Handler for calls involving the background job runner"""

from dataclasses import dataclass, asdict
from datetime import datetime
import enum
import uuid

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.postgresql import JSONB

from .models import db


class JobType(enum.Enum):
    """The different available job types"""
    COMPARIPPSON = "comparippson"
    CLUSTERBLAST = "clusterblast"
    STOREDQUERY = "storedquery"


class Job(db.Model):
    __tablename__ = "jobs"
    __table_args__ = {"schema": "asdb_jobs"}

    id = db.Column(db.Text, primary_key=True, server_default=db.FetchedValue())
    jobtype = db.Column(db.Text)
    status = db.Column(db.Text)
    runner = db.Column(db.Text)
    submitted_date = db.Column(db.Date)
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
        submitted_date=datetime.utcnow(),
        results={ "hits": []},
        version=1,
    )
    db.session.add(job)
    db.session.commit()

    return job


def dispatchStoredQuery(ids: list[int], search_type: str, return_type: str) -> Job:
    """Dispatch a stored query job"""
    job_id = str(uuid.uuid4())

    if search_type == "cluster":
        search_type = "region"

    data = StoredQueryInput(job_id, ids, search_type, return_type)

    job = Job(
        id=job_id,
        jobtype=JobType.STOREDQUERY.value,
        status="pending",
        submitted_date=datetime.utcnow(),
        data=data.to_json(),
        results="",
        version=1,
    )
    db.session.add(job)
    db.session.commit()

    return job


@dataclass
class StoredQueryInput():
    job_id: str
    ids: list[int]
    search_type: str
    return_type: str

    def to_json(self):
        return asdict(self)
