from jobman import sql

import distributed_jobman

from abstract_scheduler import AbstractScheduler
from spearmint.utils.database.jobmandb import get_table_name, JobmanDB
from spearmint.utils.jobman_helpers import status_converter


def init(*args, **kwargs):
    return JobmanScheduler(*args, **kwargs)


class JobmanScheduler(AbstractScheduler):

    """
    Parameters
    ----------

    gpu: boolean

    duree: string
        ex : 48:0:0

    mem: string
        ex: 2G

    env: string
        ex: THEANO_FLAGS=floatX=float32,device=gpu,force_device=True
    """

    def __init__(self, options):
        self.options = options
        self.jobmandb = JobmanDB(options)

        self.experiment_registered = False

    def submit(self, job_id, experiment_name, experiment_dir,
               database_address):

        if not self.experiment_registered:
            options = self.options

            # Saves an experiment for jobman-scheduler's observer
            distributed_jobman.save_experiment(
                options["experiment_name"],
                get_table_name(experiment_name, "jobs"), options["clusters"],
                options["duree"], options["mem"], options.get("env", ""),
                options.get("gpu", ""))

            self.experiment_registered = True

        return job_id

    def alive(self, job_id):

        job = self.jobmandb.load(self.options["experiment_name"], "jobs",
                                 dict(id=job_id))

        # update spearmint status
        new_status = status_converter[job['jobman']['status']]
        if new_status != job['proc_status']:
            job['proc_status'] = new_status
            self.jobmandb.save(job, self.options["experiment_name"], "jobs")

        return job["jobman"]["status"] in [sql.START, sql.RUNNING]
