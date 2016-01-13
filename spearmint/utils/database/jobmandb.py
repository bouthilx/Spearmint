import copy

from jobman import sql
from jobman.tools import flatten, expand, DD
import distributed_jobman
import numpy

from abstractdb import AbstractDB


table_name_template = "%(experiment_name)s_%(experiment_field)s"


def init(options):
    return JobmanDB(options)


class JobmanDB(AbstractDB):

    def __init__(self, options):
        pass

    def save(self, spearmint_job, experiment_name, experiment_field,
             field_filters=None):

        table_name = get_table_name(experiment_name, experiment_field)

        sql_row = distributed_jobman.save_job(
            table_name, convert_spearmint_to_jobman_sql(spearmint_job))

        return convert_jobman_sql_to_spearmint(sql_row)

    def load(self, experiment_name, experiment_field, field_filters=None):

        table_name = get_table_name(experiment_name, experiment_field)

        if field_filters is not None and "id" in field_filters:
            job_id = field_filters.pop('id')
        else:
            job_id = None

        sql_jobs = distributed_jobman.load_jobs(
            table_name, filter_eq_dct=field_filters, job_id=job_id)

        spearmint_job_dicts = []
        for job in sql_jobs:
            spearmint_job_dicts.append(
                convert_jobman_sql_to_spearmint(job))

        if len(spearmint_job_dicts) == 0:
            return None
        elif len(spearmint_job_dicts) == 1:
            return spearmint_job_dicts[0]
        else:
            return spearmint_job_dicts


def convert_ndarrays(item):
    if isinstance(item, dict):
        rval = dict()
        for key, value in item.iteritems():
            rval[key] = convert_ndarrays(value)
    elif isinstance(item, (list, tuple)):
        rval = type(item)([])
        for value in item:
            rval = rval + type(item)([convert_ndarrays(value)])
    elif isinstance(item, numpy.ndarray):
        rval = list(item)
    else:
        rval = item

    return rval


def convert_lists(item):
    if isinstance(item, dict):
        rval = dict()
        for key, value in item.iteritems():
            rval[key] = convert_lists(value)
    elif (isinstance(item, list) and
          all(isinstance(v, (float, int)) for v in item)):
        rval = numpy.array(item)
    elif isinstance(item, (list, tuple)):
        rval = type(item)([])
        for value in item:
            rval = rval + type(item)([convert_lists(value)])
    elif isinstance(item, numpy.ndarray):
        rval = list(item)
    else:
        rval = item

    return rval


def convert_spearmint_to_jobman_sql(job):
    if "main_file" in job:
        job[sql.EXPERIMENT] = job["main_file"].replace(".py", ".jobman_main")
    return flatten(DD(convert_ndarrays(job)))


def convert_jobman_sql_to_spearmint(job):
    # Lists are not converted back to ndarrays but it should not be
    # problematic as they are used for tensor operations anyway.
    converted_job = dict(expand(DD(convert_lists(job))))
#    converted_job["id"] = job.id
#    converted_job["id"] = job[sql.JOBID]

    return converted_job


def get_table_name(experiment_name, experiment_field):
    return table_name_template % dict(experiment_name=experiment_name,
                                      experiment_field=experiment_field)
