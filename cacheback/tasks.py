from celery import shared_task
from django.conf import settings
from django.apps import apps
import logging


logger = logging.getLogger("cacheback")


@shared_task(ignore_result=getattr(settings, 'CACHEBACK_TASK_IGNORE_RESULT', False))
def refresh_cache(klass_str, obj_args, obj_kwargs, call_args, call_kwargs):
    from .base import Job

    obj_args, obj_kwargs = _get_job_init_args_kwargs(obj_args, obj_kwargs)

    Job.perform_async_refresh(klass_str, obj_args, obj_kwargs, call_args, call_kwargs)


def _get_job_init_args_kwargs(obj_args, obj_kwargs):
    if obj_kwargs and 'model' in obj_kwargs:
        try:
            obj_kwargs['model'] = apps.get_model(*obj_kwargs['model'])
        except Exception as e:
            logger.info('Exception in model parsing: {}'.format(e))
            pass

    return obj_args, obj_kwargs
