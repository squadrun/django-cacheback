import time

from celery.utils.log import get_task_logger
from celery import shared_task
from django.apps import apps
try:
    import importlib
except ImportError:
    import django.utils.importlib as importlib

logger = get_task_logger(__name__)


@shared_task
def refresh_cache(klass_str, obj_args, obj_kwargs, call_args, call_kwargs):
    """
    Re-populate cache using the given job class.

    The job class is instantiated with the passed constructor args and the
    refresh method is called with the passed call args.  That is::

        data = klass(*obj_args, **obj_kwargs).refresh(
            *call_args, **call_kwargs)

    :klass_str: String repr of class (eg 'apps.twitter.jobs:FetchTweetsJob')
    :obj_args: Constructor args
    :obj_kwargs: Constructor kwargs
    :call_args: Refresh args
    :call_kwargs: Refresh kwargs
    """
    klass = _get_job_class(klass_str)
    if klass is None:
        logger.error("Unable to construct %s with args %r and kwargs %r",
                     klass_str, obj_args, obj_kwargs)
        return

    obj_args, obj_kwargs = _get_job_init_args_kwargs(obj_args, obj_kwargs)

    logger.info("Using %s with constructor args %r and kwargs %r",
                klass_str, obj_args, obj_kwargs)
    logger.info("Calling refresh with args %r and kwargs %r", call_args,
                call_kwargs)
    start = time.time()
    try:
        klass(*obj_args, **obj_kwargs).refresh(
            *call_args, **call_kwargs)
    except Exception as e:
        logger.error("Error running job: '%s'", e)
        logger.exception(e)
    else:
        duration = time.time() - start
        logger.info("Refreshed cache in %.6f seconds", duration)


def _get_job_class(klass_str):
    """
    Return the job class
    """
    mod_name, klass_name = klass_str.rsplit('.', 1)
    try:
        mod = importlib.import_module(mod_name)
    except ImportError as e:
        logger.error("Error importing job module %s: '%s'", mod_name, e)
        return
    try:
        klass = getattr(mod, klass_name)
    except AttributeError:
        logger.error("Module '%s' does not define a '%s' class", mod_name,
                     klass_name)
        return
    return klass


def _get_job_init_args_kwargs(obj_args, obj_kwargs):
    if obj_kwargs and 'model' in obj_kwargs:
        try:
            obj_kwargs['model'] = apps.get_model(*obj_kwargs['model'])
        except Exception:
            pass

    return obj_args, obj_kwargs
