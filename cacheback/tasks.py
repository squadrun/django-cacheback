from celery import shared_task


@shared_task
def refresh_cache(klass_str, obj_args, obj_kwargs, call_args, call_kwargs):
    from .base import Job

    obj_args, obj_kwargs = _get_job_init_args_kwargs(obj_args, obj_kwargs)
    Job.perform_async_refresh(klass_str, obj_args, obj_kwargs, call_args, call_kwargs)


def _get_job_init_args_kwargs(obj_args, obj_kwargs):
    if obj_kwargs and 'model' in obj_kwargs:
        try:
            obj_kwargs['model'] = apps.get_model(*obj_kwargs['model'])
        except Exception:
            pass

    return obj_args, obj_kwargs
