from cacheback.base import Job


class QuerySetJob(Job):
    """
    Helper class for wrapping ORM reads
    """

    def __init__(self, model, lifetime=None, fetch_on_miss=None):
        """
        :model: The model class to use
        """
        super(QuerySetJob, self).__init__()
        self.model = model
        if lifetime is not None:
            self.lifetime = lifetime
        if fetch_on_miss is not None:
            self.fetch_on_miss = fetch_on_miss

    def key(self, *args, **kwargs):
        return "%s-%s" % (
            self.model.__name__,
            super(QuerySetJob, self).key(*args, **kwargs)
        )

    def get_constructor_kwargs(self):
        app_label, model_name = self.model._meta.app_label, self.model._meta.model_name

        return {'model': [app_label, model_name],
                'lifetime': self.lifetime}


class QuerySetGetJob(QuerySetJob):
    """
    For ORM reads that use the ``get`` method.
    """
    def fetch(self, *args, **kwargs):
        return self.model.objects.get(**kwargs)


class QuerySetFilterJob(QuerySetJob):
    """
    For ORM reads that use the ``filter`` method.
    """
    def fetch(self, *args, **kwargs):
        return self.model.objects.filter(**kwargs)
