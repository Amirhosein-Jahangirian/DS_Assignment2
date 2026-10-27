from django.apps import AppConfig


class TransactionsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'transactions'

    def ready(self):
        # Validate the shard map (and this process's SHARD_ID) at startup so a
        # misconfigured replica fails loudly instead of serving wrong accounts.
        from . import sharding

        sharding.validate()
