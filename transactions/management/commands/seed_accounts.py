# transactions/management/commands/seed_accounts.py
from decimal import Decimal, InvalidOperation

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from transactions import sharding
from transactions.models import Account


class Command(BaseCommand):
    help = (
        'Seed every account in the range owned by this process (SHARD_ID), '
        'for example A0001-A1000 on shard-a.'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--reset',
            action='store_true',
            help="Delete this shard's accounts before seeding (other shards untouched)",
        )
        parser.add_argument(
            '--balance',
            default='1000.00',
            help='Initial balance for newly created accounts (default: 1000.00)',
        )

    def handle(self, *args, **options):
        try:
            ranges = sharding.ranges_for_shard()
            owned = sharding.account_filter()
        except Exception as exc:  # ImproperlyConfigured from the shard map
            raise CommandError(str(exc)) from exc

        try:
            balance = Decimal(options['balance'])
        except (InvalidOperation, TypeError) as exc:
            raise CommandError(f'Invalid --balance value: {options["balance"]!r}') from exc
        if balance < 0:
            raise CommandError('--balance must not be negative')

        with transaction.atomic():
            deleted_count = 0
            if options['reset']:
                deleted_count, _ = Account.objects.filter(owned).delete()

            existing = set(
                Account.objects.filter(owned).values_list('account_number', flat=True)
            )

            new_accounts = []
            for account_range in ranges:
                first = int(account_range.start[1:])
                last = int(account_range.end[1:])
                for number in range(first, last + 1):
                    account_number = f'A{number:04d}'
                    if account_number not in existing:
                        new_accounts.append(
                            Account(account_number=account_number, balance=balance)
                        )

            Account.objects.bulk_create(new_accounts, batch_size=500)

        span = ', '.join(f'{r.start}-{r.end}' for r in ranges)
        self.stdout.write(self.style.SUCCESS(
            f'Shard {settings.SHARD_ID} ({span}): deleted {deleted_count}, '
            f'created {len(new_accounts)}, already present {len(existing)}.'
        ))
