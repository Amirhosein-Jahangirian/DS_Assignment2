from django.core.validators import MinValueValidator
from django.db import models


class Account(models.Model):

    account_number = models.CharField(
        max_length=20, 
        unique=True, 
        db_index=True,
    )
    balance = models.DecimalField(
        max_digits=15, 
        decimal_places=2, 
        default=0.00,
        validators=[MinValueValidator(0.00)], # Enforces Consistency: No negative balances
    )
    is_active = models.BooleanField(default=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.CheckConstraint(
                condition=models.Q(balance__gte=0),
                name='account_balance_non_negative',
            ),
        ]

    def __str__(self):
        return f"{self.account_number} | Balance: {self.balance}"


class ParticipantCommit(models.Model):
    """
    Durable commit marker for one 2PC participant GID.

    A row means this participant has durably accepted COMMIT for the GID.
    PostgreSQL's pg_prepared_xacts remains the authoritative PREPARED state;
    this table only makes repeated COMMIT idempotent and prevents a later
    ROLLBACK from undoing a recorded commit. It is not the coordinator's
    transaction state machine.
    """

    gid = models.CharField(max_length=200, primary_key=True)
    committed_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.gid
