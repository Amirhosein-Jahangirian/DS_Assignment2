from django.db import models

# Create your models here.

from django.db import models
from django.core.validators import MinValueValidator

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