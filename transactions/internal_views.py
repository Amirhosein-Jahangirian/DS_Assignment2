"""
Internal shard API used by the 2PC coordinator and the gateway.

These endpoints are intentionally outside ``/api/``. They are only reachable
on the Docker network: replicas publish no host ports and the gateway does not
proxy ``/internal/``.
"""
from __future__ import annotations

import logging
from decimal import Decimal

from django.db import transaction
from rest_framework import serializers, status
from rest_framework.response import Response
from rest_framework.views import APIView

from .models import Account
from .participant import (
    ParticipantError,
    commit as participant_commit,
    prepare as participant_prepare,
    rollback as participant_rollback,
    status as participant_status,
)
from .sharding import owns

logger = logging.getLogger(__name__)


def _error(error_code: str, message, http_status: int) -> Response:
    return Response(
        {
            'status': 'error',
            'error_code': error_code,
            'message': message,
        },
        status=http_status,
    )


def _invalid(serializer: serializers.Serializer) -> Response:
    return _error(
        'INVALID_INPUT',
        serializer.errors,
        status.HTTP_400_BAD_REQUEST,
    )


def _participant_error(exc: ParticipantError) -> Response:
    return _error(exc.code, exc.message, exc.http_status)


class LocalTransferSerializer(serializers.Serializer):
    from_account = serializers.CharField(max_length=20)
    to_account = serializers.CharField(max_length=20)
    amount = serializers.DecimalField(
        max_digits=15,
        decimal_places=2,
        min_value=Decimal('0.01'),
    )


class PrepareSerializer(serializers.Serializer):
    txid = serializers.CharField(max_length=128)
    account = serializers.CharField(max_length=20)
    direction = serializers.ChoiceField(choices=sorted(('debit', 'credit')))
    amount = serializers.DecimalField(
        max_digits=15,
        decimal_places=2,
        min_value=Decimal('0.01'),
    )


class GidSerializer(serializers.Serializer):
    gid = serializers.CharField(max_length=200)


class LocalTransferView(APIView):
    """
    Atomic same-shard transfer.

    This is one local database transaction, not a 2PC participant operation:
    both account updates commit or neither does.
    """

    authentication_classes = []
    permission_classes = []

    def post(self, request):
        serializer = LocalTransferSerializer(data=request.data)
        if not serializer.is_valid():
            return _invalid(serializer)

        data = serializer.validated_data
        from_account = data['from_account']
        to_account = data['to_account']
        amount = data['amount']

        if from_account == to_account:
            return _error(
                'SAME_ACCOUNT',
                'Source and destination accounts must differ',
                status.HTTP_400_BAD_REQUEST,
            )

        if not owns(from_account) or not owns(to_account):
            return _error(
                'UNKNOWN_ACCOUNT',
                'Both accounts must belong to this shard',
                status.HTTP_404_NOT_FOUND,
            )

        try:
            with transaction.atomic():
                # Lock in a deterministic order so concurrent transfers in
                # opposite directions cannot deadlock.
                accounts = {}
                for account_number in sorted((from_account, to_account)):
                    accounts[account_number] = (
                        Account.objects.select_for_update().get(
                            account_number=account_number
                        )
                    )

                source = accounts[from_account]
                destination = accounts[to_account]

                if source.balance < amount:
                    return _error(
                        'INSUFFICIENT_FUNDS',
                        (
                            f'Account {from_account} has insufficient funds. '
                            f'Current balance: {source.balance}, requested: {amount}'
                        ),
                        status.HTTP_422_UNPROCESSABLE_ENTITY,
                    )

                source.balance -= amount
                source.save(update_fields=['balance', 'updated_at'])
                destination.balance += amount
                destination.save(update_fields=['balance', 'updated_at'])

        except Account.DoesNotExist:
            return _error(
                'UNKNOWN_ACCOUNT',
                'One or both accounts do not exist on this shard',
                status.HTTP_404_NOT_FOUND,
            )

        return Response(
            {
                'status': 'success',
                'from_account': from_account,
                'to_account': to_account,
                'amount': str(amount),
                'from_balance': str(source.balance),
                'to_balance': str(destination.balance),
            },
            status=status.HTTP_200_OK,
        )


class ParticipantPrepareView(APIView):
    authentication_classes = []
    permission_classes = []

    def post(self, request):
        serializer = PrepareSerializer(data=request.data)
        if not serializer.is_valid():
            return _invalid(serializer)

        data = serializer.validated_data
        try:
            result = participant_prepare(
                txid=data['txid'],
                account_number=data['account'],
                direction=data['direction'],
                amount=data['amount'],
            )
        except ParticipantError as exc:
            return _participant_error(exc)
        except Exception:
            logger.exception('2PC prepare failed')
            return _error(
                'PARTICIPANT_ERROR',
                'The participant could not prepare the transaction',
                status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        return Response(result, status=status.HTTP_200_OK)


class ParticipantCommitView(APIView):
    authentication_classes = []
    permission_classes = []

    def post(self, request):
        serializer = GidSerializer(data=request.data)
        if not serializer.is_valid():
            return _invalid(serializer)

        try:
            result = participant_commit(serializer.validated_data['gid'])
        except ParticipantError as exc:
            return _participant_error(exc)
        except Exception:
            logger.exception('2PC commit failed')
            return _error(
                'PARTICIPANT_ERROR',
                'The participant could not commit the transaction',
                status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        return Response(result, status=status.HTTP_200_OK)


class ParticipantRollbackView(APIView):
    authentication_classes = []
    permission_classes = []

    def post(self, request):
        serializer = GidSerializer(data=request.data)
        if not serializer.is_valid():
            return _invalid(serializer)

        try:
            result = participant_rollback(serializer.validated_data['gid'])
        except ParticipantError as exc:
            return _participant_error(exc)
        except Exception:
            logger.exception('2PC rollback failed')
            return _error(
                'PARTICIPANT_ERROR',
                'The participant could not roll back the transaction',
                status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        return Response(result, status=status.HTTP_200_OK)


class ParticipantStatusView(APIView):
    authentication_classes = []
    permission_classes = []

    def get(self, request, gid):
        try:
            result = participant_status(gid)
        except ParticipantError as exc:
            return _participant_error(exc)
        except Exception:
            logger.exception('2PC status query failed')
            return _error(
                'PARTICIPANT_ERROR',
                'The participant could not read the transaction state',
                status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        return Response(result, status=status.HTTP_200_OK)
