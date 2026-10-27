from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .models import Account
from .serializers import AccountDetailSerializer
from django.conf import settings
from django.db import connection
from django.db import transaction
from django.http import JsonResponse
from .serializers import AmountInputSerializer, TransactionResponseSerializer
from .sharding import account_filter, owns


def _not_owned_response(request, action, account_number):
    """
    Reject an account outside this shard's range.

    The same 404/UNKNOWN_ACCOUNT shape as a genuinely missing account is used
    on purpose: a shard must not reveal where another shard's accounts live.
    """
    request.transaction_data.update({
        'action': action,
        'account': account_number,
        'result': 'error:unknown_account',
        'balance': 'N/A',
    })
    return Response(
        {
            'status': 'error',
            'error_code': 'UNKNOWN_ACCOUNT',
            'message': f'Account {account_number} does not exist',
        },
        status=status.HTTP_404_NOT_FOUND,
    )


def health(request):
    """
    Liveness/readiness probe for the gateway and Docker.

    Checks the shard database with a trivial query so a replica whose database
    is unreachable is reported as unavailable. Deliberately outside /api/ so
    probes are not written to the transaction log.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute('SELECT 1')
            cursor.fetchone()
    except Exception:
        return JsonResponse(
            {
                'status': 'error',
                'shard_id': settings.SHARD_ID,
                'replica_id': settings.REPLICA_ID,
                'db': 'unavailable',
            },
            status=503,
        )

    return JsonResponse(
        {
            'status': 'ok',
            'shard_id': settings.SHARD_ID,
            'replica_id': settings.REPLICA_ID,
            'db': 'ok',
        }
    )


class BalanceInquiryView(APIView):
    """
    Balance Inquiry API
    GET /api/accounts/<account_number>/

    Returns the current balance of the specified account.
    """

    def get(self, request, account_number):
        if not owns(account_number):
            return _not_owned_response(request, 'inquiry', account_number)

        try:
            account = Account.objects.get(account_number=account_number)
        except Account.DoesNotExist:
            # Populate transaction data for logging
            request.transaction_data.update({
                'action': 'inquiry',
                'account': account_number,
                'result': 'error:unknown_account',
                'balance': 'N/A'
            })
            # Handles Section 2.3: Unknown account error
            return Response(
                {
                    'status': 'error',
                    'error_code': 'UNKNOWN_ACCOUNT',
                    'message': f'Account {account_number} does not exist'
                },
                status=status.HTTP_404_NOT_FOUND
            )

        serializer = AccountDetailSerializer(account)
        # Populate transaction data for logging
        request.transaction_data.update({
            'action': 'inquiry',
            'account': account.account_number,
            'result': 'success',
            'balance': account.balance
        })
        return Response(serializer.data, status=status.HTTP_200_OK)


class AccountListView(APIView):
    """
    List Accounts API
    GET /api/accounts/

    Returns the accounts owned by this shard (its configured range only).
    A shard never lists accounts that belong to another shard.
    """

    def get(self, request):
        accounts = Account.objects.filter(account_filter()).order_by('account_number')
        serializer = AccountDetailSerializer(accounts, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)


class DepositView(APIView):
    """
    Deposit API
    POST /api/accounts/<account_number>/deposit/

    Adds the specified amount to the account balance.
    Guarantees ACID properties:
    - Atomicity: Uses transaction.atomic()
    - Consistency: Balance never negative (enforced by model validator)
    - Isolation: Uses select_for_update() to lock the row
    - Durability: PostgreSQL writes to disk before response
    """

    def post(self, request, account_number):
        if not owns(account_number):
            return _not_owned_response(request, 'deposit', account_number)

        # Step 1: Validate input (handles missing fields, invalid amounts)
        # time.sleep(1)  # TESTING ONLY: 100ms delay
        serializer = AmountInputSerializer(data=request.data)
        if not serializer.is_valid():
            request.transaction_data.update({
                'action': 'deposit',
                'account': account_number,
                'result': 'error:invalid_input',
                'balance': 'N/A'
            })
            
            return Response(
                {
                    'status': 'error',
                    'error_code': 'INVALID_INPUT',
                    'message': serializer.errors
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        amount = serializer.validated_data['amount']

        # Step 2: Execute transaction with ACID guarantees
        try:
            with transaction.atomic():
                # Lock the account row for Isolation (prevents lost updates)
                account = Account.objects.select_for_update().get(
                    account_number=account_number
                )

                # Perform the deposit
                account.balance += amount
                account.save()  # Durability: PostgreSQL commits to disk

        except Account.DoesNotExist:
            # Handles Section 2.3: Unknown account
            request.transaction_data.update({
                'action': 'deposit',
                'account': account_number,
                'result': 'error:unknown_account',
                'balance': 'N/A'
            })
            
            return Response(
                {
                    'status': 'error',
                    'error_code': 'UNKNOWN_ACCOUNT',
                    'message': f'Account {account_number} does not exist'
                },
                status=status.HTTP_404_NOT_FOUND
            )

        # Step 3: Return success response in assignment format
        response_serializer = TransactionResponseSerializer({
            'status': 'success',
            'account': account.account_number,
            'balance': account.balance
        })
        
        request.transaction_data.update({
            'action': 'deposit',
            'account': account.account_number,
            'result': 'success',
            'balance': account.balance
        })

        return Response(response_serializer.data, status=status.HTTP_200_OK)


class WithdrawView(APIView):
    """
    Withdraw API
    POST /api/accounts/<account_number>/withdraw/

    Subtracts the specified amount from the account balance.
    Guarantees ACID properties:
    - Atomicity: Uses transaction.atomic()
    - Consistency: Balance never negative (checked before withdrawal)
    - Isolation: Uses select_for_update() to lock the row
    - Durability: PostgreSQL writes to disk before response
    """

    def post(self, request, account_number):
        if not owns(account_number):
            return _not_owned_response(request, 'withdraw', account_number)

        # time.sleep(1)  # TESTING ONLY: 100ms delay
        # Step 1: Validate input (handles missing fields, invalid amounts)
        serializer = AmountInputSerializer(data=request.data)
        if not serializer.is_valid():
            
        
            request.transaction_data.update({
                'action': 'withdraw',
                'account': account_number,
                'result': 'error:invalid_input',
                'balance': 'N/A'
            })
            
            return Response(
                {
                    'status': 'error',
                    'error_code': 'INVALID_INPUT',
                    'message': serializer.errors
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        amount = serializer.validated_data['amount']

        # Step 2: Execute transaction with ACID guarantees
        try:
            with transaction.atomic():
                # Lock the account row for Isolation (prevents lost updates)
                account = Account.objects.select_for_update().get(
                    account_number=account_number
                )

                # Check for sufficient funds (Consistency: no negative balances)
                if account.balance < amount:
                    request.transaction_data.update({
                        'action': 'withdraw',
                        'account': account_number,
                        'result': 'error:insufficient_funds',
                        'balance': account.balance
                    })
                    
                    return Response(
                        {
                            'status': 'error',
                            'error_code': 'INSUFFICIENT_FUNDS',
                            'message': f'Account {account_number} has insufficient funds. '
                                      f'Current balance: {account.balance}, requested: {amount}'
                        },
                        status=status.HTTP_422_UNPROCESSABLE_ENTITY
                    )

                # Perform the withdrawal
                account.balance -= amount
                account.save()  # Durability: PostgreSQL commits to disk

        except Account.DoesNotExist:
            # Handles Section 2.3: Unknown account
            request.transaction_data.update({
                'action': 'withdraw',
                'account': account_number,
                'result': 'error:unknown_account',
                'balance': 'N/A'
            })
            
            return Response(
                {
                    'status': 'error',
                    'error_code': 'UNKNOWN_ACCOUNT',
                    'message': f'Account {account_number} does not exist'
                },
                status=status.HTTP_404_NOT_FOUND
            )

        # Step 3: Return success response in assignment format
        response_serializer = TransactionResponseSerializer({
            'status': 'success',
            'account': account.account_number,
            'balance': account.balance
        })
        
        request.transaction_data.update({
            'action': 'withdraw',
            'account': account.account_number,
            'result': 'success',
            'balance': account.balance
        })

        return Response(response_serializer.data, status=status.HTTP_200_OK)
