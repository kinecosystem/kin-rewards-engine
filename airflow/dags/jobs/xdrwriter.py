# KIN Imports
from kin_base.operation import Payment
from kin_base.transaction import Transaction
from kin_base.asset import Asset
from kin_base.transaction_envelope import TransactionEnvelope as Te
from kin_base.horizon import Horizon

class XDRGenerator():

        @staticmethod
        async def create_payout_xdr(payout_info, start_date, end_date, src_wallet, write_directory, digital_service_id_kin_2, kin_2_amount, sequence_increment = 0):
                operations = []
                for ix, row in payout_info.iterrows():
                        if row['digital_service_id'] == digital_service_id_kin_2:
                                amt = str(round(row['total_payout'], 2) - kin_2_amount)
                        else:
                                amt = str(round(row['total_payout'], 2))
                        payment_op = Payment(
                            destination=row['wallet'],
                            amount=amt,
                            asset=Asset.native()
                        )
                        operations.append(payment_op)

                async with Horizon(horizon_uri='https://horizon.kinfederation.com') as horizon:
                        total_fee = len(payout_info.index) * 100
                        network_id = "PUBLIC"
                        account_result = await horizon.account(src_wallet)

                sequence = int(account_result['sequence']) + 1

                tx = Transaction(
                        source = src_wallet,
                        sequence = int(sequence) + sequence_increment,
                        time_bounds = {'minTime': 0, 'maxTime': 0},
                        fee = total_fee,
                        operations = operations,
                        memo = None
                )

                envelope = Te(tx=tx, network_id="PUBLIC")
                xdr = envelope.xdr() 
                f = open(write_directory + "/xdr" + str(sequence_increment) + ".xdr", "w")
                f.write(xdr.decode("utf-8"))
                f.close()
                print(xdr.decode("utf-8"))
