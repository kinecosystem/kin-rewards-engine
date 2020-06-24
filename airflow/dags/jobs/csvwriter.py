import pandas as pd
from datetime import datetime, timedelta


class CSVGenerator():
        @staticmethod
        def print_csv_sheet(writer, sheet_name, print_date, start_date, i, info, column_list):
                if print_date == start_date:
                        pd.DataFrame({print_date.strftime('%Y-%m-%d'): [print_date.strftime('%Y-%m-%d')]}).to_excel(writer, sheet_name=sheet_name, float_format="%.2f", startcol=2,startrow=1, index=False)
                        info.sort_values(by=['digital_service_id']).loc[info['date'].astype(str) == print_date.strftime('%Y-%m-%d')][['digital_service_id'] + column_list].to_excel(writer, sheet_name=sheet_name, float_format="%.2f", startcol=1,startrow=2, index=False)

                else:
                        pd.DataFrame({print_date.strftime('%Y-%m-%d'): [print_date.strftime('%Y-%m-%d')]}).to_excel(writer, sheet_name=sheet_name, float_format="%.2f", startcol=2 + len(column_list) * i,startrow=1, index=False)
                        info.sort_values(by=['digital_service_id']).loc[info['date'].astype(str) == print_date.strftime('%Y-%m-%d')][column_list].to_excel(writer, sheet_name=sheet_name, float_format="%.2f", startcol=2 + len(column_list) * i,startrow=2, index=False)

        @staticmethod
        def create_payout_csv(payout_info, payout_info_date, spender_info, holding_info, buying_info, prior_payout_info, start_date, end_date, write_directory):
                s = start_date
                e = end_date - timedelta(days=1)
                dates = pd.date_range(s, e).tolist()

                payout_info.to_csv(path_or_buf=write_directory + "/payout.csv", float_format="%.2f")

                # Create a Pandas Excel writer using XlsxWriter as the engine.
                writer = pd.ExcelWriter(write_directory + "/payout.xlsx", engine='xlsxwriter')

                payout_info.loc[:, ~payout_info.columns.isin(['email', 'contact'])].to_excel(writer, sheet_name='Summary', float_format="%.2f")
                prior_payout_info.to_excel(writer, sheet_name='Prior Lifetime KRE Payouts', float_format="%.2f")

                for i, d in enumerate(dates):
                        CSVGenerator.print_csv_sheet(writer, 'Payouts', d, s, i, payout_info_date, ['spend_payout', 'hold_payout', 'buy_payout', 'total_payout'])
                        CSVGenerator.print_csv_sheet(writer, 'Spend', d, s, i, spender_info, ['das', 's1_9', 's10_99', 's100_999', 's1000'])
                        CSVGenerator.print_csv_sheet(writer, 'Hold', d, s, i, holding_info, ['holdings'])
                        CSVGenerator.print_csv_sheet(writer, 'Buy', d, s, i, buying_info, ['lifetime_earns_minus_spends', 'prior_buy_demand_payouts', 'min_balance', 'buy_demand'])

                writer.save()
