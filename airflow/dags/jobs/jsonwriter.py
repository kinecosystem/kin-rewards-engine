import pandas as pd
import json

class JSONGenerator():
    @staticmethod
    def create_payout_json(payout_info, start_date, end_date, write_directory):
      payout_info.sort_values(by='total_payout', ascending=False, axis=0, inplace=True)
      print(payout_info)
      json_data = pd.DataFrame(columns=['rank', 'app', 'pay'])
      r = 0
      for ix, row in payout_info.iterrows():
          json_data.loc[r] = pd.Series({'rank': r + 1,
                                        'app': row['digital_service_name'],
                                        'pay': round(row['total_payout'], 2)})
          r += 1

      json_data = json_data.to_dict(orient='records')
      res = {}
      res['data'] = json_data
      res['config'] = {'start_date': start_date, 'end_date': end_date }

      with open(write_directory + "/kre_payout_leaderboard.json", 'w') as fp:
            json.dump(res, fp)
