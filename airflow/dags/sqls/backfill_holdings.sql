WITH payments AS
     (SELECT SOURCE AS wallet,
                       -1 * amount AS amount,
                       digital_service_name,
                       digital_service_id,
                       TIME,
                       tx_hash
      FROM `kin-bi.stellar.payments_and_creates_view`
      WHERE SOURCE IN
          (SELECT wallet
           FROM `kin-bi.kre_2.developer_wallets`
           where purpose = 'hold' or purpose = 'kre')
           and date = DATE('{{ ds }}')
      UNION ALL SELECT destination AS wallet,
                       amount,
                       digital_service_name,
                       digital_service_id,
                       TIME,
                       tx_hash
      FROM `kin-bi.stellar.payments_and_creates_view`
      WHERE destination IN
          (SELECT wallet
           FROM `kin-bi.kre_2.developer_wallets`
           where purpose = 'hold' or purpose = 'kre')
           and date = DATE('{{ ds }}')),
        balances AS
     (SELECT wallet,
             amount,
             digital_service_name,
             digital_service_id,
             TIME,
             sum(amount) OVER (PARTITION BY wallet
                               ORDER BY TIME ASC, tx_hash ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) AS balance,
             tx_hash
      FROM payments),
        balances_per_day_w_txn AS
     (SELECT wallet,
             cast(TIME AS date) AS date,
             min(balance) AS min_balance,
             max(concat(string(TIME), tx_hash)) AS max_time_tx_hash
      FROM balances
      GROUP BY wallet,
               cast(TIME AS date))
      select a.wallet, date_add(a.date, interval + 1 day) as date, 
      case when b.min_balance is null then a.end_of_day_balance else 
        case when a.end_of_day_balance < b.min_balance + a.end_of_day_balance then a.end_of_day_balance else b.min_balance + a.end_of_day_balance end
      end as min_balance,
      case when c.balance is null then a.end_of_day_balance else c.balance + a.end_of_day_balance end as end_of_day_balance,
      a.digital_service_name, a.digital_service_id from `kin-bi.kre_2.holding` a left join balances_per_day_w_txn b on a.wallet = b.wallet
      left join balances c on b.wallet = c.wallet and concat(string(c.TIME), c.tx_hash) = b.max_time_tx_hash
      where a.date = date_add(DATE('{{ ds }}'), interval - 1 day)
