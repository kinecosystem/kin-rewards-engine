WITH devs_to_pay AS (
    SELECT
      DISTINCT digital_service_id
    FROM
      `kin-bi.stellar.payments_and_creates_view`
    WHERE
      date = DATE('{{ ds }}')
      AND digital_service_name NOT IN ('Kinit', 'other', 'anonymous', 'TestApp')
     and digital_service_id NOT IN ('kik', 'kit', 'TIPC', 'other')),
      payments AS
  (SELECT SOURCE AS wallet,
                    amount AS amount,
                    digital_service_name,
                    digital_service_id,
                    TIME, date
   FROM `kin-bi.stellar.payments_and_creates_view`
   WHERE (tx_type = 'earn'
          OR tx_type = 'create')
     AND date <= DATE('{{ ds }}')
   UNION ALL SELECT destination AS wallet,
                    -1 * amount AS amount,
                    digital_service_name,
                    digital_service_id,
                    TIME, date
   FROM `kin-bi.stellar.payments_and_creates_view`
   WHERE tx_type = 'spend'
     AND date <= DATE('{{ ds }}')),
     wallet_changes AS
  (SELECT sum(amount) AS earn_minus_spends,
          digital_service_name,
          digital_service_id
   FROM payments
   GROUP BY digital_service_name,
            digital_service_id),
     buy_demand_payouts AS
     (
     select sum(buy_payout) as buy_payout, digital_service_id from `kin-bi.kre_2.kre_2_payouts` where date < DATE('{{ ds }}') group by digital_service_id
     )
     SELECT
     a.digital_service_id,
     a.digital_service_name,
      a.earn_minus_spends,
      ifnull(b.buy_payout, 0) as total_buy_demand_payouts,
      0.0 as min_balance,
      a.earn_minus_spends - ifnull(b.buy_payout, 0) AS buy_demand,
     DATE('{{ ds }}') as date
      from wallet_changes a
      left join buy_demand_payouts b on a.digital_service_id = b.digital_service_id
      join devs_to_pay d on d.digital_service_id = a.digital_service_id
where not exists (select * from `kin-bi.kre_2.buy_demand` where date = DATE('{{ ds }}'))
