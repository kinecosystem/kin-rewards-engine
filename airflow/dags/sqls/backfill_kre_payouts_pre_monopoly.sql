WITH
	    devs_to_pay AS ( -- Only pay developers with at least one transaction
	    SELECT
	      DISTINCT digital_service_id
	    FROM
	      `kin-bi.stellar.payments_and_creates_view`
	    WHERE
	      date =  DATE('{{ ds }}')),
	    carryover_pool AS ( -- calculate today's carryover pool
	    SELECT
	      CASE
	        WHEN MAX(date) IS NULL THEN 0
	      ELSE
	      500000000 * DATE_DIFF(MAX(date), DATE('2019-12-31'), day) - SUM(total_payout)
	      + (select ifnull(sum(amount), 0) from `kin-bi.kre_2.carryover_additions` where date < date_add(DATE('{{ ds }}'), interval 1 day))
	    END
	      AS carryover_total
	    FROM
	      `kin-bi.kre_2.kre_2_payouts`
	    WHERE
	      date <  DATE('{{ ds }}')
	      ),
	    daily_max_payouts AS ( -- calculate the daily max payouts based on carryover and track proportions
	    SELECT
	      b.carryover_total,
	      DATE_DIFF('2021-01-01', a.day, day) AS days_left,
	      a.spend_proportion * (500000000 + b.carryover_total / DATE_DIFF('2021-01-01', a.day, day)) AS spend_max_payout,
	      a.hold_proportion * (500000000 + b.carryover_total / DATE_DIFF('2021-01-01', a.day, day)) AS hold_max_payout,
	      a.buy_proportion * (500000000 + b.carryover_total / DATE_DIFF('2021-01-01', a.day, day)) AS buy_max_payout
	    FROM
	      `kin-bi.kre_2.track_proportions` a
	    JOIN
	      carryover_pool b
	    ON
	      TRUE
	    WHERE
	      a.day =  DATE('{{ ds }}')),
	    prior_payouts AS ( -- Calculate how much devs have been paid previously
	    SELECT
	      IFNULL(a.payout,
	        0) + IFNULL(b.amount,
	        0) AS prior_payout,
	      coalesce(a.digital_service_id,
	        b.digital_service_id) AS digital_service_id
	    FROM (
	      SELECT
	        SUM(total_payout) AS payout,
	        digital_service_id
	      FROM
	        `kin-bi.kre_2.kre_2_payouts`
	      WHERE
	        date <  DATE('{{ ds }}')
	      GROUP BY
	        digital_service_id) a
	    FULL OUTER JOIN
	      `kin-bi.kre_2.kre_1_payouts_summary` b
	    ON
	      a.digital_service_id = b.digital_service_id ),
	    capped_holding AS ( -- Cap the KRE holdings to prior payouts
	    SELECT
	      CASE
	        WHEN min_balance > IFNULL(b.prior_payout, 0) THEN b.prior_payout
	      ELSE
	      min_balance
	    END
	      AS min_balance,
	      a.digital_service_id,
	      a.digital_service_name,
	      date
	    FROM (
	      SELECT
	        SUM(min_balance) AS min_balance,
	        digital_service_id,
	        digital_service_name,
	        date
	      FROM
	        `kin-bi.kre_2.holding`
	      WHERE
	        date =  DATE('{{ ds }}')
	        AND digital_service_id IN (
	        SELECT
	          digital_service_id
	        FROM
	          devs_to_pay)
	      GROUP BY
	        date,
	        digital_service_id,
	        digital_service_name) a
	    LEFT JOIN
	      prior_payouts b
	    ON
	      a.digital_service_id = b.digital_service_id ),
	    curr_payout_spend AS (
	    SELECT
	      CASE
	        WHEN date < '2020-02-01' THEN SUM(max_payout_jan)
	      ELSE
	      SUM(max_payout_feb_onwards)
	    END
	      AS summed_payout,
	      date
	    FROM
	      `kin-bi.kre_2.spender_buckets`
	    WHERE
	      date =  DATE('{{ ds }}')
	    GROUP BY
	      date),
	    new_payout_spend AS -- If sum(all) for a day is bigger than daily_max_payouts readjust
	    (
	    SELECT
	      c.digital_service_name,
	      digital_service_id,
	      CASE
	        WHEN a.date < '2020-02-01' THEN max_payout_jan
	      ELSE
	      max_payout_feb_onwards
	    END
	      AS prior_spend_payout,
	      CASE
	        WHEN a.summed_payout > b.spend_max_payout THEN (CASE
	          WHEN a.date < '2020-02-01' THEN max_payout_jan
	        ELSE
	        max_payout_feb_onwards
	      END
	        ) / a.summed_payout * b.spend_max_payout
	      ELSE
	      CASE
	        WHEN a.date < '2020-02-01' THEN max_payout_jan
	      ELSE
	      max_payout_feb_onwards
	    END
	    END
	      AS spend_payout,
	      a.date
	    FROM
	      curr_payout_spend a
	    JOIN
	      daily_max_payouts b
	    ON
	      TRUE
	    JOIN
	      `kin-bi.kre_2.spender_buckets` c
	    ON
	      a.date = c.date
	    WHERE
	      c.date =  DATE('{{ ds }}')
	      AND c.digital_service_id IN (
	      SELECT
	        digital_service_id
	      FROM
	        devs_to_pay)),
	    curr_payout_hold AS -- Compute payout for hold
	    (
	    SELECT
	      SUM(min_balance)*0.00136986301 AS summed_payout,
	      date
	    FROM
	      capped_holding
	    WHERE
	      date =  DATE('{{ ds }}')
	    GROUP BY
	      date),
	    new_payout_hold AS -- IF summ bigger than daily_max_payouts readjust
	    (
	    SELECT
	      c.digital_service_name,
	      digital_service_id,
	      c.min_balance*0.00136986301 AS prior_hold_payout,
	      CASE
	        WHEN a.summed_payout > b.hold_max_payout THEN min_balance*0.00136986301 / a.summed_payout * b.hold_max_payout
	      ELSE
	      min_balance*0.00136986301
	    END
	      AS hold_payout,
	      a.date
	    FROM
	      curr_payout_hold a
	    JOIN
	      daily_max_payouts b
	    ON
	      TRUE
	    LEFT JOIN
	      capped_holding c
	    ON
	      a.date = c.date
	    WHERE
	      c.date =  DATE('{{ ds }}')),
	    curr_payout_buy_demand AS -- -- Compute payouts based on kin-bi.kre_2.buy_demand
	    (
	    SELECT
	      SUM(CASE
	          WHEN buy_demand < 0 THEN 0
	        ELSE
	        buy_demand
	      END
	        ) AS summed_payout,
	      date
	    FROM
	      `kin-bi.kre_2.buy_demand`
	    WHERE
	      date =  DATE('{{ ds }}')
	    GROUP BY
	      date),
	    new_payout_buy AS -- IF summ bigger than daily_max_payouts readjust
	    (
	    SELECT
	      c.digital_service_name,
	      c.digital_service_id,
	      CASE
	        WHEN c.buy_demand < 0 THEN 0
	      ELSE
	      c.buy_demand
	    END
	      AS prior_buy_payout,
	      CASE
	        WHEN a.summed_payout > b.buy_max_payout THEN CASE
	        WHEN c.buy_demand < 0 THEN 0
	      ELSE
	      c.buy_demand
	    END
	      / a.summed_payout * b.buy_max_payout
	      ELSE
	      CASE
	        WHEN c.buy_demand < 0 THEN 0
	      ELSE
	      c.buy_demand
	    END
	    END
	      AS buy_payout,
	      a.date
	    FROM
	      curr_payout_buy_demand a
	    JOIN
	      daily_max_payouts b
	    ON
	      TRUE
	    JOIN
	      `kin-bi.kre_2.buy_demand` c
	    ON
	      a.date = c.date
	    WHERE
	      c.date =  DATE('{{ ds }}')
	      AND digital_service_id IN (
	      SELECT
	        digital_service_id
	      FROM
	        devs_to_pay)),
	    total_payouts AS -- Compute total payouts
	    (
	    SELECT
	      coalesce(a.digital_service_id,
	        b.digital_service_id,
	        c.digital_service_id) AS digital_service_id,
	      coalesce(a.digital_service_name,
	        b.digital_service_name,
	        c.digital_service_name) AS digital_service_name,
	      ifnull(a.spend_payout,
	        0) AS spend_payout,
	      ifnull(b.hold_payout,
	        0) AS hold_payout,
	      ifnull(c.buy_payout,
	        0) AS buy_payout,
	      ifnull(a.spend_payout,
	        0) + ifnull(b.hold_payout,
	        0) + ifnull(c.buy_payout,
	        0) AS total_payout,
	      coalesce(a.date,
	        b.date,
	        c.date) AS date
	    FROM
	      new_payout_spend a
	    FULL OUTER JOIN
	      new_payout_hold b
	    ON
	      a.digital_service_id = b.digital_service_id
	    FULL OUTER JOIN
	      new_payout_buy c
	    ON
	      c.digital_service_id = coalesce(a.digital_service_id, b.digital_service_id))
	  SELECT
	    *
	  FROM
	    total_payouts
	  WHERE
	    total_payout > 0
