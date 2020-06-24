WITH totals AS
	  (SELECT sum(spend_payout) AS spend_payout_t,
	          max(spend_payout) as spend_payout_m,
	          case when sum(spend_payout) = 0 then false else
	          	max(spend_payout) / sum(spend_payout) > 0.5 end as spend_adjust,
	          sum(hold_payout) AS hold_payout_t,
	          max(hold_payout) as hold_payout_m,
	          case when sum(hold_payout) = 0 then false else
	          	max(hold_payout) / sum(hold_payout) > 0.5 end as hold_adjust,
	          sum(buy_payout) AS buy_payout_t,
	          max(buy_payout) as buy_payout_m,
	          case when sum(buy_payout) = 0 then false else
	          	max(buy_payout) / sum(buy_payout) > 0.5 end as buy_adjust,
	   FROM `kin-bi.kre_2.kre_2_payouts_pre_monopoly`
	   WHERE date = DATE('{{ ds }}')),
	fixed_payouts as
	(
	SELECT a.digital_service_id, a.digital_service_name,
	       case when spend_adjust and spend_payout = spend_payout_m then
	            spend_payout_t * (0.5 + ((spend_payout / spend_payout_t - 0.5) / 0.5) * (2.0 / 3.0 - 1.0 / 2.0))
	       when spend_adjust then
	            (spend_payout_t - spend_payout_t * (0.5 + ((spend_payout_m / spend_payout_t - 0.5) / 0.5) * (2.0 / 3.0 - 1.0 / 2.0))) * (spend_payout / (spend_payout_t - spend_payout_m))
	       else
	            spend_payout
	       end as spend_payout,
	       
	       case when hold_adjust and hold_payout = hold_payout_m then
	            hold_payout_t * (0.5 + ((hold_payout / hold_payout_t - 0.5) / 0.5) * (2.0 / 3.0 - 1.0 / 2.0))
	       when hold_adjust then
	            (hold_payout_t - hold_payout_t * (0.5 + ((hold_payout_m / hold_payout_t - 0.5) / 0.5) * (2.0 / 3.0 - 1.0 / 2.0))) * (hold_payout / (hold_payout_t - hold_payout_m))
	       else
	            hold_payout
	       end as hold_payout,
	       
	       case when buy_adjust and buy_payout = buy_payout_m then
	            buy_payout_t * (0.5 + ((buy_payout / buy_payout_t - 0.5) / 0.5) * (2.0 / 3.0 - 1.0 / 2.0))
	       when buy_adjust then
	            (buy_payout_t - buy_payout_t * (0.5 + ((buy_payout_m / buy_payout_t - 0.5) / 0.5) * (2.0 / 3.0 - 1.0 / 2.0))) * (buy_payout / (buy_payout_t - buy_payout_m))
	       else
	            buy_payout
	       end as buy_payout,
	       a.date
	FROM `kin-bi.kre_2.kre_2_payouts_pre_monopoly` a
	JOIN totals b ON TRUE
	WHERE a.date = DATE('{{ ds }}'))
	select digital_service_id, digital_service_name, spend_payout, hold_payout, buy_payout, 
	spend_payout + hold_payout + buy_payout as total_payout, date
	from fixed_payouts
