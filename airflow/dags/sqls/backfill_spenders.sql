with DAS AS
	  (SELECT sum(amount) AS amount,
	          SOURCE,
	          digital_service_id,
	          digital_service_name,
	          date
	   FROM `kin-bi.stellar.payments_with_tx_types_view`
	   WHERE (tx_type = 'spend'
	          OR tx_type = 'p2p')
	          and date = DATE('{{ ds }}')
	     AND digital_service_id NOT IN ('kik',
	                                    'kit',
	                                    'TIPC',
	                                    'other')
	     AND digital_service_name NOT IN ('Kinit',
	                                      'other',
	                                      'anonymous',
	                                      'TestApp')
	   GROUP BY date, digital_service_id,
	                  digital_service_name, date, SOURCE)
	SELECT count(*) AS das,
	count(case when amount >= 1 and amount < 10 then true else null end) as s1_9,
	count(case when amount >= 10 and amount < 100 then true else null end) as s10_99,
	count(case when amount >= 100 and amount < 1000 then true else null end) as s100_999,
	count(case when amount >= 1000 then true else null end) as s1000,
	        3000 * count(case when amount >= 1 and amount < 10 then true else null end) +
	        6000 * count(case when amount >= 10 and amount < 100 then true else null end) +
	        12000 * count(case when amount >= 100 and amount < 1000 then true else null end) +
	        30000 * count(case when amount >= 1000 then true else null end) AS max_payout_feb_onwards,
	        count(*) * 15000 as max_payout_jan,
	       digital_service_id,
	       digital_service_name, 
	       date
	FROM DAS
	where not exists (select * from `kin-bi.kre_2.spender_buckets` where date = DATE('{{ ds }}'))
	GROUP BY digital_service_id,
	         digital_service_name, date
