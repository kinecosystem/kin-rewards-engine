DELETE from `kin-bi.kre_2.spender_buckets` where date = DATE('{{ ds }}');

DELETE from `kin-bi.kre_2.holding` where date = DATE('{{ ds }}');

DELETE from `kin-bi.kre_2.buy_demand` where date = DATE('{{ ds }}');

DELETE from `kin-bi.kre_2.kre_2_payouts_pre_monopoly` where date = DATE('{{ ds }}');

DELETE from `kin-bi.kre_2.kre_2_payouts` where date = DATE('{{ ds }}');
