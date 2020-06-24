![KRE](kinrewardsengine.png)
# Kin Rewards Engine

This repository contains code which powers the Kin Rewards Engine. Note that this repository is a work in progress and additional code will be added periodically.

## Information flow

The Kin Rewards Engine has 4 primary steps:<br/>
1) Data is collected from the blockchain and pushed to a PostgreSQL database via the [History Collector](https://github.com/kinecosystem/history-collector).<br/>
2) PostgreSQL data is extracted and pushed to BigQuery*.<br/>
3) This raw data is processed daily to calculate payouts (and also serves as a basis for [dashboards](https://public.tableau.com/profile/kinfoundation#!/)).<br/>
4) Every week this processed data it used to create the XDR (and other) payout files used to execute payments.<br/>

**Note that at this time, this step is beyond the scope of this repository.*

The core components of the Kin Rewards Engine run as pipelines (DAGs) on Airflow. You can read more about Airflow [here](https://airflow.apache.org/docs/stable/).
Airflow schedules bodies of work known as DAGs. These DAGs are directed acyclic graphs of tasks. The work for steps 3) and 4) of the Kin Reward Engine are done in [kre_2.py](https://github.com/kinecosystem/kin-rewards-engine/blob/master/airflow/dags/kre_2.py) and [payout_report.py](https://github.com/kinecosystem/kin-rewards-engine/blob/master/airflow/dags/payout_report.py) respectively. These DAGs are run with daily and weekly schedules respectively.
While the code detailing 2) is absent, we have included the schemas of relevant BigQuery tables.

## Contributing
Review the [HowToContribute](HowToContribute.md) guide before contributing.

## License

[![License](http://img.shields.io/:license-mit-blue.svg?style=flat-square)](http://badges.mit-license.org)

- **[MIT license](http://opensource.org/licenses/mit-license.php)**
- Copyright 2015 © <a href="http://fvcproductions.com" target="_blank">FVCproductions</a>.
