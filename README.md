![KRE](kinrewardsengine.png)
# Kin Rewards Engine

This repository contains code which powers the Kin Rewards Engine. Note that this repository is a work in progress and additional code will be added periodically.

## Information flow

The Kin Rewards Engine has 3 primary steps:
1) Data is collected from the blockchain and pushed to a PostgreSQL database via the [History Collector](https://github.com/kinecosystem/history-collector).
2) PostgreSQL data is extracted and pushed to BigQuery*.
3) This raw data is processed daily to calculate payouts (and also serves as a basis for [dashboards](https://public.tableau.com/profile/kinfoundation#!/)).
4) Every week this processed data it used to create the XDR (and other) payout files used to execute payments.

*Note that at this time, this step is beyond the scope of this repository.

[TODO INSERT PICTURE]

The core components of the Kin Rewards Engine run as pipelines (DAGs) on Airflow. You can read more about Airflow [here](https://airflow.apache.org/docs/stable/).
Airflow schedules bodies of work known as DAGs. These DAGs are directed acyclic graphs of tasks. The work for steps 2) and 3) of the Kin Reward Engine are done in airflow/dags/kre_2.py and airflow/dags/payout_report.py respectively. These DAGs are run with daily and weekly schedules respectively.
While the code detailing the History Collector is absent, we have included the schemas of relevant BigQuery tables.

## Contributing
Review the [HowToContribute](HowToContribute.md) guide before contributing.

## License

[![License](http://img.shields.io/:license-mit-blue.svg?style=flat-square)](http://badges.mit-license.org)

- **[MIT license](http://opensource.org/licenses/mit-license.php)**
- Copyright 2015 © <a href="http://fvcproductions.com" target="_blank">FVCproductions</a>.
