from open_meteo_lab.open_meteo import run_pipeline
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import RRuleSchedule, CronSchedule

rr = RRuleSchedule(rrule="FREQ=MINUTELY;INTERVAL=1;COUNT=4")
deployment = Deployment.build_from_flow(
    flow=run_pipeline,
    name="RRule scheduled Python deployment",
    parameters={'coordinates':[(40.7128, 74.0060, 'NYC')]},
    schedule=rr
)

if __name__ == '__main__':
    deployment.apply()