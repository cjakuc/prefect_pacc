from open_meteo_lab.open_meteo import run_pipeline
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import RRuleSchedule, CronSchedule
from prefect.filesystems import GitHub

github_block = GitHub.load("github-access")

rr = RRuleSchedule(rrule="FREQ=MINUTELY;INTERVAL=1;COUNT=4")
deployment = Deployment.build_from_flow(
    flow=run_pipeline,
    name="RRule scheduled Python deployment via GitHub",
    parameters={'coordinates':[(40.7128, 74.0060, 'NYC')]},
    schedule=rr,
    storage=github_block
)

if __name__ == '__main__':
    deployment.apply()
