from open_meteo import run_pipeline
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import RRuleSchedule, CronSchedule
from prefect.filesystems import GitHub
from prefect.infrastructure.docker import DockerContainer

github_block = GitHub.load("github-access")
docker_container_block = DockerContainer.load("open-meteo-container")

rr = RRuleSchedule(rrule="FREQ=MINUTELY;INTERVAL=1;COUNT=4")
deployment = Deployment.build_from_flow(
    flow=run_pipeline,
    name="RRule scheduled Python deployment via GitHub w/ container infrastructure",
    parameters={'coordinates':[(40.7128, 74.0060, 'NYC')]},
    schedule=rr,
    storage=github_block,
    infrastructure=docker_container_block
)

if __name__ == '__main__':
    deployment.apply()
