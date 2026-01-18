import pytest
from taskturbine import Config, TaskturbineApp

@pytest.fixture
def config() -> Config:
    return Config(database_url="postgres://app:password@localhost:5432/taskturbine")


def test_add_channel(config) -> None:
    app = TaskturbineApp(config)
    app.add_channel("reports")
    assert app.channels == ["default", "reports"]

