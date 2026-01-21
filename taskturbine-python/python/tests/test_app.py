import pytest

from taskturbine import Config, TaskturbineApp, Task

@pytest.fixture
def config() -> Config:
    return Config(app_module="", database_url="postgres://app:password@localhost:5432/taskturbine")


def test_add_channel(config) -> None:
    app = TaskturbineApp(config)
    app.add_channel("reports")
    assert app.channels == ["default", "reports"]


def test_register_task(config) -> None:
    app = TaskturbineApp(config)

    @app.register_task(name="first-task")
    def first_task(a: str) -> str:
        return f"called {a}"

    # App has some basic methods to get tasks
    # The worker will use this API
    assert app.has_task("nope") is False
    assert app.has_task("First-task") is False
    assert app.has_task("first-task")

    with pytest.raises(KeyError):
        app.get_task("nope")

    # Task functions get wrapped in decorator objects
    task = app.get_task("first-task")
    assert isinstance(task, Task)
    # Name attribute is added
    assert task.name == "first-task"
    # The class will proxy to the wrapped function
    assert task("one") == "called one"
