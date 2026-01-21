from taskturbine import Config


def test_config_build_defaults() -> None:
    config = Config(
        app_module="examples.tasks:app",
        database_url="postgres://app:password@localhost:5432/tests"
    )
    assert config.app_module == "examples.tasks:app"
    assert config.database_url == "postgres://app:password@localhost:5432/tests"
    assert config.usecase == "default"
    assert config.default_channel == "default"
    assert config.worker_concurrency == 3

    # Test property mutations
    config.worker_concurrency = 10
    assert config.worker_concurrency == 10
