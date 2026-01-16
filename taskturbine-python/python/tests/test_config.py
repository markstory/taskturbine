from taskturbine import Config

def test_config_build() -> None:
    config = Config(
        database_url="postgres://app:password@localhost:5432/tests"
    )
    assert config.database_url == "postgres://app:password@localhost:5432/tests"
