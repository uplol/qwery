from setuptools import setup, find_packages

setup(
    name="qwery",
    version="0.0.1",
    packages=find_packages(),
    install_requires=["asyncpg==0.20.1", "pydantic==1.5.1"],
    setup_requires=["pytest-runner"],
    tests_require=["pytest==5.4.1", "pytest-asyncio"],
)
