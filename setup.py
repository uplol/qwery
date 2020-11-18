from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="qwery",
    version="0.0.8",
    url="https://github.com/uplol/qwery",
    description="small and lightweight query builder and data layer based on Pydantic and asyncpg",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    install_requires=["asyncpg==0.21.0", "pydantic==1.7.2"],
    setup_requires=["pytest-runner"],
    tests_require=["pytest==5.4.1", "pytest-asyncio"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)
