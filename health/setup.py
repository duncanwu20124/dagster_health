from setuptools import find_packages, setup

setup(
    name="health",
    packages=find_packages(exclude=["health_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
