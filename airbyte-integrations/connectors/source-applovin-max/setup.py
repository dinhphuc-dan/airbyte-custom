#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk",
    "pydantic~=1.9.2",
    "jsonschema~=3.2.0",
    "jsonref~=0.2",
    "dpath~=2.0.1"

]

TEST_REQUIREMENTS = [
]

setup(
    name="source_applovin_max",
    description="Source implementation for Applovin Max.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json", "*.yaml", "schemas/*.json", "schemas/shared/*.json"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
