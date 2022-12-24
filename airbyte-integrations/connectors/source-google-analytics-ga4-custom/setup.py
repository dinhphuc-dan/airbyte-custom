#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk~=0.2",
    "pendulum~=2.1.2",
    "PyJWT~=2.6.0", 
    "requests~=2.28.1",
    "cryptography~=37.0.4"
]

TEST_REQUIREMENTS = [
    "pytest~=6.2",
    "pytest-mock~=3.6.1",
    "source-acceptance-test",
]

setup(
    name="source_google_analytics_ga4_custom",
    description="Source implementation for Google Analytics Ga4 Custom.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json", "*.yaml", "schemas/*.json", "schemas/shared/*.json"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
